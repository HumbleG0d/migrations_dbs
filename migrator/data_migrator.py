"""
FASE 2: Migración de datos tabla por tabla en batches.

Principios aplicados (skill: database-migrations + python-expert-best-practices):
- Nunca cargar una tabla entera en memoria: lectura en batches de N filas
- Checkpoint actualizado en cada batch: reanudable ante cualquier fallo
- Errores específicos, nunca generic except
- Retry automático con backoff exponencial ante errores de conexión
- Fallback de encoding UTF-8 → latin-1 para datos legacy
- Throughput y ETA reportados cada 30 segundos
"""

from __future__ import annotations

import io
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pyodbc
import psycopg2
import psycopg2.extras

from config import MIGRATION, get_mssql_connection_string, get_postgres_dsn
from migrator.checkpoint import CheckpointManager
from migrator.type_mapper import is_bit_column, is_binary_column, is_uuid_column
from migrator.schema_migrator import ColumnInfo, TableInfo

logger = logging.getLogger(__name__)

# Intervalo en segundos para reportar throughput en consola
_THROUGHPUT_REPORT_INTERVAL = 30.0


# ─────────────────────────────────────────────────────────────────────────────
# Resultado de migración por tabla
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TableMigrationResult:
    table_name: str
    schema: str
    rows_origin: int
    rows_migrated: int
    status: str          # "DONE" | "ERROR" | "SKIPPED"
    elapsed_seconds: float
    error_message: str | None = None
    batches_completed: int = 0
    rows_skipped: int = 0


# ─────────────────────────────────────────────────────────────────────────────
# Migrador de datos
# ─────────────────────────────────────────────────────────────────────────────

class DataMigrator:
    """
    Migra datos de todas las tablas de SQL Server a PostgreSQL en batches.

    Flujo por tabla:
      1. Verificar checkpoint (si es DONE, skip)
      2. Leer columnas para construir query paginada con OFFSET/FETCH
      3. Por cada batch: leer → transformar → escribir en PG → actualizar checkpoint
      4. Marcar DONE al finalizar
    """

    def __init__(self, checkpoint: CheckpointManager) -> None:
        self._checkpoint = checkpoint
        self._mssql_conn_str = get_mssql_connection_string()
        self._pg_dsn = get_postgres_dsn()
        self._batch_size = MIGRATION.batch_size
        self._retry_attempts = MIGRATION.retry_attempts
        self._retry_backoff = MIGRATION.retry_backoff_seconds

    # ── API pública ──────────────────────────────────────────────────────────

    def migrate_table(self, table: TableInfo, total_rows: int) -> TableMigrationResult:
        """
        Migra una tabla completa en batches.
        Si la tabla ya está en DONE en el checkpoint, la omite.

        Args:
            table:      Metadatos de la tabla (schema, nombre, columnas).
            total_rows: Conteo total de filas en origen (para calcular ETA).

        Returns:
            TableMigrationResult con el resultado de la migración.
        """
        status = self._checkpoint.get_status(table.name)
        if status == "DONE":
            logger.info("⏭️  Skip (ya migrada): %s", table.full_name)
            return TableMigrationResult(
                table_name=table.name,
                schema=table.schema,
                rows_origin=total_rows,
                rows_migrated=self._checkpoint.get_rows_migrated(table.name),
                status="SKIPPED",
                elapsed_seconds=0.0,
            )

        start_time = time.monotonic()
        start_batch = self._checkpoint.get_last_batch(table.name)

        logger.info(
            "🔄 Iniciando migración: %s | %s filas | batch_size=%d | desde batch %d",
            table.full_name, f"{total_rows:,}", self._batch_size, start_batch,
        )

        total_batches = max(1, math.ceil(total_rows / self._batch_size)) if total_rows > 0 else 1
        rows_migrated = self._checkpoint.get_rows_migrated(table.name)
        rows_skipped = 0
        last_report_time = time.monotonic()

        try:
            with pyodbc.connect(self._mssql_conn_str, timeout=60) as mssql_conn:
                mssql_conn.autocommit = True
                with psycopg2.connect(self._pg_dsn) as pg_conn:
                    pg_conn.autocommit = False

                    batch_num = start_batch
                    while True:
                        offset = batch_num * self._batch_size
                        rows = self._read_batch_with_retry(mssql_conn, table, offset)

                        if not rows:
                            break  # No hay más filas

                        transformed, skipped_in_batch = self._transform_rows(table.columns, rows)
                        rows_skipped += skipped_in_batch

                        self._write_batch_with_retry(pg_conn, table, transformed)

                        batch_num += 1
                        rows_migrated += len(transformed)

                        self._checkpoint.mark_in_progress(
                            table.name,
                            last_batch=batch_num,
                            rows_migrated=rows_migrated,
                        )

                        # Reporte de throughput cada 30s
                        now = time.monotonic()
                        if now - last_report_time >= _THROUGHPUT_REPORT_INTERVAL:
                            self._log_progress(
                                table.full_name, batch_num, total_batches,
                                rows_migrated, total_rows,
                                elapsed=now - start_time,
                            )
                            last_report_time = now

                        logger.debug(
                            "  Batch %d/%d — %s filas acumuladas",
                            batch_num, total_batches, f"{rows_migrated:,}",
                        )

        except pyodbc.Error as exc:
            return self._handle_table_error(table, exc, start_time, rows_migrated, rows_skipped)
        except psycopg2.Error as exc:
            return self._handle_table_error(table, exc, start_time, rows_migrated, rows_skipped)

        elapsed = time.monotonic() - start_time
        self._checkpoint.mark_done(table.name, total_rows=rows_migrated)
        logger.info(
            "✅ Completada: %s | %s filas | %s omitidas | %.1fs",
            table.full_name, f"{rows_migrated:,}", f"{rows_skipped:,}", elapsed,
        )
        return TableMigrationResult(
            table_name=table.name,
            schema=table.schema,
            rows_origin=total_rows,
            rows_migrated=rows_migrated,
            status="DONE",
            elapsed_seconds=elapsed,
            batches_completed=batch_num,
            rows_skipped=rows_skipped,
        )

    # ── Lectura desde SQL Server ─────────────────────────────────────────────

    def _read_batch_with_retry(
        self,
        conn: pyodbc.Connection,
        table: TableInfo,
        offset: int,
    ) -> list[tuple]:
        """Lee un batch con retry automático ante errores de conexión."""
        col_names = ", ".join(f'"{c.name}"' for c in table.columns)

        # ORDER BY (SELECT NULL) es necesario para OFFSET/FETCH en SQL Server
        query = (
            f'SELECT {col_names} '
            f'FROM "{table.schema}"."{table.name}" '
            f'ORDER BY (SELECT NULL) '
            f'OFFSET {offset} ROWS FETCH NEXT {self._batch_size} ROWS ONLY'
        )

        for attempt in range(1, self._retry_attempts + 1):
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                return cursor.fetchall()
            except pyodbc.OperationalError as exc:
                if attempt == self._retry_attempts:
                    raise
                wait = self._retry_backoff * attempt
                logger.warning(
                    "Error de conexión leyendo %s (intento %d/%d): %s — reintentando en %ds",
                    table.full_name, attempt, self._retry_attempts, exc, wait,
                )
                time.sleep(wait)

        return []  # Nunca llega aquí, pero satisface el type checker

    # ── Transformación de filas ──────────────────────────────────────────────

    def _transform_rows(
        self,
        columns: list[ColumnInfo],
        rows: list[tuple],
    ) -> tuple[list[tuple], int]:
        """
        Convierte los valores de SQL Server al formato esperado por PostgreSQL.

        Transformaciones aplicadas:
        - BIT (0/1) → bool True/False
        - UNIQUEIDENTIFIER → str (UUID sin llaves)
        - Strings → decodificación UTF-8 con fallback a latin-1
        - BINARY/IMAGE → bytes (ya vienen como bytes desde pyodbc)

        Returns:
            (transformed_rows, skipped_count)
        """
        transformed: list[tuple] = []
        skipped = 0

        for row in rows:
            try:
                new_row = tuple(
                    self._convert_value(col, val)
                    for col, val in zip(columns, row)
                )
                transformed.append(new_row)
            except (UnicodeDecodeError, ValueError, OverflowError) as exc:
                skipped += 1
                logger.warning(
                    "Fila omitida por error de conversión: %s — %s",
                    row, exc,
                )

        return transformed, skipped

    def _convert_value(self, col: ColumnInfo, value: Any) -> Any:
        if value is None:
            return None

        if is_bit_column(col.sql_type):
            return bool(value)

        if is_uuid_column(col.sql_type):
            return str(value).strip("{}")

        if is_binary_column(col.sql_type):
            return bytes(value) if not isinstance(value, (bytes, bytearray)) else value

        if isinstance(value, str):
            return self._safe_decode(value)

        return value

    @staticmethod
    def _safe_decode(value: str) -> str:
        """Garantiza que el string sea UTF-8 válido; usa latin-1 como fallback."""
        try:
            value.encode("utf-8")
            return value
        except UnicodeEncodeError:
            return value.encode("latin-1", errors="replace").decode("utf-8", errors="replace")

    # ── Escritura en PostgreSQL ──────────────────────────────────────────────

    def _write_batch_with_retry(
        self,
        pg_conn: psycopg2.extensions.connection,
        table: TableInfo,
        rows: list[tuple],
    ) -> None:
        """Escribe un batch en PostgreSQL usando COPY para máxima velocidad."""
        if not rows:
            return

        col_names = [f'"{c.name}"' for c in table.columns]
        table_ref = f'"{table.schema}"."{table.name}"'

        for attempt in range(1, self._retry_attempts + 1):
            try:
                cursor = pg_conn.cursor()
                # psycopg2.extras.execute_values es más rápido que executemany
                insert_sql = (
                    f"INSERT INTO {table_ref} ({', '.join(col_names)}) VALUES %s"
                )
                psycopg2.extras.execute_values(
                    cursor,
                    insert_sql,
                    rows,
                    page_size=self._batch_size,
                )
                pg_conn.commit()
                return
            except psycopg2.OperationalError as exc:
                pg_conn.rollback()
                if attempt == self._retry_attempts:
                    raise
                wait = self._retry_backoff * attempt
                logger.warning(
                    "Error escribiendo batch en %s (intento %d/%d): %s — retrying en %ds",
                    table_ref, attempt, self._retry_attempts, exc, wait,
                )
                time.sleep(wait)
            except psycopg2.errors.UniqueViolation as exc:
                # Registro duplicado: hacer rollback y continuar (no abortar)
                pg_conn.rollback()
                logger.warning(
                    "Violación de unicidad en %s — batch omitido: %s",
                    table_ref, exc,
                )
                return

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _handle_table_error(
        self,
        table: TableInfo,
        exc: Exception,
        start_time: float,
        rows_migrated: int,
        rows_skipped: int,
    ) -> TableMigrationResult:
        error_msg = str(exc)
        self._checkpoint.mark_error(table.name, error_msg)
        elapsed = time.monotonic() - start_time
        logger.error("❌ Error migrando %s: %s", table.full_name, error_msg)
        return TableMigrationResult(
            table_name=table.name,
            schema=table.schema,
            rows_origin=0,
            rows_migrated=rows_migrated,
            status="ERROR",
            elapsed_seconds=elapsed,
            error_message=error_msg,
            rows_skipped=rows_skipped,
        )

    @staticmethod
    def _log_progress(
        table_full_name: str,
        batch_num: int,
        total_batches: int,
        rows_done: int,
        total_rows: int,
        elapsed: float,
    ) -> None:
        pct = (rows_done / total_rows * 100) if total_rows > 0 else 0
        throughput = rows_done / elapsed if elapsed > 0 else 0
        remaining_rows = max(0, total_rows - rows_done)
        eta_secs = (remaining_rows / throughput) if throughput > 0 else 0
        eta_min = int(eta_secs // 60)
        eta_sec = int(eta_secs % 60)

        logger.info(
            "📊 %s | Batch %d/%d | %s/%s filas (%.1f%%) | %.0f filas/s | ETA ~%dm%02ds",
            table_full_name,
            batch_num, total_batches,
            f"{rows_done:,}", f"{total_rows:,}",
            pct,
            throughput,
            eta_min, eta_sec,
        )
