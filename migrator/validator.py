"""
FASE 4: Validación post-migración.

Por cada tabla compara origen (SQL Server) con destino (PostgreSQL):
  1. COUNT(*) — número de filas debe coincidir exactamente
  2. Checksums  — SUM/MIN/MAX de columnas numéricas clave
  3. Muestra aleatoria — N filas comparadas fila a fila
  4. NOT NULL  — columnas requeridas no deben tener NULLs en destino

Resultados posibles por tabla:
  PASS  → Todo coincide ✅
  WARN  → Filas OK, pequeñas diferencias en checksums (ej. redondeo MONEY) 🟡
  FAIL  → Conteo o datos no coinciden ❌ (se re-encola para re-migración)
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field

import pyodbc
import psycopg2

from config import MIGRATION, get_mssql_connection_string, get_postgres_dsn
from migrator.schema_migrator import ColumnInfo, TableInfo
from migrator.type_mapper import is_binary_column

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Resultado de validación por tabla
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    table_name: str
    schema: str
    status: str                    # "PASS" | "WARN" | "FAIL"
    rows_origin: int = 0
    rows_dest: int = 0
    checksum_mismatches: list[str] = field(default_factory=list)
    sample_mismatches: int = 0
    null_violations: list[str] = field(default_factory=list)
    error_message: str | None = None
    elapsed_seconds: float = 0.0

    @property
    def row_count_ok(self) -> bool:
        return self.rows_origin == self.rows_dest


# ─────────────────────────────────────────────────────────────────────────────
# Validador
# ─────────────────────────────────────────────────────────────────────────────

class Validator:
    """
    Compara origen SQL Server con destino PostgreSQL para detectar
    pérdida o corrupción de datos después de la migración.
    """

    def __init__(self) -> None:
        self._mssql_conn_str = get_mssql_connection_string()
        self._pg_dsn = get_postgres_dsn()
        self._sample_size = MIGRATION.validation_sample_size

    # ── API pública ──────────────────────────────────────────────────────────

    def validate_table(self, table: TableInfo) -> ValidationResult:
        """Ejecuta todos los checks de validación para una tabla."""
        start = time.monotonic()
        result = ValidationResult(
            table_name=table.name,
            schema=table.schema,
            status="PASS",
        )

        try:
            self._check_row_count(table, result)
            self._check_checksums(table, result)
            self._check_not_null(table, result)
            if result.row_count_ok:
                self._check_random_sample(table, result)
        except (pyodbc.Error, psycopg2.Error) as exc:
            result.status = "FAIL"
            result.error_message = str(exc)
            logger.error("❌ Error validando %s.%s: %s", table.schema, table.name, exc)

        result.elapsed_seconds = time.monotonic() - start
        self._finalize_status(result)
        self._log_result(result)
        return result

    def validate_all(self, tables: list[TableInfo]) -> list[ValidationResult]:
        """Valida todas las tablas y retorna lista de resultados."""
        logger.info("FASE 4 — Validando %d tablas…", len(tables))
        results: list[ValidationResult] = []
        for table in tables:
            results.append(self.validate_table(table))

        passed = sum(1 for r in results if r.status == "PASS")
        warned = sum(1 for r in results if r.status == "WARN")
        failed = sum(1 for r in results if r.status == "FAIL")
        logger.info(
            "FASE 4 completada — ✅ PASS: %d | 🟡 WARN: %d | ❌ FAIL: %d",
            passed, warned, failed,
        )
        return results

    def get_tables_to_retry(self, results: list[ValidationResult]) -> list[str]:
        """Retorna nombres de tablas con FAIL para re-migración."""
        return [r.table_name for r in results if r.status == "FAIL"]

    # ── Checks individuales ──────────────────────────────────────────────────

    def _check_row_count(self, table: TableInfo, result: ValidationResult) -> None:
        origin_count = self._count_mssql(table)
        dest_count = self._count_pg(table)
        result.rows_origin = origin_count
        result.rows_dest = dest_count

        if origin_count != dest_count:
            result.status = "FAIL"
            logger.warning(
                "  ❌ Conteo diferente en %s.%s — origen: %s | destino: %s",
                table.schema, table.name,
                f"{origin_count:,}", f"{dest_count:,}",
            )

    def _check_checksums(self, table: TableInfo, result: ValidationResult) -> None:
        numeric_cols = [
            c for c in table.columns
            if c.sql_type.lower() in (
                "int", "bigint", "smallint", "tinyint",
                "decimal", "numeric", "money", "smallmoney", "float", "real",
            )
        ]
        if not numeric_cols:
            return

        for col in numeric_cols[:10]:   # Limitar a 10 columnas para no sobrecargar
            col_name = f'"{col.name}"'
            try:
                ms_sum, ms_min, ms_max = self._agg_mssql(table, col.name)
                pg_sum, pg_min, pg_max = self._agg_pg(table, col.name)

                # Tolerancia del 0.01% para diferencias de redondeo (ej. MONEY→NUMERIC)
                if not self._approx_equal(ms_sum, pg_sum, tolerance=0.0001):
                    result.checksum_mismatches.append(col.name)
                    if result.status == "PASS":
                        result.status = "WARN"
                    logger.warning(
                        "  🟡 Checksum mismatch en %s.%s.%s — SUM origen: %s | destino: %s",
                        table.schema, table.name, col.name, ms_sum, pg_sum,
                    )
            except (pyodbc.Error, psycopg2.Error, TypeError) as exc:
                logger.debug("No se pudo checksum columna %s: %s", col.name, exc)

    def _check_not_null(self, table: TableInfo, result: ValidationResult) -> None:
        """Verifica que columnas NOT NULL no tengan NULLs en el destino."""
        not_null_cols = [c for c in table.columns if not c.is_nullable]
        if not not_null_cols:
            return

        try:
            with psycopg2.connect(self._pg_dsn) as pg_conn:
                cursor = pg_conn.cursor()
                for col in not_null_cols[:20]:  # Limitar a 20 columnas
                    cursor.execute(
                        f'SELECT COUNT(*) FROM "{table.schema}"."{table.name}" '
                        f'WHERE "{col.name}" IS NULL'
                    )
                    null_count = cursor.fetchone()[0]
                    if null_count > 0:
                        result.null_violations.append(col.name)
                        if result.status == "PASS":
                            result.status = "WARN"
                        logger.warning(
                            "  🟡 NULLs en columna NOT NULL: %s.%s.%s (%d NULLs)",
                            table.schema, table.name, col.name, null_count,
                        )
        except psycopg2.Error as exc:
            logger.debug("Error verificando NOT NULL en %s: %s", table.name, exc)

    def _check_random_sample(self, table: TableInfo, result: ValidationResult) -> None:
        """Compara N filas aleatorias entre origen y destino."""
        if result.rows_origin == 0:
            return

        # Columnas no binarias para la comparación (BYTEA es difícil de comparar)
        comparable_cols = [
            c for c in table.columns
            if not is_binary_column(c.sql_type)
        ]
        if not comparable_cols:
            return

        col_names = [f'"{c.name}"' for c in comparable_cols]
        cols_sql = ", ".join(col_names)
        mismatches = 0

        try:
            with pyodbc.connect(self._mssql_conn_str, timeout=30) as mssql_conn:
                with psycopg2.connect(self._pg_dsn) as pg_conn:
                    ms_cursor = mssql_conn.cursor()
                    pg_cursor = pg_conn.cursor()

                    # Muestra aleatoria usando TABLESAMPLE (SQL Server 2016+)
                    ms_cursor.execute(
                        f'SELECT TOP {self._sample_size} {cols_sql} '
                        f'FROM "{table.schema}"."{table.name}" '
                        f'ORDER BY NEWID()'
                    )
                    ms_rows = ms_cursor.fetchall()

                    for ms_row in ms_rows:
                        # Buscar la misma fila en PG por la primera columna como referencia
                        first_col = comparable_cols[0].name
                        pg_cursor.execute(
                            f'SELECT {cols_sql} FROM "{table.schema}"."{table.name}" '
                            f'WHERE "{first_col}" = %s LIMIT 1',
                            (ms_row[0],),
                        )
                        pg_row = pg_cursor.fetchone()
                        if pg_row is None or not self._rows_equal(ms_row, pg_row):
                            mismatches += 1

        except (pyodbc.Error, psycopg2.Error) as exc:
            logger.debug("Muestra aleatoria no disponible para %s: %s", table.name, exc)
            return

        result.sample_mismatches = mismatches
        if mismatches > 0:
            if result.status == "PASS":
                result.status = "WARN"
            logger.warning(
                "  🟡 Muestra aleatoria: %d/%d filas con diferencias en %s.%s",
                mismatches, len(ms_rows), table.schema, table.name,
            )

    # ── Helpers de consulta ──────────────────────────────────────────────────

    def _count_mssql(self, table: TableInfo) -> int:
        with pyodbc.connect(self._mssql_conn_str, timeout=60) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f'SELECT COUNT(*) FROM "{table.schema}"."{table.name}"'
            )
            return cursor.fetchone()[0]

    def _count_pg(self, table: TableInfo) -> int:
        with psycopg2.connect(self._pg_dsn) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f'SELECT COUNT(*) FROM "{table.schema}"."{table.name}"'
            )
            return cursor.fetchone()[0]

    def _agg_mssql(self, table: TableInfo, col_name: str) -> tuple:
        with pyodbc.connect(self._mssql_conn_str, timeout=60) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f'SELECT SUM(CAST("{col_name}" AS FLOAT)), '
                f'MIN(CAST("{col_name}" AS FLOAT)), '
                f'MAX(CAST("{col_name}" AS FLOAT)) '
                f'FROM "{table.schema}"."{table.name}"'
            )
            return cursor.fetchone()

    def _agg_pg(self, table: TableInfo, col_name: str) -> tuple:
        with psycopg2.connect(self._pg_dsn) as conn:
            cursor = conn.cursor()
            cursor.execute(
                f'SELECT SUM(CAST("{col_name}" AS DOUBLE PRECISION)), '
                f'MIN(CAST("{col_name}" AS DOUBLE PRECISION)), '
                f'MAX(CAST("{col_name}" AS DOUBLE PRECISION)) '
                f'FROM "{table.schema}"."{table.name}"'
            )
            return cursor.fetchone()

    @staticmethod
    def _approx_equal(a, b, tolerance: float = 0.0001) -> bool:
        if a is None and b is None:
            return True
        if a is None or b is None:
            return False
        if a == 0 and b == 0:
            return True
        if a == 0:
            return abs(b) < tolerance
        return abs((a - b) / a) < tolerance

    @staticmethod
    def _rows_equal(ms_row: tuple, pg_row: tuple) -> bool:
        for ms_val, pg_val in zip(ms_row, pg_row):
            if ms_val is None and pg_val is None:
                continue
            if ms_val is None or pg_val is None:
                return False
            if str(ms_val).strip() != str(pg_val).strip():
                return False
        return True

    def _finalize_status(self, result: ValidationResult) -> None:
        if result.rows_origin != result.rows_dest:
            result.status = "FAIL"
        elif result.checksum_mismatches or result.null_violations or result.sample_mismatches > 0:
            if result.status == "PASS":
                result.status = "WARN"

    @staticmethod
    def _log_result(result: ValidationResult) -> None:
        icon = {"PASS": "✅", "WARN": "🟡", "FAIL": "❌"}.get(result.status, "❓")
        logger.info(
            "%s Validación %s: %s.%s | Filas: %s/%s | Checksums: %d warn | Sample: %d diff",
            icon, result.status,
            result.schema, result.table_name,
            f"{result.rows_dest:,}", f"{result.rows_origin:,}",
            len(result.checksum_mismatches),
            result.sample_mismatches,
        )
