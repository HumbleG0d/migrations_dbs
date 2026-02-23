"""
FASE 1: Migración de esquemas (DDL) de SQL Server → PostgreSQL.

Lee el catálogo de tablas de SQL Server usando INFORMATION_SCHEMA y genera
las sentencias CREATE TABLE en PostgreSQL SIN índices, SIN FKs y SIN constraints.

Esto sigue el principio del skill database-migrations:
  "Schema and data migrations are separate — never mix DDL and DML in one migration"
  "New columns have defaults or are nullable — never add NOT NULL without default"
  (Los NOT NULL se preservan solo cuando SQL Server los define explícitamente)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import pyodbc
import psycopg2

from config import MIGRATION, get_mssql_connection_string, get_postgres_dsn, sanitize_schema_name
from migrator.type_mapper import map_column

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Estructuras de datos
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ColumnInfo:
    name: str
    sql_type: str
    max_length: int | None
    precision: int | None
    scale: int | None
    is_nullable: bool
    column_default: str | None
    ordinal_position: int


@dataclass
class TableInfo:
    schema: str          # Nombre original en SQL Server (puede tener \ o .)
    name: str
    columns: list[ColumnInfo]
    pg_schema: str = ""  # Nombre sanitizado para PostgreSQL (sin caracteres inválidos)

    def __post_init__(self) -> None:
        if not self.pg_schema:
            self.pg_schema = sanitize_schema_name(self.schema)

    @property
    def full_name(self) -> str:
        """Nombre completo en SQL Server (origen)."""
        return f"{self.schema}.{self.name}"

    @property
    def pg_full_name(self) -> str:
        """Nombre completo en PostgreSQL (destino)."""
        return f"{self.pg_schema}.{self.name}"


# ─────────────────────────────────────────────────────────────────────────────
# Consultas SQL Server
# ─────────────────────────────────────────────────────────────────────────────

_TABLES_QUERY = """
SELECT
    TABLE_SCHEMA,
    TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
  AND TABLE_SCHEMA IN ({schema_placeholders})
ORDER BY TABLE_SCHEMA, TABLE_NAME
"""

_COLUMNS_QUERY = """
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ?
  AND TABLE_NAME   = ?
ORDER BY ORDINAL_POSITION
"""


# ─────────────────────────────────────────────────────────────────────────────
# Lógica principal
# ─────────────────────────────────────────────────────────────────────────────

class SchemaMigrator:
    """
    Lee el catálogo de SQL Server y crea las tablas en PostgreSQL.
    Solo DDL puro: sin índices, sin FKs, sin constraints secundarios.
    """

    def __init__(self) -> None:
        self._mssql_conn_str = get_mssql_connection_string()
        self._pg_dsn = get_postgres_dsn()
        self._include_schemas = MIGRATION.include_schemas
        self._exclude_tables = set(MIGRATION.exclude_tables)

    # ── API pública ──────────────────────────────────────────────────────────

    def fetch_table_catalog(self) -> list[TableInfo]:
        """
        Lee el catálogo completo de tablas y columnas desde SQL Server.
        Retorna la lista ordenada por schema y nombre de tabla.
        """
        logger.info("Leyendo catálogo de tablas desde SQL Server…")
        start = time.monotonic()

        with pyodbc.connect(self._mssql_conn_str, timeout=30) as conn:
            tables = self._query_tables(conn)
            result: list[TableInfo] = []
            for schema, name in tables:
                if name in self._exclude_tables:
                    logger.debug("Tabla excluida por config: %s.%s", schema, name)
                    continue
                columns = self._query_columns(conn, schema, name)
                result.append(TableInfo(schema=schema, name=name, columns=columns))

        elapsed = time.monotonic() - start
        logger.info(
            "Catálogo leído: %d tablas en %.1fs", len(result), elapsed
        )
        return result

    def migrate_all_schemas(self, tables: list[TableInfo]) -> tuple[int, int]:
        """
        Crea todas las tablas en PostgreSQL (sin índices ni FKs).

        Returns:
            (created, failed): conteo de tablas creadas y fallidas.
        """
        logger.info("FASE 1 — Creando %d tablas en PostgreSQL…", len(tables))
        created = 0
        failed = 0

        with psycopg2.connect(self._pg_dsn) as pg_conn:
            pg_conn.autocommit = False
            for table in tables:
                try:
                    self._create_table(pg_conn, table)
                    pg_conn.commit()
                    created += 1
                    logger.info("  ✅ Creada: %s (%d cols)", table.full_name, len(table.columns))
                except psycopg2.errors.DuplicateTable:
                    pg_conn.rollback()
                    logger.warning("  ⚠️  Ya existe: %s — omitiendo.", table.full_name)
                    created += 1  # No es un fallo real
                except (psycopg2.Error, ValueError) as exc:
                    pg_conn.rollback()
                    failed += 1
                    logger.error("  ❌ Error creando %s: %s", table.full_name, exc)

        logger.info(
            "FASE 1 completada — Creadas: %d | Fallidas: %d",
            created, failed,
        )
        return created, failed

    def get_table_row_counts(self, tables: list[TableInfo]) -> dict[str, int]:
        """
        Obtiene el COUNT(*) de cada tabla para clasificación (WHALE/MEDIUM/SMALL).
        Usa sp_spaceused para mayor velocidad en tablas grandes.
        """
        logger.info("Obteniendo conteo de filas para clasificación de %d tablas…", len(tables))
        row_counts: dict[str, int] = {}

        with pyodbc.connect(self._mssql_conn_str, timeout=30) as conn:
            cursor = conn.cursor()
            for table in tables:
                try:
                    # sys.partitions es más rápido que COUNT(*) en tablas grandes
                    cursor.execute(
                        """
                        SELECT SUM(p.rows)
                        FROM sys.partitions p
                        JOIN sys.tables t ON t.object_id = p.object_id
                        JOIN sys.schemas s ON s.schema_id = t.schema_id
                        WHERE p.index_id IN (0, 1)
                          AND t.name    = ?
                          AND s.name    = ?
                        """,
                        table.name, table.schema,
                    )
                    row = cursor.fetchone()
                    count = int(row[0]) if row and row[0] is not None else 0
                    row_counts[table.name] = count
                except pyodbc.Error as exc:
                    logger.warning(
                        "No se pudo obtener count de %s: %s — asumiendo 0",
                        table.full_name, exc,
                    )
                    row_counts[table.name] = 0

        return row_counts

    # ── Internos ─────────────────────────────────────────────────────────────

    def _query_tables(self, conn: pyodbc.Connection) -> list[tuple[str, str]]:
        schema_placeholders = ", ".join("?" * len(self._include_schemas))
        query = _TABLES_QUERY.format(schema_placeholders=schema_placeholders)
        cursor = conn.cursor()
        cursor.execute(query, self._include_schemas)
        return [(row.TABLE_SCHEMA, row.TABLE_NAME) for row in cursor.fetchall()]

    def _query_columns(
        self, conn: pyodbc.Connection, schema: str, table_name: str
    ) -> list[ColumnInfo]:
        cursor = conn.cursor()
        cursor.execute(_COLUMNS_QUERY, schema, table_name)
        columns: list[ColumnInfo] = []
        for row in cursor.fetchall():
            columns.append(ColumnInfo(
                name=row.COLUMN_NAME,
                sql_type=row.DATA_TYPE,
                max_length=row.CHARACTER_MAXIMUM_LENGTH,
                precision=row.NUMERIC_PRECISION,
                scale=row.NUMERIC_SCALE,
                is_nullable=(row.IS_NULLABLE.upper() == "YES"),
                column_default=row.COLUMN_DEFAULT,
                ordinal_position=row.ORDINAL_POSITION,
            ))
        return columns

    def _create_table(self, pg_conn: psycopg2.extensions.connection, table: TableInfo) -> None:
        ddl = self._build_create_table_ddl(table)
        logger.debug("DDL generado para %s:\n%s", table.pg_full_name, ddl)
        cursor = pg_conn.cursor()
        # Usar el nombre sanitizado del schema en PostgreSQL
        cursor.execute(
            f'CREATE SCHEMA IF NOT EXISTS "{table.pg_schema}"'
        )
        cursor.execute(ddl)

    def _build_create_table_ddl(self, table: TableInfo) -> str:
        col_defs: list[str] = []
        for col in sorted(table.columns, key=lambda c: c.ordinal_position):
            pg_type = map_column(
                sql_type=col.sql_type,
                max_length=col.max_length,
                precision=col.precision,
                scale=col.scale,
            )
            nullable_clause = "" if col.is_nullable else " NOT NULL"
            # Escapar nombre de columna para evitar conflictos con palabras reservadas
            col_defs.append(f'    "{col.name}" {pg_type}{nullable_clause}')

        cols_sql = ",\n".join(col_defs)
        # Usar pg_schema (nombre sanitizado) para el CREATE TABLE en PostgreSQL
        return (
            f'CREATE TABLE IF NOT EXISTS "{table.pg_schema}"."{table.name}" (\n'
            f"{cols_sql}\n"
            f");"
        )
