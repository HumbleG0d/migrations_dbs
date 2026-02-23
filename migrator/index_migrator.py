"""
FASE 3: Migración de índices y Foreign Keys (post-carga de datos).

Principios aplicados (skill: database-migrations):
  - CREATE INDEX CONCURRENTLY: no bloquea escrituras en tablas grandes
  - CONCURRENTLY no puede ejecutarse dentro de una transacción
  - FKs se aplican AL FINAL, nunca durante la carga de datos
  - Cada índice/FK se loggea con su tiempo de creación
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import pyodbc
import psycopg2

from config import MIGRATION, get_mssql_connection_string, get_postgres_dsn

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Estructuras de datos
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class IndexInfo:
    schema: str
    table_name: str
    index_name: str
    columns: list[str]
    is_unique: bool
    is_primary_key: bool


@dataclass
class ForeignKeyInfo:
    constraint_name: str
    schema: str
    table_name: str
    columns: list[str]
    ref_schema: str
    ref_table: str
    ref_columns: list[str]


@dataclass
class IndexMigrationResult:
    indexes_created: int
    indexes_failed: int
    fks_created: int
    fks_failed: int
    elapsed_seconds: float


# ─────────────────────────────────────────────────────────────────────────────
# Consultas SQL Server
# ─────────────────────────────────────────────────────────────────────────────

_INDEXES_QUERY = """
SELECT
    s.name                           AS schema_name,
    t.name                           AS table_name,
    i.name                           AS index_name,
    i.is_unique                      AS is_unique,
    i.is_primary_key                 AS is_primary_key,
    STRING_AGG(c.name, ',' )
        WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
FROM sys.indexes i
JOIN sys.tables       t  ON t.object_id  = i.object_id
JOIN sys.schemas      s  ON s.schema_id  = t.schema_id
JOIN sys.index_columns ic ON ic.object_id = i.object_id
                          AND ic.index_id = i.index_id
JOIN sys.columns      c  ON c.object_id  = ic.object_id
                          AND c.column_id = ic.column_id
WHERE i.type > 0                         -- excluye heap (type=0)
  AND i.is_disabled = 0
  AND t.is_ms_shipped = 0
  AND ic.is_included_column = 0          -- excluye columnas INCLUDE
  AND s.name IN ({schema_placeholders})
GROUP BY s.name, t.name, i.name, i.is_unique, i.is_primary_key
ORDER BY s.name, t.name, i.name
"""

_FKS_QUERY = """
SELECT
    fk.name                          AS constraint_name,
    ps.name                          AS parent_schema,
    pt.name                          AS parent_table,
    STRING_AGG(pc.name, ',' )
        WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS parent_columns,
    rs.name                          AS ref_schema,
    rt.name                          AS ref_table,
    STRING_AGG(rc.name, ',' )
        WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS ref_columns
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
JOIN sys.tables       pt  ON pt.object_id  = fk.parent_object_id
JOIN sys.schemas      ps  ON ps.schema_id  = pt.schema_id
JOIN sys.columns      pc  ON pc.object_id  = fkc.parent_object_id
                          AND pc.column_id = fkc.parent_column_id
JOIN sys.tables       rt  ON rt.object_id  = fk.referenced_object_id
JOIN sys.schemas      rs  ON rs.schema_id  = rt.schema_id
JOIN sys.columns      rc  ON rc.object_id  = fkc.referenced_object_id
                          AND rc.column_id = fkc.referenced_column_id
WHERE ps.name IN ({schema_placeholders})
GROUP BY fk.name, ps.name, pt.name, rs.name, rt.name
ORDER BY ps.name, pt.name, fk.name
"""


# ─────────────────────────────────────────────────────────────────────────────
# Migrador de índices y FKs
# ─────────────────────────────────────────────────────────────────────────────

class IndexMigrator:
    """
    Lee índices y FKs de SQL Server y los aplica en PostgreSQL post-carga.
    Usa CREATE INDEX CONCURRENTLY para no bloquear tablas durante la creación.
    """

    def __init__(self) -> None:
        self._mssql_conn_str = get_mssql_connection_string()
        self._pg_dsn = get_postgres_dsn()
        self._include_schemas = MIGRATION.include_schemas

    # ── API pública ──────────────────────────────────────────────────────────

    def run(self) -> IndexMigrationResult:
        """Ejecuta la fase completa: índices primero, FKs después."""
        logger.info("FASE 3 — Iniciando migración de índices y FKs…")
        start = time.monotonic()

        indexes = self._fetch_indexes()
        fks = self._fetch_foreign_keys()

        idx_created, idx_failed = self._apply_indexes(indexes)
        fk_created, fk_failed = self._apply_foreign_keys(fks)

        elapsed = time.monotonic() - start
        logger.info(
            "FASE 3 completada — Índices: %d creados / %d fallidos | "
            "FKs: %d creadas / %d fallidas | %.1fs",
            idx_created, idx_failed, fk_created, fk_failed, elapsed,
        )
        return IndexMigrationResult(
            indexes_created=idx_created,
            indexes_failed=idx_failed,
            fks_created=fk_created,
            fks_failed=fk_failed,
            elapsed_seconds=elapsed,
        )

    # ── Lectura de SQL Server ────────────────────────────────────────────────

    def _fetch_indexes(self) -> list[IndexInfo]:
        placeholders = ", ".join("?" * len(self._include_schemas))
        query = _INDEXES_QUERY.format(schema_placeholders=placeholders)
        indexes: list[IndexInfo] = []

        with pyodbc.connect(self._mssql_conn_str, timeout=30) as conn:
            cursor = conn.cursor()
            cursor.execute(query, self._include_schemas)
            for row in cursor.fetchall():
                columns = [c.strip() for c in row.columns.split(",")]
                indexes.append(IndexInfo(
                    schema=row.schema_name,
                    table_name=row.table_name,
                    index_name=row.index_name,
                    columns=columns,
                    is_unique=bool(row.is_unique),
                    is_primary_key=bool(row.is_primary_key),
                ))

        logger.info("Índices encontrados en SQL Server: %d", len(indexes))
        return indexes

    def _fetch_foreign_keys(self) -> list[ForeignKeyInfo]:
        placeholders = ", ".join("?" * len(self._include_schemas))
        query = _FKS_QUERY.format(schema_placeholders=placeholders)
        fks: list[ForeignKeyInfo] = []

        with pyodbc.connect(self._mssql_conn_str, timeout=30) as conn:
            cursor = conn.cursor()
            cursor.execute(query, self._include_schemas)
            for row in cursor.fetchall():
                fks.append(ForeignKeyInfo(
                    constraint_name=row.constraint_name,
                    schema=row.parent_schema,
                    table_name=row.parent_table,
                    columns=[c.strip() for c in row.parent_columns.split(",")],
                    ref_schema=row.ref_schema,
                    ref_table=row.ref_table,
                    ref_columns=[c.strip() for c in row.ref_columns.split(",")],
                ))

        logger.info("Foreign Keys encontrados en SQL Server: %d", len(fks))
        return fks

    # ── Aplicación en PostgreSQL ─────────────────────────────────────────────

    def _apply_indexes(self, indexes: list[IndexInfo]) -> tuple[int, int]:
        created = 0
        failed = 0

        # CONCURRENTLY requiere autocommit=True (no puede estar en transacción)
        with psycopg2.connect(self._pg_dsn) as pg_conn:
            pg_conn.autocommit = True
            cursor = pg_conn.cursor()

            for idx in indexes:
                ddl = self._build_index_ddl(idx)
                if ddl is None:
                    continue
                start = time.monotonic()
                try:
                    cursor.execute(ddl)
                    elapsed = time.monotonic() - start
                    created += 1
                    logger.info(
                        "  ✅ Índice creado: %s.%s.%s (%.1fs)",
                        idx.schema, idx.table_name, idx.index_name, elapsed,
                    )
                except psycopg2.errors.DuplicateTable:
                    logger.debug("  ⚠️  Índice ya existe: %s — omitiendo", idx.index_name)
                    created += 1
                except psycopg2.Error as exc:
                    failed += 1
                    logger.error(
                        "  ❌ Error creando índice %s: %s",
                        idx.index_name, exc,
                    )

        return created, failed

    def _apply_foreign_keys(self, fks: list[ForeignKeyInfo]) -> tuple[int, int]:
        created = 0
        failed = 0

        with psycopg2.connect(self._pg_dsn) as pg_conn:
            pg_conn.autocommit = False
            cursor = pg_conn.cursor()

            for fk in fks:
                ddl = self._build_fk_ddl(fk)
                start = time.monotonic()
                try:
                    cursor.execute(ddl)
                    pg_conn.commit()
                    elapsed = time.monotonic() - start
                    created += 1
                    logger.info(
                        "  ✅ FK creada: %s (%.1fs)", fk.constraint_name, elapsed,
                    )
                except psycopg2.errors.DuplicateObject:
                    pg_conn.rollback()
                    logger.debug("  ⚠️  FK ya existe: %s — omitiendo", fk.constraint_name)
                    created += 1
                except psycopg2.Error as exc:
                    pg_conn.rollback()
                    failed += 1
                    logger.error(
                        "  ❌ Error creando FK %s: %s", fk.constraint_name, exc,
                    )

        return created, failed

    # ── DDL builders ─────────────────────────────────────────────────────────

    def _build_index_ddl(self, idx: IndexInfo) -> str | None:
        cols = ", ".join(f'"{c}"' for c in idx.columns)
        table_ref = f'"{idx.schema}"."{idx.table_name}"'
        idx_name = f'"{idx.index_name}_pg"'   # sufijo _pg para evitar colisiones de nombre

        if idx.is_primary_key:
            return (
                f"ALTER TABLE {table_ref} "
                f"ADD CONSTRAINT {idx_name} PRIMARY KEY ({cols})"
            )

        unique_clause = "UNIQUE " if idx.is_unique else ""
        # CONCURRENTLY no bloquea la tabla durante la creación del índice
        return (
            f"CREATE {unique_clause}INDEX CONCURRENTLY IF NOT EXISTS {idx_name} "
            f"ON {table_ref} ({cols})"
        )

    @staticmethod
    def _build_fk_ddl(fk: ForeignKeyInfo) -> str:
        parent_cols = ", ".join(f'"{c}"' for c in fk.columns)
        ref_cols = ", ".join(f'"{c}"' for c in fk.ref_columns)
        return (
            f'ALTER TABLE "{fk.schema}"."{fk.table_name}" '
            f'ADD CONSTRAINT "{fk.constraint_name}" '
            f'FOREIGN KEY ({parent_cols}) '
            f'REFERENCES "{fk.ref_schema}"."{fk.ref_table}" ({ref_cols})'
        )
