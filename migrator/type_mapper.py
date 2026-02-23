"""
Mapeo de tipos de datos SQL Server → PostgreSQL.

Reglas aplicadas (skill: database-migrations):
- MONEY → NUMERIC(19,4): PostgreSQL no tiene tipo MONEY nativo
- BIT → BOOLEAN: conversión explícita 0/1 → false/true
- TINYINT → SMALLINT: PostgreSQL no tiene TINYINT
- NVARCHAR(MAX) / VARCHAR(MAX) → TEXT: sin límite de longitud
- IMAGE / VARBINARY → BYTEA: datos binarios
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Mapa base: tipo MSSQL (normalizado a minúsculas) → tipo PG
# Tipos que requieren parámetros (precision, scale, length) se resuelven
# en map_column() con lógica adicional.
# ─────────────────────────────────────────────────────────────────────────────
_SIMPLE_TYPE_MAP: dict[str, str] = {
    # Enteros
    "tinyint":   "SMALLINT",         # PG no tiene TINYINT (rango 0-255 → cabe en SMALLINT)
    "smallint":  "SMALLINT",
    "int":       "INTEGER",
    "bigint":    "BIGINT",

    # Punto flotante
    "real":             "REAL",
    "float":            "DOUBLE PRECISION",

    # Monetarios (sin tipo nativo en PG)
    "money":       "NUMERIC(19,4)",
    "smallmoney":  "NUMERIC(10,4)",

    # Booleano
    "bit":  "BOOLEAN",

    # Texto sin longitud
    "text":      "TEXT",
    "ntext":     "TEXT",

    # Fecha y hora
    "date":              "DATE",
    "time":              "TIME",
    "datetime":          "TIMESTAMP",
    "datetime2":         "TIMESTAMP",
    "smalldatetime":     "TIMESTAMP",
    "datetimeoffset":    "TIMESTAMPTZ",

    # Binarios
    "image":         "BYTEA",
    "varbinary":     "BYTEA",
    "binary":        "BYTEA",
    "timestamp":     "BYTEA",   # MSSQL timestamp es rowversion (binario), NO fecha
    "rowversion":    "BYTEA",

    # Identificadores únicos
    "uniqueidentifier": "UUID",

    # XML
    "xml":  "XML",

    # Geoespaciales (mapeo básico)
    "geography":  "TEXT",
    "geometry":   "TEXT",

    # SQL_VARIANT → TEXT como fallback
    "sql_variant": "TEXT",
}


def map_column(
    sql_type: str,
    max_length: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
) -> str:
    """
    Convierte un tipo SQL Server al equivalente PostgreSQL.

    Args:
        sql_type:   Nombre del tipo de dato (case-insensitive). Ej: 'nvarchar', 'decimal'.
        max_length: Longitud máxima para tipos char/binary. -1 = MAX en MSSQL.
        precision:  Precisión total para tipos numéricos.
        scale:      Escala (dígitos decimales) para tipos numéricos.

    Returns:
        String con el tipo PostgreSQL listo para usar en DDL.

    Raises:
        ValueError: Si sql_type es None o vacío.
    """
    if not sql_type:
        raise ValueError("sql_type no puede ser None o vacío")

    normalized = sql_type.strip().lower()

    # ── Tipos con longitud (char/varchar/nchar/nvarchar/binary/varbinary) ──
    if normalized in ("char", "nchar"):
        if max_length is None or max_length <= 0:
            return "TEXT"
        # NCHAR almacena Unicode; en PG CHAR es equivalente
        return f"CHAR({max_length})"

    if normalized in ("varchar", "nvarchar"):
        if max_length is None or max_length == -1:   # -1 = MAX en MSSQL
            return "TEXT"
        return f"VARCHAR({max_length})"

    # varbinary con longitud explícita sigue siendo BYTEA en PG
    if normalized == "varbinary":
        return "BYTEA"

    # ── Tipos numéricos con precisión/escala ──
    if normalized in ("decimal", "numeric"):
        if precision is not None and scale is not None:
            return f"NUMERIC({precision},{scale})"
        if precision is not None:
            return f"NUMERIC({precision})"
        return "NUMERIC"

    # ── Consultar mapa simple ──
    if normalized in _SIMPLE_TYPE_MAP:
        return _SIMPLE_TYPE_MAP[normalized]

    # ── Tipo desconocido: usar TEXT como fallback y loggear advertencia ──
    logger.warning(
        "Tipo SQL Server desconocido '%s' — usando TEXT como fallback. "
        "Revisa el reporte de errores para validar el resultado.",
        sql_type,
    )
    return "TEXT"


def is_bit_column(sql_type: str) -> bool:
    """Retorna True si la columna es de tipo BIT (requiere conversión 0/1 → bool)."""
    return sql_type.strip().lower() == "bit"


def is_binary_column(sql_type: str) -> bool:
    """Retorna True si la columna requiere manejo de datos binarios."""
    return sql_type.strip().lower() in ("image", "varbinary", "binary", "timestamp", "rowversion")


def is_uuid_column(sql_type: str) -> bool:
    """Retorna True si la columna es UNIQUEIDENTIFIER (requiere cast a UUID en PG)."""
    return sql_type.strip().lower() == "uniqueidentifier"


def requires_encoding_check(sql_type: str) -> bool:
    """Retorna True si la columna puede tener problemas de encoding (texto Unicode)."""
    return sql_type.strip().lower() in ("nvarchar", "nchar", "ntext", "varchar", "char", "text")
