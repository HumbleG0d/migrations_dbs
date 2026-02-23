"""
Configuración central del proceso de migración SQL Server → PostgreSQL.
Ajusta los valores de conexión y parámetros antes de ejecutar migrate.py.
"""

from dataclasses import dataclass, field
from pathlib import Path
import os


# ─────────────────────────────────────────────
# Rutas base del proyecto
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
LOGS_DIR = BASE_DIR / "logs"
CHECKPOINTS_DIR = BASE_DIR / "checkpoints"
REPORTS_DIR = BASE_DIR / "reports"

# Crear directorios si no existen
LOGS_DIR.mkdir(exist_ok=True)
CHECKPOINTS_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)


# ─────────────────────────────────────────────
# Conexión: SQL Server (origen)
# ─────────────────────────────────────────────
MSSQL_CONFIG = {
    "driver":   "ODBC Driver 17 for SQL Server",
    "server":   os.environ.get("MSSQL_SERVER", "localhost"),
    "database": os.environ.get("MSSQL_DATABASE", "MXDBAJE"),      # Nombre real de la DB
    "username": os.environ.get("MSSQL_USER", ""),
    "password": os.environ.get("MSSQL_PASSWORD", ""),
    "trusted_connection": os.environ.get("MSSQL_TRUSTED", "yes"),  # Windows Auth activo
}

def get_mssql_connection_string() -> str:
    cfg = MSSQL_CONFIG
    if cfg["trusted_connection"].lower() == "yes":
        return (
            f"DRIVER={{{cfg['driver']}}};"
            f"SERVER={cfg['server']};"
            f"DATABASE={cfg['database']};"
            "Trusted_Connection=yes;"
        )
    return (
        f"DRIVER={{{cfg['driver']}}};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['username']};"
        f"PWD={cfg['password']};"
    )


# ─────────────────────────────────────────────
# Conexión: PostgreSQL (destino)
# ─────────────────────────────────────────────
POSTGRES_CONFIG = {
    "host":     os.environ.get("PG_HOST", "localhost"),
    "port":     int(os.environ.get("PG_PORT", "5432")),
    "database": os.environ.get("PG_DATABASE", "dbajemex_pg"),
    "user":     os.environ.get("PG_USER", "postgres"),
    "password": os.environ.get("PG_PASSWORD", ""),
}

def get_postgres_dsn() -> str:
    cfg = POSTGRES_CONFIG
    return (
        f"host={cfg['host']} "
        f"port={cfg['port']} "
        f"dbname={cfg['database']} "
        f"user={cfg['user']} "
        f"password={cfg['password']}"
    )


# ─────────────────────────────────────────────
# Parámetros de migración
# ─────────────────────────────────────────────
@dataclass
class MigrationConfig:
    # Tamaño de batch (filas leídas y escritas por iteración)
    batch_size: int = 50_000

    # Umbral de filas para clasificar tablas
    whale_threshold: int = 1_000_000   # >= whale_threshold → WHALE (1 worker dedicado)
    medium_threshold: int = 100_000    # >= medium_threshold → MEDIUM (paralelo moderado)
                                       # <  medium_threshold → SMALL  (paralelo máximo)

    # Workers en paralelo por categoría
    max_workers_medium: int = 4
    max_workers_small:  int = 8

    # Resiliencia ante fallos de conexión
    retry_attempts: int = 3
    retry_backoff_seconds: int = 30

    # Límite de RAM a utilizar (de 64 GB disponibles)
    memory_limit_gb: float = 48.0

    # Rutas de archivos de estado y reportes
    checkpoint_file: Path = field(default_factory=lambda: CHECKPOINTS_DIR / "state.json")
    log_dir: Path = field(default_factory=lambda: LOGS_DIR)
    reports_dir: Path = field(default_factory=lambda: REPORTS_DIR)

    # Esquemas de SQL Server a migrar — incluye todos los schemas con datos
    include_schemas: list = field(default_factory=lambda: [
        "dbo",
        "AJE\\karla.ramirez.mx",
        "AJE\\tania.ordonez",
        "AJE\\cristopher.osomo.mx",
        "AJE\\dulce.espinosa.mx",
        "SQLcompliance_Data_Change",
    ])

    # Tablas a excluir explícitamente (ej. tablas de sistema)
    exclude_tables: list = field(default_factory=list)

    # Tamaño de muestra aleatoria para validación post-migración
    validation_sample_size: int = 100

    # Si es True, re-migra automáticamente tablas que fallen validación
    auto_retry_failed_validation: bool = True


# Instancia global de configuración (importar desde aquí)
MIGRATION = MigrationConfig()


# ─────────────────────────────────────────────────────────────────────────────
# Mapeo de nombres de schema: SQL Server → PostgreSQL
#
# PostgreSQL NO acepta backslashes ni puntos en nombres de schema.
# Este diccionario traduce el nombre original de MSSQL al nombre seguro en PG.
# ─────────────────────────────────────────────────────────────────────────────
SCHEMA_NAME_MAP: dict[str, str] = {
    "dbo":                        "dbo",
    "AJE\\karla.ramirez.mx":      "aje_karla_ramirez_mx",
    "AJE\\tania.ordonez":         "aje_tania_ordonez",
    "AJE\\cristopher.osomo.mx":   "aje_cristopher_osomo_mx",
    "AJE\\dulce.espinosa.mx":     "aje_dulce_espinosa_mx",
    "SQLcompliance_Data_Change":  "sqlcompliance_data_change",
}


def sanitize_schema_name(mssql_schema: str) -> str:
    """
    Convierte un nombre de schema de SQL Server a un nombre válido en PostgreSQL.
    Si no hay mapeo explícito, aplica una sanitización automática:
    reemplaza caracteres inválidos por guión bajo y convierte a minúsculas.
    """
    if mssql_schema in SCHEMA_NAME_MAP:
        return SCHEMA_NAME_MAP[mssql_schema]
    # Fallback automático: reemplazar \ . @ - por _ y lowercase
    sanitized = mssql_schema.lower()
    for char in ("\\", ".", "@", "-", " "):
        sanitized = sanitized.replace(char, "_")
    return sanitized
