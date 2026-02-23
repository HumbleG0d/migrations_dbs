"""
Sistema de checkpoints para migración reanudable.

Persiste el estado de cada tabla en un archivo JSON.
Si el proceso se cae, al reiniciar detecta el checkpoint y continúa
desde el último batch completado sin re-migrar datos ya insertados.

Estados posibles por tabla:
    PENDING      → Aún no iniciada
    IN_PROGRESS  → Migrando (guarda último batch y filas procesadas)
    DONE         → Completada exitosamente ✅
    ERROR        → Falló (guarda mensaje de error)
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

TableStatus = Literal["PENDING", "IN_PROGRESS", "DONE", "ERROR"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class CheckpointManager:
    """
    Gestiona el estado de migración por tabla con persistencia en disco.

    Thread-safe: usa un lock para operaciones concurrentes desde múltiples workers.
    """

    def __init__(self, checkpoint_file: Path) -> None:
        self._file = checkpoint_file
        self._lock = threading.Lock()
        self._state: dict = self._load()

    # ─────────────────────────────────────────────
    # API pública
    # ─────────────────────────────────────────────

    def get_status(self, table_name: str) -> TableStatus:
        """Retorna el estado actual de una tabla."""
        with self._lock:
            entry = self._state.get(table_name)
            if entry is None:
                return "PENDING"
            return entry["status"]

    def mark_in_progress(
        self,
        table_name: str,
        last_batch: int,
        rows_migrated: int,
    ) -> None:
        """Actualiza el checkpoint de una tabla en progreso."""
        with self._lock:
            existing = self._state.get(table_name, {})
            self._state[table_name] = {
                "status": "IN_PROGRESS",
                "last_batch": last_batch,
                "rows_migrated": rows_migrated,
                "started_at": existing.get("started_at", _now_iso()),
                "updated_at": _now_iso(),
                "error": None,
            }
            self._persist()

    def mark_done(self, table_name: str, total_rows: int) -> None:
        """Marca una tabla como completada."""
        with self._lock:
            existing = self._state.get(table_name, {})
            self._state[table_name] = {
                "status": "DONE",
                "last_batch": existing.get("last_batch", 0),
                "rows_migrated": total_rows,
                "started_at": existing.get("started_at", _now_iso()),
                "completed_at": _now_iso(),
                "error": None,
            }
            self._persist()
        logger.info("✅ Checkpoint DONE: %s (%s filas)", table_name, f"{total_rows:,}")

    def mark_error(self, table_name: str, error_message: str) -> None:
        """Marca una tabla como fallida con el mensaje de error."""
        with self._lock:
            existing = self._state.get(table_name, {})
            self._state[table_name] = {
                "status": "ERROR",
                "last_batch": existing.get("last_batch", 0),
                "rows_migrated": existing.get("rows_migrated", 0),
                "started_at": existing.get("started_at", _now_iso()),
                "failed_at": _now_iso(),
                "error": error_message,
            }
            self._persist()
        logger.error("❌ Checkpoint ERROR: %s — %s", table_name, error_message)

    def get_last_batch(self, table_name: str) -> int:
        """Retorna el último batch completado (0 si no hay checkpoint previo)."""
        with self._lock:
            entry = self._state.get(table_name)
            if entry is None:
                return 0
            return entry.get("last_batch", 0)

    def get_rows_migrated(self, table_name: str) -> int:
        """Retorna las filas ya migradas según el checkpoint."""
        with self._lock:
            entry = self._state.get(table_name)
            if entry is None:
                return 0
            return entry.get("rows_migrated", 0)

    def filter_pending(self, table_names: list[str]) -> list[str]:
        """
        Filtra la lista de tablas retornando solo las que NO están en estado DONE.
        Las tablas IN_PROGRESS se incluyen (se reanudan desde el ultimo batch).
        """
        with self._lock:
            return [
                t for t in table_names
                if self._state.get(t, {}).get("status") != "DONE"
            ]

    def get_tables_by_status(self, status: TableStatus) -> list[str]:
        """Retorna todas las tablas en un estado dado."""
        with self._lock:
            return [
                name for name, entry in self._state.items()
                if entry.get("status") == status
            ]

    def reset_table(self, table_name: str) -> None:
        """Resetea el estado de una tabla a PENDING para re-intentar."""
        with self._lock:
            if table_name in self._state:
                del self._state[table_name]
                self._persist()
        logger.info("🔄 Checkpoint RESET: %s → PENDING", table_name)

    def summary(self) -> dict[str, int]:
        """Retorna conteos por estado para el reporte."""
        with self._lock:
            counts: dict[str, int] = {
                "PENDING": 0,
                "IN_PROGRESS": 0,
                "DONE": 0,
                "ERROR": 0,
            }
            for entry in self._state.values():
                status = entry.get("status", "PENDING")
                if status in counts:
                    counts[status] += 1
            return counts

    # ─────────────────────────────────────────────
    # Internos
    # ─────────────────────────────────────────────

    def _load(self) -> dict:
        if not self._file.exists():
            logger.info("No se encontró checkpoint previo. Iniciando desde cero.")
            return {}
        try:
            with self._file.open("r", encoding="utf-8") as f:
                data = json.load(f)
            done = sum(1 for e in data.values() if e.get("status") == "DONE")
            logger.info(
                "Checkpoint cargado: %d tablas registradas (%d DONE)",
                len(data), done,
            )
            return data
        except json.JSONDecodeError as exc:
            logger.error(
                "Checkpoint corrupto en %s: %s — iniciando desde cero.",
                self._file, exc,
            )
            return {}

    def _persist(self) -> None:
        """Escribe el estado al disco de forma atómica (write + rename)."""
        tmp = self._file.with_suffix(".tmp")
        try:
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(self._state, f, indent=2, ensure_ascii=False)
            tmp.replace(self._file)
        except OSError as exc:
            logger.error("No se pudo persistir el checkpoint: %s", exc)
