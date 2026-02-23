"""
Orquestador principal de migración SQL Server → PostgreSQL.

Ejecuta las 4 fases en orden:
  FASE 1 — Schema (DDL sin índices ni FKs)
  FASE 2 — Datos en batches con paralelismo inteligente:
            🐋 WHALE  (>1M filas)  → 1 worker dedicado, secuencial
            🐟 MEDIUM (100K–1M)   → ThreadPoolExecutor(max_workers=4)
            🦐 SMALL  (<100K)     → ThreadPoolExecutor(max_workers=8)
  FASE 3 — Índices CONCURRENTLY + FKs post-carga
  FASE 4 — Validación: conteo + checksums + muestras aleatorias

Uso:
    python migrate.py                     # Migración completa
    python migrate.py --schema-only       # Solo FASE 1
    python migrate.py --data-only         # Solo FASE 2 (reanuda checkpoints)
    python migrate.py --validate-only     # Solo FASE 4
    python migrate.py --reset TABLA       # Resetea checkpoint de una tabla
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from config import MIGRATION, LOGS_DIR, REPORTS_DIR
from migrator.checkpoint import CheckpointManager
from migrator.data_migrator import DataMigrator, TableMigrationResult
from migrator.index_migrator import IndexMigrator
from migrator.schema_migrator import SchemaMigrator, TableInfo
from migrator.validator import ValidationResult, Validator

console = Console()


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = LOGS_DIR / f"migration_{today}.log"
    error_file = LOGS_DIR / f"errors_{today}.log"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Consola — rich handler (INFO y superior)
    console_handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        show_path=False,
        markup=True,
    )
    console_handler.setLevel(logging.INFO)

    # Archivo completo — todos los niveles
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    # Archivo de errores — solo ERROR y CRITICAL
    error_handler = logging.FileHandler(error_file, encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s — %(message)s\n%(exc_info)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    root.addHandler(console_handler)
    root.addHandler(file_handler)
    root.addHandler(error_handler)


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Clasificación de tablas
# ─────────────────────────────────────────────────────────────────────────────

def classify_tables(
    tables: list[TableInfo],
    row_counts: dict[str, int],
) -> tuple[list[tuple[TableInfo, int]], list[tuple[TableInfo, int]], list[tuple[TableInfo, int]]]:
    """
    Clasifica tablas en WHALE / MEDIUM / SMALL según su conteo de filas.

    Returns:
        (whales, mediums, smalls): listas de (TableInfo, row_count)
        Cada lista está ordenada de mayor a menor por conteo de filas.
    """
    whales: list[tuple[TableInfo, int]] = []
    mediums: list[tuple[TableInfo, int]] = []
    smalls: list[tuple[TableInfo, int]] = []

    for table in tables:
        count = row_counts.get(table.name, 0)
        if count >= MIGRATION.whale_threshold:
            whales.append((table, count))
        elif count >= MIGRATION.medium_threshold:
            mediums.append((table, count))
        else:
            smalls.append((table, count))

    whales.sort(key=lambda x: x[1], reverse=True)
    mediums.sort(key=lambda x: x[1], reverse=True)
    smalls.sort(key=lambda x: x[1], reverse=True)

    logger.info(
        "Clasificación: 🐋 %d WHALE | 🐟 %d MEDIUM | 🦐 %d SMALL",
        len(whales), len(mediums), len(smalls),
    )
    return whales, mediums, smalls


# ─────────────────────────────────────────────────────────────────────────────
# Fases de migración
# ─────────────────────────────────────────────────────────────────────────────

def run_phase1(schema_migrator: SchemaMigrator) -> tuple[list[TableInfo], dict[str, int]]:
    """FASE 1: Lee catálogo y crea schemas en PostgreSQL."""
    console.rule("[bold blue]FASE 1 — Schema Migration[/bold blue]")
    tables = schema_migrator.fetch_table_catalog()
    row_counts = schema_migrator.get_table_row_counts(tables)
    schema_migrator.migrate_all_schemas(tables)
    return tables, row_counts


def run_phase2(
    tables: list[TableInfo],
    row_counts: dict[str, int],
    checkpoint: CheckpointManager,
) -> list[TableMigrationResult]:
    """FASE 2: Migra datos con paralelismo por categoría."""
    console.rule("[bold blue]FASE 2 — Data Migration[/bold blue]")
    whales, mediums, smalls = classify_tables(tables, row_counts)
    migrator = DataMigrator(checkpoint)
    results: list[TableMigrationResult] = []

    # ── 🐋 WHALEs — secuencial, mayor a menor ───────────────────────────────
    if whales:
        console.print(f"[bold yellow]🐋 Migrando {len(whales)} tablas WHALE (secuencial)…[/bold yellow]")
        for table, count in whales:
            result = migrator.migrate_table(table, count)
            results.append(result)
            _write_progress_csv(result)

    # ── 🐟 MEDIUMs — paralelo moderado ──────────────────────────────────────
    if mediums:
        console.print(f"[bold cyan]🐟 Migrando {len(mediums)} tablas MEDIUM ({MIGRATION.max_workers_medium} workers)…[/bold cyan]")
        results.extend(
            _run_parallel(migrator, mediums, MIGRATION.max_workers_medium)
        )

    # ── 🦐 SMALLs — paralelo máximo ─────────────────────────────────────────
    if smalls:
        console.print(f"[bold green]🦐 Migrando {len(smalls)} tablas SMALL ({MIGRATION.max_workers_small} workers)…[/bold green]")
        results.extend(
            _run_parallel(migrator, smalls, MIGRATION.max_workers_small)
        )

    done = sum(1 for r in results if r.status == "DONE")
    errors = sum(1 for r in results if r.status == "ERROR")
    skipped = sum(1 for r in results if r.status == "SKIPPED")
    logger.info(
        "FASE 2 completada — ✅ %d DONE | ❌ %d ERROR | ⏭️  %d SKIPPED",
        done, errors, skipped,
    )
    return results


def run_phase3() -> None:
    """FASE 3: Aplica índices CONCURRENTLY y FKs."""
    console.rule("[bold blue]FASE 3 — Indexes & Foreign Keys[/bold blue]")
    IndexMigrator().run()


def run_phase4(tables: list[TableInfo]) -> list[ValidationResult]:
    """FASE 4: Validación post-migración."""
    console.rule("[bold blue]FASE 4 — Validation[/bold blue]")
    return Validator().validate_all(tables)


# ─────────────────────────────────────────────────────────────────────────────
# Paralelismo
# ─────────────────────────────────────────────────────────────────────────────

def _run_parallel(
    migrator: DataMigrator,
    items: list[tuple[TableInfo, int]],
    max_workers: int,
) -> list[TableMigrationResult]:
    results: list[TableMigrationResult] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Migrando tablas…", total=len(items))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(migrator.migrate_table, table, count): table.name
                for table, count in items
            }
            for future in concurrent.futures.as_completed(futures):
                table_name = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    _write_progress_csv(result)
                except Exception as exc:
                    logger.error("Worker falló para tabla %s: %s", table_name, exc)
                finally:
                    progress.advance(task)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# CSV de progreso
# ─────────────────────────────────────────────────────────────────────────────

def _write_progress_csv(result: TableMigrationResult) -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    csv_path = LOGS_DIR / f"progress_{today}.csv"
    write_header = not csv_path.exists()

    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "tabla", "schema", "filas_origen", "filas_destino",
            "estado", "tiempo_segundos", "batches", "filas_omitidas", "error",
        ])
        if write_header:
            writer.writeheader()
        writer.writerow({
            "tabla": result.table_name,
            "schema": result.schema,
            "filas_origen": result.rows_origin,
            "filas_destino": result.rows_migrated,
            "estado": result.status,
            "tiempo_segundos": f"{result.elapsed_seconds:.1f}",
            "batches": result.batches_completed,
            "filas_omitidas": result.rows_skipped,
            "error": result.error_message or "",
        })


# ─────────────────────────────────────────────────────────────────────────────
# Reporte HTML final
# ─────────────────────────────────────────────────────────────────────────────

def generate_html_report(
    data_results: list[TableMigrationResult],
    validation_results: list[ValidationResult],
    total_elapsed: float,
) -> Path:
    today = datetime.now().strftime("%Y-%m-%d_%H-%M")
    report_path = REPORTS_DIR / f"migration_report_{today}.html"

    done = sum(1 for r in data_results if r.status == "DONE")
    errors = sum(1 for r in data_results if r.status == "ERROR")
    skipped = sum(1 for r in data_results if r.status == "SKIPPED")
    total_rows = sum(r.rows_migrated for r in data_results)

    val_pass = sum(1 for r in validation_results if r.status == "PASS")
    val_warn = sum(1 for r in validation_results if r.status == "WARN")
    val_fail = sum(1 for r in validation_results if r.status == "FAIL")

    hours = int(total_elapsed // 3600)
    minutes = int((total_elapsed % 3600) // 60)

    rows_html = ""
    for r in sorted(data_results, key=lambda x: x.elapsed_seconds, reverse=True):
        status_icon = {"DONE": "✅", "ERROR": "❌", "SKIPPED": "⏭️"}.get(r.status, "❓")
        val_result = next(
            (v for v in validation_results if v.table_name == r.table_name), None
        )
        val_icon = ""
        if val_result:
            val_icon = {"PASS": "🟢", "WARN": "🟡", "FAIL": "🔴"}.get(val_result.status, "")

        rows_html += f"""
        <tr>
            <td>{r.schema}.{r.table_name}</td>
            <td>{status_icon} {r.status}</td>
            <td>{r.rows_origin:,}</td>
            <td>{r.rows_migrated:,}</td>
            <td>{r.elapsed_seconds:.1f}s</td>
            <td>{r.batches_completed}</td>
            <td>{val_icon} {val_result.status if val_result else '—'}</td>
            <td style="color:red">{r.error_message or ''}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Reporte de Migración — {today}</title>
    <style>
        body {{ font-family: 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 24px; }}
        h1 {{ color: #38bdf8; }}
        .cards {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 24px 0; }}
        .card {{ background: #1e293b; border-radius: 12px; padding: 20px 28px; min-width: 140px; }}
        .card .num {{ font-size: 2.5rem; font-weight: 700; }}
        .card .lbl {{ font-size: 0.85rem; color: #94a3b8; }}
        .green {{ color: #22c55e; }}
        .red {{ color: #ef4444; }}
        .yellow {{ color: #eab308; }}
        .blue {{ color: #38bdf8; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 24px; }}
        th {{ background: #1e293b; padding: 10px 12px; text-align: left; font-size: 0.8rem; color: #94a3b8; text-transform: uppercase; }}
        td {{ padding: 8px 12px; border-bottom: 1px solid #1e293b; font-size: 0.85rem; }}
        tr:hover td {{ background: #1e293b55; }}
        .badge {{ border-radius: 99px; padding: 2px 10px; font-size: 0.75rem; font-weight: 600; }}
    </style>
</head>
<body>
    <h1>📊 Reporte de Migración SQL Server → PostgreSQL</h1>
    <p style="color:#64748b">Generado: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Tiempo total: {hours}h {minutes}m</p>

    <div class="cards">
        <div class="card"><div class="num blue">{done + skipped}</div><div class="lbl">✅ Tablas Migradas</div></div>
        <div class="card"><div class="num red">{errors}</div><div class="lbl">❌ Con Error</div></div>
        <div class="card"><div class="num green">{total_rows:,}</div><div class="lbl">📦 Filas Totales</div></div>
        <div class="card"><div class="num green">{val_pass}</div><div class="lbl">🟢 Validación PASS</div></div>
        <div class="card"><div class="num yellow">{val_warn}</div><div class="lbl">🟡 Validación WARN</div></div>
        <div class="card"><div class="num red">{val_fail}</div><div class="lbl">🔴 Validación FAIL</div></div>
    </div>

    <h2 style="color:#94a3b8">Detalle por tabla (ordenado por tiempo de migración)</h2>
    <table>
        <thead>
            <tr>
                <th>Tabla</th>
                <th>Estado</th>
                <th>Filas Origen</th>
                <th>Filas Destino</th>
                <th>Tiempo</th>
                <th>Batches</th>
                <th>Validación</th>
                <th>Error</th>
            </tr>
        </thead>
        <tbody>
            {rows_html}
        </tbody>
    </table>
</body>
</html>"""

    report_path.write_text(html, encoding="utf-8")
    logger.info("📄 Reporte HTML generado: %s", report_path)
    return report_path


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrador SQL Server → PostgreSQL (723GB, 2833 tablas)"
    )
    parser.add_argument("--schema-only", action="store_true", help="Solo ejecutar FASE 1 (schema)")
    parser.add_argument("--data-only", action="store_true", help="Solo ejecutar FASE 2 (datos)")
    parser.add_argument("--indexes-only", action="store_true", help="Solo ejecutar FASE 3 (índices/FKs)")
    parser.add_argument("--validate-only", action="store_true", help="Solo ejecutar FASE 4 (validación)")
    parser.add_argument("--reset", metavar="TABLA", help="Resetear checkpoint de una tabla")
    parser.add_argument("--skip-indexes", action="store_true", help="Omitir FASE 3 en migración completa")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    checkpoint = CheckpointManager(MIGRATION.checkpoint_file)

    # ── Reset de tabla específica ────────────────────────────────────────────
    if args.reset:
        checkpoint.reset_table(args.reset)
        console.print(f"[green]Checkpoint de '{args.reset}' reseteado a PENDING.[/green]")
        return

    console.rule("[bold magenta]🚀 INICIO DE MIGRACIÓN SQL Server → PostgreSQL[/bold magenta]")
    global_start = time.monotonic()

    schema_migrator = SchemaMigrator()
    tables: list[TableInfo] = []
    row_counts: dict[str, int] = {}
    data_results: list[TableMigrationResult] = []
    validation_results: list[ValidationResult] = []

    # ── Modo schema only ─────────────────────────────────────────────────────
    if args.schema_only:
        tables, row_counts = run_phase1(schema_migrator)
        return

    # ── Modo data only ───────────────────────────────────────────────────────
    if args.data_only:
        tables = schema_migrator.fetch_table_catalog()
        row_counts = schema_migrator.get_table_row_counts(tables)
        data_results = run_phase2(tables, row_counts, checkpoint)
        generate_html_report(data_results, [], time.monotonic() - global_start)
        return

    # ── Modo indexes only ────────────────────────────────────────────────────
    if args.indexes_only:
        run_phase3()
        return

    # ── Modo validate only ───────────────────────────────────────────────────
    if args.validate_only:
        tables = schema_migrator.fetch_table_catalog()
        validation_results = run_phase4(tables)
        generate_html_report([], validation_results, time.monotonic() - global_start)
        return

    # ── Migración completa ───────────────────────────────────────────────────
    tables, row_counts = run_phase1(schema_migrator)
    data_results = run_phase2(tables, row_counts, checkpoint)

    if not args.skip_indexes:
        run_phase3()

    validation_results = run_phase4(tables)

    # Re-migrar automáticamente tablas con FAIL en validación
    if MIGRATION.auto_retry_failed_validation:
        retry_tables_names = Validator().get_tables_to_retry(validation_results)
        if retry_tables_names:
            retry_tables = [t for t in tables if t.name in retry_tables_names]
            logger.info("🔁 Re-migrando %d tablas con FAIL en validación…", len(retry_tables))
            migrator = DataMigrator(checkpoint)
            for table in retry_tables:
                checkpoint.reset_table(table.name)
                result = migrator.migrate_table(table, row_counts.get(table.name, 0))
                data_results.append(result)

    total_elapsed = time.monotonic() - global_start
    report_path = generate_html_report(data_results, validation_results, total_elapsed)

    hours = int(total_elapsed // 3600)
    minutes = int((total_elapsed % 3600) // 60)
    console.rule("[bold green]✅ MIGRACIÓN COMPLETADA[/bold green]")
    console.print(f"[green]Tiempo total: {hours}h {minutes}m[/green]")
    console.print(f"[blue]Reporte: {report_path}[/blue]")
    _print_summary_table(data_results, validation_results)


def _print_summary_table(
    data_results: list[TableMigrationResult],
    val_results: list[ValidationResult],
) -> None:
    table = Table(title="Resumen de Migración", style="bold")
    table.add_column("Métrica", style="cyan")
    table.add_column("Valor", justify="right")

    done = sum(1 for r in data_results if r.status == "DONE")
    errors = sum(1 for r in data_results if r.status == "ERROR")
    skipped = sum(1 for r in data_results if r.status == "SKIPPED")
    total_rows = sum(r.rows_migrated for r in data_results)

    table.add_row("Total tablas", str(len(data_results)))
    table.add_row("✅ DONE", f"[green]{done}[/green]")
    table.add_row("❌ ERROR", f"[red]{errors}[/red]")
    table.add_row("⏭️  SKIPPED", str(skipped))
    table.add_row("📦 Filas migradas", f"{total_rows:,}")
    table.add_row("🟢 Validación PASS", str(sum(1 for r in val_results if r.status == "PASS")))
    table.add_row("🟡 Validación WARN", str(sum(1 for r in val_results if r.status == "WARN")))
    table.add_row("🔴 Validación FAIL", str(sum(1 for r in val_results if r.status == "FAIL")))

    console.print(table)


if __name__ == "__main__":
    main()
