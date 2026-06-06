"""
main.py — Ejecutor principal del pipeline ETL IBEX 35
Ejecuta las fases en orden: EXTRACT → CLEAN → TRANSFORM → LOAD
"""
import logging
import sys

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def main():
    log.info("╔══════════════════════════════════════════════╗")
    log.info("║  PIPELINE ETL IBEX 35 — Ejecución completa  ║")
    log.info("╚══════════════════════════════════════════════╝")

    # Fase 1: Extracción
    log.info("\n▶ FASE 1/4: EXTRACT")
    from extract import run_extraction
    run_extraction()

    # Fase 2: Limpieza y transformación ETL
    log.info("\n▶ FASE 2/4: CLEAN")
    from clean import run_cleaning
    run_cleaning()

    # Fase 3: Construcción modelo dimensional
    log.info("\n▶ FASE 3/4: TRANSFORM (Modelo Dimensional)")
    from transform import run_transform
    run_transform()

    # Fase 4: Carga en MySQL
    log.info("\n▶ FASE 4/4: LOAD (MySQL)")
    try:
        from load import run_load
        run_load()
    except Exception as e:
        log.warning(f"LOAD saltado (sin conexión MySQL): {e}")
        log.info("→ Los CSV finales están en data/final/ listos para carga manual.")

    log.info("\n✅ PIPELINE COMPLETADO. Revisa data/final/ y logs/pipeline_tracking.json")


if __name__ == "__main__":
    main()
