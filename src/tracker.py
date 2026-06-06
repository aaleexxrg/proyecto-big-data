"""
tracker.py — Módulo de Tracking del Pipeline ETL
Registra entrada/salida/descartados/motivo de cada fase en JSON.
"""

import json
import datetime
import os

TRACKING_FILE = "logs/pipeline_tracking.json"


class Tracker:
    def __init__(self):
        self.entries = []
        os.makedirs("logs", exist_ok=True)

    def log(self, fase: str, fuente: str, registros_entrada: int,
            registros_salida: int, descartados: int, motivo: str):
        entry = {
            "timestamp":          datetime.datetime.now().isoformat(),
            "fase":               fase,
            "fuente":             fuente,
            "registros_entrada":  registros_entrada,
            "registros_salida":   registros_salida,
            "descartados":        descartados,
            "motivo":             motivo,
        }
        self.entries.append(entry)

    def save(self):
        with open(TRACKING_FILE, "w", encoding="utf-8") as f:
            json.dump(self.entries, f, indent=2, ensure_ascii=False)

    def load(self):
        if os.path.exists(TRACKING_FILE):
            with open(TRACKING_FILE, "r", encoding="utf-8") as f:
                self.entries = json.load(f)

    def to_dataframe(self):
        import pandas as pd
        return pd.DataFrame(self.entries)
