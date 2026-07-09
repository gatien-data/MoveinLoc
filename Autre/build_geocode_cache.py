"""
Script à lancer UNE FOIS (en ligne de commande, avec accès internet) pour
pré-calculer le cache de géocodage de toutes les villes du dataset.
Cela évite d'attendre le géocodage au premier lancement de l'app Streamlit.

Usage :
    python build_geocode_cache.py
"""

import pandas as pd
from geocode_utils import geocode_locations

CSV_PATH = "dfjade.csv"  # adapte le chemin si besoin


def main():
    df = pd.read_csv(CSV_PATH)
    unique_locations = df[["cp", "ville"]].drop_duplicates().reset_index(drop=True)
    total = len(unique_locations)
    print(f"{total} couples (code postal, ville) uniques à géocoder...")

    def progress(done, total_):
        if total_:
            pct = 100 * done / total_
            print(f"\r{done}/{total_} ({pct:.1f}%)", end="", flush=True)

    geocode_locations(unique_locations, progress_callback=progress)
    print("\nTerminé. Cache enregistré dans geocode_cache.csv")


if __name__ == "__main__":
    main()
