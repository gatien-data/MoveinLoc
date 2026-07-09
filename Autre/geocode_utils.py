"""
Utilitaires de géocodage pour l'application.

Utilise l'API Adresse du gouvernement français (api-adresse.data.gouv.fr),
gratuite, sans clé, et fiable pour des adresses françaises.

Pour éviter de geocoder 40 000 lignes individuellement, on geocode uniquement
les couples (code postal, ville) UNIQUES (~10 000 dans ce dataset), et on
met le résultat en cache sur disque (CSV) pour ne jamais refaire le travail
deux fois.
"""

import os
import time
import unicodedata
import pandas as pd
import requests

BAN_URL = "https://api-adresse.data.gouv.fr/search/"
CACHE_FILE = os.path.join(os.path.dirname(__file__), "geocode_cache.csv")


def normalize_key(cp, ville):
    """Clé stable pour identifier un couple (cp, ville) dans le cache."""
    ville_norm = (
        unicodedata.normalize("NFKD", str(ville))
        .encode("ascii", "ignore")
        .decode("ascii")
        .strip()
        .upper()
    )
    return f"{str(cp).strip()}_{ville_norm}"


def geocode_one(cp, ville, timeout=5):
    """Interroge l'API BAN pour un couple (cp, ville). Renvoie (lat, lon) ou (None, None)."""
    try:
        params = {"q": f"{ville}", "postcode": str(cp), "limit": 1, "type": "municipality"}
        r = requests.get(BAN_URL, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        feats = data.get("features", [])
        if not feats:
            # fallback : recherche sans filtrer le type
            params = {"q": f"{ville} {cp}", "limit": 1}
            r = requests.get(BAN_URL, params=params, timeout=timeout)
            r.raise_for_status()
            feats = r.json().get("features", [])
        if feats:
            lon, lat = feats[0]["geometry"]["coordinates"]
            return lat, lon
    except requests.RequestException:
        pass
    return None, None


def load_cache():
    if os.path.exists(CACHE_FILE):
        return pd.read_csv(CACHE_FILE)
    return pd.DataFrame(columns=["key", "cp", "ville", "lat", "lon"])


def save_cache(df):
    df.to_csv(CACHE_FILE, index=False)


def geocode_locations(unique_locations, progress_callback=None):
    """
    unique_locations : DataFrame avec colonnes ['cp', 'ville']  (valeurs uniques)
    progress_callback : fonction(int_done, int_total) appelée régulièrement (optionnel,
                         utile pour afficher une barre de progression Streamlit)

    Retourne un DataFrame ['cp', 'ville', 'lat', 'lon'] complet (cache + nouveaux).
    """
    cache = load_cache()
    cache["key"] = cache.apply(lambda r: normalize_key(r["cp"], r["ville"]), axis=1)

    unique_locations = unique_locations.copy()
    unique_locations["key"] = unique_locations.apply(
        lambda r: normalize_key(r["cp"], r["ville"]), axis=1
    )

    known_keys = set(cache["key"])
    to_geocode = unique_locations[~unique_locations["key"].isin(known_keys)]

    new_rows = []
    total = len(to_geocode)
    for i, (_, row) in enumerate(to_geocode.iterrows()):
        lat, lon = geocode_one(row["cp"], row["ville"])
        new_rows.append(
            {"key": row["key"], "cp": row["cp"], "ville": row["ville"], "lat": lat, "lon": lon}
        )
        if progress_callback:
            progress_callback(i + 1, total)
        time.sleep(0.05)  # courtoisie envers l'API publique

    if new_rows:
        cache = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True)
        save_cache(cache)

    return cache[["key", "cp", "ville", "lat", "lon"]]
