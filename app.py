import streamlit as st
import folium
from streamlit_geolocation import streamlit_geolocation
from streamlit_folium import st_folium
import pandas as pd
import requests
import re
import json
from datetime import datetime, timedelta, date
import ast
from bs4 import BeautifulSoup
import time
# ======================================
# IMPORT DE L AGENT IA
# ======================================



col1, col2 = st.columns([3,3], vertical_alignment="bottom")
with col1:
    st.image("Assets/IMG_logo/logo_streamlit.png")
with col2:
    st.title("Autour de vous ...")

# =======================================
# SESSION_STATE
# =======================================
if "lat" not in st.session_state:
    st.session_state["lat"] = None
if "lon" not in st.session_state:
    st.session_state["lon"] = None
if "liste_ville" not in st.session_state:
    st.session_state["liste_ville"] = []
if "rayon" not in st.session_state:
    st.session_state["rayon"] = 100
if "cp" not in st.session_state:
    st.session_state["cp"] = None
if "recherche_faite" not in st.session_state:
    st.session_state["recherche_faite"] = False
if "carte_key" not in st.session_state:
    st.session_state["carte_key"] = 0

# =======================================
# FONCTION
# =======================================
def API_adresse(adresse_postale):
  url = f"https://data.geopf.fr/geocodage/search/?q={adresse_postale}"
  try :
    r = requests.get(url).json()
    coor = r['features'][0]['geometry']['coordinates'][::-1]
    return coor
  except :
    return None

@st.cache_data(ttl=3600)
def villes_voisines(cp, rayon):
    url = f"https://www.villes-voisines.fr/getcp.php?cp={cp}&rayon={rayon}"
    try:
        response = requests.get(url)
        ville_json = response.json()
        liste_ville = []
        if isinstance(ville_json, dict):
            for i in range(len(ville_json)):
                if str(i) in ville_json:
                    liste_ville.append(ville_json[str(i)]["nom_commune"])
        elif isinstance(ville_json, list):
            for ville in ville_json:
                liste_ville.append(ville["nom_commune"])
        return liste_ville
    except:
        return []
    
# ==========================================
# AGENT IA
# ==========================================
THEMES = {
    "Marché": "marche.png",
    "Concert": "concert.png",
    "Exposition": "expo.png",
    "Théâtre": "theatre.png",
    "Festival": "festival.png",
    "Sport": "sport.png",
    "Nature": "nature.png",
    "Conférence": "conference.png",
}

############################
#        USER LOCATION
############################
#location = streamlit_geolocation()
#if location and location["latitude"] is not None:
#    lat = location["latitude"]
#    lon = location["longitude"]
#else:
#    st.info("Autorisez l'accès à votre localisation.")

# ==========================================
# FILTRES
# ==========================================


with st.form("filtres"):
    # DATE
    start_date = date.today()
    end_date = start_date + timedelta(weeks=25)
    selected_date_range = st.slider(
        "Sélectionnez une plage de dates",
        min_value=start_date,
        max_value=end_date,
        value=(start_date, end_date),
        step=timedelta(days=1),
        )
    # VILLE - RAYON
    col1, col2, col3 = st.columns([3,1.5,1], vertical_alignment="bottom")
    with col1:
        search_ville = st.text_input("Ville", placeholder="Ex : Aix-en-Provence")
    with col2:
        rayon = st.slider("Rayon (km)",
            min_value=5,
            max_value=100,
            value=st.session_state["rayon"],
            step=5
            )
    with col3:

        rechercher = st.form_submit_button("🔍 Rechercher", use_container_width=True)

st.session_state["rayon"] = rayon

    

# ==========================================
# TRAITEMENT DES FILTRES
# ==========================================

if rechercher:
    if search_ville.strip():
        # Géocodage barre de recherche
        geo_url = f'https://data.geopf.fr/geocodage/search/?q={search_ville}'
        try:
            r = requests.get(geo_url).json()
            coor = r['features'][0]['geometry']['coordinates'][::-1]
            st.session_state['lat'] = float(coor[0])
            st.session_state['lon'] = float(coor[1])
            # USER CP
            user_loc = [st.session_state["lat"], st.session_state["lon"]]
            url = f"https://data.geopf.fr/geocodage/reverse?lon={user_loc[1]}&lat={user_loc[0]}&limit=1"
            response = requests.get(url).json()
            cp_temp = pd.json_normalize(response, record_path='features')
            st.session_state["cp"] = cp_temp["properties.postcode"].iloc[0]          
                   
        except Exception:
            st.error("Ville introuvable")
    else:
        st.warning("Veuillez entrer une ville ou un code postal.")

# CALCUL DES VILLES DANS LE RAYON
if st.session_state["cp"] is not None:
    st.session_state["liste_ville"] = villes_voisines(
                st.session_state["cp"],
                st.session_state["rayon"])

# ==========================================
#       WEBSCRAPPING
# ==========================================

#MOIS = {
#    1:"janvier",2:"février",3:"mars",4:"avril",
#    5:"mai",6:"juin",7:"juillet",8:"août",
#    9:"septembre",10:"octobre",11:"novembre",12:"décembre"
#}
#@st.cache_data(ttl=3600)
#def scrape_agendaculturel(ville):
#    url = f"https://www.agendaculturel.fr/?q={ville}"
#    try:
#        html = requests.get(url, timeout=10)
#        soup = BeautifulSoup(html.text, "html.parser")
#    except:
#        return pd.DataFrame()
#    events = []
#    for card in soup.select("div.card-body"):
#        data = {}
#
#        # ---------------- DATE ----------------
#        badge = card.select_one("span.card-main-badge")
#        date_d = None
#        date_f = None
#        if badge:
#            times = badge.find_all("time")
#            if len(times) == 1:
#                d = datetime.fromisoformat(times[0]["datetime"])
#                date_d = d
#                date_f = d
#            elif len(times) == 2:
#                date_d = datetime.fromisoformat(times[0]["datetime"])
#                date_f = datetime.fromisoformat(times[1]["datetime"])
#
#        # ---------------- TITRE ----------------
#        titre = card.select_one("[itemprop='name']")
#        titre = titre.get_text(strip=True) if titre else ""
#
#        # ---------------- LIEU ----------------
#        lieu = card.select_one('[itemprop="location"] [itemprop="name"]')
#        lieu = lieu.get_text(strip=True) if lieu else ""
#
#        # ---------------- DESCRIPTION ----------------
#        desc = card.select_one('[itemprop="description"]')
#        desc = desc.get_text(" ", strip=True) if desc else ""
#
#        adresse_complete = f"{lieu}, {ville}"
#
#        events.append({
#            "nom_event": titre,
#            "adresse": lieu,
#            "adresse_complete": adresse_complete,
#            "ville": ville,
#            "date_d": date_d,
#            "date_f": date_f,
#            "description": desc,
#            "coor": None,
#            "source": "AgendaCulturel"
#        })
#
#    return pd.DataFrame(events)

# ==========================================
#           CARTE
# ==========================================

if st.session_state["lat"] is not None:
    if st.session_state['liste_ville']:
                
        df = pd.read_csv("data/df_flanerbouger.csv")
        
        # ==========================================
        #                  WEBSCRAPPING
        # ==========================================

        #with st.spinner("Recherche AgendaCulturel..."):
        #    liste_agenda = []
        #    for ville in st.session_state["liste_ville"]:
        #        df_ville = scrape_agendaculturel(ville)
        #        if not df_ville.empty:
        #            liste_agenda.append(df_ville)
        #            time.sleep(0.3)      # évite de bombarder le site
        #    if liste_agenda:
        #        df_agenda = pd.concat(liste_agenda, ignore_index=True)
        #        df = pd.concat([df, df_agenda], ignore_index=True)
        # ==========================================


        # DATA FILTRE PAR LES VILLES
        df["ville"] = df["ville"].apply(lambda x: str(str(x).lower().capitalize()))
        df = df[df['ville'].isin(st.session_state['liste_ville'])].reset_index(drop=True)

        # DATA FILTRE PAR LES DATES
        choix_debut = pd.Timestamp(selected_date_range[0])
        choix_fin = pd.Timestamp(selected_date_range[1])
        df["date_d"] = pd.to_datetime(df["date_d"])
        df["date_f"] = pd.to_datetime(df["date_f"])
        df = df[(df["date_d"] <= choix_fin) & (df["date_f"] >= choix_debut)].reset_index(drop=True)

       
        
        # ==========================================
        # GEOCODAGE Manquant (evite les appel repetés)
        def verif_coor(x):
            if pd.isna(x):
                return None
            if isinstance(x, str):
                x = x.strip()
                if x == "" or x.lower() == "none":
                    return None
                return ast.literal_eval(x)
                
        df["coor"] = df["coor"].apply(verif_coor)        
        if "geocache" not in st.session_state:
            st.session_state["geocache"] = {}
        
        def get_coords(adresse):
            if adresse not in st.session_state["geocache"]:
                st.session_state["geocache"][adresse] = API_adresse(adresse)
            return st.session_state["geocache"][adresse]
        
        with st.spinner("Géocodage des adresses..."):
            mask = df["coor"].isna()
            df.loc[mask, "coor"] = df.loc[mask, "adresse_complete"].apply(get_coords)
    
        # ==========================================
        # GROUPEMENT PAR ADRESSE (pour markers multi-événements)
        df["coor_key"] = df["coor"].apply(lambda x: tuple(x) if x is not None else None)
        groupes = df[df["coor_key"].notna()].groupby("coor_key")


        m = folium.Map(location=[st.session_state["lat"], st.session_state["lon"]], zoom_start=10)

        for coor, groupe in groupes:
            if len(groupe) == 1:
                row = groupe.iloc[0]
                date_d_str = row['date_d'].strftime('%d/%m/%Y') if pd.notna(row['date_d']) else "?"
                date_f_str = f" → {row['date_f'].strftime('%d/%m/%Y')}" if pd.notna(row['date_f']) else ""
                adresse = row.get("adresse", "")
                if pd.notna(adresse) and str(adresse).strip().lower() != "nan":
                    ligne_adresse = f"{adresse}, {row['ville']}<br>"
                else:
                    ligne_adresse = f"{row['ville']}<br>"
                popup_html = f"""
                    <b>{row['nom_event']}</b><br>
                    📅 {date_d_str}{date_f_str}<br>
                    📍 {ligne_adresse}
                """
                tooltip = row['nom_event']

            else:
                popup_html = f"<b>{len(groupe)} événements:</b><br><br>"
                for _, row in groupe.iterrows():
                    date_d_str = row['date_d'].strftime('%d/%m/%Y') if pd.notna(row['date_d']) else "?"
                    popup_html += f"🔹 <b>{row['nom_event']}</b><br>📅 {date_d_str}<br><br>"
                tooltip = f"{len(groupe)} événements ici"

            folium.Marker(
                location=row['coor'],
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=tooltip,
                icon=folium.Icon(icon="info-sign")
                ).add_to(m)

        st.subheader(f"{len(df)} evenements trouvées dans {len(st.session_state['liste_ville'])} villes")

        map_data = st_folium(m, width=700, height=500, key=f"carte_evenements_{st.session_state['carte_key']}")
        
        # ==========================================
        # SELECTION SUR LA CARTE
        lat_clic, lon_clic = None, None

        if map_data.get("last_object_clicked"):
            lat_clic = map_data["last_object_clicked"]["lat"]
            lon_clic = map_data["last_object_clicked"]["lng"]
        elif map_data.get("last_clicked"):
            lat_clic = map_data["last_clicked"]["lat"]
            lon_clic = map_data["last_clicked"]["lng"]

        indices_selectionnes = []

        if lat_clic is not None:
            distances = df.apply(
                lambda row: ((row['coor'][0] - lat_clic)**2 + (row['coor'][1] - lon_clic)**2)**0.5
                if row['coor'] is not None else float('inf'),
                axis=1
            )
            tolerance = 0.001 if map_data.get("last_object_clicked") else 0.05
            if distances.min() < tolerance:
                idx_proche = distances.idxmin()
                coor_cible = df.loc[idx_proche, "coor_key"]
                indices_selectionnes = df[df["coor_key"] == coor_cible].index.tolist()

        # ==========================================
        # VIGNETTES
        def has_value(val):
            return pd.notna(val) and str(val).strip() != ""

        def afficher_vignette(event, mise_en_avant=False):
            with st.container(border=True):
                if mise_en_avant:
                    st.markdown("⭐ **Sélectionné sur la carte**")

                if has_value(event["url_image"]):
                    st.markdown(
                        f"""<img src="{event['url_image']}" 
                        style="width:100%; height:180px; object-fit:cover; border-radius:8px;">""",
                        unsafe_allow_html=True
                    )
                else:
                    st.markdown(
                        """<div style="width:100%; height:180px; background-color:#f0f0f0; 
                        border-radius:8px; display:flex; align-items:center; justify-content:center; 
                        color:#999;">Pas d'image</div>""",
                        unsafe_allow_html=True
                    )

                st.markdown(f"**{event['nom_event']}**")
                if has_value(event["theme"]):
                    st.caption(f"🏷️ {event['theme']}")
                st.caption(f"📌 {event['adresse_complete']}")
                if has_value(event["desc"]):
                    desc = event["desc"]
                    st.write(desc[:150] + "..." if len(desc) > 150 else desc)

        if indices_selectionnes:
                    # Sécurité : garder uniquement les index encore présents
            indices_selectionnes = [
                i for i in indices_selectionnes
                if i in df.index
            ]

            if indices_selectionnes:
                df_selection = df.loc[indices_selectionnes]
                df_reste = df.drop(indices_selectionnes)

                st.subheader(f"⭐ {len(df_selection)} événement(s) à cette adresse")

            else:
                df_selection = pd.DataFrame()
                df_reste = df


            st.subheader(f"⭐ {len(df_selection)} événement(s) à cette adresse")

            n_cols = 3
            cols = st.columns(n_cols)
            for i, (idx, event) in enumerate(df_selection.iterrows()):
                with cols[i % n_cols]:
                    afficher_vignette(event, mise_en_avant=True)

            if st.button("↩️ Voir tous les événements"):
                st.session_state["carte_key"] += 1
                st.rerun()

            st.divider()
            st.subheader(f"📍 Tous les événements ({len(df_reste)})")
        else:
            df_selection = pd.DataFrame()
            df_reste = df
            #st.subheader(f"📍 {len(df_reste)} événements trouvés")
            st.subheader(f"📍 {len(df_reste)} evenements trouvées dans {len(st.session_state['liste_ville'])} villes")
        n_cols = 3
        cols = st.columns(n_cols)
        for i, (idx, event) in enumerate(df_reste.iterrows()):
            with cols[i % n_cols]:
                afficher_vignette(event, mise_en_avant=False)




    else :
        m = folium.Map(location=[st.session_state["lat"], st.session_state["lon"]], zoom_start=10)
        st_folium(m, width=700, height=500)
        st.info("Aucune ville trouvée dans le rayon.")

else :
    st.info("Recherchez une ville pour afficher la carte.")


##### RESTE A FAIRE

# Tuile de l'evenement apres selection
# image de l'evenement si elle existe
# image bateau en fonction du theme?
# tool tip a definir de maniere général