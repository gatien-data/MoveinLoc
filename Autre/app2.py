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

st.title("Autour de vous ...")

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



############################
#        USER LOCATION
############################
#location = streamlit_geolocation()
#if location and location["latitude"] is not None:
#    lat = location["latitude"]
#    lon = location["longitude"]
#else:
#    st.info("Autorisez l'accès à votre localisation.")



############################
#           DATE
############################

start_date = date.today()
end_date = start_date + timedelta(weeks=25)



##### BARRE DE RECHERCHE MANUELLE
with st.expander("⚙️ Filtres", expanded=not st.session_state["recherche_faite"]):

    col1, col2, col3, col4 = st.columns([2.5, 2, 1.2, 1.2], vertical_alignment="bottom")

    with col1:
        search_ville = st.text_input(
            "Ville ou CP",
            placeholder="Ex : 13100 ou Aix-en-Provence",
            label_visibility="collapsed"
        )

    with col2:
        selected_date_range = st.slider(
            "Période",
            min_value=start_date,
            max_value=end_date,
            value=(start_date, end_date),
            label_visibility="collapsed"
        )

    with col3:
        rayon = st.slider(
            "Rayon",
            min_value=5,
            max_value=100,
            value=st.session_state["rayon"],
            step=5,
            label_visibility="collapsed"
        )

    with col4:
        rechercher = st.button(
            "🔍 Rechercher",
            use_container_width=True
        )

    st.session_state["rayon"] = rayon

    if rechercher:
        if search_ville.strip():
            # Géocodage barre de recherche
            geo_url = f'https://data.geopf.fr/geocodage/search/?q={search_ville}'
            r = requests.get(geo_url).json()
            coor = r['features'][0]['geometry']['coordinates'][::-1]
            if coor:
                st.session_state['lat'] = float(coor[0])
                st.session_state['lon'] = float(coor[1])

                # USER CP
                user_loc = [st.session_state["lat"], st.session_state["lon"]]
                url = f"https://data.geopf.fr/geocodage/reverse?lon={user_loc[1]}&lat={user_loc[0]}&limit=1"
                response = requests.get(url).json()
                cp_temp = pd.json_normalize(response, record_path='features')
                
                # USER CIRCLE
                cp = cp_temp["properties.postcode"].iloc[0]
                st.session_state["cp"] = cp      
                   
            else:
                st.error("Aucun résultat trouvé. Essayez un autre terme.")
        else:
            st.warning("Veuillez entrer une ville ou un code postal.")

#CALCUL DES VILLES DANS LE RAYON
if st.session_state["cp"] is not None:
    st.session_state["liste_ville"] = villes_voisines(
                st.session_state["cp"],
                st.session_state["rayon"])

############################
#           CARTE
############################

if st.session_state["lat"] is not None:

    if st.session_state['liste_ville']:
        st.subheader(f"{len(st.session_state['liste_ville'])} villes trouvées")
        
        df = pd.read_csv("df_OpenAgenda.csv")

        # filtrage par ville
        df["ville"] = df["ville"].apply(lambda x: str(str(x).lower().capitalize()))
        df = df[df['ville'].isin(st.session_state['liste_ville'])]

        # gestion des dates
        choix_debut = pd.Timestamp(selected_date_range[0])
        choix_fin = pd.Timestamp(selected_date_range[1])
        df["date_d"] = pd.to_datetime(df["date_d"])
        df["date_f"] = pd.to_datetime(df["date_f"])
        df = df[(df["date_d"] <= choix_fin) & (df["date_f"] >= choix_debut)]

        # géocodage
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

####################################################
     #  GROUPEMENT PAR ADRESSE (pour markers multi-événements)
####################################################
        df["coor_key"] = df["coor"].apply(lambda x: tuple(x) if x is not None else None)
        groupes = df[df["coor_key"].notna()].groupby("coor_key")

        m = folium.Map(location=[st.session_state["lat"], st.session_state["lon"]], zoom_start=10)

        for coor, groupe in groupes:
            if len(groupe) == 1:
                row = groupe.iloc[0]
                date_d_str = row['date_d'].strftime('%d/%m/%Y') if pd.notna(row['date_d']) else "?"
                date_f_str = f" → {row['date_f'].strftime('%d/%m/%Y')}" if pd.notna(row['date_f']) else ""
                popup_html = f"""
                    <b>{row['nom_event']}</b><br>
                    📅 {date_d_str}{date_f_str}<br>
                    📍 {row['adresse']}, {row['ville']}<br>
                """
                tooltip = row['nom_event']
            else:
                popup_html = f"<b>{len(groupe)} événements à cette adresse :</b><br><br>"
                for _, row in groupe.iterrows():
                    date_d_str = row['date_d'].strftime('%d/%m/%Y') if pd.notna(row['date_d']) else "?"
                    popup_html += f"🔹 <b>{row['nom_event']}</b><br>📅 {date_d_str}<br><br>"
                tooltip = f"{len(groupe)} événements ici"

            folium.Marker(
                location=list(coor),
                popup=folium.Popup(popup_html, max_width=280),
                tooltip=tooltip,
                icon=folium.Icon(icon="info-sign")
            ).add_to(m)

        st.subheader(f"{len(df)} evenements trouvées")

        # --- key dynamique pour permettre le reset via bouton ---
        if "carte_key" not in st.session_state:
            st.session_state["carte_key"] = 0

        st_data = st_folium(
            m, width=700, height=500,
            key=f"carte_evenements_{st.session_state['carte_key']}"
        )

####################################################
        #  DÉTECTION DU CLIC
####################################################
        lat_clic, lon_clic = None, None

        if st_data.get("last_object_clicked"):
            lat_clic = st_data["last_object_clicked"]["lat"]
            lon_clic = st_data["last_object_clicked"]["lng"]
        elif st_data.get("last_clicked"):
            lat_clic = st_data["last_clicked"]["lat"]
            lon_clic = st_data["last_clicked"]["lng"]

        indices_selectionnes = []

        if lat_clic is not None:
            distances = df.apply(
                lambda row: ((row['coor'][0] - lat_clic)**2 + (row['coor'][1] - lon_clic)**2)**0.5
                if row['coor'] is not None else float('inf'),
                axis=1
            )
            tolerance = 0.001 if st_data.get("last_object_clicked") else 0.05
            if distances.min() < tolerance:
                idx_proche = distances.idxmin()
                coor_cible = df.loc[idx_proche, "coor_key"]
                indices_selectionnes = df[df["coor_key"] == coor_cible].index.tolist()

####################################################
        #       VIGNETTES
####################################################
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
            df_selection = df.loc[indices_selectionnes]
            df_reste = df.drop(indices_selectionnes)

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
            df_reste = df
            st.subheader(f"📍 {len(df_reste)} événements trouvés")

        n_cols = 3
        cols = st.columns(n_cols)
        for i, (idx, event) in enumerate(df_reste.iterrows()):
            with cols[i % n_cols]:
                afficher_vignette(event, mise_en_avant=False)

    else:
        m = folium.Map(location=[st.session_state["lat"], st.session_state["lon"]], zoom_start=10)
        st_folium(m, width=700, height=500)
        st.info("Aucune ville trouvée dans le rayon.")

else:
    st.info("Recherchez une ville pour afficher la carte.")

# ajout d'un slider rayon
# Tuile de l'evenement apres selection
# image de l'evenement si elle existe
# image bateau en fonction du theme?
# tool tip a definir de maniere général