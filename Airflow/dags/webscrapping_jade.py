from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import csv
import os
import pandas as pd
import time
import re
import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from collections import defaultdict
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

CSV_PATH = "/opt/airflow/data/df_jade.csv" #chemin où sera stocké notre fichier books.csv (donc dans le dossier data)

def scrape_events(): #fonction visant à scraper les evenements
    HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

    session = requests.Session()
    session.headers.update(HEADERS)

    # ============== FONCTIONS UTILITAIRES ==============

    def clean_text(text):
        if text is None:
            return None
        return re.sub(r"\s+", " ", text).strip()

    def split_lieu(lieu_brut):
        if not lieu_brut:
            return None, None
        parts = lieu_brut.split(" - ", 1)
        ville = parts[0].strip() if len(parts) > 0 else None
        adresse = parts[1].strip() if len(parts) > 1 else None
        return ville, adresse

    def parse_dates(raw):
        if not raw:
            return None, None
        raw = clean_text(raw)
        m = re.match(r"Du\s+(.+?)\s+au\s+(.+)", raw, re.IGNORECASE)
        if m:
            return m.group(1).strip(), m.group(2).strip()
        m2 = re.match(r"Le\s+(.+)", raw, re.IGNORECASE)
        if m2:
            return m2.group(1).strip(), None
        return raw, None

    def get_event_details(url, session):
        try:
            r = session.get(url, timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")
        except requests.RequestException:
            return None, None, None, None

        # Date
        date_raw = None
        for div in soup.find_all("div", class_="field-item"):
            if "Date :" in div.get_text():
                b = div.find("b")
                if b:
                    date_raw = clean_text(b.get_text(" ", strip=True))
                    break
        date_debut, date_fin = parse_dates(date_raw)

        # Website
        site_web = None
        for div in soup.find_all("div", class_="titreb"):
            if "Website" in div.get_text():
                next_div = div.find_next_sibling("div", class_="field-items")
                if next_div:
                    a = next_div.find("a")
                    if a:
                        site_web = a.get("href")
                break

        # Code postal — prend le 2ème s'il y en a deux
        cp = None
        for div in soup.find_all("div", class_="titreb"):
            if "Adresse" in div.get_text():
                next_div = div.find_next_sibling("div", class_="field-items")
                if next_div:
                    texte = next_div.get_text(" ", strip=True)
                    tous_les_cp = re.findall(r"\b\d{5}\b", texte)
                    if len(tous_les_cp) >= 2:
                        cp = tous_les_cp[1]
                    elif len(tous_les_cp) == 1:
                        cp = tous_les_cp[0]
                break

        return date_debut, date_fin, site_web, cp

    def scrape_row(row, numero):
        title_div = row.find("div", class_="views-field-title")
        if not title_div:
            return None
        a_titre = title_div.find("a")
        titre = clean_text(a_titre.get_text(" ", strip=True)) if a_titre else None
        lien_relatif = a_titre.get("href") if a_titre else None
        lien = f"https://flanerbouger.fr{lien_relatif}" if lien_relatif and lien_relatif.startswith("/") else lien_relatif

        categorie, lieu = None, None
        info_divs = row.find_all("div", class_="views-field-field-location-taxonomize-terms-location-taxonomize-longname")
        for div in info_divs:
            label_span = div.find("span", class_="field-content")
            label = label_span.get_text(strip=True) if label_span else ""
            if "Catégorie" in label:
                cat_link = div.find("span", class_="views-field-field-tags")
                if cat_link:
                    a_cat = cat_link.find("a")
                    categorie = clean_text(a_cat.get_text(" ", strip=True)) if a_cat else None
            elif "Lieu" in label:
                value_span = div.find("span", class_="views-field-field-postaladdress-postal-code")
                if value_span:
                    value_content = value_span.find("span", class_="field-content")
                    lieu = clean_text(value_content.get_text(" ", strip=True)) if value_content else None

        ville_evt, adresse_evt = split_lieu(lieu)

        date_debut, date_fin, site_web, cp = None, None, None, None
        if lien:
            date_debut, date_fin, site_web, cp = get_event_details(lien, session)

        return {
            "theme": categorie,
            "nom_event": titre,
            "adresse": adresse_evt,
            "ville": ville_evt,
            "cp": cp,
            "dep": numero,
            "date_d": date_debut,
            "date_f": date_fin,
            "site_web": site_web,
        }

    # ============== SCRAPING PRINCIPAL ==============

    all_data = []

    departements = {
        "01":"ain","02":"aisne","03":"allier","04":"alpes-de-haute-provence",
        "05":"hautes-alpes","06":"alpes-maritimes","07":"ardeche","08":"ardennes",
        "09":"ariege","10":"aube","11":"aude","12":"aveyron","13":"bouches-du-rhone",
        "14":"calvados","15":"cantal","16":"charente","17":"charente-maritime",
        "18":"cher","19":"correze","2A":"corse-du-Sud","2B":"haute-Corse",
        "21":"cote-d-or","22":"Cotes-d-armor","23":"creuse","24":"dordogne",
        "25":"doubs","26":"drome","27":"eure","28":"eure-et-loir","29":"finistere",
        "30":"gard","31":"haute-garonne","32":"gers","33":"gironde","34":"herault",
        "35":"ille-et-vilaine","36":"indre","37":"indre-et-loire","38":"isere",
        "39":"jura","40":"landes","41":"loir-et-cher","42":"loire","43":"haute-loire",
        "44":"loire-atlantique","45":"loiret","46":"lot","47":"lot-et-garonne",
        "48":"lozere","49":"maine-et-loire","50":"manche","51":"marne",
        "52":"haute-marne","53":"mayenne","54":"meurthe-et-moselle","55":"meuse",
        "56":"morbihan","57":"moselle","58":"nievre","59":"nord","60":"oise",
        "61":"orne","62":"pas-de-calais","63":"puy-de-dome","64":"Pyrenees-Atlantiques",
        "65":"hautes-pyrenees","66":"pyrenees-orientales","67":"bas-rhin","68":"haut-rhin",
        "69":"rhone","70":"haute-saone","71":"saone-et-loire","72":"Sarthe",
        "73":"savoie","74":"haute-savoie","75":"ile-de-france","76":"seine-maritime",
        "77":"seine-et-marne","78":"yvelines","79":"deux-sevres","80":"somme",
        "81":"tarn","82":"tarn-et-garonne","83":"var","84":"vaucluse","85":"vendee",
        "86":"vienne","87":"haute-vienne","88":"vosges","89":"yonne",
        "90":"territoire-de-belfort","91":"essonne","92":"hauts-de-seine",
        "93":"seine-saint-denis","94":"val-de-marne","95":"val-d-oise"
    }

    for numero, nom in departements.items():
        base_url = f"https://flanerbouger.fr/event/{numero}-event-{nom}"
        print(f"\n--- Département {numero} : {nom} ---")

        try:
            r = session.get(base_url, timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            print(f"Erreur sur {base_url} : {e}")
            continue

        page_numbers = []

        for a in soup.find_all("a", href=True):
            m = re.search(r"[?&]page=(\d+)", a["href"])
            if m:
                page_numbers.append(int(m.group(1)))
        last_page = max(page_numbers) if page_numbers else 1
        print(f"Pages détectées : {last_page}")

        for page in range(1, last_page + 1):
            url_page = base_url if page == 1 else f"{base_url}?page={page}"

            try:
                r = session.get(url_page, timeout=10)
                soup = BeautifulSoup(r.text, "html.parser")
            except Exception as e:
                print(f"Erreur page {page} : {e}")
                continue

            rows = soup.find_all("div", class_="views-row")
            if not rows:
                print(f"Page {page} vide, arrêt.")
                break

            page_data = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(scrape_row, row, numero): row for row in rows}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            page_data.append(result)
                    except Exception as e:
                        print(f"Erreur sur un événement : {e}")

            print(f"  Page {page} → {len(page_data)} événements")
            all_data.extend(page_data)
            time.sleep(0.5)

            df_flanerbouger = pd.DataFrame(all_data)

            import locale
            locale.setlocale(locale.LC_TIME, 'C')  # remet anglais

            df_flanerbouger['date_d'] = pd.to_datetime(df_flanerbouger['date_d'].astype('object'), format='%d %B %Y', errors='coerce').dt.strftime('%Y-%m-%d')
            df_flanerbouger['date_f'] = pd.to_datetime(df_flanerbouger['date_f'].astype('object'), format='%d %B %Y', errors='coerce').dt.strftime('%Y-%m-%d')
            df_flanerbouger['ville'] = df_flanerbouger['ville'].str.strip().str.rstrip('-').str.strip()
            df_flanerbouger["adresse_complete"] = df_flanerbouger["adresse"].fillna("") + " " + df_flanerbouger["cp"].fillna("") + " " + df_flanerbouger["ville"].fillna("")
            df_flanerbouger['adresse'] = df_flanerbouger['adresse'].str.replace(r'\s*\d{5}\s*-\s*.+$', '', regex=True).str.strip()
            df_flanerbouger[['url_event', 'url_image', 'age', 'prix', 'keyword', 'desc']] = np.nan
            df_flanerbouger = df_flanerbouger[['theme', 'nom_event', 'adresse', 'ville', 'cp', 'dep', 'adresse_complete', 'date_d', 'date_f', 'url_event', 'url_image', 'age', 'prix', 'keyword', 'desc']]
            df_flanerbouger["date_f"] = df_flanerbouger["date_f"].fillna(df_flanerbouger["date_d"])
            df_flanerbouger = df_flanerbouger.drop_duplicates(subset=["nom_event", "cp"], keep="first").drop_duplicates(subset=["nom_event", "cp"], keep="first")
            df_flanerbouger = df_flanerbouger.dropna(subset=["cp"])
            
            df_flanerbouger.to_csv("/opt/airflow/data/df_jade.csv",index=False,encoding="utf-8-sig")
            
            print(f"{len(df_flanerbouger)} événements enregistrés.")

def scrape_openagenda():
    MOIS = {
    "janvier": 1,
    "février": 2,
    "fevrier": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "août": 8,
    "aout": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "décembre": 12,
    "decembre": 12,}

    # Récupération des agendas
    headers = {'key':'oa_pk_IttDjfMFnvJZfuumozNLWwTGAorNMAlHxNItNOAVEJgJwyKsYwMBZIgWxhpxDsjn'}

    liste_data_temp = []
    after = 0
    for i in range(1,10000,100):
        url = f"https://api.openagenda.com/v2/agendas?size=100&official=1&after[]=1&after[]={after}"
        response = requests.get(url, headers=headers)
        api_response = response.json()
        data_temp = pd.json_normalize(api_response, record_path='agendas')
        liste_data_temp.append(data_temp)
        after = api_response['after'][1]

    # Création du df_agenda
    df_Agenda = pd.concat(liste_data_temp)
    df_Agenda = df_Agenda.drop_duplicates(subset=['uid'])

    # Récupération des événements
    list_id_evenement = df_Agenda['uid'].to_list()
    liste_data_temp = []

    for uid in list_id_evenement:
        after = 0
        url = f"https://api.openagenda.com/v2/agendas/{uid}/events?relative[]=current&relative[]=upcoming"
        response = requests.get(url, headers=headers)
        api_response = response.json()
        data_temp = pd.json_normalize(api_response, record_path='events')
        liste_data_temp.append(data_temp)

    # Création du df evenements
    df_OA_brut = pd.concat(liste_data_temp)

        # outil de test valeurs manquantes colonne -----  cible < 5% + mots clé
    df_OA = df_OA_brut.copy()
    colonne = []
    for col in df_OA.columns:
        # ratio = pourcentage de valeurs manquantes
        ratio = df_OA[col].isna().sum() / len(df_OA)
        x = re.search(r"keywords.fr", col)
        if ratio < 0.05:
            colonne.append(col)
        elif x != None:
            colonne.append(col)

    df_OA = df_OA[colonne]
    df_OA = df_OA.drop_duplicates(subset=['uid'])

        # FONCTION DE NETTOYAGE
    def parse_date(txt):
        txt = txt.lower()
        m = re.search(r'(\d{1,2})\s+([a-zàâäéèêëîïôöùûüç]+)', txt, flags=re.IGNORECASE)
        if not m:
            return None
        jour = int(m.group(1))
        mois = MOIS[m.group(2)]
        aujourd = date.today()
        annee = aujourd.year
        d = date(annee, mois, jour)
        # si déjà passée -> année suivante
        if d < aujourd:
            d = date(annee + 1, mois, jour)
        return d

    def extract_dates(s):
        s = re.sub(r"\d{1,2}h\d{0,2}", "", s.lower())
        s = re.sub(r"lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche", "", s)
        s = s.replace("undefined", "").strip()

        # Cas : "9 et 10 juillet"
        m = re.search(r'(\d{1,2})\s+et\s+(\d{1,2})\s+([a-zàâäéèêëîïôöùûüç]+)', s, flags=re.IGNORECASE)   

        if m:
            mois = m.group(3)
            morceaux = [
                f"{m.group(1)} {mois}",
                f"{m.group(2)} {mois}"
            ]
        else:
            morceaux = re.findall(r"\d{1,2}\s+[a-zéûôîà]+", s)

        if len(morceaux) == 1:
            d = parse_date(morceaux[0])
            return pd.Series([d, d])

        if len(morceaux) >= 2:
            d1 = parse_date(morceaux[0])
            d2 = parse_date(morceaux[1])

            # si le range traverse le nouvel an
            if d2 < d1:
                d2 = date(d2.year + 1, d2.month, d2.day)

            return pd.Series([d1, d2])

        return pd.Series([pd.NaT, pd.NaT])

    def adresse(adresse):
        # extraction de l'adresse sans CP et VIlle
        try:
            x = re.search(r"^(.*?)\s+\d{5}\b", adresse)
            if x == None:
                y = re.split(r"^(.*?)\s+\d{5}\b", adresse)
                return y[0]
            else:
                return x.group(1)
        except:   
            pass

    def normaliser(texte):
        """Remplace tirets/espaces multiples par un seul espace, en minuscule"""
        if pd.isna(texte):
            return texte
        texte = texte.lower()
        texte = re.sub(r'[-\s]+', ' ', texte)  # tirets ET espaces -> un espace
        return texte.strip()

    def supprimer_ville(row):
        adresse = row['adresse']
        ville = row['location.city']
        if pd.isna(adresse) or pd.isna(ville) or ville == '':
            return adresse
        ville_norm = normaliser(ville)
        mots = ville_norm.split(' ')
        pattern = r'[\s-]+'.join(re.escape(m) for m in mots)
        adresse_nettoyee = re.sub(pattern, '', adresse, flags=re.IGNORECASE)
        adresse_nettoyee = re.sub(r'\s{2,}', ' ', adresse_nettoyee)
        adresse_nettoyee = adresse_nettoyee.strip(' ,')
        return adresse_nettoyee


    # Date
    df_OA[["date_d", "date_f"]] = df_OA["dateRange.fr"].apply(extract_dates)

    # Images
    df_OA["url_image"] = df_OA["image.base"] + df_OA["image.filename"]


    # Adresse, CP, dep, Ville 
    df_OA = df_OA.dropna(subset=["location.address"])
    df_OA["adresse"] = df_OA["location.address"].apply(adresse)
    df_OA["cp"] = df_OA["location.address"].str.extract(r"\b(\d{5})\b")
    ville_nan = df_OA["location.city"].isna()
    df_OA.loc[ville_nan, "location.city"] = (df_OA.loc[ville_nan, "location.city"].str.extract(r"\b\d{5}\s+(.+)$"))

    df_OA['adresse'] = df_OA.apply(supprimer_ville, axis=1)
    df_OA["cp"] = pd.to_numeric(df_OA["cp"], errors="coerce").astype("Int64")
    df_OA["cp"] = df_OA["cp"].astype(str)
    df_OA["adresse_complete"] = df_OA["adresse"].fillna("") + " " + df_OA["cp"].fillna("") + " " + df_OA["location.city"].fillna("")


    # DROP
    df_OA = df_OA.drop(columns=[
        'uid', 'featured', 'slug', 'status',  'attendanceMode', 'timezone', 'image.size.width', 'image.size.height', 'location.address', 
        'image.variants', 'image.filename', 'image.base', 'originAgenda.uid', 'originAgenda.title', 'dateRange.ar', 'dateRange.de',
        'dateRange.en', 'dateRange.it', 'dateRange.es', 'dateRange.nl', 'dateRange.fr', 'lastTiming.end', 'lastTiming.begin',
        'firstTiming.end', 'firstTiming.begin', 'nextTiming.begin', 'nextTiming.end', 'location.latitude', 'location.longitude'])
    df_OA = df_OA.dropna(subset=["title.fr"])
    df_OA.reset_index(inplace=True, drop=True)

    # --------------------------------------------------------------
    # Ajout, Rename et Ordonnancement des colonnes des DF Temporaire
    #               >>>>> A ADAPTER ! <<<<<
    # --------------------------------------------------------------
    df_OA[["theme", "dep", "url_event", "age", "prix"]] = np.nan

    rename = {"theme":'theme', "title.fr":'nom_event', "adresse":'adresse', "location.city":'ville', "cp":'cp', "dep":'dep', "adresse_complete":'adresse_complete',
            "date_d" :'date_d', "date_f":'date_f', "url_event":'url_event', "url_image":'url_image', "age":'age', "prix":'prix', "keywords.fr":'keyword', "description.fr":'desc'}

    df_OA = df_OA.rename(columns=rename)
    df_OA = df_OA[['theme', 'nom_event', 'adresse', 'ville', 'cp', 'dep', 'adresse_complete', 'date_d', 'date_f', 'url_event', 'url_image', 'age', 'prix', 'keyword', 'desc']]

    # Création du DF vide selon notre norme
    df = pd.DataFrame(columns=['theme', 'nom_event', 'adresse', 'ville', 'cp', 'dep', 'adresse_complete', 'date_d', 'date_f', 'url_event', 'url_image', 'age', 'prix', 'keyword', 'desc'])

    # Concat 
    df = pd.concat([df, df_OA])
    df.to_csv("/opt/airflow/data/df_gatien.csv",index=False,encoding="utf-8-sig")

def api_tourisme():
    data_temporaire = []
    departements = {"01":"ain", "02":"aisne", "03":"allier", "04":"alpes de haute provence", "05":"hautes alpes", "06":"alpes maritimes", "07":"ardèche", "08":"ardennes", "09":"ariège", "10":"aube", "11":"aude", "12":"aveyron", "13":"bouches du rhône", "14":"calvados", "15":"cantal", "16":"charente", "17":"charente maritime", "18":"cher", "19":"corrèze", "2A":"corse du Sud", "2B":"haute Corse", "21":"cote d or", "22":"Cotes d armor", "23":"creuse", "24":"dordogne", "25":"doubs", "26":"drôme", "27":"eure", "28":"eure et loir", "29":"finistère", "30":"gard", "31":"haute-garonne", "32":"gers", "33":"gironde", "34":"herault", "35":"ille et vilaine", "36":"indre", "37":"indre et loire", "38":"isère", "39":"jura", "40":"landes", "41":"loir et cher", "42":"loire", "43" : "haute-loire", "44":"loire atlantique","45": "loiret", "46":"lot", "47":"lot et garonne", "48" : "lozère", "49":"maine et loire","50":"manche","51":"marne","52":"haute marne","53":"mayenne","54":"meurthe et moselle","55":"meuse","56":"morbihan","57":"moselle","58":"nièvre","59":"nord","60":"oise","61":"orne","62":"pas de calais","63":"puy de dôme", "64" : "Pyrénées-Atlantiques", "65":"hautes pyrénées","66":"pyrénées orientales","67":"bas-rhin","68":"haut-rhin","69":"rhône","70":"haute saône","71":"saône et loire", "72" : "Sarthe", "73":"savoie","74":"haute savoie","75":"ile de france","76":"seine maritime","77":"seine et marne","78":"yvelines",
    "79":"deux sèvres",
    "80":"somme",
    "81":"tarn",
    "82":"tarn et garonne",
    "83":"var",
    "84":"vaucluse",
    "85":"vendée",
    "86":"vienne",
    "87":"haute vienne",
    "88":"vosges",
    "89":"yonne",
    "90":"territoire de belfort",
    "91":"essonne",
    "92":"hauts de seine",
    "93":"seine saint denis",
    "94":"val de marne",
    "95":"val d oise"}
    # ETAPE 1 : JE DEFINIS MA VARIABLE URL
    for i in departements.values():
        url = f"https://france-evasion-regions.com/api/open/evenements.json?dept={i}&limit=500"
        API_KEY = "mon_token"
        headers = {'X-API-KEY': API_KEY,}

        # ETAPE 2 : J'EFFECTUE MA REQUETE
        response = requests.get(url)

        # ETAPE 3 : CONVERSION DE LA REPONSE HTTP JSON EN OBJET PYTHON 
        api_reponse = response.json()
        df = pd.json_normalize(api_reponse, record_path=['evenements'])
        data_temporaire.append(df)


    df_final = pd.concat(data_temporaire, ignore_index=True)

    # Renommage des colonnes existantes
    df_final = df_final.rename(columns={
        "categorie": "theme",
        "nom": "nom_event",
        "date_debut": "date_d",
        "date_fin": "date_f",
        "url": "url_event",
        "photo": "url_image",
        "description": "desc",
        "departement.nom": "dep"
    })

    # Colonnes finales souhaitées
    colonnes_finales = [
        'theme', 'nom_event', 'adresse', 'ville', 'cp', 'dep',
        'date_d', 'date_f', 'url_event', 'url_image',
        'age', 'prix', 'keyword', 'desc'
    ]

    # Ajouter les colonnes manquantes
    for col in colonnes_finales:
        if col not in df_final.columns:
            df_final[col] = None

    # Réordonner
    df_final = df_final[colonnes_finales]

    # Inversion du dictionnaire
    dep_to_code = {v.lower(): k for k, v in departements.items()}

    # Remplacer les noms des départements par le numéro de département
    df_final["dep"] = (
        df_final["dep"]
        .str.lower()
        .map(dep_to_code)
    )

    ''' df_final'''
    df_final["adresse"] = df_final["adresse"].fillna(" ")

    ''' df_final '''
    df_final["adresse complete"] = df_final["adresse"] + " " + df_final["ville"]

    '''df_final / Réorganiser les colonnes'''
    col = df_final.pop("adresse complete")  # Retire la colonne et la retourne

    # Insère la colonne juste après "dep"
    df_final.insert(df_final.columns.get_loc("dep") + 1, "adresse complete", col)


    df_final.reset_index(drop=True, inplace=True)

    df_final = df_final.rename(columns={
        "adresse complete": "adresse_complete"
    })

    '''FINAL'''
    
    df_final.to_csv("/opt/airflow/data/df_tourisme.csv",index=False,encoding="utf-8-sig")

def api_loire():
    departements = {"01":"ain", "02":"aisne", "03":"allier", "04":"alpes de haute provence", "05":"hautes alpes", "06":"alpes maritimes", "07":"ardèche", "08":"ardennes", "09":"ariège", "10":"aube", "11":"aude", "12":"aveyron", "13":"bouches du rhône", "14":"calvados", "15":"cantal", "16":"charente", "17":"charente maritime", "18":"cher", "19":"corrèze", "2A":"corse du Sud", "2B":"haute Corse", "21":"cote d or", "22":"Cotes d armor", "23":"creuse", "24":"dordogne", "25":"doubs", "26":"drôme", "27":"eure", "28":"eure et loir", "29":"finistère", "30":"gard", "31":"haute-garonne", "32":"gers", "33":"gironde", "34":"herault", "35":"ille et vilaine", "36":"indre", "37":"indre et loire", "38":"isère", "39":"jura", "40":"landes", "41":"loir et cher", "42":"loire", "43" : "haute-loire", "44":"loire-atlantique","45": "loiret", "46":"lot", "47":"lot et garonne", "48" : "lozère", "49":"maine-et-loire","50":"manche","51":"marne","52":"haute marne","53":"mayenne","54":"meurthe et moselle","55":"meuse","56":"morbihan","57":"moselle","58":"nièvre","59":"nord","60":"oise","61":"orne","62":"pas de calais","63":"puy de dôme", "64" : "Pyrénées-Atlantiques", "65":"hautes pyrénées","66":"pyrénées orientales","67":"bas-rhin","68":"haut-rhin","69":"rhône","70":"haute saône","71":"saône et loire", "72" : "Sarthe", "73":"savoie","74":"haute savoie","75":"ile de france","76":"seine maritime","77":"seine et marne","78":"yvelines",
    "79":"deux sèvres",
    "80":"somme",
    "81":"tarn",
    "82":"tarn et garonne",
    "83":"var",
    "84":"vaucluse",
    "85":"vendée",
    "86":"vienne",
    "87":"haute vienne",
    "88":"vosges",
    "89":"yonne",
    "90":"territoire de belfort",
    "91":"essonne",
    "92":"hauts de seine",
    "93":"seine saint denis",
    "94":"val de marne",
    "95":"val d oise"}

    # ETAPE 1 : JE DEFINIS MA VARIABLE URL
    url = "https://data.paysdelaloire.fr/api/explore/v2.1/catalog/datasets/234400034_070-002_offre-touristique-fetes_et_manifestations-rpdl/records?limit=100"
        #API_KEY = "mon_token"
        #A changer# headers = {'X-API-KEY': API_KEY,}

        # ETAPE 2 : J'EFFECTUE MA REQUETE
    response = requests.get(url)

        # ETAPE 3 : CONVERSION DE LA REPONSE HTTP JSON EN OBJET PYTHON 
    api_reponse = response.json()
    df_loire = pd.json_normalize(api_reponse, record_path=['results'])


    '''Extraire date_d et date_f'''
    def extract_dates(s):
        if pd.isna(s):
            return pd.Series([pd.NaT, pd.NaT])

        starts = []
        ends = []

        # Plusieurs périodes séparées par des virgules
        for period in str(s).split(","):
            parts = period.split("||")
            if len(parts) >= 2:
                try:
                    starts.append(pd.to_datetime(parts[0], format="%d/%m/%Y"))
                    ends.append(pd.to_datetime(parts[1], format="%d/%m/%Y"))
                except ValueError:
                    pass

        if starts:
            return pd.Series([min(starts), max(ends)])

        return pd.Series([pd.NaT, pd.NaT])

    df_loire[["date_d", "date_f"]] = df_loire["ouverturegranule"].apply(extract_dates)

    '''Mettre les dep en chiffre'''
    # Inversion du dictionnaire
    dep_to_code = {v.lower(): k for k, v in departements.items()}

    # Remplacer les noms des départements par le numéro de département
    df_loire["dep"] = (
        df_loire["departement"]
        .str.lower()
        .map(dep_to_code)
    )

    '''Pour adressse'''
    df_loire["adresse1"] = df_loire["adresse1"].fillna(" ")
    df_loire["adresse1suite"] = df_loire["adresse1suite"].fillna(" ")
    df_loire["adresse2"] = df_loire["adresse2"].fillna(" ")
    df_loire["adresse3"] = df_loire["adresse3"].fillna(" ")

    '''Pour adresse'''
    df_loire["adresse"] = df_loire["adresse1"] + " " + df_loire["adresse1suite"] + " " + df_loire["adresse2"] +" " + df_loire["adresse3"]

    '''Pour adresse'''
    df_loire["adresse_complete"] = df_loire["adresse"] + " " + df_loire["commune"]

    '''Renommmer les colonnes et mettre les colonnes manquantes'''
    # Renommer les colonnes existantes
    df_loire = df_loire.rename(columns={
        "categorie": "theme",
        "nomoffre": "nom_event",
        "commune": "ville",
        "codepostal": "cp"
    })



    # Ajouter les colonnes manquantes si elles n'existent pas
    colonnes_manquantes = [
        "date_d", "date_f", "url_event", "url_image",
        "age", "prix", "keyword", "desc"
    ]

    for col in colonnes_manquantes:
        if col not in df_loire.columns:
            df_loire[col] = pd.NA

    # Ordre final des colonnes
    ordre = [
        "theme",
        "nom_event",
        "adresse",
        "ville",
        "cp",
        "dep",
        "adresse_complete",
        "date_d",
        "date_f",
        "url_event",
        "url_image",
        "age",
        "prix",
        "keyword",
        "desc",
        
    ]

    df_loire = df_loire[ordre]

    '''Loire'''
    df_loire.to_csv("loire_clean.csv", index=False)
    df_loire.to_csv("/opt/airflow/data/df_loire.csv",index=False,encoding="utf-8-sig")

def concat_df():
    df_loire = pd.read_csv("/opt/airflow/data/df_loire.csv")
    df_tourisme = pd.read_csv("/opt/airflow/data/df_tourisme.csv")
    df_jade = pd.read_csv("/opt/airflow/data/df_jade.csv")
    df_gatien = pd.read_csv("/opt/airflow/data/df_gatien.csv")

    df = pd.concat(
        [df_loire, df_tourisme, df_jade, df_gatien],
        ignore_index=True
    )
    df = df.drop_duplicates(subset=["nom_event"])
    df.to_csv("/opt/airflow/data/events.csv", index=False)

def recherche_theme():
    df = pd.read_csv("/opt/airflow/data/events.csv")
    themes_keywords = {'Fête nationale': [
        "14 juillet", "13 juillet", "feu d'artifice", "feux d'artifice",
        "retraite aux flambeaux", "fête nationale", "bal des pompiers",
        "bal populaire", "cérémonie patriotique"
    ],

    'Fête patronale': [
        "fête patronale", "fête votive", "fête locale", "ducasse",
        "fête du village", "saint", "sainte", "comice"
    ],

    'Marché de Noël': [
        'marché de noël', 'marché noël', 'noel', 'noël'
    ],

    'Marché': [
        'marché', 'marché des producteurs', 'marché producteur',
        'marché hebdomadaire', 'bio', 'producteurs', 'producteur',
        'hebdomadaire', 'legumes', 'fermier', 'artisanat', 'vente directe à la ferme',
        'braderie', 'floralies'
    ],

    'Brocante / Vide-grenier': [
        'brocante', 'vide-grenier', 'vide grenier', 'vide-greniers',
        'vide greniers', 'marché puces', 'puces',
        'bourse aux jouets', 'bourse aux vêtements'
    ],

    'Concert': [
        'concert', 'live', 'groupe', 'chorale', 'chorales',
        'fanfare', 'fanfarons', 'jazz', 'rock', 'pop',
        'dj', 'gospel', 'musique', 'musical', 'orchestre',
        'lyrique', 'bridgerton', 'bertrand belin',
        'showcase', 'tribute', 'chanson', 'chansons', 'guinguette'
    ],

    'Festival': [
        'festival', 'festi', 'festimusic', 'meeting'
    ],

    'Sport': [
        'sport', 'tournoi', 'course', 'match',
        'cyclo', 'vélo', 'velo', 'vtt',
        'trail', 'ultra trail', 'randonnée',
        'rando', 'géorando',
        'triathlon', 'tennis',
        'pétanque', 'petanque',
        'pêche', 'peche',
        'auto cross', 'cross',
        'challenge',
        'grand prix',
        'gravity race', "olympiades",
        'joutes', 'joutes nautiques', 'escalade','tour de france'
    ],
    'Visite': [
        'visite', 'visite guidée',
        'balade', 'promenade',
        'sortie nature',
        'route des',
        'nuit des églises',
        'journées du patrimoine',
        'journées européennes du patrimoine',
        'médiéval', 'médiévale', 'médiévales'
    ],
    'Exposition': [
        'exposition', 'vernissage',
        'galerie',
        'minéraux', 'mineraux',
        'fossiles',
        'patrimoine',
        'sculpture',
        'photo', 'photographie','Préhistorama'
    ],

   'Atelier': [
        'atelier',
        'initiation',
        'stage',
        'workshop',
        'sculpture',
        'aquarelle', 'dessin', 'peinture'
    ],

    'Spectacle': [
        'spectacle',
        'théâtre', 'theatre',
        'comédie',
        'projection',
        'ciné', 'cinema', 'cinéma',
        'escape game',
        'jeu de piste',
        'cirque', 'humour', 'one man show', 'stand up'
    ],

    'Enfants': [
        'enfants',
        'famille',
        'jeux',
        'jeu',
        'chasse au trésor',
        'espace jeunes',
        'lecture', 'colonie', "colo'"
    ],

    'Atelier': [
        'atelier',
        'initiation',
        'stage',
        'workshop',
        'sculpture',
        'aquarelle', 'dessin', 'peinture'
    ],

    'Foire': [
        'foire',
        'salon',
        'forum',
        'portes ouvertes'
    ],

    'Fête': [
        'fête', 'fete',
        'soirée', 'soiree',
        'bal',
        'boom',
        'halloween',
        'beaujolais',
        'after',
        'loto',
        'réunion',
        'rencontre',
        'dansant', 'dansante', 'gala', 'karaoké', 'karaoke', 'apéro', 'apero'
    ],

    'Dégustation': [
        'dégustation', 'degustation',
        'paëlla', 'paella',
        'huîtres', 'huitres',
        'boudin',
        'saucisson',
        'choucroute',
        'marron',
        
        'vente à emporter',
        'foodtruck', 'food truck', 'repas'
    ],

    'Conférence / Débat': [
        'conférence', 'conference', 'débat', 'debat', 'table ronde',
        'rencontre-débat', 'colloque'
    ],

    'Solidarité': [
        'don du sang', 'collecte de sang', 'don de sang','depistage'
    ]}



    def detecter_theme(texte):

        texte = str(texte).lower()

        scores = defaultdict(int)

        for theme, mots_cles in themes_keywords.items():

            for mot in mots_cles:

                if mot in texte:
                    scores[theme] += 1

        if not scores:
            return np.nan

        meilleur_theme = max(scores, key=scores.get)

        return meilleur_theme
    df['texte_complet'] = (df['nom_event'].fillna('') + ' ' + df['desc'].fillna('') + ' ' + df['keyword'].fillna('')).str.lower()
    df['theme'] = df['texte_complet'].apply(detecter_theme)

    df.to_csv("/opt/airflow/data/df_final.csv",index=False,encoding="utf-8-sig")

with DAG(dag_id="scrape_events_jade", start_date=datetime(2025, 1, 1), schedule_interval="@weekly",catchup=False) as dag:

    t1 = PythonOperator(   # création de la tâche scrape_books_task qui exécute la fonction Python scrape_books
        task_id="scrape_events",
        python_callable=scrape_events
    )
    
    t2 = PythonOperator(   # création de la tâche scrape_books_task qui exécute la fonction Python scrape_books
            task_id="scrape_openagenda",
            python_callable=scrape_openagenda
        )
    t3 = PythonOperator(
            task_id="api_tourisme",
            python_callable=api_tourisme
    
    )
    t4 = PythonOperator(
            task_id = "api_loire",
            python_callable=api_loire
    )

    t5 = PythonOperator(
            task_id="concat_df",
            python_callable=concat_df
    )

    t6 = PythonOperator(
            task_id="recherche_theme",
            python_callable=recherche_theme
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
    task_id="upload_to_gcs",
    src="/opt/airflow/data/df_final.csv",
    dst="df_final.csv",                 
    bucket="projet3-events",
    gcp_conn_id="google_cloud_default",      
    mime_type="text/csv",
    )
[t1,t2,t3,t4] >> t5 >> t6 >> upload_to_gcs