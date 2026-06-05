# MoveinLoc
# ===================================
#       DESCRIPTION DU PROJET
# ===================================


url_ppt = ""

url_kaban = "https://canva.link/7i45p78tpxakjoo"

url_git = "https://github.com/gatien-data/MoveinLoc.git"



# ------------ USER STORY ------------

A partir de sa localisation (ou d'un choix de localisation), 
l'utilisateur pourra visualiser sur une cartographie, les différentes activités disponibles
Pour filtrer ses résultats, il disposera de différents choix (segments/filtres discret):
 - type d'évenement (concert / activité physique / festival / culturel / détente / marchés / balades) // liste à définir
 - période (on vise aussi les vacanciers!)
 - enfant / adulte
 - rayon d'action



# ------------ OBJECTIFS / METHODOLOGIE ------------

Objectif du projet:
 /// création d'une cartographie avec géolocalisation ou choix de localisation
 /// filtre de la cartographie par types d'événéments / activités, types d'ages
 /// affichage dynamique sur cartographie via marqueurs et pop up infobulles détaillées


Méthodologie:
 - Webscrapping par types d'événements 
 - Webscrapping par region / départements / villes
 - API existante?
 - Création d'un DF le plus détaillé possible. 
    Features à définir (type d'évenement_large, type d'évenement_détail (si type dans le type (hierarchie)), 
                        exterieur/interieur, prix/gratuit, 
                        journée/soirée,
                        date unique/toute l'année
                        ... ) 
    A confirmer une fois les données collectées - Servira de features au ML pour les données manquantes
 - ML Clustering des activités (si absent du webscrapping)
 - ML Clustering des tranches d'âges (si absent du webscrapping)
 - map Folium // point zero = localisation // taille map = rayon
 - Affichage des marqueurs en fonction des filtres pour en pas surcharger la map
 - Utilisation de Streamlit pour la portabilité

Il conviendra en fonction du délai de réponse de l'applciation et du temps de mise à jour des données d'adopter le modèle le plus efficace entre:
    - actualisation hebdomadaire ( Airflow ), 
    - actualisation en temps réel sur une base réduite (après application des filtres)

    
A des fins statistiques et décisionnelles, une visualisation sur PowerBi sera mise en place.
Elle recensera à minima, 
    - le nombre d'évenement scrawler ou provenant d'API à l'instant T de l'utilisation de l'applciation,
    - une analyse des dévénements (proportions, nombres, catégories),
    - le temps de réponse de l'actualisation des données


