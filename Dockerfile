#On part d'une image Python officielle et légère
FROM python:3.10-slim

#On définit le dossier de travail à l'intérieur du conteneur
WORKDIR /app

#On installe les outils système nécessaires (souvent requis pour Folium/Geopandas ou le scraping)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

#On copie le fichier des dépendances dans le conteneur
COPY requirements.txt .

#On installe les packages Python
RUN pip install --no-cache-dir -r requirements.txt

#On copie tout le reste du code de ton projet dans le conteneur
COPY . .

#On indique à Docker que l'application écoute sur le port 8501 (port par défaut de Streamlit)
EXPOSE 8501

#La commande qui se lance automatiquement au démarrage du conteneur
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]