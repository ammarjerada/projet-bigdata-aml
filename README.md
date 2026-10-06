# Plateforme anti-blanchiment Bitcoin — Projet Big Data ENSIASD

## Lancer l'infrastructure
1. Installer Docker Desktop (WSL 2) et régler WSL à 10 Go (fichier .wslconfig)
2. Mettre les CSV Elliptic dans le dossier data/
3. docker compose up -d
4. Kibana : http://localhost:5601 — Elasticsearch : http://localhost:9200
5. Recréer l'index : exécuter elasticsearch/requetes_manipulation.txt dans Kibana Dev Tools