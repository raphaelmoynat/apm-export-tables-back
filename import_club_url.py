import os
from dotenv import load_dotenv
import requests
import csv
import re

load_dotenv()

#configuration
API_KEY = os.getenv("PROD_KEY")
TABLE_ID = "348959970"
CSV_FILE = 'exports/club_urls.csv'

def get_existing_hubdb_rows():
    url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{TABLE_ID}/rows"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            existing_ids = [str(row['id']) for row in response.json().get('results', [])]
            print(f"IDs existants dans HubDB: {len(existing_ids)} lignes")
            return existing_ids
        else:
            print(f"Erreur lors de la récupération des IDs existants: {response.status_code}")
            return []
    except Exception as e:
        print(f"Exception lors de la récupération des IDs: {e}")
        return []

def clean_hubspot_id(hubspot_id):
    if not hubspot_id:
        return ""
    
    cleaned_id = re.sub(r'\D', '', hubspot_id)
    
    return cleaned_id

def update_hubdb_rows():    
    #récupérer les id existants
    existing_ids = get_existing_hubdb_rows()
    
    #lire le csv et créer un dictionnaire de mapping
    updates_data = {}
    invalid_ids = []
    
    with open(CSV_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            original_id = row.get('ID HubSpot HubDB', '').strip()
            cleaned_id = clean_hubspot_id(original_id)
            nom_club = row.get('Nom du club', '').strip()
            slug_url = row.get('Slug/URL du club', '').strip()
            
            if original_id and (nom_club or slug_url):
                #vérifier si l'id existe dans hubdb
                if cleaned_id in existing_ids:
                    updates_data[cleaned_id] = {
                        'nom_club': nom_club,
                        'slug_url': slug_url,
                        'original_id': original_id
                    }
                else:
                    invalid_ids.append({
                        'original': original_id,
                        'cleaned': cleaned_id,
                        'nom_club': nom_club
                    })
    
    print(f"Trouvé {len(updates_data)} lignes valides à mettre à jour")
    print(f"Trouvé {len(invalid_ids)} IDs invalides/inexistants")
    
    if len(updates_data) == 0:
        print("Aucune mise à jour à effectuer.")
        return

    
    #mettre à jour chaque ligne 
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    success_count = 0
    error_count = 0
    
    for hubspot_id, data in updates_data.items():
        try:
            #mettre à jour une ligne spécifique
            update_url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{TABLE_ID}/rows/{hubspot_id}/draft"
            
            #préparer les données à mettre à jour avec les vrais noms de colonnes
            update_payload = {
                
                    
                    "path": data['slug_url'],
                    "name": data['nom_club']
            
            }
            
            #faire la requête de mise à jour
            response = requests.patch(update_url, json=update_payload, headers=headers)
            
            if response.status_code == 200:
                success_count += 1
                print(f"mis à jour id {hubspot_id}: {data['nom_club']}")
            else:
                error_count += 1
                print(f"erreur id {hubspot_id}: {response.status_code} - {response.text}")
                
        except Exception as e:
            error_count += 1
            print(f"exception pour id {hubspot_id}: {e}")
    
    print(f"\nRésultats:")
    print(f"- Succès: {success_count}")
    print(f"- Erreurs: {error_count}")
    
    #publier les modifications si il y a eu des succès
    if success_count > 0:
        print("\nPublication des modifications...")
        publish_url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{TABLE_ID}/draft/publish"
        pub_response = requests.post(publish_url, headers=headers)
        
        if pub_response.status_code == 200:
            print("table HubDB publiée avec succès")
        else:
            print(f"erreur lors de la publication: {pub_response.status_code}")

def main():
    print("début de la mise à jour hubdb...")
    update_hubdb_rows()
    print("process terminé")

if __name__ == "__main__":
    main()
