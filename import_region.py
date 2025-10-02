import os
import requests
from dotenv import load_dotenv
import csv
import tempfile
import json

load_dotenv()

# compte APM
API_KEY = os.getenv("PROD_KEY")
TABLE_ID = "414751957"  
CSV_FILE = "/root/apm/infocentre/apm-export-tables-back/exports/dwh.mv_region.csv"

# récupère les clés existantes dans hubdb
def get_hubdb_keys():
    url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{TABLE_ID}/rows"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            # Retourner un dictionnaire : {region_key: hs_id}
            return {str(row['values'].get('region_key', '')).strip(): row['id'] 
                   for row in response.json().get('results', [])
                   if row['values'].get('region_key')}
        return {}
    except Exception as e:
        print(f"erreur HubDB: {e}")
        return {}


# filtre le csv pour garder les nouvelles entrées
def process_csv_data(hubdb_keys):
    all_data = []
    with open(CSV_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            key_value = str(row.get('key', '')).strip()
            if key_value:
                # Ajouter l'ID HubDB si la ligne existe déjà
                if key_value in hubdb_keys:
                    row['hs_id'] = hubdb_keys[key_value]
                else:
                    row['hs_id'] = ''  # Nouvelle ligne
                all_data.append(row)
    return all_data

def prepare_import_file(data):
    if not data:
        return None
        
    temp_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False)
    
    fieldnames = [ 'hs_id',
        'key', 'reg_id', 'fk_referent', 'telreferent', 
        'nomreferent', 'emailreferent', 'prenomreferent', 'nom'
    ]
    
    writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in data:
        writer.writerow({
            'hs_id': row.get('hs_id', ''),
            'key': row['key'],
            'reg_id': row.get('reg_id', ''),
            'fk_referent': row['fk_referent'],
            'telreferent': row['telreferent'],
            'nomreferent': "",
            'emailreferent': row['emailreferent'],
            'prenomreferent': f"{row['prenomreferent']} {row['nomreferent']}".strip(),
            'nom': row['nom']
        })
    
    temp_file.seek(0)
    return temp_file

# import des données csv dans hubdb
def import_to_hubdb(temp_file):
    if not temp_file:
        return None

    import_url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{TABLE_ID}/draft/import"
    
    config = {
        "skipRows": 1,
        "separator": ",",
        "resetTable": False,
        "encoding": "utf-8",
        "format": "csv",
        "idSourceColumn": 1,
        "columnMappings": [
            {"source": 2, "target": 5},   # key → target 6
            {"source": 3, "target": 4},   # reg_id → target 8
            {"source": 9, "target": 1},   # nom → target 1
            {"source": 6, "target": 7},   # nomreferent → target 3
            {"source": 4, "target": 6},   # fk_referent → target 7
            {"source": 7, "target": 9},   # emailreferent → target 4
            {"source": 8, "target": 7},    # prenomreferent → target 9
            {"source": 5, "target": 8},
        ]
    }
    
    try:
        with open(temp_file.name, 'rb') as f:
            files = {
                'config': (None, json.dumps(config), 'application/json'),
                'file': (temp_file.name, f, 'text/csv')
            }
            
            headers = {
                "Authorization": f"Bearer {API_KEY}"
            }
            
            response = requests.post(import_url, files=files, headers=headers)
            print(f"réponse de l'API: {response.status_code} - {response.text}")
            return response
    except Exception as e:
        print(f"erreur lors de l'import: {e}")
        return None

def main():
    print("import/mise à jour filtré")
    
    # récupérer les clés existantes avec leurs IDs
    hubdb_keys = get_hubdb_keys()
    print(f"{len(hubdb_keys)} clés uniques trouvées dans HubDB")
    
    # traiter toutes les données (nouvelles + mises à jour)
    print("traitement de toutes les données...")
    all_data = process_csv_data(hubdb_keys)
    
    new_count = sum(1 for row in all_data if not row.get('hs_id'))
    update_count = sum(1 for row in all_data if row.get('hs_id'))
    
    print(f"{new_count} nouvelles lignes à importer")
    print(f"{update_count} lignes à mettre à jour")
    print(f"{len(all_data)} lignes au total à traiter")
    
    if not all_data:
        print("aucune donnée à traiter")
        return
    
    # préparer le fichier d'import
    temp_file = prepare_import_file(all_data)
    if not temp_file:
        print("erreur lors de la préparation du fichier")
        return
    
    # import/mise à jour
    print("import/mise à jour vers HubDB...")
    response = import_to_hubdb(temp_file)
    
    if response and response.status_code == 200:
        print("import/mise à jour réussi")
        # publication
        publish_url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{TABLE_ID}/draft/publish"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        pub_response = requests.post(publish_url, headers=headers)
        if pub_response.status_code == 200:
            print("table publiée")
        else:
            print(f"erreur de publication: {pub_response.status_code} - {pub_response.text}")
    else:
        print("échec de l'import/mise à jour")
    
    temp_file.close()
    print("terminé")

if __name__ == "__main__":
    main()
