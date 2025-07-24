import os
import requests
import csv
import tempfile
import json

# compte APM
API_KEY = os.getenv("PROD_KEY")
TABLE_ID = "573542642"  
CSV_FILE = "./exports/dwh.mv_region.csv"

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
            return {str(row['values'].get('region_key', '')).strip() 
                   for row in response.json().get('results', [])}
        return set()
    except Exception as e:
        print(f"erreur HubDB: {e}")
        return set()

# filtre le csv pour garder les nouvelles entrées
def filter_csv(hubdb_keys):
    new_data = []
    with open(CSV_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            key_value = str(row.get('key', '')).strip()
            if key_value and key_value not in hubdb_keys:
                new_data.append(row)
    return new_data

def prepare_import_file(data):
    if not data:
        return None
        
    temp_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False)
    
    fieldnames = [
        'key', 'reg_id', 'fk_referent', 'telreferent', 
        'nomreferent', 'emailreferent', 'prenomreferent', 'nom'
    ]
    
    writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in data:
        writer.writerow({
            'key': row['key'],
            'reg_id': row.get('reg_id', ''),
            'fk_referent': row['fk_referent'],
            'telreferent': row['telreferent'],
            'nomreferent': f"{row['prenomreferent']} {row['nomreferent']}".strip(),
            'emailreferent': row['emailreferent'],
            'prenomreferent': row['prenomreferent'],
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
        "columnMappings": [
            {"source": 1, "target": 6},
            {"source": 2, "target": 8},
            {"source": 8, "target": 1},
            {"source": 5, "target": 3},
            {"source": 3, "target": 7},
            {"source": 6, "target": 4},
            {"source": 7, "target": 9}
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
    print("import filtré")
    
    # récupérer les clés existantes
    hubdb_keys = get_hubdb_keys()
    print(f"{len(hubdb_keys)} clés uniques trouvées")
    
    # filtrer le csv
    print("filtrage des doublons...")
    new_data = filter_csv(hubdb_keys)
    print(f"{len(new_data)} nouvelles lignes à importer")
    
    if not new_data:
        print("fin")
        return
    
    # préparer le fichier d'import
    temp_file = prepare_import_file(new_data)
    if not temp_file:
        print("erreur lors de la préparation du fichier")
        return
    
    # import
    print("import vers HubDB...")
    response = import_to_hubdb(temp_file)
    
    if response and response.status_code == 200:
        print("import réussi")
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
        print("échec de l'import")
    
    temp_file.close()

if __name__ == "__main__":
    main()
