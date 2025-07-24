import os
from dotenv import load_dotenv
import pandas as pd
import requests
import csv
import tempfile
import json
from datetime import datetime
from country_converter import CountryConverter 

load_dotenv()

# config
API_KEY =  os.getenv("PROD_KEY")
TABLE_ID = "348959970"  
CSV_FILE = "./exports/dwh.mv_club.csv"  

def convert_date_format(date_string):
    if not date_string or date_string.strip() == '':
        return ''
    
    date_string = date_string.strip()
    
    input_formats = [
        '%Y-%m-%d',    # YYYY-MM-DD
        '%d/%m/%Y',    # DD/MM/YYYY  
        '%m/%d/%Y',    # MM/DD/YYYY
        '%Y/%m/%d',    # YYYY/MM/DD
    ]
    
    for input_format in input_formats:
        try:
            date_obj = datetime.strptime(date_string, input_format)
            result = date_obj.strftime('%Y-%m-%d')
            return result
        except ValueError:
            continue
    
    return date_string

def convert_country_for_club(value):
    if not value or pd.isna(value) if 'pd' in globals() else not str(value).strip():
        return ""
    
    original_value = str(value).strip()
    converted = CountryConverter.convert_iso_to_country(original_value)
    
    return converted

def get_hubdb_keys():
    url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{TABLE_ID}/rows"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return {str(row['values'].get('pk_club', '')).strip() 
                   for row in response.json().get('results', [])}
        return set()
    except Exception as e:
        print(f"erreur hubdb: {e}")
        return set()

def filter_csv(hubdb_keys):
    new_data = []
    with open(CSV_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            key_value = str(row.get('PKClub', '')).strip()
            if key_value and key_value not in hubdb_keys:
                new_data.append(row)
    return new_data

def prepare_import_file(data):
    if not data:
        return None
        
    temp_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False)
    
    fieldnames = [
        'PKClub', 'IdClub', 'NomClub', 'FK_President', 'PrenomPresident', 
        'NomPresident', 'FK_Animateur', 'PrenomAnimateur', 'NomAnimateur',
        'FK_Permanent', 'PrenomPermanent', 'NomPermanent', 'Adresse1Club',
        'CPClub', 'VilleClub', 'PaysClub', 'AxeAnalytique', 'Statut',
        'DateCreation', 'DateFin', 'AgeMoyen', 'Tarif', 'Evaluation'
    ]
    
    writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in data:
        processed_row = {}
        for field in fieldnames:
            if field == 'DateCreation':
                processed_row[field] = convert_date_format(row.get(field, ''))
                
            elif field == 'DateFin':
                processed_row[field] = convert_date_format(row.get(field, ''))
                
            elif field == 'PaysClub': 
                original_value = row.get(field, '')
                converted_value = convert_country_for_club(original_value)
                processed_row[field] = converted_value
                    
            else:
                processed_row[field] = row.get(field, '')
        
        writer.writerow(processed_row)
    
    temp_file.seek(0)
    return temp_file

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
            {"source": 1, "target": 44},   # PKClub → target 44
            {"source": 2, "target": 3},    # IdClub → target 3
            {"source": 3, "target": 2},    # NomClub → target 2
            {"source": 4, "target": 24},   # FK_President → target 24
            {"source": 5, "target": 29},   # PrenomPresident → target 29
            {"source": 6, "target": 30},   # NomPresident → target 30
            {"source": 7, "target": 25},   # FK_Animateur → target 25
            {"source": 8, "target": 31},   # PrenomAnimateur → target 31
            {"source": 9, "target": 32},   # NomAnimateur → target 32
            {"source": 10, "target": 33},  # FK_Permanent → target 33
            {"source": 11, "target": 35},  # PrenomPermanent → target 35
            {"source": 12, "target": 34},  # NomPermanent → target 34
            {"source": 13, "target": 19},  # Adresse1Club → target 19
            {"source": 14, "target": 37},  # CPClub → target 37
            {"source": 15, "target": 5},   # VilleClub → target 5
            {"source": 16, "target": 4},   # PaysClub → target 4 
            {"source": 17, "target": 11},  # AxeAnalytique → target 11
            {"source": 18, "target": 21},  # Statut → target 21
            {"source": 19, "target": 38},  # DateCreation → target 38
            {"source": 20, "target": 39},  # DateFin → target 39
            {"source": 21, "target": 40},  # AgeMoyen → target 40
            {"source": 22, "target": 41},  # Tarif → target 41
            {"source": 23, "target": 42}   # Evaluation → target 42
        ]
    }

    try:
        with open(temp_file.name, 'rb') as f:
            files = {
                'config': (None, json.dumps(config), 'application/json'),
                'file': (temp_file.name, f, 'text/csv')
            }
            
            headers = {"Authorization": f"Bearer {API_KEY}"}
            response = requests.post(import_url, files=files, headers=headers)

            return response
    except Exception as e:
        print(f"erreur import: {e}")
        return None

def main():    
    hubdb_keys = get_hubdb_keys()
    print(f"{len(hubdb_keys)} clés existantes trouvées dans HubDB")
    
    new_data = filter_csv(hubdb_keys)
    print(f"{len(new_data)} nouvelles lignes à importer")
    
    if not new_data:
        print("aucune nouvelle donnée à importer")
        return
    
    temp_file = prepare_import_file(new_data)
    if not temp_file:
        print("erreur lors de la préparation du fichier")
        return
    
    print("import hubdb")
    response = import_to_hubdb(temp_file)
    
    if response and response.status_code == 200:
        print("import réussi")
        
        publish_url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{TABLE_ID}/draft/publish"
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        pub_response = requests.post(publish_url, headers=headers)
        
        if pub_response.status_code == 200:
            print("table HubDB publiée")
        else:
            print(f"erreur publication: {pub_response.status_code}")
    else:
        status_code = response.status_code if response else "N/A"
        print(f"echec import HubDB (Code: {status_code})")
        if response:
            print(f"détails: {response.text}")
    
    temp_file.close()
    print("terminé")

if __name__ == "__main__":
    main()
