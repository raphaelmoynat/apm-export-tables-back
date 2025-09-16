from dotenv import load_dotenv
import pandas as pd
import os
import requests
import json
from datetime import datetime
from country_converter import CountryConverter

load_dotenv()

output_dir = 'filtered'
HUBSPOT_API_KEY = os.getenv("PROD_KEY")
HUBSPOT_IMPORT_API_URL = 'https://api.hubapi.com/crm/v3/imports'

CLUB_COLUMNS = [
    'PKClub',
    'IdClub',
    'NomClub',
    'FK_President',
    'PrenomPresident',
    'NomPresident',
    'FK_Animateur',
    'PrenomAnimateur',
    'NomAnimateur',
    'FK_Permanent',
    'PrenomPermanent',
    'NomPermanent',
    'FK_Referent',
    'PrenomReferent',
    'NomReferent',
    'FK_Region',
    'Region',
    'NomRegion',
    'Adresse1Club',
    'Adresse2Club',
    'CPClub',
    'VilleClub',
    'PaysClub',
    'Nbadherents',
    'AxeAnalytique',
    'IdStatut',
    'Statut',
    'DateCreation',
    'DateFin',
    'AgeMoyen',
    'Tarif',
    'Evaluation',
    'stats__age_average',
    'Date_Creation_Club',
    'Date_1ere_Rencontre',
    'Date_fin_cycle',
    'Date_debut_cycle',
    'avatar_url'
]

def convert_country_for_club(value):
    if not value or pd.isna(value):
        return ""
    
    original_value = str(value).strip()
    if original_value in ['X', '*']:
        return ""
    
    converted = CountryConverter.convert_iso_to_country(original_value)
    return converted

def parse_date(date_str):
    """Convertit une date au format YYYY-MM-DD vers le format DD/MM/YYYY pour HubSpot"""
    if not date_str or pd.isna(date_str) or str(date_str).strip() == '':
        return ""
    
    try:
        date_obj = pd.to_datetime(date_str)
        return date_obj.strftime('%d/%m/%Y')
    except:
        return ""

def clean_club_data(df):
    processed_data = []
    
    for _, row in df.iterrows():
        data = {}
        
        for column in df.columns:
            value = row[column] if pd.notna(row[column]) else ""
            
            if column == 'PaysClub':
                data[column] = convert_country_for_club(value)
            elif column in ['IdClub', 'FK_President', 'FK_Animateur', 'FK_Permanent', 'FK_Referent', 'FK_Region', 'IdStatut']:
                try:
                    data[column] = int(float(value)) if value and str(value).replace('.', '').isdigit() else ""
                except:
                    data[column] = ""
            elif column in ['AgeMoyen', 'Tarif', 'Evaluation', 'stats__age_average']:
                try:
                    data[column] = float(value) if value and str(value).replace('.', '').replace(',', '').isdigit() else ""
                except:
                    data[column] = ""
            elif column in ['DateCreation', 'DateFin', 'Date_Creation_Club', 'Date_1ere_Rencontre', 'Date_fin_cycle', 'Date_debut_cycle']:
                data[column] = parse_date(value)
            else:
                data[column] = str(value).strip() if value else ""
        
        processed_data.append(data)
    
    return pd.DataFrame(processed_data)

def process_clubs():
    input_file = './exports/dwh.mv_club.csv'
    output_file = 'dwh_club_filtered.csv'
    output_path = os.path.join(output_dir, output_file)
    
    try:
        df = pd.read_csv(input_file)
        print(f"nombre total de clubs: {len(df)}")
        
        available_columns = [col for col in CLUB_COLUMNS if col in df.columns]
        missing_columns = [col for col in CLUB_COLUMNS if col not in df.columns]
        
        if missing_columns:
            print(f"colonnes manquantes: {missing_columns}")
        
        df_filtered = df[available_columns]
        
        df_cleaned = clean_club_data(df_filtered)
        
        df_cleaned.to_csv(output_path, index=False)
        print(f"{len(df_cleaned)} clubs exportés vers {output_path}")
        
        upload_success = upload_clubs_to_hubspot(output_path, available_columns)
        
        if upload_success:
            print("import des clubs réussi")
        else:
            print("échec de l'import des clubs")
            
        return upload_success
        
    except Exception as e:
        print(f"erreur lors du traitement des clubs: {e}")
        return False

def upload_clubs_to_hubspot(csv_file_path, available_columns):
    try:
        headers = {'Authorization': f'Bearer {HUBSPOT_API_KEY}'}
        
        payload = {
            "name": f"Import clubs APM - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "importOperations": {
                "2-191825137": "UPSERT"
            },
            "dateFormat": "DAY_MONTH_YEAR",
            "marketableContactImport": False,
            "createContactListFromImport": False,
            "files": [
                {
                    "fileName": "dwh_club_filtered.csv",
                    "fileFormat": "CSV",
                    "fileImportPage": {
                        "hasHeader": True,
                        "columnMappings": [
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "PKClub",
                                "propertyName": "pk_club",
                                "columnType": "HUBSPOT_ALTERNATE_ID"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "IdClub",
                                "propertyName": "id_club"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "NomClub",
                                "propertyName": "nom_du_club"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "FK_President",
                                "propertyName": "cle_president"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "PrenomPresident",
                                "propertyName": "prenom_president"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "NomPresident",
                                "propertyName": "nom_president"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "FK_Animateur",
                                "propertyName": "cle_animateur"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "PrenomAnimateur",
                                "propertyName": "prenom_animateur"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "NomAnimateur",
                                "propertyName": "nom_animateur"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "FK_Permanent",
                                "propertyName": "cle_permanent"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "PrenomPermanent",
                                "propertyName": "prenom_permanent"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "NomPermanent",
                                "propertyName": "nom_permanent"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "FK_Referent",
                                "propertyName": "cle_referent"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "PrenomReferent",
                                "propertyName": "prenom_referent"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "NomReferent",
                                "propertyName": "nom_referent"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "FK_Region",
                                "propertyName": "fk_region"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "Region",
                                "propertyName": "id_region"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "NomRegion",
                                "propertyName": "region_du_club"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "Adresse1Club",
                                "propertyName": "adresse_du_club"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "Adresse2Club",
                                "propertyName": "adresse_2_club"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "CPClub",
                                "propertyName": "code_postal"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "VilleClub",
                                "propertyName": "ville_du_club"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "PaysClub",
                                "propertyName": "pays_du_club"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "Nbadherents",
                                "propertyName": "nb_adherents"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "AxeAnalytique",
                                "propertyName": "axe_analytique"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "IdStatut",
                                "propertyName": "id_statut"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "Statut",
                                "propertyName": "statut_du_club"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "DateCreation",
                                "propertyName": "date_creation"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "DateFin",
                                "propertyName": "date_de_fin"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "AgeMoyen",
                                "propertyName": "age_moyen"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "Tarif",
                                "propertyName": "tarif"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "Evaluation",
                                "propertyName": "evaluation"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "stats__age_average",
                                "propertyName": "stats_age_average"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "Date_Creation_Club",
                                "propertyName": "date_de_creation"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "Date_1ere_Rencontre",
                                "propertyName": "date_premiere_rencontre"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "Date_fin_cycle",
                                "propertyName": "date_de_fin_de_cycle"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "Date_debut_cycle",
                                "propertyName": "date_de_debut_de_cycle"
                            },
                            {
                                "columnObjectTypeId": "2-191825137",
                                "columnName": "avatar_url",
                                "propertyName": "url_avatar"
                            }
                        ]
                    }
                }
            ]
        }
        
        print("upload vers hs en cours...")
        
        with open(csv_file_path, 'rb') as csv_file:
            files = {'files': ('dwh_club_filtered.csv', csv_file, 'text/csv')}
            data = {'importRequest': json.dumps(payload)}
            
            response = requests.post(HUBSPOT_IMPORT_API_URL, headers=headers, files=files, data=data)
            
            if response.status_code == 200:
                result = response.json()
                print("upload réussi")
                print(f"Import ID: {result.get('id', 'N/A')}")
                return True
            else:
                print(f"erreur API HubSpot: {response.status_code}")
                print(f"response: {response.text}")
                return False
                
    except Exception as e:
        print(f"erreur lors de l'upload: {e}")
        return False

def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    success = process_clubs()
    
    if success:
        print("import terminé avec succès")
    else:
        print("échec de l'import")

if __name__ == "__main__":
    main()
