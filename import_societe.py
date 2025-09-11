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

COMPANY_COLUMNS = [
    'PKSociete',
    'Nom',           
    'Email',         
    'Phone', 
    'Secteur',     
    'Pays',         
    'City',         
    'ZipCode',      
    'Address', 
    'Dept',
    'Region',
    'IdEffectif',      
    'Effectif', 
    'Revenue',      
    'SIRET',          
    'SIREN',         
    'TVA',            
    'TVAInter',     
    'TVAOption',    
    'IdMode',       
    'Mode',
    'TiersPayeur'         
]

def convert_country_for_company(value):
    if not value or pd.isna(value):
        return ""
    
    original_value = str(value).strip()
    converted = CountryConverter.convert_iso_to_country(original_value)
    
    return converted

def clean_company_data(df):
    processed_data = []
    
    for _, row in df.iterrows():
        data = {}
        
        for column in df.columns:
            value = row[column] if pd.notna(row[column]) else ""
            
            if column == 'Pays':  
                data[column] = convert_country_for_company(value)
            elif column in ['Effectif', 'IdMode', 'IdEffectif']:  
                try:
                    data[column] = int(float(value)) if value and str(value).replace('.', '').isdigit() else ""
                except:
                    data[column] = ""
            elif column == 'Revenue':  
                try:
                    data[column] = float(value) if value and str(value).replace('.', '').replace(',', '').isdigit() else ""
                except:
                    data[column] = ""
            else:
                data[column] = str(value).strip() if value else ""
        
        processed_data.append(data)
    
    return pd.DataFrame(processed_data)

def process_companies():
    input_file = './exports/dwh.mv_societe.csv'
    output_file = 'dwh_societe_filtered.csv'
    output_path = os.path.join(output_dir, output_file)
    
    try:
        df = pd.read_csv(input_file)
        print(f"nombre total d'entreprises: {len(df)}")
        
        available_columns = [col for col in COMPANY_COLUMNS if col in df.columns]
        missing_columns = [col for col in COMPANY_COLUMNS if col not in df.columns]
        
        if missing_columns:
            print(f"colonnes manquantes: {missing_columns}")
        
        df_filtered = df[available_columns]
        
        df_cleaned = clean_company_data(df_filtered)
        
        df_cleaned.to_csv(output_path, index=False)
        print(f"{len(df_cleaned)} entreprises exportées vers {output_path}")
        
        upload_success = upload_companies_to_hubspot(output_path, available_columns)
        
        if upload_success:
            print("import des entreprises réussi")
        else:
            print("échec de l'import des entreprises")
            
        return upload_success
        
    except Exception as e:
        print(f"erreur lors du traitement des entreprises: {e}")
        return False

# upload les entreprises vers HubSpot
def upload_companies_to_hubspot(csv_file_path, available_columns):
    try:
        headers = {'Authorization': f'Bearer {HUBSPOT_API_KEY}'}
        
        payload = {
            "name": f"Import sociétés APM - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "importOperations": {
                "0-2": "UPSERT"
            },
            "dateFormat": "DAY_MONTH_YEAR",
            "marketableContactImport": False,
            "createContactListFromImport": False,
            "files": [
                {
                    "fileName": "dwh_societe_filtered.csv",
                    "fileFormat": "CSV",
                    "fileImportPage": {
                        "hasHeader": True,
                        "columnMappings": [
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "PKSociete",
                                "propertyName": "pk_societe",
                                "columnType": "HUBSPOT_ALTERNATE_ID"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "Nom",
                                "propertyName": "name"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "Email",
                                "propertyName": "mail"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "Phone",
                                "propertyName": "phone"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "Secteur",
                                "propertyName": "secteur"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "Pays",
                                "propertyName": "country"  
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "City",
                                "propertyName": "city"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "ZipCode",
                                "propertyName": "zip"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "Address",
                                "propertyName": "address"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "Dept",
                                "propertyName": "departement"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "Region",
                                "propertyName": "region"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "IdEffectif",
                                "propertyName": "id_tranche_effectif"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "Effectif",
                                "propertyName": "effectif"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "Revenue",
                                "propertyName": "chiffres_affaires"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "SIRET",
                                "propertyName": "siret"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "SIREN",
                                "propertyName": "siren"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "TVA",
                                "propertyName": "zone_tva"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "TVAInter",
                                "propertyName": "tva"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "TVAOption",
                                "propertyName": "tva_option"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "IdMode",
                                "propertyName": "id_mode"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "Mode",
                                "propertyName": "mode_de_r_glement"
                            },
                            {
                                "columnObjectTypeId": "0-2",
                                "columnName": "TiersPayeur",
                                "propertyName": "compte_tiers_payeur"
                            }
                        ]
                    }
                }
            ]
        }
        
        print("upload vers hs en cours...")
        
        with open(csv_file_path, 'rb') as csv_file:
            files = {'files': ('dwh_societe_filtered.csv', csv_file, 'text/csv')}
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
    
    success = process_companies()
    
    if success:
        print("import terminé avec succès")
    else:
        print("échec de l'import")

if __name__ == "__main__":
    main()
