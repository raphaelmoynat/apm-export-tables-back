from dotenv import load_dotenv
import pandas as pd
import os
import requests
import json
from datetime import datetime
from country_converter import CountryConverter
import time

load_dotenv()

output_dir = 'filtered'
HUBSPOT_API_KEY = os.getenv("PROD_KEY")
HUBSPOT_IMPORT_API_URL = 'https://api.hubapi.com/crm/v3/imports'

EVENT_COLUMNS = [
    'pk_evt',
    'IdEvt',
    'Nom',
    'IdTypeEvt',
    'Date',
    'TypePresence',
    'DateAnnulation',
    'Format',
    'NbAdherents',
    'NbParticipants',
    'NbPresents',
    'TxPresence',
    'Statut',
    'SatisfactionGlobale',
    'NbEvaluations',
    'ZIP',
    'Annulation',
    'Pays'
]

TYPE_PRESENCE_MAPPING = {
    'À distance': 'Club à distance',
    'Présentiel': 'Présentiel',
    'Classique': 'Présentiel',
    'Mixte': 'Mixte'
}

def convert_type_presence(value):
    if not value or pd.isna(value):
        return ""
    
    value_str = str(value).strip()
    return TYPE_PRESENCE_MAPPING.get(value_str, value_str)

def convert_country_for_event(value):
    if not value or pd.isna(value):
        return ""
    
    original_value = str(value).strip()
    
    if original_value == "0":
        return ""
    
    try:
        if float(original_value) == 0:
            return ""
    except (ValueError, TypeError):
        pass
    
    converted = CountryConverter.convert_iso_to_country(original_value)
    
    return converted

def format_datetime_for_hubspot(value):
    if not value or pd.isna(value):
        return ""
    
    try:
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        
        value_str = str(value).strip()
        if value_str:
            formats_to_try = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%Y-%m-%d',
                '%d/%m/%Y %H:%M:%S',
                '%d/%m/%Y %H:%M',
                '%d/%m/%Y'
            ]
            
            for fmt in formats_to_try:
                try:
                    dt = datetime.strptime(value_str, fmt)
                    if fmt in ['%Y-%m-%d', '%d/%m/%Y']:
                        dt = dt.replace(hour=9, minute=0, second=0)
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue
        
        print(f"⚠️ Format de date non reconnu: {value}")
        return ""
        
    except Exception as e:
        print(f"Erreur formatage datetime {value}: {e}")
        return ""

def format_date_for_hubspot(value):
    if not value or pd.isna(value):
        return ""
    
    try:
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d')
        
        value_str = str(value).strip()
        if value_str:
            formats_to_try = [
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%Y-%m-%d %H:%M:%S'
            ]
            
            for fmt in formats_to_try:
                try:
                    dt = datetime.strptime(value_str, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
        
        return ""
        
    except Exception as e:
        print(f"Erreur formatage date {value}: {e}")
        return ""

def clean_event_data(df):
    processed_data = []
    
    for _, row in df.iterrows():
        data = {}
        
        for column in df.columns:
            value = row[column] if pd.notna(row[column]) else ""
            
            if column == 'Nom':
                if not value or str(value).strip().lower() in ['', 'none', 'null']:
                    data[column] = "A renommer"
                else:
                    data[column] = str(value).strip()
            
            elif column == 'TypePresence':
                data[column] = convert_type_presence(value)
            
            elif column == 'Pays':
                data[column] = convert_country_for_event(value)
            
            elif column == 'Date':  
                data[column] = format_datetime_for_hubspot(value)
        
            elif column == 'DateAnnulation': 
                data[column] = format_date_for_hubspot(value)
            
            elif column in ['IdTypeEvt', 'NbAdherents', 'NbParticipants', 'NbPresents', 'TxPresence', 'SatisfactionGlobale', 'NbEvaluations']:
                try:
                    data[column] = int(float(value)) if value and str(value).replace('.', '').isdigit() else ""
                except:
                    data[column] = ""
            
            else:
                data[column] = str(value).strip() if value else ""
        
        processed_data.append(data)
    
    return pd.DataFrame(processed_data)

def process_events():
    input_file = './exports/dwh.mv_evt.csv'
    output_file = 'dwh_evenement_filtered.csv'
    output_path = os.path.join(output_dir, output_file)
    
    try:
        df = pd.read_csv(input_file)
        print(f"nombre total d'événements: {len(df)}")
        
        available_columns = [col for col in EVENT_COLUMNS if col in df.columns]
        missing_columns = [col for col in EVENT_COLUMNS if col not in df.columns]
        
        if missing_columns:
            print(f"colonnes manquantes: {missing_columns}")
        
        df_filtered = df[available_columns]
        
        df_cleaned = clean_event_data(df_filtered)
        
        df_cleaned.to_csv(output_path, index=False)
        print(f"{len(df_cleaned)} événements exportés vers {output_path}")
        
        upload_success = upload_events_to_hubspot(output_path)
        
        if upload_success:
            print("import des événements réussi")
        else:
            print("échec de l'import des événements")
            
        return upload_success
        
    except Exception as e:
        print(f"erreur lors du traitement des événements: {e}")
        return False

def upload_events_to_hubspot(csv_file_path):
    try:
        headers = {'Authorization': f'Bearer {HUBSPOT_API_KEY}'}
        
        payload = {
            "name": f"Import événements 2-139503358 APM - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "importOperations": {
                "2-139503358": "UPSERT"
            },
            "dateFormat": "YEAR_MONTH_DAY",
            "marketableContactImport": False,
            "createContactListFromImport": False,
            "files": [
                {
                    "fileName": "dwh_evenement_filtered.csv",
                    "fileFormat": "CSV",
                    "fileImportPage": {
                        "hasHeader": True,
                        "columnMappings": [
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "pk_evt",
                                "propertyName": "pk_event",
                                "columnType": "HUBSPOT_ALTERNATE_ID"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "IdEvt",
                                "propertyName": "id_next_apm_evenement"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "Nom",
                                "propertyName": "event"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "IdTypeEvt",
                                "propertyName": "id_type_d_evenement"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "Date",
                                "propertyName": "start_datetime"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "TypePresence",
                                "propertyName": "type_de_presence"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "DateAnnulation",
                                "propertyName": "date_annulation"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "Format",
                                "propertyName": "format"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "NbAdherents",
                                "propertyName": "nombre_d_adherents"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "NbParticipants",
                                "propertyName": "nombre_d_inscrits_nb"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "NbPresents",
                                "propertyName": "nombre_de_participants_nb"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "TxPresence",
                                "propertyName": "taux_de_presence_nb"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "Statut",
                                "propertyName": "statut_de_l_evenement"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "SatisfactionGlobale",
                                "propertyName": "satisfaction_globale"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "NbEvaluations",
                                "propertyName": "nombre_d_evaluations_nb"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "ZIP",
                                "propertyName": "code_postal_de_l_evenement"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "Annulation",
                                "propertyName": "motif_d_annulation"
                            },
                            {
                                "columnObjectTypeId": "2-139503358",
                                "columnName": "Pays",
                                "propertyName": "pays_de_l_evenement"
                            }
                        ]
                    }
                }
            ]
        }
        
        print("upload vers hs en cours...")
        
        with open(csv_file_path, 'rb') as csv_file:
            files = {'files': ('dwh_evenement_filtered.csv', csv_file, 'text/csv')}
            data = {'importRequest': json.dumps(payload)}
            
            response = requests.post(HUBSPOT_IMPORT_API_URL, headers=headers, files=files, data=data)
            
            if response.status_code == 200:
                result = response.json()
                print("upload réussi")
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
    
    success = process_events()
    
    if success:
        print("import terminé")
    else:
        print("échec de l'import")

if __name__ == "__main__":
    main()
