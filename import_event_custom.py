from dotenv import load_dotenv
import pandas as pd
import os
import requests
import json
from datetime import datetime, timezone
from country_converter import CountryConverter

load_dotenv()

output_dir = 'filtered'
HUBSPOT_API_KEY = os.getenv("PROD_KEY")
HUBSPOT_IMPORT_API_URL = 'https://api.hubapi.com/crm/v3/imports'

# colonnes à traiter du fichier csv
EVENT_COLUMNS = [
    'pk_evt', 'IdEvt', 'IdInter', 'Nom', 'IdTypeEvt', 'TypeEvt', 'Date',
    'TypePresence', 'DateAnnulation', 'Ordre', 'Format', 'NbAdherents',
    'NbInvites', 'NbParticipants', 'TxPresence', 'TxPresence2', 'IdStatut',
    'Statut', 'SatisfactionGlobale', 'SatisfactionGlobale2', 'SatisfactionGlobale3',
    'NbEvaluations', 'Adresse', 'Pays', 'Region', 'LieuEvt', 'Dept', 'Ville',
    'ZIP', 'Annulation', 'IdModePaiement', 'ModePaiement', 'Date_Creation', 'Date_MAJ'
]

# correspondance csv -> hubspot
columns_mapping = {
    'pk_evt': 'pk_event',
    'IdEvt': 'id_next_apm_evenement', 
    'IdInter': "id_intervention",
    'Nom': 'event',
    'IdTypeEvt': 'id_type_d_evenement',
    'TypeEvt': 'type_d_evenement',
    'Date': 'start_datetime',
    'TypePresence': 'type_de_presence',
    'DateAnnulation': 'date_annulation',
    'Ordre':'ordre_du_jour',
    'Format': 'format',
    'NbAdherents': 'nombre_d_adherents',
    'NbInvites': 'nombre_d_invites_nb',
    'NbParticipants': 'nombre_de_participants_nb',
    'TxPresence': 'taux_de_presence_nb',
    'TxPresence2': 'taux_de_presence_2',
    'IdStatut': 'id_statut_de_l_evenement',
    'Statut': 'statut_de_l_evenement',
    'SatisfactionGlobale': 'satisfaction_globale',
    'SatisfactionGlobale2': 'satisfaction_globale_2',
    'SatisfactionGlobale3': 'satisfaction_globale_3',
    'NbEvaluations': 'nombre_d_evaluations_nb',
    'Adresse': 'adresse_de_l_evenement',
    'Pays': 'pays_de_l_evenement',
    'Region': 'region_de_l_evenement',
    'LieuEvt': 'lieu_de_l_evenement',
    'Dept': "departement",
    'Ville': 'ville_de_l_evenement',
    'ZIP': 'code_postal_de_l_evenement',
    'Annulation': 'motif_d_annulation',
    'IdModePaiement': 'id_mode_paiement',
    'ModePaiement': 'mode_paiement',
    'Date_Creation': 'createdat',
    'Date_MAJ': 'updated_at'
}

# mapping des types de présence
TYPE_PRESENCE_MAPPING = {
    'À distance': 'Club à distance',
    'Présentiel': 'Présentiel',
    'Classique': 'Présentiel',
    'Mixte': 'Mixte'
}

TYPE_EVENT_MAPPING = {
    'Rencontre': 'Rencontre de club'
}

def convert_type_presence(value):
    if not value or pd.isna(value):
        return ""
    return TYPE_PRESENCE_MAPPING.get(str(value).strip(), str(value).strip())

def convert_country(value):
    if not value or pd.isna(value) or str(value).strip() == "0":
        return ""
    
    try:
        return CountryConverter.convert_iso_to_country(str(value).strip())
    except:
        return str(value).strip()

def convert_date_to_timestamp(date_string):
    if not date_string or str(date_string).lower() in ['null', 'none', '', 'nan']:
        return ""  
    try:
        date_string = str(date_string).strip()
        
        #différents formats de date
        date_formats = [
            '%Y-%m-%d %H:%M:%S%z',      
            '%Y-%m-%d %H:%M:%S+00:00',  
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M'
        ]
        
        for date_format in date_formats:
            try:
                if date_format == '%Y-%m-%d':
                    date_obj = datetime.strptime(date_string, date_format)
                    date_obj = date_obj.replace(hour=9, minute=0, second=0, tzinfo=timezone.utc)  # 9h par défaut
                elif '%z' in date_format or '+00:00' in date_format:
                    date_obj = datetime.strptime(date_string, date_format)
                    if date_obj.tzinfo is None:
                        date_obj = date_obj.replace(tzinfo=timezone.utc)
                else:
                    date_obj = datetime.strptime(date_string, date_format)
                    date_obj = date_obj.replace(tzinfo=timezone.utc)
                
                timestamp_ms = int(date_obj.timestamp() * 1000)
                return str(timestamp_ms)
            except ValueError:
                continue
        
        print(f"Format de date non reconnu: {date_string}")
        return ""
    except Exception as e:
        print(f"Erreur conversion date {date_string}: {e}")
        return ""


def format_date(value):
    if not value or pd.isna(value):
        return ""
    
    try:
        value_str = str(value).strip()
        formats = ['%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']
        
        for fmt in formats:
            try:
                dt = datetime.strptime(value_str, fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        return ""
    except:
        return ""

def convert_to_int(value):
    try:
        if value and str(value).replace('.', '').replace('-', '').isdigit():
            return int(float(value))
        return ""
    except:
        return ""

def convert_type_event(value):
    if not value or pd.isna(value):
        return ""
    return TYPE_EVENT_MAPPING.get(str(value).strip(), str(value).strip())

def convert_to_float(value):
    try:
        if value and str(value).strip() not in ['', 'null', 'none', 'nan']:
            return float(value)
        return ""
    except:
        return ""


def clean_data(df):
    # colonnes dates
    date_columns = ['DateAnnulation']
    
    # colonnes datetime (date + heure)
    datetime_columns = ['Date', 'Date_Creation', 'Date_MAJ']

    float_columns = ['TxPresence', 'TxPresence2', 'SatisfactionGlobale', 
                    'SatisfactionGlobale2', 'SatisfactionGlobale3']
    
    numeric_columns = ['IdEvt', 'IdInter', 'IdTypeEvt', 'NbAdherents', 'NbInvites', 
                      'NbParticipants', 'IdStatut', 'NbEvaluations', 'IdModePaiement']
    
    cleaned_data = []
    
    for _, row in df.iterrows():
        data = {}
        
        for column in df.columns:
            value = row[column] if pd.notna(row[column]) else ""
            
            # traitement spécifique par colonne
            if column == 'Nom':
                data[column] = "A renommer" if not value or str(value).strip().lower() in ['', 'none', 'null'] else str(value).strip()
            elif column == 'TypePresence':
                data[column] = convert_type_presence(value)
            elif column == 'TypeEvt':  
                data[column] = convert_type_event(value)
            elif column == 'Pays':
                data[column] = convert_country(value)
            elif column in datetime_columns:  
                data[column] = convert_date_to_timestamp(value)
            elif column in date_columns:
                data[column] = format_date(value)
            elif column in float_columns:  
                data[column] = convert_to_float(value)
            elif column in numeric_columns:
                data[column] = convert_to_int(value)
            else:
                data[column] = str(value).strip() if value else ""
        
        cleaned_data.append(data)
    
    return pd.DataFrame(cleaned_data)

def process_events():
    input_file = './exports/dwh.mv_evt.csv'
    output_file = 'dwh_evenement_filtered.csv'
    output_path = os.path.join(output_dir, output_file)
    
    try:
        df = pd.read_csv(input_file)
        
        # vérifier les colonnes disponibles
        available_columns = [col for col in EVENT_COLUMNS if col in df.columns]
        missing_columns = [col for col in EVENT_COLUMNS if col not in df.columns]
        
        if missing_columns:
            print(f"colonnes manquantes: {len(missing_columns)}")
        
        # filtrer et nettoyer
        df_filtered = df[available_columns]
        df_cleaned = clean_data(df_filtered)
        
        # sauvegarder
        df_cleaned.to_csv(output_path, index=False)
        print(f"{len(df_cleaned)} événements traités")
        
        # uploader vers hubspot
        return upload_to_hubspot(output_path, available_columns)
        
    except Exception as e:
        print(f"erreur traitement: {e}")
        return False

def upload_to_hubspot(csv_file_path, available_columns):
    try:
        headers = {'Authorization': f'Bearer {HUBSPOT_API_KEY}'}
        
        # créer les mappings de colonnes
        column_mappings = []
        
        # clé primaire
        if 'pk_evt' in available_columns:
            column_mappings.append({
                "columnObjectTypeId": "2-139503358",
                "columnName": "pk_evt",
                "propertyName": "pk_event",
                "columnType": "HUBSPOT_ALTERNATE_ID"
            })
        
        # autres colonnes
        for csv_column in available_columns:
            if csv_column != 'pk_evt' and csv_column in columns_mapping:
                column_mappings.append({
                    "columnObjectTypeId": "2-139503358",
                    "columnName": csv_column,
                    "propertyName": columns_mapping[csv_column]
                })
        
        # configuration d'import
        payload = {
            "name": f"Import événements APM - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "importOperations": {"2-139503358": "UPSERT"},
            "dateFormat": "YEAR_MONTH_DAY",
            "marketableContactImport": False,
            "createContactListFromImport": False,
            "files": [{
                "fileName": "dwh_evenement_filtered.csv",
                "fileFormat": "CSV",
                "fileImportPage": {
                    "hasHeader": True,
                    "columnMappings": column_mappings
                }
            }]
        }
        
        # envoyer vers hubspot
        with open(csv_file_path, 'rb') as csv_file:
            files = {'files': ('dwh_evenement_filtered.csv', csv_file, 'text/csv')}
            data = {'importRequest': json.dumps(payload)}
            
            response = requests.post(HUBSPOT_IMPORT_API_URL, headers=headers, files=files, data=data)
            
            if response.status_code == 200:
                print("upload réussi")
                return True
            else:
                print(f"erreur upload: {response.status_code}")
                return False
                
    except Exception as e:
        print(f"erreur upload: {e}")
        return False

def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    success = process_events()
    print("terminé" if success else "échec")

if __name__ == "__main__":
    main()
