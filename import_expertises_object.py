from dotenv import load_dotenv
import pandas as pd
import os
import requests
import json
from datetime import datetime, timezone

load_dotenv()

output_dir = '/root/apm/infocentre/apm-export-tables-back/filtered'
HUBSPOT_API_KEY = os.getenv("PROD_KEY")
HUBSPOT_IMPORT_API_URL = 'https://api.hubapi.com/crm/v3/imports'

EXPERTISE_COLUMNS = [
    'PKExpertise', 'FK_Expert', 'FK_Experts', 'IdExpert', 'IdStatut', 'Statut',
    'theme', 'Expertise', 'SsExpertise', 'Id_TypeIntervention', 'TypeIntervention',
    'IdModalite', 'Modalite', 'Avantages', 'exti_id__id', 'exti_id__value',
    'theme_under_surveillance', 'order_of_preference', 'format__id', 'expert__exp_id',
    'opca__progression', 'opca__benefit', 'opca__key_points', 'opco__progression',
    'opco__benefit', 'opco__key_points', 'benefit', 'fo_id', 'progression',
    'summary__content', 'collecting_org__progression', 'collecting_org__key_points',
    'key_points', 'domain__id', 'domain__subdomain__id', 'domain__subdomain__value',
    'domain__value', 'is_opca', 'is_opco', 'event_preparation_info', 'key_ideas_to_share',
    'retex_of_the_day', 'format__value', 'make_a_success', 'attachment_url',
    'detailled_educational_program_url', 'can_be_remote', 'interclub_max_club', 'voyage',
    'stats__global_satisfaction', 'stats__interest_for_concept', 'stats__capacity_for_dialogue',
    'stats__clarity_of_ideas', 'stats__number_of_subscribers_evaluations', 'stats__number_of_events',
    'stats__presence_rate', 'stats__innovation_source', 'DateCreation', 'DateMAJ'
]

def clean_expertise_data(df):
    processed_data = []

    if 'PKExpertise' in df.columns:
        for i in range(min(5, len(df))):
            raw_value = df.iloc[i]['PKExpertise']
    
    for _, row in df.iterrows():
        data = {}
        
        for column in df.columns:
            value = row[column] if pd.notna(row[column]) else ""

            if column in ['PKExpertise']:
                data[column] = str(value).strip() if value and str(value).strip() and str(value).lower() not in ['nan', 'null', 'none'] else ""

            elif column in ['FK_Expert', 'FK_Experts', 'IdExpert', 'expert__exp_id']:
                data[column] = str(value).strip() if value and str(value).strip() and str(value).lower() not in ['nan', 'null', 'none'] else ""

            elif column in ['IdStatut', 'Id_TypeIntervention', 'IdModalite', 'exti_id__id', 
                        'order_of_preference', 'format__id', 'fo_id', 'domain__id', 
                        'domain__subdomain__id', 'interclub_max_club', 'stats__number_of_subscribers_evaluations', 
                        'stats__number_of_events']:
                try:
                    if value and str(value).strip() and str(value).lower() not in ['nan', 'null', 'none']:
                        clean_value = str(value).replace('.0', '').replace(',', '').strip()
                        data[column] = int(float(clean_value)) if clean_value.replace('-', '').isdigit() else ""
                    else:
                        data[column] = ""
                except Exception as e:
                    data[column] = ""
            
            # conversion des stats et pourcentages
            elif column in ['stats__global_satisfaction', 'stats__interest_for_concept', 'stats__capacity_for_dialogue',
                          'stats__clarity_of_ideas', 'stats__presence_rate', 'stats__innovation_source']:
                try:
                    if value and str(value).strip() and str(value).lower() not in ['nan', 'null', 'none']:
                        clean_value = str(value).replace(',', '.').strip()
                        data[column] = float(clean_value) if clean_value.replace('.', '').replace('-', '').isdigit() else ""
                    else:
                        data[column] = ""
                except:
                    data[column] = ""

            elif column in ['theme_under_surveillance', 'is_opca', 'is_opco', 'can_be_remote', 'voyage']:
                data[column] = convert_boolean(value)
                        
            #conversion des dates 
            elif column in ['DateCreation', 'DateMAJ']:
                data[column] = convert_date_to_hubspot_format(value, "datetime")

            else:
                data[column] = str(value).strip() if value and str(value).lower() not in ['nan', 'null', 'none'] else ""
        
        processed_data.append(data)
    
    return pd.DataFrame(processed_data)

def convert_boolean(value):
    if not value:
        return ""
    
    value_str = str(value).strip().lower()
    
    if value_str in ['t', 'true', '1', 'yes', 'y', 'oui']:
        return "TRUE"
    elif value_str in ['f', 'false', '0', 'no', 'n', 'non']:
        return "FALSE"
    else:
        return ""


def convert_date_to_hubspot_format(date_string, field_type="datetime"):
    if not date_string or str(date_string).lower() in ['null', 'none', '', 'nan']:
        return ""
    
    try:
        date_string = str(date_string).strip()
        
        if date_string.endswith('+00'):
            date_string = date_string[:-3] + '+00:00'
        
        #formats de date supportés
        date_formats = [
            '%Y-%m-%d %H:%M:%S+00:00',  
            '%Y-%m-%d %H:%M:%S%z',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M'
        ]
        
        date_obj = None
        for date_format in date_formats:
            try:
                if date_format == '%Y-%m-%d':
                    date_obj = datetime.strptime(date_string, date_format)
                    date_obj = date_obj.replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
                elif '%z' in date_format or '+00:00' in date_format:
                    # Pour les dates avec timezone
                    date_obj = datetime.strptime(date_string, date_format)
                    # Si pas de timezone info, ajouter UTC
                    if date_obj.tzinfo is None:
                        date_obj = date_obj.replace(tzinfo=timezone.utc)
                else:
                    date_obj = datetime.strptime(date_string, date_format)
                    if date_obj.tzinfo is None:
                        date_obj = date_obj.replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue
        
        if not date_obj:
            print(f"Format de date non reconnu: {date_string}")
            return ""
        
        if field_type == "date":
            return date_obj.strftime('%Y-%m-%d')
        else:
            timestamp_ms = int(date_obj.timestamp() * 1000)
            return str(timestamp_ms)
            
    except Exception as e:
        print(f"Erreur conversion date {date_string}: {e}")
        return ""

def process_expertises():
    input_file = '/root/apm/infocentre/apm-export-tables-back/exports/dwh.mv_expertise.csv'
    output_file = 'expertise_filtered.csv'
    output_path = os.path.join(output_dir, output_file)
    
    try:
        print("Lecture du fichier CSV...")
        df = pd.read_csv(input_file, dtype=str, low_memory=False)

        available_columns = [col for col in EXPERTISE_COLUMNS if col in df.columns]
        
        print(f"colonnes disponibles: {len(available_columns)}/{len(EXPERTISE_COLUMNS)}")
        
        df_filtered = df[available_columns]
        
        df_cleaned = clean_expertise_data(df_filtered)       
        
        df_cleaned.to_csv(output_path, index=False)
        
        upload_success = upload_expertises_to_hubspot(output_path, available_columns)
        
        if upload_success:
            print("import des expertises réussi")
        else:
            print("échec de l'import des expertises")
            
        return upload_success
        
    except Exception as e:
        print(f"erreur lors du traitement des expertises: {e}")
        return False

def upload_expertises_to_hubspot(csv_file_path, available_columns):
    try:
        headers = {'Authorization': f'Bearer {HUBSPOT_API_KEY}'}
        
        payload = {
            "name": f"Import expertises APM - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "importOperations": {
                "2-140990150": "UPSERT"  
            },
            "dateFormat": "YEAR_MONTH_DAY",
            "marketableContactImport": False,
            "createContactListFromImport": False,
            "files": [
                {
                    "fileName": "expertise_filtered.csv",
                    "fileFormat": "CSV",
                    "fileImportPage": {
                        "hasHeader": True,
                        "columnMappings": [
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "PKExpertise",
                                "propertyName": "pk_expertise",
                                "columnType": "HUBSPOT_ALTERNATE_ID"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "FK_Expert",
                                "propertyName": "fk_expert"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "FK_Experts",
                                "propertyName": "fk_experts"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "IdExpert",
                                "propertyName": "id_expert"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "IdStatut",
                                "propertyName": "id_statut"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "Statut",
                                "propertyName": "statut_de_l_expertise"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "theme",
                                "propertyName": "theme"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "Expertise",
                                "propertyName": "titre_de_l_expertise"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "SsExpertise",
                                "propertyName": "sous_expertise"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "Id_TypeIntervention",
                                "propertyName": "id_typeintervention"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "TypeIntervention",
                                "propertyName": "type_intervention"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "IdModalite",
                                "propertyName": "idmodalite"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "Modalite",
                                "propertyName": "modalite_de_l_expertise"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "Avantages",
                                "propertyName": "avantages"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "exti_id__id",
                                "propertyName": "exti_id_id"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "exti_id__value",
                                "propertyName": "exti_id_value"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "theme_under_surveillance",
                                "propertyName": "theme_sous_surveillance"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "order_of_preference",
                                "propertyName": "ordre_de_preference"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "format__id",
                                "propertyName": "format_id"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "expert__exp_id",
                                "propertyName": "expert_exp_id"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "opca__progression",
                                "propertyName": "opca_progression"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "opca__benefit",
                                "propertyName": "opca_benefice"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "opca__key_points",
                                "propertyName": "opca_points_cles"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "opco__progression",
                                "propertyName": "opco_progression"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "opco__benefit",
                                "propertyName": "opco_benefice"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "opco__key_points",
                                "propertyName": "opco_points_cles"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "benefit",
                                "propertyName": "benefice"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "fo_id",
                                "propertyName": "fo_id"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "progression",
                                "propertyName": "progression"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "summary__content",
                                "propertyName": "resume_du_contenu"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "collecting_org__progression",
                                "propertyName": "progression_organisme_collecteur"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "collecting_org__key_points",
                                "propertyName": "organisme_collecteur_points_cles"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "key_points",
                                "propertyName": "points_cles"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "domain__id",
                                "propertyName": "domain_id"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "domain__subdomain__id",
                                "propertyName": "domain_subdomain_id"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "domain__subdomain__value",
                                "propertyName": "sous_domaine_d_expertise"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "domain__value",
                                "propertyName": "domaine_de_l_expertise"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "is_opca",
                                "propertyName": "is_opca"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "is_opco",
                                "propertyName": "is_opco"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "event_preparation_info",
                                "propertyName": "infos_preparation_event"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "key_ideas_to_share",
                                "propertyName": "idees_cles_a_partager"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "retex_of_the_day",
                                "propertyName": "retex_du_jour"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "format__value",
                                "propertyName": "valeur_format"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "make_a_success",
                                "propertyName": "make_a_success"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "attachment_url",
                                "propertyName": "url_de_piece_jointe"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "detailled_educational_program_url",
                                "propertyName": "url_du_programme_detaille"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "can_be_remote",
                                "propertyName": "peut_etre_a_distance"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "interclub_max_club",
                                "propertyName": "interclub_max_club"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "voyage",
                                "propertyName": "voyage"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "stats__global_satisfaction",
                                "propertyName": "satisfaction_globale"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "stats__interest_for_concept",
                                "propertyName": "interet_pour_le_concept"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "stats__capacity_for_dialogue",
                                "propertyName": "capacite_au_dialogue"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "stats__clarity_of_ideas",
                                "propertyName": "clarte_des_idees"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "stats__number_of_subscribers_evaluations",
                                "propertyName": "nombre_d_evaluations_d_adherents"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "stats__number_of_events",
                                "propertyName": "nombre_d_evenements"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "stats__presence_rate",
                                "propertyName": "taux_de_presence"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "stats__innovation_source",
                                "propertyName": "source_d_innovation"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "DateCreation",
                                "propertyName": "date_de_creation_de_l_expertise"
                            },
                            {
                                "columnObjectTypeId": "2-140990150",
                                "columnName": "DateMAJ",
                                "propertyName": "date_maj"
                            }
                        ]
                    }
                }
            ]
        }
        
        print("upload vers hs en cours...")
        
        with open(csv_file_path, 'rb') as csv_file:
            files = {'files': ('expertise_filtered.csv', csv_file, 'text/csv')}
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
    
    success = process_expertises()
    
    if success:
        print("import terminé avec succès")
    else:
        print("échec de l'import")

if __name__ == "__main__":
    main()
