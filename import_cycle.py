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

TRANSACTION_COLUMNS = [
    'PKCycle',
    'DateCreationCycle',
    'DateDebut',
    'DateFin',
    'DateDesinscription',
    'FK_Club',
    'FK_Club2',
    'FK_Animateur',
    'FK_Region',
    'Id_MotifSortie',
    'MotifSortie',
    'Id_TypeSortie',
    'TypeSortie',
    'FK_Invoice',
    'FlagActif',
    'DiscountManuel',
    'Membre_Index',
    'Membre_email',
    'Membre_Prenom',
    'Id_Membre',
    'Membre_Nom',
    'Membre_Type',
    'Taux',
    'Id_Proba_renew',
    'Proba_renew',
    'Renew',
    'sent_count_cpp_to_sign',
    'sent_cpp',
    'sent_cpp_to_sign',
    'signed_cpp__asset_filename',
    'signed_cpp__date',
    'signed_cpp__filename',
    'signed_cpp__url',
    'Id_Offrespeciale',
    'Offrespeciale',
    'FK_Adherent',
    'subscriber_info__active_subscription__key',
    'Id_Statut_Adherent',
    'Statut_Adherent',
    'Cotisation_TTC',
    'Cotisation_HT',
    'TVA',
    'DateMAJ',
    'DateCreation',
    'dealname',
    'pipeline',
    'dealstage'
]

def clean_transaction_data(df):
    processed_data = []

    if 'PKCycle' in df.columns:
        for i in range(min(5, len(df))):
            raw_value = df.iloc[i]['PKCycle']
    
    for _, row in df.iterrows():
        data = {}
        
        for column in df.columns:
            value = row[column] if pd.notna(row[column]) else ""

            if column in ['PKCycle']:
                data[column] = str(value).strip() if value and str(value).strip() and str(value).lower() not in ['nan', 'null', 'none'] else ""

            elif column in ['FK_Club', 'FK_Club2', 'FK_Animateur', 'FK_Region', 'FK_Adherent']:
                data[column] = str(value).strip() if value and str(value).strip() and str(value).lower() not in ['nan', 'null', 'none'] else ""

            elif column in ['Id_MotifSortie', 'Id_TypeSortie', 'FK_Invoice', 'Membre_Index', 
                        'Id_Membre', 'Id_Taux', 'Id_Proba_renew', 'sent_count_cpp_to_sign', 
                        'Id_Offrespeciale', 'Id_Statut_Adherent']:
                try:
                    if value and str(value).strip() and str(value).lower() not in ['nan', 'null', 'none']:
                        clean_value = str(value).replace('.0', '').replace(',', '').strip()
                        data[column] = int(float(clean_value)) if clean_value.replace('-', '').isdigit() else ""
                    else:
                        data[column] = ""
                except Exception as e:
                    data[column] = ""
            
            # conversion des montants et pourcentages
            elif column in ['DiscountManuel', 'Taux', 'Proba_renew', 'Cotisation_TTC', 'Cotisation_HT', 'TVA']:
                try:
                    if value and str(value).strip() and str(value).lower() not in ['nan', 'null', 'none']:
                        clean_value = str(value).replace(',', '.').strip()
                        data[column] = float(clean_value) if clean_value.replace('.', '').replace('-', '').isdigit() else ""
                    else:
                        data[column] = ""
                except:
                    data[column] = ""
            
            elif column in ['FlagActif']:
                if str(value).lower() in ['true', '1', 'yes', 'oui', 'vrai', '1.0']:
                    data[column] = True
                elif str(value).lower() in ['false', '0', 'no', 'non', 'faux', '0.0']:
                    data[column] = False
                else:
                    data[column] = ""

            elif column in ['Renew', 'sent_cpp', 'sent_cpp_to_sign']:
                data[column] = normalize_boolean(value)
                        
            #conversion des dates 
            elif column in ['DateCreationCycle', 'DateMAJ', 'DateCreation', 'signed_cpp__date']:
                data[column] = convert_date_to_hubspot_format(value, "datetime")

            elif column in ['DateDebut', 'DateFin', 'DateDesinscription']:
                data[column] = convert_date_to_hubspot_format(value, "date")

            elif column == 'Membre_Type':
                data[column] = normalize_membre_type(value)

            else:
                data[column] = str(value).strip() if value and str(value).lower() not in ['nan', 'null', 'none'] else ""
        
        prenom = data.get('Membre_Prenom', '').strip()
        nom = data.get('Membre_Nom', '').strip()
        
        if prenom and nom:
            data['dealname'] = f"[CYCLE] {prenom} {nom}"
        elif nom:  
            data['dealname'] = f"[CYCLE] {nom}"
        else:  
            pk_cycle = data.get('PKCycle', '')
            data['dealname'] = f"[CYCLE] {pk_cycle}" if pk_cycle else "[CYCLE] Inconnu"
        
        #pipeline et dealstage fixes
        data['pipeline'] = "1902842079"
        data['dealstage'] = "2585825492"
        
        processed_data.append(data)
    
    return pd.DataFrame(processed_data)



def normalize_membre_type(value):
    if not value or str(value).lower() in ['nan', 'null', 'none', '']:
        return ""
    
    membre_type = str(value).strip().lower()
    
    # mapping des membres
    type_mapping = {
        'member': 'Membre',
    }
    
    return type_mapping.get(membre_type, membre_type.capitalize())

def normalize_boolean(value):
    if not value or str(value).lower() in ['nan', 'null', 'none', '']:
        return ""
    
    value_str = str(value).lower().strip()
    
    #mapping des valeurs booléennes vers français
    boolean_mapping = {
        'true': 'Oui',
        'yes': 'Oui',
        'oui': 'Oui',
        'vrai': 'Oui',
        'false': 'Non',
        'no': 'Non',
        'non': 'Non',
        'faux': 'Non'
    }
    
    return boolean_mapping.get(value_str, "")

def convert_date_to_hubspot_format(date_string, field_type="datetime"):
    if not date_string or str(date_string).lower() in ['null', 'none', '', 'nan']:
        return ""
    
    try:
        date_string = str(date_string).strip()
        
        #formats de date supportés
        date_formats = [
            '%Y-%m-%d %H:%M:%S%z',
            '%Y-%m-%d %H:%M:%S+00:00',
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

def process_transactions():
    input_file = '/root/apm/infocentre/apm-export-tables-back/exports/dwh.mv_cycle.csv'
    output_file = 'dwh_transactions_filtered.csv'
    output_path = os.path.join(output_dir, output_file)
    
    try:
        print("Lecture du fichier CSV...")
        df = pd.read_csv(input_file, dtype=str, low_memory=False)

        available_columns = [col for col in TRANSACTION_COLUMNS if col in df.columns]
        
        print(f"colonnes disponibles: {len(available_columns)}/{len(TRANSACTION_COLUMNS)}")
        
        df_filtered = df[available_columns]
        
        df_cleaned = clean_transaction_data(df_filtered)       
        
        df_cleaned.to_csv(output_path, index=False)
        
        upload_success = upload_transactions_to_hubspot(output_path, available_columns)
        
        if upload_success:
            print("import des transactions réussi")
        else:
            print("échec de l'import des transactions")
            
        return upload_success
        
    except Exception as e:
        print(f"erreur lors du traitement des transactions: {e}")
        return False


def upload_transactions_to_hubspot(csv_file_path, available_columns):
    try:
        headers = {'Authorization': f'Bearer {HUBSPOT_API_KEY}'}
        
        payload = {
            "name": f"Import cycles APM - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "importOperations": {
                "0-3": "UPSERT"
            },
            "dateFormat": "YEAR_MONTH_DAY",
            "marketableContactImport": False,
            "createContactListFromImport": False,
            "files": [
                {
                    "fileName": "dwh_transactions_filtered.csv",
                    "fileFormat": "CSV",
                    "fileImportPage": {
                        "hasHeader": True,
                        "columnMappings": [
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "PKCycle",
                                "propertyName": "pk_cycle",
                                "columnType": "HUBSPOT_ALTERNATE_ID"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "DateCreationCycle",
                                "propertyName": "date_de_cr_ation_du_cycle"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "DateDebut",
                                "propertyName": "date_d_but_de_cycle"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "DateFin",
                                "propertyName": "date_de_fin_de_cycle"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "DateDesinscription",
                                "propertyName": "date_de_desinscription"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "FK_Club",
                                "propertyName": "fk_club"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "FK_Club2",
                                "propertyName": "fk_club_2"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "FK_Animateur",
                                "propertyName": "fk_animateur"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "FK_Region",
                                "propertyName": "fk_region"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Id_MotifSortie",
                                "propertyName": "id_motif_de_sortie"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "MotifSortie",
                                "propertyName": "motif_de_sortie"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Id_TypeSortie",
                                "propertyName": "id_type_de_sortie"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "TypeSortie",
                                "propertyName": "type_de_sortie"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "FK_Invoice",
                                "propertyName": "fk_invoice"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "FlagActif",
                                "propertyName": "flag_actif"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "DiscountManuel",
                                "propertyName": "discount_manuel"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Membre_Index",
                                "propertyName": "membre_index"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Membre_email",
                                "propertyName": "membre_email"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Membre_Prenom",
                                "propertyName": "prenom_du_membre"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Id_Membre",
                                "propertyName": "id_membre"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Membre_Nom",
                                "propertyName": "nom_du_membre"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Membre_Type",
                                "propertyName": "type_de_membre"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Taux",
                                "propertyName": "taux"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Id_Proba_renew",
                                "propertyName": "id_probabilite_renewal"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Proba_renew",
                                "propertyName": "probabilite_renewal"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Renew",
                                "propertyName": "renouvellement"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "sent_count_cpp_to_sign",
                                "propertyName": "nombre_d_envois_de_cpp_a_signer"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "sent_cpp",
                                "propertyName": "cpp_d_j_envoy_"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "sent_cpp_to_sign",
                                "propertyName": "cpp_a_signer_envoye"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "signed_cpp__asset_filename",
                                "propertyName": "nom_du_fichier_du_cpp_asset"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "signed_cpp__date",
                                "propertyName": "date_signature_cpp"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "signed_cpp__filename",
                                "propertyName": "cpp_signe_nom_du_fichier"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "signed_cpp__url",
                                "propertyName": "url_du_cpp_signe"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "subscriber_info__active_subscription__key",
                                "propertyName": "key_de_souscription_active"
                            },

                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Id_Offrespeciale",
                                "propertyName": "id_offre_speciale"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Offrespeciale",
                                "propertyName": "offre_sp_ciale"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "FK_Adherent",
                                "propertyName": "fk_adherent"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Id_Statut_Adherent",
                                "propertyName": "id_statut_adherent"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Statut_Adherent",
                                "propertyName": "statut_adherent"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Cotisation_TTC",
                                "propertyName": "cotisation_ttc"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "Cotisation_HT",
                                "propertyName": "cotisation_ht"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "TVA",
                                "propertyName": "tva"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "DateMAJ",
                                "propertyName": "updated_at"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "DateCreation",
                                "propertyName": "created_at"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "dealname",
                                "propertyName": "dealname"
                            },
                            {
                                "columnObjectTypeId": "0-3", 
                                "columnName": "pipeline",
                                "propertyName": "pipeline"
                            },
                            {
                                "columnObjectTypeId": "0-3",
                                "columnName": "dealstage", 
                                "propertyName": "dealstage"
                            }
                        ]
                    }
                }
            ]
        }
        
        print("upload vers hs en cours...")
        
        with open(csv_file_path, 'rb') as csv_file:
            files = {'files': ('dwh_transactions_filtered.csv', csv_file, 'text/csv')}
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
    
    success = process_transactions()
    
    if success:
        print("import terminé avec succès")
    else:
        print("échec de l'import")

if __name__ == "__main__":
    main()
