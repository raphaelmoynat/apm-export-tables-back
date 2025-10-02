from dotenv import load_dotenv
import pandas as pd
import os
import requests
import json
from datetime import datetime
import sys
from country_converter import CountryConverter  

load_dotenv()

# config 
output_dir = '/root/apm/infocentre/apm-export-tables-back/filtered'
HUBSPOT_API_KEY = os.getenv("PROD_KEY")
HUBSPOT_IMPORT_API_URL = 'https://api.hubapi.com/crm/v3/imports'

# configuration pour les 4 types
FILE_TYPES = {
    'expert': {
        'pk': 'PKExpert', 
        'hubspot_pk': 'pk_membre', 
        'profil_apm': 'Expert',
        'specific_columns': ['SocieteFacturation', 'TypeTVA', 'IdTVAInter', 'accounting__vat_international', 'Id Permanent', 'IdExpert']
    },
    'permanent': {
        'pk': 'PKPermanent', 
        'hubspot_pk': 'pk_membre', 
        'profil_apm': 'Permanent',
        'specific_columns': ['Id']
    }, 
    'referent': {
        'pk': 'PKReferent', 
        'hubspot_pk': 'pk_membre', 
        'profil_apm': 'Référent',
        'specific_columns': ['Id']
    },
    'adherent_actif': {
        'pk': 'PKAdherent', 
        'hubspot_pk': 'pk_membre', 
        'profil_apm': 'Adhérent',
        'specific_columns': ['active_subscription__signed_cpp__filename', 'active_subscription__signed_cpp__date', 
                           'active_subscription__signed_cpp__asset_filename', 'active_subscription__signed_cpp__url', 'FK_Societe', 'Id']
    }
}

# colonnes communes
BASE_COLUMNS = [
    'Email', 'Civilite', 'Nom', 'Prenom', 'Statut expert', 'Tel', 'Portable',
    'Pays', 'Ville', 'CP', 'Adresse', 'Nationalite', 'Date_naissance', 'Dept', 'StatutPro', 'Club',
    'FlagCoordinateur', 'FlagExpert', 'FlagAnimateur', 
    'FlagPermanent', 'FlagReferent', 'FlagActif', 'FlagMembre', 'DernDateEntree', 
    'subscriber_info__status__value', 'active_subscription__club_info__name'
]

def convert_date_for_hubspot(date_value):
    if not date_value or pd.isna(date_value):
        return ""
    try:
        return pd.to_datetime(str(date_value), format='%Y-%m-%d').strftime('%d/%m/%Y')
    except:
        return ""

def convert_civilite(value):
    if not value:
        return ""
    value_clean = str(value).strip().upper()
    mapping = {'M': 'Monsieur', 'MME': 'Madame'}
    return mapping.get(value_clean, value)

def convert_to_boolean(value):
    if not value or str(value).strip() == "":
        return ""
    
    value_str = str(value).lower().strip()
    if value_str in ['true', '1', 'yes', 'oui', 'vrai']:
        return True
    elif value_str in ['false', '0', 'no', 'non', 'faux']:
        return False
    else:
        return ""

def convert_country_field(value, field_name=""):
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

def convert_statut_pro(value):
    if not value or pd.isna(value):
        return "Autre"
    
    value_clean = str(value).strip()
    
    if not value_clean or value_clean.lower() in ["nan", "", "0"]:
        return "Autre"
    
    # mapping vers les noms internes hs
    mapping = {
        'salarie': 'Salarié',
        'salarié': 'Salarié',
        'independant': 'Indépendant',
        'indépendant': 'Indépendant',
        'sans emploi': 'Sans emploi',
        'chomeur': 'Sans emploi',
        'chômeur': 'Sans emploi',
        'gerant': 'Gérant d\'entreprise',
        'gérant': 'Gérant d\'entreprise',
        'gerant d\'entreprise': 'Gérant d\'entreprise',
        'gérant d\'entreprise': 'Gérant d\'entreprise',
        'enseignant': 'Enseignant',
        'chercheur': 'Chercheur',
        'autre': 'Autre'
    }
    
    value_lower = value_clean.lower()
    if value_lower in mapping:
        return mapping[value_lower]
    
    return "Autre"


# traite un fichier selon son type
def process_file(file_type):
    config = FILE_TYPES[file_type]
    input_file = f'/root/apm/infocentre/apm-export-tables-back/exports/dwh.mv_{file_type}.csv'
    output_file = f'dwh_{file_type}_filtered.csv'
    output_path = os.path.join(output_dir, output_file)

    if not os.path.exists(input_file):
        print(f"Fichier manquant: {input_file}")
        return False
    
    try:
        df = pd.read_csv(input_file)
        
        if 'Prénom' in df.columns:
            df = df.rename(columns={'Prénom': 'Prenom'})
        
        # inclure les colonnes de base + PK + colonnes spécifiques
        columns_to_keep = [config['pk']] + BASE_COLUMNS + config.get('specific_columns', [])

        if file_type == 'expert' and 'Statut expert' not in df.columns:
            print("Attention: colonne 'Statut expert' manquante dans le fichier expert")

        available_columns = [col for col in columns_to_keep if col in df.columns]
        df_filtered = df[available_columns]
        
        processed_data = []
        for _, row in df_filtered.iterrows():
            data = {}
            
            for column in available_columns:
                value = row[column] if pd.notna(row[column]) else ""
                
                if column.startswith('Flag'):
                    data[column] = convert_to_boolean(value)
                elif column == 'Civilite':
                    data[column] = convert_civilite(value)
                elif column in ['Date_naissance', 'DernDateEntree', 'active_subscription__signed_cpp__date']:
                    data[column] = convert_date_for_hubspot(value)
                elif column == 'Pays': 
                    data[column] = convert_country_field(value, "Pays")
                elif column == 'Nationalite':  
                    data[column] = convert_country_field(value, "Nationalité")
                elif column == 'StatutPro':
                    data[column] = convert_statut_pro(value)
                elif column == 'Statut expert':
                    data[column] = str(value).strip() if value else ""
                else:
                    data[column] = str(value).strip() if value else ""
            
            # ➡️ Ajout des colonnes PK
            data['pk_membre'] = data[config['pk']]
            role_property = f"pk_{file_type}" if file_type != "adherent_actif" else "pk_adherent"
            data[role_property] = data[config['pk']]

            if config['pk'] in data:
                data.pop(config['pk'])
            
            # profil apm
            data['profil_apm'] = ";" + config['profil_apm']
            
            processed_data.append(data)
        
        pd.DataFrame(processed_data).to_csv(output_path, index=False)
        print(f"{len(processed_data)} {file_type}s exportés")
        
        upload_success = upload_to_hubspot(output_path, file_type, config)
        
        if upload_success:
            print(f"import {file_type} réussi")
        else:
            print(f"échec import {file_type}")
            
        return upload_success
        
    except Exception as e:
        print(f"erreur {file_type}: {e}")
        return False


#upload vers hubspot
def upload_to_hubspot(csv_file_path, file_type, config):
    try:
        headers = {'Authorization': f'Bearer {HUBSPOT_API_KEY}'}

        role_property = f"pk_{file_type}" if file_type != "adherent_actif" else "pk_adherent"

        # mapping de base avec PKs
        mappings = [
            {"columnObjectTypeId": "0-1", "columnName": "pk_membre", "propertyName": "pk_membre"},
            {"columnObjectTypeId": "0-1", "columnName": role_property, "propertyName": role_property},
            {"columnObjectTypeId": "0-1", "columnName": "Email", "propertyName": "email", "columnType": "HUBSPOT_ALTERNATE_ID"},
            {"columnObjectTypeId": "0-1", "columnName": "Civilite", "propertyName": "civilite"},
            {"columnObjectTypeId": "0-1", "columnName": "Nom", "propertyName": "lastname"},
            {"columnObjectTypeId": "0-1", "columnName": "Prenom", "propertyName": "firstname"},
            {"columnObjectTypeId": "0-1", "columnName": "Tel", "propertyName": "phone"},
            {"columnObjectTypeId": "0-1", "columnName": "Portable", "propertyName": "mobilephone"},
            {"columnObjectTypeId": "0-1", "columnName": "Pays", "propertyName": "pays"}, 
            {"columnObjectTypeId": "0-1", "columnName": "Ville", "propertyName": "city"},
            {"columnObjectTypeId": "0-1", "columnName": "CP", "propertyName": "zip"},
            {"columnObjectTypeId": "0-1", "columnName": "Adresse", "propertyName": "address"},
            {"columnObjectTypeId": "0-1", "columnName": "Nationalite", "propertyName": "nationalite"}, 
            {"columnObjectTypeId": "0-1", "columnName": "Date_naissance", "propertyName": "date_de_naissance"},
            {"columnObjectTypeId": "0-1", "columnName": "Dept", "propertyName": "departement"},
            {"columnObjectTypeId": "0-1", "columnName": "StatutPro", "propertyName": "statut_professionnel"},
            {"columnObjectTypeId": "0-1", "columnName": "Club", "propertyName": "club"},
            {"columnObjectTypeId": "0-1", "columnName": "FlagCoordinateur", "propertyName": "flag_coordinateur"},
            {"columnObjectTypeId": "0-1", "columnName": "FlagExpert", "propertyName": "flag_expert"},
            {"columnObjectTypeId": "0-1", "columnName": "FlagAnimateur", "propertyName": "flag_animateur"},
            {"columnObjectTypeId": "0-1", "columnName": "FlagPermanent", "propertyName": "flag_permanent"},
            {"columnObjectTypeId": "0-1", "columnName": "FlagReferent", "propertyName": "flag_referent"},
            {"columnObjectTypeId": "0-1", "columnName": "FlagActif", "propertyName": "flag_actif"},
            {"columnObjectTypeId": "0-1", "columnName": "FlagMembre", "propertyName": "flag_membre"},
            {"columnObjectTypeId": "0-1", "columnName": "DernDateEntree", "propertyName": "derniere_date_entree"},
            {"columnObjectTypeId": "0-1", "columnName": "subscriber_info__status__value", "propertyName": "subscriber_info_status_value"},
            {"columnObjectTypeId": "0-1", "columnName": "active_subscription__club_info__name", "propertyName": "active_subscription_club_info_name"},
            {"columnObjectTypeId": "0-1", "columnName": "profil_apm", "propertyName": "profil_apm"}
        ]

        # mappings spécifiques
        if file_type == 'expert':
            mappings.extend([
                {"columnObjectTypeId": "0-1", "columnName": "IdExpert", "propertyName": "next_apm_id"},
                {"columnObjectTypeId": "0-1", "columnName": "Statut expert", "propertyName": "statut_next_apm"},
                {"columnObjectTypeId": "0-1", "columnName": "SocieteFacturation", "propertyName": "societe_facturation"},
                {"columnObjectTypeId": "0-1", "columnName": "TypeTVA", "propertyName": "type_tva"},
                {"columnObjectTypeId": "0-1", "columnName": "IdTVAInter", "propertyName": "id_tva_inter"},
                {"columnObjectTypeId": "0-1", "columnName": "accounting__vat_international", "propertyName": "expert_comptabilite_tva_international"},
                {"columnObjectTypeId": "0-1", "columnName": "Id Permanent", "propertyName": "id_permanent"}
            ])
        
        elif file_type == 'permanent':
            mappings.extend([
                {"columnObjectTypeId": "0-1", "columnName": "Id", "propertyName": "next_apm_id"},
            ])

        elif file_type == 'referent':
            mappings.extend([
                {"columnObjectTypeId": "0-1", "columnName": "Id", "propertyName": "next_apm_id"},
            ])
        
        elif file_type == 'adherent_actif':
            mappings.extend([
                {"columnObjectTypeId": "0-1", "columnName": "Id", "propertyName": "next_apm_id"},
                {"columnObjectTypeId": "0-1", "columnName": "active_subscription__signed_cpp__filename", "propertyName": "active_subscription_signed_cpp_filename"},
                {"columnObjectTypeId": "0-1", "columnName": "active_subscription__signed_cpp__date", "propertyName": "active_subscription_signed_cpp_date"},
                {"columnObjectTypeId": "0-1", "columnName": "active_subscription__signed_cpp__asset_filename", "propertyName": "active_subscription_signed_cpp_asset_filename"},
                {"columnObjectTypeId": "0-1", "columnName": "active_subscription__signed_cpp__url", "propertyName": "active_subscription_signed_cpp_url"},
                {"columnObjectTypeId": "0-1", "columnName": "FK_Societe", "propertyName": "key_entreprise"}
            ])
        
        payload = {
            "name": f"Import {file_type}s APM - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "importOperations": {"0-1": "UPSERT"},
            "dateFormat": "DAY_MONTH_YEAR",
            "marketableContactImport": False,
            "createContactListFromImport": True,
            "files": [{
                "fileName": f"dwh_{file_type}_filtered.csv",
                "fileFormat": "CSV",
                "fileImportPage": {
                    "hasHeader": True,
                    "columnMappings": mappings
                }
            }]
        }
        
        with open(csv_file_path, 'rb') as csv_file:
            files = {'files': (f"dwh_{file_type}_filtered.csv", csv_file, 'text/csv')}
            data = {'importRequest': json.dumps(payload)}
            
            response = requests.post(HUBSPOT_IMPORT_API_URL, headers=headers, files=files, data=data)
            
            if response.status_code == 200:
                return True
            else:
                print(f"erreur API: {response.status_code}")
                print(f"Réponse: {response.text}")  
                return False
            
    except Exception as e:
        print(f"erreur upload {file_type}: {e}")
        return False


def main():
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    if len(sys.argv) > 1:
        file_type = sys.argv[1].lower()
        
        if file_type in FILE_TYPES:
            success = process_file(file_type)
        else:
            return
    else:
        results = {}
        for file_type in FILE_TYPES.keys():
            results[file_type] = process_file(file_type)
        
        for file_type, success in results.items():
            status = "ok" if success else "ko"
            print(f"{file_type}: {status}")

if __name__ == "__main__":
    main()
