import os
from dotenv import load_dotenv
import requests
import csv
import tempfile
import json
from datetime import datetime, timezone

load_dotenv()

#configuration
API_KEY = os.getenv("PROD_KEY")
TABLE_ID = "583148785"
CSV_FILE = "/root/apm/infocentre/apm-export-tables-back/exports/dwh.mv_expertise.csv"
SUBDOMAIN_TABLE_ID = "348960978"

#normaliser la clé
def normalize_key(key_value):
    if not key_value:
        return ""
    
    key_str = str(key_value).strip().replace(',', '_')
    
    if key_str.replace('_', '').isdigit():
        return f"EXP_{key_str}"
    
    return key_str

def clean_value(value):
    if not value:
        return ""
    
    value_str = str(value).strip()
    value_str = value_str.replace(',', ';')
    value_str = value_str.replace('\n', ' ').replace('\r', ' ')
    value_str = value_str.replace('"', "'")
    
    return value_str

#convert date
def convert_date_iso(date_string):
    if not date_string or str(date_string).lower() in ['null', 'none', '', 'nan']:
        return ""
    
    try:
        date_string = str(date_string).strip()
        
        if len(date_string) == 19 and ' ' in date_string:
            date_obj = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
            
            iso_date = date_obj.strftime('%Y-%m-%dT%H:%M:%S')
            
            return iso_date
            
    except Exception as e:
        return ""



#convert boolean
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

#convert domaine d'expertise
def convert_domain(value):
    if not value:
        return ""
    
    domain_mapping = {
        "développement d'entreprise": "développement_dentreprise",
        "techniques d'entreprise": "techniques_dentreprise", 
        "management": "management",
        "environnement de l'entreprise": "environnement_de_lentreprise",
        "développement du dirigeant": "développement_du_dirigeant",
        "université": "universite",
        "tous experts": "tous_experts",
    }
    
    value_str = str(value).strip().lower()
    
    if value_str in domain_mapping:
        return domain_mapping[value_str]
    else:
        print(f"domaine non reconnu: '{value_str}'")
        return ""

#convert type d'intervention
def convert_type_intervention(value):
    if not value:
        return ""
    
    type_mapping = {
        "club": "club",
        "université": "universite", 
        "voyage": "voyage",
        "sans thème": "sans_theme"
    }
    
    value_str = str(value).strip().lower()
    
    if value_str in type_mapping:
        return type_mapping[value_str]
    else:
        print(f"type d'intervention non reconnu: '{value_str}'")
        return ""


#récupère les clés existantes dans la hubdb
def get_existing_keys():
    url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{TABLE_ID}/rows"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            
            hubdb_keys = {}
            for row in results:
                key_value = str(row['values'].get('key_de_l_expertise', '')).strip()
                if key_value:
                    hubdb_keys[key_value] = row['id']
            
            return hubdb_keys
        else:
            print(f"erreur récupération hubdb: {response.text}")
            return {}
    except Exception as e:
        print(f"exception récupération hubdb: {e}")
        return {}


#crée le mapping des sous-domaine
def get_subdomain_mapping():
    subdomain_url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{SUBDOMAIN_TABLE_ID}/rows"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    try:
        response = requests.get(subdomain_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            mapping = {}
            
            for row in data.get('results', []):
                values = row.get('values', {})
                
                subdomain_name = (
                    values.get('name') or
                    values.get('sous_domaine_d_expertise') or
                    values.get('sous_domaine_dexpertise') or
                    values.get('libelle') or
                    values.get('label')
                )
                
                if subdomain_name:
                    normalized_name = str(subdomain_name).lower().strip()
                    mapping[normalized_name] = row['id']
            
            print(f"mapping sous-domaines créé avec {len(mapping)} entrées")
            return mapping
        else:
            print(f"erreur récupération sous-domaines: {response.status_code}")
            return {}
            
    except Exception as e:
        print(f"exception récupération sous-domaines: {e}")
        return {}


#convertit les sous-domaines
def convert_subdomain(value, subdomain_mapping):
    if not value or not subdomain_mapping:
        return ""
    
    value_str = str(value).strip().lower()
    
    # Recherche exacte
    if value_str in subdomain_mapping:
        result_id = subdomain_mapping[value_str]
        return str(result_id)
    
    # Recherche partielle
    for key in subdomain_mapping.keys():
        if value_str in key or key in value_str:
            result_id = subdomain_mapping[key]
            return str(result_id)
    
    print(f"sous-domaine non trouvé: '{value_str}'")
    return ""

#traite les données du csv
def process_csv_data(hubdb_keys):
    all_data = []
    seen_keys = set()
    duplicate_count = 0
    
    with open(CSV_FILE, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            original_key = str(row.get('PKExpertise', '')).strip()
            
            if not original_key:
                continue
            
            normalized_key = normalize_key(original_key)
            
            if normalized_key in seen_keys:
                duplicate_count += 1
                continue
                
            seen_keys.add(normalized_key)
            
            row['PKExpertise'] = normalized_key
            
            if normalized_key in hubdb_keys:
                row['hs_id'] = hubdb_keys[normalized_key]
            else:
                row['hs_id'] = ''
                
            all_data.append(row)
    
    print(f"{duplicate_count} doublons ignorés")
    print(f"{len(all_data)} lignes à traiter")
    return all_data


#convertit les valeurs exti_id__value
def convert_exti_id_value(value):
    if not value or str(value).strip().lower() in ['', 'null', 'none', 'nan', 'non renseigne']:
        return ""
    
    exti_mapping = {
        "le developpement de la capacite strategique": "le_developpement_de_la_capacite_strategique",
        "les systemes et techniques de gestion d'organisation et de production": "les_systemes_et_techniques_de_gestion_dorganisation_et_de_production",
        "le management et les ressources humaines": "le_management_et_les_ressources_humaines",
        "la dimension marketing et l'action commerciale": "la_dimension_marketing_et_laction_commerciale",
        "la macro-economie et la geopolitique": "la_macro-economie_et_la_geopolitique",
        "l'environnement social culturel et institutionnel de l'entreprise": "lenvironnement_social_culturel_et_institutionnel_de_lentreprise",
        "la communication interne/externe et les systemes d'information": "la_communication_interneexterne_et_les_systemes_dinformation",
        "technologie, prospective et recherche au service de l'entreprise": "technologie_prospective_et_recherche_au_service_de_lentreprise",
        "le comportement du dirigeant": "le_comportement_du_dirigeant"
    }
    
    value_str = str(value).strip().lower()
    
    if value_str in exti_mapping:
        return exti_mapping[value_str]
    else:
        if value_str not in ['non renseigne']:
            print(f"exti_id_value non reconnu: '{value_str}'")
        return ""


#prépare le fichier csv pour l'import
def prepare_import_file(data, subdomain_mapping):
    if not data:
        return None
        
    temp_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False, encoding='utf-8')
    
    fieldnames = [
        "hs_id", 'PKExpertise', 'FK_Expert', 'FK_Experts', 'IdExpert', 'IdStatut', 'Statut',
        'theme', 'Expertise', 'SsExpertise', 'Id_TypeIntervention', 'TypeIntervention',
        'IdModalite', 'Modalite', 'Avantages', 'exti_id__id', 'exti_id__value',
        'theme_under_surveillance', 'order_of_preference', 'format__id', 'expert__exp__id',
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
    
    writer = csv.DictWriter(temp_file, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    
    boolean_fields = ['theme_under_surveillance', 'is_opca', 'is_opco', 'can_be_remote', 'voyage']
    domain_fields = ['domain__value']
    subdomain_fields = ['domain__subdomain__value']
    date_fields = ['DateCreation', 'DateMAJ']
    exti_fields = ['exti_id__value']
    type_intervention_fields = ['TypeIntervention']

    converted_count = 0
    exti_converted_count = 0

    for row in data:
        processed_row = {}
        processed_row['hs_id'] = row.get('hs_id', '')

        for field in fieldnames:
            if field in date_fields:
                original_date = row.get(field, '')
                converted_date = convert_date_iso(original_date)
                processed_row[field] = converted_date
                
                #debug dates
                if original_date and converted_date:
                    print(f"🕐 {field}: '{original_date}' → '{converted_date}'")
                elif original_date:
                    print(f"❌ {field}: '{original_date}' → ÉCHEC CONVERSION")
            elif field in boolean_fields:
                processed_row[field] = convert_boolean(row.get(field, ''))
            elif field in domain_fields:
                processed_row[field] = convert_domain(row.get(field, ''))
            elif field in subdomain_fields:
                original_value = row.get(field, '')
                converted_id = convert_subdomain(original_value, subdomain_mapping)
                processed_row[field] = converted_id
                
                if converted_id:
                    converted_count += 1
            elif field in exti_fields:  
                original_value = row.get(field, '')
                converted_value = convert_exti_id_value(original_value)
                processed_row[field] = converted_value
                
                if converted_value:
                    exti_converted_count += 1
            elif field in type_intervention_fields:
                original_value = row.get(field, '')
                converted_value = convert_type_intervention(original_value)
                processed_row[field] = converted_value
                
            
            else:
                processed_row[field] = clean_value(row.get(field, ''))
        
        writer.writerow(processed_row)

    temp_file.flush()
    temp_file.seek(0) 
    
    file_size = os.path.getsize(temp_file.name)
    print(f"fichier préparé: {file_size:,} bytes")
    print(f"{converted_count} sous-domaines convertis")
    print(f"{exti_converted_count} exti_id convertis")
    
    return temp_file

#importe les données vers la hubdb
def import_to_hubdb(temp_file):
    if not temp_file:
        return None

    import_url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{TABLE_ID}/draft/import"
    
    config = {
        "skipRows": 1,
        "separator": ",",
        "resetTable": True,
        "encoding": "utf-8",
        "format": "csv",
        "dateFormat": "ISO8601",
        "idSourceColumn": 1,
        "columnMappings": [
            {"source": 2, "target": "key_de_l_expertise"},
            {"source": 3, "target": "fk_expert"},
            {"source": 4, "target": "fk_experts"},
            {"source": 5, "target": "expert_exp_id"},
            {"source": 6, "target": "id_statut"},
            {"source": 7, "target": "statut_de_lexpertise"},
            {"source": 8, "target": "theme"},
            {"source": 9, "target": "name"},
            {"source": 10, "target": "sous_expertise"},
            {"source": 11, "target": "id_typeintervention"},
            {"source": 12, "target": "type_intervention"},
            {"source": 13, "target": "idmodalite"},
            {"source": 14, "target": "modalite"},
            {"source": 15, "target": "avantages"},
            {"source": 16, "target": "exti_id_id"},
            {"source": 17, "target": "exti_id_value"},
            {"source": 18, "target": "theme_sous_surveillance"},
            {"source": 19, "target": "ordre_de_preference"},
            {"source": 20, "target": "format_id"},
            {"source": 21, "target": "expert_exp_id"},
            {"source": 22, "target": "opca_progression"},
            {"source": 23, "target": "opca_benefice"},
            {"source": 24, "target": "opca_points_cles"},
            {"source": 25, "target": "opco_progression"},
            {"source": 26, "target": "opco_benefice"},
            {"source": 27, "target": "opco_points_cles"},
            {"source": 28, "target": "benefice"},
            {"source": 29, "target": "fo_id"},
            {"source": 30, "target": "progression"},
            {"source": 31, "target": "resume_du_contenu"},
            {"source": 32, "target": "progression_organisme_collecteur"},
            {"source": 33, "target": "organisme_collecteur_points_cles"},
            {"source": 34, "target": "points_cles"},
            {"source": 35, "target": "domain_id"},
            {"source": 36, "target": "domain_subdomain_id"},
            {"source": 37, "target": "sous_domaine_dexpertise"},
            {"source": 38, "target": "domaine_de_lexpertise"},
            {"source": 39, "target": "is_opca"},
            {"source": 40, "target": "is_opco"},
            {"source": 41, "target": "infos_preparation_event"},
            {"source": 42, "target": "idees_cles_a_partager"},
            {"source": 43, "target": "retex_du_jour"},
            {"source": 44, "target": "valeur_format"},
            {"source": 45, "target": "make_a_success"},
            {"source": 46, "target": "url_de_piece_jointe"},
            {"source": 47, "target": "url_du_programme_detaille"},
            {"source": 48, "target": "peut_etre_a_distance"},
            {"source": 49, "target": "interclub_max_club"},
            {"source": 50, "target": "voyage"},
            {"source": 51, "target": "satisfaction_globale"},
            {"source": 52, "target": "interet_pour_le_concept"},
            {"source": 53, "target": "capacite_au_dialogue"},
            {"source": 54, "target": "clarte_des_idees"},
            {"source": 55, "target": "nombre_devaluations_dadherents"},
            {"source": 56, "target": "nombre_d_evenements"},
            {"source": 57, "target": "taux_de_presence"},
            {"source": 58, "target": "source_dinnovation"},
            {"source": 59, "target": "date_de_creation"},
            {"source": 60, "target": "date_maj"}
        ]
    }

    try:
        with open(temp_file.name, 'rb') as f:
            files = {
                'config': (None, json.dumps(config), 'application/json'),
                'file': (temp_file.name, f, 'text/csv')
            }
            
            headers = {"Authorization": f"Bearer {API_KEY}"}
            response = requests.post(import_url, files=files, headers=headers, timeout=300)

            return response
            
    except Exception as e:
        print(f"erreur import: {e}")
        return None

#publier table hubdb
def publish_table():
    publish_url = f"https://api.hubapi.com/cms/v3/hubdb/tables/{TABLE_ID}/draft/publish"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    
    response = requests.post(publish_url, headers=headers)
    
    if response.status_code == 200:
        print("table publiée avec succès")
        return True
    else:
        print(f"erreur publication: {response.text}")
        return False

def main():  
    print("import hubdb en cours...")
    
    # 1 récupérer les mappings
    subdomain_mapping = get_subdomain_mapping()
    if not subdomain_mapping:
        print("erreur: aucun mapping de sous-domaines")
        return
    
    hubdb_keys = get_existing_keys()
    
    # 2 traiter toutes les données
    all_data = process_csv_data(hubdb_keys)
    
    if not all_data:
        print("aucune donnée à importer")
        return
    
    # 3 préparer le fichier
    temp_file = prepare_import_file(all_data, subdomain_mapping)
    
    if temp_file:
        
        temp_file.seek(0)
        
        response = import_to_hubdb(temp_file)
        
        if response and response.status_code == 200:
            print("import réussi")
            if publish_table():
                print("processus terminé avec succès")
            else:
                print("import réussi mais erreur de publication")
        else:
            print(f"échec import: {response.text if response else 'pas de réponse'}")
        
        temp_file.close()
        os.unlink(temp_file.name)

if __name__ == "__main__":
    main()
