import csv
import json
import os
import time
from dotenv import load_dotenv
import requests
from datetime import datetime, timezone
from country_converter import CountryConverter

load_dotenv()

# configuration
filename = "/root/apm/infocentre/apm-export-tables-back/exports/dwh.mv_evt.csv"
access_token = os.getenv("PROD_KEY")


def read_csv_data(filename, max_rows=100, start_row=0):
    # mapping des colonnes csv vers les propriétés hubspot
    columns_mapping = {
        'pk_evt': 'pk_evenement',
        'IdEvt': 'id_next_apm_de_l_evenement', 
        'IdInter': "id_intervention",
        'Nom': 'hs_event_name',
        'IdTypeEvt': 'id_type_d_evenement',
        'TypeEvt': 'type_d_evenement',
        'Date': 'hs_start_datetime',
        'TypePresence': 'type_de_presence',
        'IdTypePresence':'idtypepresence',
        'DateAnnulation': 'date_annulation',
        'IdFormat':'idformat',
        'Ordre':'ordre_du_jour',
        'Format': 'format',
        'NbAdherents': 'nombre_d_adherents',
        'NbInvites': 'nombre_d_invites_nb',
        'NbParticipants': 'nombre_de_participants_nb',
        'NbPresents': 'nombre_de_presents',
        'NbPresents2': 'nombre_de_presents_2',
        'TxPresence': 'taux_de_presence_nb',
        'TxPresence2': 'taux_de_presence_2',
        'IdStatut': 'id_statut_de_l_evenement',
        'Statut': 'statut_de_l_evenement',
        'SatisfactionGlobale': 'satisfaction_globale',
        'SatisfactionGlobale2': 'satisfaction_globale_2',
        'SatisfactionGlobale3': 'satisfaction_globale_3',
        'NbEvaluations': 'nombre_d_evaluations',
        'Adresse': 'adresse_de_l_evenement',
        'Pays': 'pays_de_l_evenement',
        'Region': 'region_de_l_evenement',
        'LieuEvt': 'lieu_de_l_evenement',
        'NumDept': 'n_departement',
        'Dept': "departement",
        'Ville': 'ville_de_l_evenement',
        'ZIP': 'code_postal_de_l_evenement',
        'IdAnnulation': 'id_annulation',
        'Annulation': 'motif_d_annulation',
        'Timzeone': 'fuseau_horaire',
        'IdModePaiement': 'id_mode_paiement',
        'ModePaiement': 'mode_paiement',
        'Date_Creation': 'created_at',
        'Date_MAJ': 'updated_at'
    }
    
    events_data = []
    current_row = 0
    
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=',')
        
        for row in reader:
            if current_row < start_row:
                current_row += 1
                continue
            
            # limiter le nombre d'événements par lot
            if len(events_data) >= max_rows:
                break
            
            event_data = {}
            
            # transformation des données selon le mapping
            for csv_column, hubspot_property in columns_mapping.items():
                if csv_column in row:
                    value = row[csv_column].strip() if row[csv_column] else ""
                    if value and value.lower() not in ['null', 'none', '', "0"]:
                        
                        # conversion des dates
                        if hubspot_property in ['date_annulation', 'created_at', 'updated_at']:
                            converted_date = convert_date_to_timestamp(value)
                            if converted_date:
                                event_data[hubspot_property] = converted_date
                        
                        #conversion des nombres entiers 
                        elif hubspot_property in [
                            'pk_evenement', 'id_next_apm_de_l_evenement', 'id_intervention',
                            'id_type_d_evenement', 'nombre_d_adherents', 'nombre_d_invites_nb',
                            'nombre_de_participants_nb', 'nombre_d_evaluations', 'id_statut_de_l_evenement',
                            'id_mode_paiement'
                        ]:
                            converted_int = convert_to_int(value)
                            if converted_int is not None:
                                event_data[hubspot_property] = converted_int
                        
                        #conversion des float
                        elif hubspot_property in [
                            'taux_de_presence_nb', 'taux_de_presence_2', 'satisfaction_globale',
                            'satisfaction_globale_2', 'satisfaction_globale_3'
                        ]:
                            converted_float = convert_to_float(value)
                            if converted_float is not None:
                                event_data[hubspot_property] = converted_float
                        
                        # mappings spéciaux
                        elif hubspot_property == 'type_de_presence': 
                            event_data[hubspot_property] = map_type_presence(value)
                        elif hubspot_property == 'pays_de_l_evenement':  
                            event_data[hubspot_property] = CountryConverter.convert_iso_to_country(value)

                        elif hubspot_property == 'type_d_evenement':
                            mapping_type_evenement = {
                                'Rencontre': 'Rencontre de club',
                                'CODEV Apm': 'CODEV APM',
                                'Autre': 'Evènements hors rencontres',     
                                'Université': 'Séminaires réseaux',       
                                'Rencontre de club': 'Rencontre de club',
                                'Séminaires réseaux': 'Séminaires réseaux',
                                'Événements hors rencontres': 'Événements hors rencontres',  
                                'Séminaires d\'accueil': 'Séminaires d\'accueil',
                                'CODEV APM': 'CODEV APM',
                                'Tous experts': 'Tous experts',
                                'Voyage': 'Voyage',
                                'Interclubs': 'Interclubs',
                                'Convention': 'Convention'
                            }
                            
                            mapped_value = mapping_type_evenement.get(value)
                            if mapped_value:
                                event_data[hubspot_property] = mapped_value
                            else:
                                print(f"Valeur type événement non mappée ignorée: '{value}'")


                        
                        else:
                            event_data[hubspot_property] = value

            # champs obligatoires pour hubspot
            event_data['external_event_id'] = row.get('IdEvt', '')
            event_data['event_name'] = row.get('Nom', '')
            
            # conversion de la date au format iso pour hubspot
            if row.get('Date'):
                try:
                    date_obj = datetime.strptime(row['Date'], '%Y-%m-%d %H:%M')
                    event_data['start_datetime'] = date_obj.isoformat() + '.000Z'
                except:
                    event_data['start_datetime'] = ''
            else:
                event_data['start_datetime'] = ''
            
            events_data.append(event_data)
            current_row += 1
    
    print(f"lu {len(events_data)} événements depuis la ligne {start_row}")
    return events_data


def convert_to_int(value):
    try:
        if isinstance(value, str):
            cleaned_value = value.strip().replace(',', '').replace(' ', '')
            if cleaned_value and cleaned_value.replace('.', '').isdigit():
                return int(float(cleaned_value))
        elif isinstance(value, (int, float)):
            return int(value)
        return None
    except (ValueError, TypeError):
        return None


def convert_to_float(value):
    try:
        if isinstance(value, str):
            cleaned_value = value.strip().replace(',', '.').replace(' ', '')
            if cleaned_value and cleaned_value.replace('.', '').replace('-', '').isdigit():
                return float(cleaned_value)
        elif isinstance(value, (int, float)):
            return float(value)
        return None
    except (ValueError, TypeError):
        return None


def create_hubspot_payload(events_data):
    hubspot_inputs = []
    
    for event in events_data:
        event_name = event.get('event_name', '').strip()
        
        if not event_name or event_name.lower() in ['none', 'null', '']:
            event_name = "A renommer"
        
        custom_properties = []
        
        # séparation entre champs standard hubspot et propriétés personnalisées
        for key, value in event.items():
            if key not in ['external_event_id', 'event_name', 'start_datetime', 'hs_event_name'] and value is not None:
                custom_properties.append({
                    "name": key,
                    "value": str(value)
                })
        
        hubspot_event = {
            "externalAccountId": "apm-system",
            "externalEventId": event.get('external_event_id', ''),
            "eventOrganizer": "dwh_apm",
            "eventName": event_name, 
            "customProperties": custom_properties,
        }

        if event.get('start_datetime'):
            hubspot_event["startDateTime"] = event.get('start_datetime')
        
        hubspot_inputs.append(hubspot_event)
    
    return {"inputs": hubspot_inputs}


def send_to_hubspot(payload, access_token):
    url = "https://api.hubapi.com/marketing/v3/marketing-events/events/upsert"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code in [200, 201]:
        print("import réussi")
        return response.json()
    else:
        print(f"erreur {response.status_code}: {response.text}")
        return None

def convert_date_to_timestamp(date_string):
    """Convertit une date en timestamp milliseconds pour HubSpot"""
    if not date_string or date_string.lower() in ['null', 'none', '']:
        return None
    try:
        date_string = str(date_string).strip()
        
        # 🚀 CORRECTION : Normaliser +00 vers +00:00
        if date_string.endswith('+00'):
            date_string = date_string[:-3] + '+00:00'
        
        # Essayer différents formats de date
        date_formats = [
            '%Y-%m-%d %H:%M:%S+00:00',  # ← Format principal avec timezone normalisée
            '%Y-%m-%d %H:%M:%S%z',      # avec timezone
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
                    date_obj = date_obj.replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)
                elif '%z' in date_format or '+00:00' in date_format:
                    # Pour les dates avec timezone
                    date_obj = datetime.strptime(date_string, date_format)
                    # Si pas de timezone info, ajouter UTC
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
        return None
    except Exception as e:
        print(f"Erreur conversion date {date_string}: {e}")
        return None



def map_type_presence(value):
    # normalisation des valeurs de type de présence
    mapping = {
        'À distance': 'Club à distance',
        'Présentiel': 'Présentiel',
        'Classique':'Présentiel',
        'Mixte': 'Mixte'
    }
    return mapping.get(value, value)  


def process_all_events_in_batches(filename, access_token, batch_size=100):
    total_processed = 0
    total_errors = 0
    start_row = 0
    
    # traitement par lots pour éviter les timeouts et limites api
    while True:
        events_data = read_csv_data(filename, batch_size, start_row)
        
        if not events_data:
            break
        
        payload = create_hubspot_payload(events_data)
        
        # ignorer les lots vides (événements sans nom valide)
        if not payload["inputs"]:
            print(f"lot ignoré: aucun événement valide")
            start_row += batch_size
            continue
        
        result = send_to_hubspot(payload, access_token)

        time.sleep(1)
        
        if result:
            total_processed += len(payload["inputs"])
            print(f"lot terminé: {len(payload['inputs'])} événements (total: {total_processed})")
        else:
            total_errors += len(payload["inputs"])
            print(f"erreur dans le lot - {len(payload['inputs'])} événements échoués")
        
        if len(events_data) < batch_size:
            break
            
        start_row += batch_size
    
    print(f"résumé final: {total_processed} événements importés, {total_errors} erreurs")
    return total_processed


total_imported = process_all_events_in_batches(filename, access_token)
print(f"import termine: {total_imported} evenements")
