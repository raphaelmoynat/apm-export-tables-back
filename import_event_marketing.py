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
filename = "./exports/dwh.mv_evt.csv"
access_token = os.getenv("PROD_KEY")


def read_csv_data(filename, max_rows=100, start_row=0):
    # mapping des colonnes csv vers les propriétés hubspot
    columns_mapping = {
        'pk_evt': 'pk_evenement',
        'IdEvt': 'id_next_apm_de_l_evenement', 
        'Nom': 'hs_event_name',
        'IdTypeEvt': 'id_type_d_evenement',
        'Date': 'hs_start_datetime',
        'TypePresence': 'type_de_presence',
        'DateAnnulation': 'date_annulation',
        'Format': 'format',
        'NbAdherents': 'nombre_d_adherents',
        'NbParticipants': 'nombre_d_inscrits_nb',
        'NbPresents': 'nombre_de_participants_nb',
        'TxPresence': 'taux_de_presence_nb',
        'Statut': 'statut_de_l_evenement',
        'SatisfactionGlobale': 'satisfaction_globale',
        'NbEvaluations': 'nombre_d_evaluations',
        'ZIP': 'code_postal_de_l_evenement',
        'Annulation': 'motif_d_annulation',
        'Pays': 'pays_de_l_evenement'
    }
    
    events_data = []
    current_row = 0
    
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=',')
        
        for row in reader:
            # pagination: ignorer les lignes avant le point de départ
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
                        if hubspot_property == 'date_annulation':
                            converted_date = convert_date_to_timestamp(value)
                            if converted_date:
                                event_data[hubspot_property] = converted_date
                        elif hubspot_property == 'type_de_presence': 
                            event_data[hubspot_property] = map_type_presence(value)
                        elif hubspot_property == 'pays_de_l_evenement':  
                            event_data[hubspot_property] = CountryConverter.convert_iso_to_country(value)
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


def create_hubspot_payload(events_data):
    hubspot_inputs = []
    
    for event in events_data:
        event_name = event.get('event_name', '').strip()
        
        if not event_name or event_name.lower() in ['none', 'null', '']:
            event_name = "A renommer"
        
        custom_properties = []
        
        # séparation entre champs standard hubspot et propriétés personnalisées
        for key, value in event.items():
            if key not in ['external_event_id', 'event_name', 'start_datetime', 'hs_event_name'] and value:
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
    if not date_string or date_string.lower() in ['null', 'none', '']:
        return None
    try:
        date_obj = datetime.strptime(date_string + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
        date_obj = date_obj.replace(tzinfo=timezone.utc)
        timestamp_ms = int(date_obj.timestamp() * 1000)
        return str(timestamp_ms)
    except:
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
        
        # arrêter si c'est le dernier lot (moins d'événements que la taille du lot)
        if len(events_data) < batch_size:
            break
            
        start_row += batch_size
    
    print(f"résumé final: {total_processed} événements importés, {total_errors} erreurs")
    return total_processed


# lancement du traitement 
total_imported = process_all_events_in_batches(filename, access_token)
print(f"import termine: {total_imported} evenements")
