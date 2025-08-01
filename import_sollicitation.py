import csv
import json
import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timezone
import time

load_dotenv()

filename = "./exports/dwh.mv_sollicitation.csv"  
access_token = os.getenv("PROD_KEY")
max_solicitations = 1000 
batch_size = 10

def get_existing_solicitations(access_token, event_type="pe145807702_sollicitation_v2", limit=100):    
    url = f"https://api.hubapi.com/events/v3/events"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    all_solicitations = []
    after = None
    page_count = 0
        
    while True:
        page_count += 1
        params = {
            "eventType": event_type,
            "limit": limit
        }
        
        if after:
            params["after"] = after
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                solicitations = data.get("results", [])
                all_solicitations.extend(solicitations)
                                
                
                paging = data.get("paging", {})
                if "next" in paging:
                    after = paging["next"].get("after")
                    time.sleep(0.1)  
                else:
                    print("Fin de pagination atteinte")
                    break
            else:
                print(f"ERREUR API: {response.status_code} - {response.text}")
                break
                
        except Exception as e:
            print(f"erreur : {e}")
            break
    
    print(f"total: {len(all_solicitations)} sollicitations existantes\n")
    return all_solicitations

def filter_new_solicitations(solicitations_data, existing_solicitation_keys):
    new_solicitations = []
    skipped_count = 0
    duplicates = []
    
    for i, solicitation in enumerate(solicitations_data):
        key = solicitation.get('key')
        
        if not key:
            print(f"sollicitation sans clé à l'index {i}")
            continue
        
        if key in existing_solicitation_keys:
            skipped_count += 1
            duplicates.append(key)
            continue
        
        new_solicitations.append(solicitation)
    
    print(f"Ssollicitations ignorées (déjà existantes): {skipped_count}")
 
    
    return new_solicitations

def extract_existing_solicitation_keys(existing_solicitations):
    existing_keys = set()
    
    for solicitation in existing_solicitations:
        properties = solicitation.get("properties", {})
        key = properties.get("key")
        if key:
            existing_keys.add(key)
    return existing_keys

def read_solicitation_data(filename, max_rows=100):
    columns_mapping = {
        'key': 'key',
        'solicitation_status': 'solicitation_status',
        'expert_key': 'expert_key',
        'Expert_name': 'expert_name',
        'Expert_firstname': 'expert_firstname',
        'Expert_email': 'expert_email',
        'permanent_key': 'permanent_key',
        'Perm_name': 'perm_name',
        'Perm_firstname': 'perm_firstname',
        'fk_event': 'fk_event',
        'Evt_id': 'evt_id',
        'renc_id': 'renc_id',
        'title': 'title',
        'TypeEvt_id': 'typeevt_id',
        'TypeEvt': 'typeevt',
        'Evt_status_id': 'evt_status_id',
        'Evt_status': 'evt_status',
        'event_date': 'event_date',
        'intervention_key': 'intervention_key',
        'exp_response_date': 'exp_response_date',
        'created': 'created',
        'updated': 'updated'
    }
    
    solicitations_data = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter=',')
                        
            for row in reader:
                solicitation_data = {}
                
                for csv_column, hubspot_property in columns_mapping.items():
                    if csv_column in row:
                        value = row[csv_column].strip() if row[csv_column] else ""
                        
                        if csv_column in ['renc_id', 'TypeEvt_id', 'Evt_status_id']:
                            if value and value.lower() not in ['null', 'none', '']:
                                try:
                                    solicitation_data[hubspot_property] = int(value)
                                except ValueError:
                                    print(f"ATTENTION: Valeur numérique invalide pour {csv_column}: {value}")
                        elif value and value.lower() not in ['null', 'none', '']:
                            solicitation_data[hubspot_property] = value
                
                solicitations_data.append(solicitation_data)
    
        print(f"\nLu {len(solicitations_data)} sollicitations du CSV\n")
        
    except Exception as e:
        print(f"erreur lecture CSV: {e}")
        
    return solicitations_data

def convert_date_to_iso(date_string):
    if not date_string or date_string.lower() in ['null', 'none', '']:
        return None
    
    try:
        formats = ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y']
        
        for fmt in formats:
            try:
                date_obj = datetime.strptime(date_string, fmt)
                if fmt == '%Y-%m-%d' or fmt == '%d/%m/%Y':
                    date_obj = date_obj.replace(hour=12, minute=0, second=0)
                
                date_obj = date_obj.replace(tzinfo=timezone.utc)
                return date_obj.isoformat().replace('+00:00', '.000Z')
            except ValueError:
                continue
        
        print(f"date non convertible: '{date_string}'")
        return None
    except Exception as e:
        print(f"erreur conversion date: {e}")
        return None

def convert_date_to_timestamp(date_string):
    if not date_string or date_string.lower() in ['null', 'none', '']:
        return None
    
    try:
        formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y']
        
        for fmt in formats:
            try:
                date_obj = datetime.strptime(date_string, fmt)
                date_obj = date_obj.replace(tzinfo=timezone.utc)
                return int(date_obj.timestamp() * 1000)
            except ValueError:
                continue
        
        return None
    except Exception as e:
        return None

def convert_date_to_hubspot_date(date_string):
    if not date_string or date_string.lower() in ['null', 'none', '']:
        return None
    
    try:
        formats = ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y']
        
        for fmt in formats:
            try:
                date_obj = datetime.strptime(date_string, fmt)
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        return None
    except Exception as e:
        return None

def create_hubspot_payload(solicitations_data):
    hubspot_inputs = []
    skipped_no_date = 0
    
    for i, solicitation in enumerate(solicitations_data):
        solicitation_copy = solicitation.copy()
        
        date_creation = solicitation_copy.get('created')
        date_evt = solicitation_copy.get('event_date')
        
        occurred_at = convert_date_to_iso(date_creation or date_evt)
        
        if not occurred_at:
            skipped_no_date += 1
            continue
        
        if solicitation_copy.get('created'):
            timestamp = convert_date_to_timestamp(solicitation_copy.get('created'))
            if timestamp:
                solicitation_copy['created'] = timestamp
        
        if solicitation_copy.get('updated'):
            timestamp = convert_date_to_timestamp(solicitation_copy.get('updated'))
            if timestamp:
                solicitation_copy['updated'] = timestamp
        
        if solicitation_copy.get('event_date'):
            date_formatted = convert_date_to_hubspot_date(solicitation_copy.get('event_date'))
            if date_formatted:
                solicitation_copy['event_date'] = date_formatted
            else:
                solicitation_copy.pop('event_date', None)
        
        if solicitation_copy.get('exp_response_date'):
            date_formatted = convert_date_to_hubspot_date(solicitation_copy.get('exp_response_date'))
            if date_formatted:
                solicitation_copy['exp_response_date'] = date_formatted
            else:
                solicitation_copy.pop('exp_response_date', None)

        clean_properties = {}
        for k, v in solicitation_copy.items(): 
            if k != 'expert_email': 
                if v != "" and v is not None:
                    clean_properties[k] = v
        
        hubspot_event = {
            "occurredAt": occurred_at,
            "eventName": "pe145807702_sollicitation_v2",
            "email": solicitation_copy.get('expert_email', ''),
            "properties": clean_properties
        }
        
        hubspot_inputs.append(hubspot_event)
    
    print(f"Événements ignorés (pas de date): {skipped_no_date}")
    print(f"Payload créé: {len(hubspot_inputs)} événements")
    return {"inputs": hubspot_inputs}

def send_solicitations_to_hubspot(payload, access_token, batch_size=100):    
    url = "https://api.hubapi.com/events/v3/send/batch"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    inputs = payload["inputs"]
    total_solicitations = len(inputs)
    successful_batches = 0
    total_sent = 0 
    
    for i in range(0, total_solicitations, batch_size):
        batch = inputs[i:i + batch_size]
        batch_payload = {"inputs": batch}
        batch_num = i//batch_size + 1
        batch_size_actual = len(batch)  
        
        try:
            response = requests.post(url, json=batch_payload, headers=headers)
            
            print(f"BATCH {batch_num}: {response.status_code} - {batch_size_actual} sollicitations")
            
            if response.status_code == 204:
                successful_batches += 1
                total_sent += batch_size_actual  
                print(f"BATCH {batch_num} réussi")
            else:
                print(f"ERREUR BATCH {batch_num}")
                try:
                    error_data = response.json()
                    print(f"Détails erreur: {json.dumps(error_data, indent=2)}")
                except:
                    print(f"Réponse brute: {response.text}")
                
        except Exception as e:
            print(f"erreur réseau batch {batch_num}: {e}")
        
        time.sleep(0.1) 
    
    total_batches = (total_solicitations + batch_size - 1) // batch_size
    print(f"batches réussis: {successful_batches}/{total_batches}")
    print(f"sollicitations envoyées: {total_sent}/{total_solicitations}") 
    
    return successful_batches > 0


try:    
    # récupérer les sollicitations existantes
    print("récupération des sollicitations existantes...")
    existing_solicitations = get_existing_solicitations(access_token)
    existing_solicitation_keys = extract_existing_solicitation_keys(existing_solicitations)
    
    #lire les données du CSV
    print("lecture des données CSV...")
    solicitations_data = read_solicitation_data(filename, max_solicitations)
    
    if not solicitations_data:
        print("aucune donnée à traiter")
        exit()
    
    #filtrer les nouvelles sollicitations
    print("filtrage des nouvelles sollicitations...")
    new_solicitations = filter_new_solicitations(solicitations_data, existing_solicitation_keys)
    
    if not new_solicitations:
        print("aucune nouvelle sollicitation à importer")
        exit()
    
    #créer le payload HubSpot
    print("création du payload HubSpot...")
    payload = create_hubspot_payload(new_solicitations)
    
    if payload['inputs']:
        print(f"envoi de {len(payload['inputs'])} nouvelles sollicitations...")
        result = send_solicitations_to_hubspot(payload, access_token, batch_size)
        
        if result:
            print("import terminé avec succès")
        else:
            print("import échoué")
    else:
        print("aucune sollicitation à envoyer après traitement")
        
except Exception as e:
    print(f"erreur générale: {e}")
    import traceback
    traceback.print_exc()
