import csv
import json
import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timezone
import time

load_dotenv()

# Config
filename = "/root/apm/infocentre/apm-export-tables-back/exports/dwh.mv_participation.csv"  
access_token = os.getenv("PROD_KEY")
max_participations = 1
batch_size = 10

def get_existing_participations(access_token, event_type="pe144476884_evenement___participation", limit=100):    
    url = f"https://api.hubapi.com/events/v3/events"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    all_participations = []
    after = None
    page_count = 0

    print(f"début récupérations des participations")
        
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
                participations = data.get("results", [])
                all_participations.extend(participations)
                
            
                
                
                paging = data.get("paging", {})
                if "next" in paging:
                    after = paging["next"].get("after")
                    time.sleep(0.1)  
                else:
                    print("Fin de pagination atteinte")
                    break
            else:
                print(f"erreur api : {response.status_code} - {response.text}")
                break
                
        except Exception as e:
            print(f"erreur récupération: {e}")
            break
    
    print(f"total : {len(all_participations)} participations existantes\n")
    return all_participations

def filter_new_participations(participations_data, existing_participation_keys):
    new_participations = []
    skipped_count = 0
    duplicates = []
    
    for i, participation in enumerate(participations_data):
        pkparticipation = participation.get('pkparticipation')
        
        if not pkparticipation:
            print(f"participation sans PK à l'index {i}")
            continue
        
        if pkparticipation in existing_participation_keys:
            skipped_count += 1
            duplicates.append(pkparticipation)
            continue
        
        new_participations.append(participation)
    
    print(f"participations ignorées (déjà existantes): {skipped_count}")
    
    #vérifier les clés des nouvelles participations
    new_keys = [p.get('pkparticipation') for p in new_participations]
    
    return new_participations


def extract_existing_participation_keys(existing_participations):
    existing_keys = set()
    
    for participation in existing_participations:
        properties = participation.get("properties", {})
        pkparticipation = properties.get("pkparticipation")
        if pkparticipation:
            existing_keys.add(pkparticipation)
    return existing_keys

def read_participation_data(filename, max_rows=100):
    columns_mapping = {
        'PKParticipation': 'pkparticipation',
        'FK_Membre': 'fk_membre', 
        'email': 'email',
        'FK_Evt': 'fk_evt',
        'DateEvt': 'dateevt',
        'FlagPresent': 'flagpresent',
        'FlagCandidat': 'flagcandidat',
        'FlagExpert': 'flagexpert',
        'FlagInvite': 'flaginvite',
        'FlagAnimateur': 'flaganimateur',
        'FlagPermanent': 'flagpermanent',
        'FlagPresident': 'flagpresident',
        'FlagAdherent': 'flagadherent',
        'MotifAnnulation': 'motifannulation',
        'IdAdh': 'idadh',
        'IdRenc': 'idrenc',
        'DateMAJ': 'datemaj'
    }
    
    participations_data = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter=',')
                        
            for row in reader:

                
                participation_data = {}
                
                for csv_column, hubspot_property in columns_mapping.items():
                    if csv_column in row:
                        value = row[csv_column].strip() if row[csv_column] else ""
                        if hubspot_property == 'motifannulation':
                            if not value or value.lower() in ['null', 'none']:
                                participation_data[hubspot_property]= ""  
                            else:
                                participation_data[hubspot_property] = value
                        
                        if csv_column.startswith('Flag'):
                            converted_value = convert_to_boolean(value)
                            if converted_value != "":
                                participation_data[hubspot_property] = converted_value
                        elif value and value.lower() not in ['null', 'none', '']:
                            participation_data[hubspot_property] = value
                
                participations_data.append(participation_data)

    
        print(f"\nLu {len(participations_data)} participations du CSV\n")
        
    except Exception as e:
        print(f"erreur lecture CSV: {e}")
        
    return participations_data


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

def convert_date_to_iso(date_string):
    if not date_string or date_string.lower() in ['null', 'none', '']:
        return None
    
    try:
        formats = [  '%Y-%m-%dT%H:%M:%S',  '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y']
        
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

def create_hubspot_payload(participations_data):
    hubspot_inputs = []
    skipped_no_date = 0
    
    for i, participation in enumerate(participations_data):
        participation_copy = participation.copy()
        
        date_creation = participation_copy.get('datecreation')
        date_evt = participation_copy.get('dateevt')
        
        occurred_at = convert_date_to_iso(date_creation or date_evt)
        
        if not occurred_at:
            skipped_no_date += 1
            continue
        
        if participation_copy.get('datemaj'):
            timestamp = convert_date_to_timestamp(participation_copy.get('datemaj'))
            if timestamp:
                participation_copy['datemaj'] = timestamp  
        
        if participation_copy.get('dateevt'):
            date_formatted = convert_date_to_hubspot_date(participation_copy.get('dateevt'))
            if date_formatted:
                participation_copy['dateevt'] = date_formatted
            else:
                participation_copy.pop('dateevt', None)

        clean_properties = {}
        for k, v in participation_copy.items(): 
            if k != 'email':
                if k == 'motifannulation':
                    clean_properties[k] = v
                elif v != "" and v is not None:
                    clean_properties[k] = v
        
        hubspot_event = {
            "occurredAt": occurred_at,
            "eventName": "pe144476884_evenement___participation",
            "email": participation_copy.get('email', ''),
            "properties": clean_properties
        }
        
        hubspot_inputs.append(hubspot_event)
    
    print(f"\nPayload créé: {len(hubspot_inputs)} événements")
    return {"inputs": hubspot_inputs}


def send_participations_to_hubspot(payload, access_token, batch_size=25):    
    url = "https://api.hubapi.com/events/v3/send/batch"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    inputs = payload["inputs"]
    total_participations = len(inputs)
    successful_batches = 0
    
    for i in range(0, total_participations, batch_size):
        batch = inputs[i:i + batch_size]
        batch_payload = {"inputs": batch}
        batch_num = i//batch_size + 1
        
        success = False
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                print(f"Envoi du batch {batch_num}/{(total_participations + batch_size - 1) // batch_size}")
                response = requests.post(url, json=batch_payload, headers=headers, timeout=30)
                
                if response.status_code == 204:
                    print(f"Batch {batch_num} réussi")
                    successful_batches += 1
                    success = True
                    break
                else:
                    print(f"Erreur batch {batch_num}: {response.status_code}")
                    
            except Exception as e:
                print(f"ERREUR RÉSEAU BATCH {batch_num}: {e}")
                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1)
                    print(f"Nouvelle tentative dans {wait_time} secondes...")
                    time.sleep(wait_time)
        
        # pause entre les lots pour éviter les problèmes de débit
        time.sleep(1)
    
    total_batches = (total_participations + batch_size - 1) // batch_size
    print(f"Batches réussis: {successful_batches}/{total_batches}")
    print(f"Participations envoyées: {successful_batches * batch_size}/{total_participations}")
    
    return successful_batches > 0



try:
    existing_participations = get_existing_participations(access_token)
    existing_participation_keys = extract_existing_participation_keys(existing_participations)
    participations_data = read_participation_data(filename, max_participations)
    
    if not participations_data:
        exit()
    
    new_participations = filter_new_participations(participations_data, existing_participation_keys)
    
    if not new_participations:
        exit()
    
    payload = create_hubspot_payload(new_participations)
    
    if payload['inputs']:
        print(f"envoie de {len(payload['inputs'])} nouvelles participations")
        result = send_participations_to_hubspot(payload, access_token, batch_size)
        
        if result:
            print("import terminé")
        else:
            print("import échoué")
    else:
        print("aucune participation à envoyer après traitement")
        
except Exception as e:
    import traceback
    traceback.print_exc()
