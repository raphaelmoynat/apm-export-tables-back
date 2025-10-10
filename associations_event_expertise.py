import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

#config
HUBSPOT_API_KEY = os.getenv("PROD_KEY")
CSV_FILE = "/root/apm/infocentre/apm-export-tables-back/filtered/dwh.event_expertise.csv"  

#id des objets custom
EXPERTISE_OBJECT_ID = "2-140990150"  
EVENT_OBJECT_ID = "2-139503358"  


#rechercher plusieurs objets à partir de leur pk
def search_objects_batch(object_id, property_name, values, headers):
    response = requests.post(
        f"https://api.hubapi.com/crm/v3/objects/{object_id}/search",
        headers=headers,
        json={
            "filterGroups": [{
                "filters": [{
                    "propertyName": property_name,
                    "operator": "IN",
                    "values": [str(value) for value in values]
                }]
            }],
            "limit": 100,
            "properties": [property_name]
        }
    )
    
    return response

#créé plusieurs associations en batch
def create_associations_batch(associations, headers):
    batch_data = {
        "inputs": []
    }
    
    for assoc in associations:
        batch_data["inputs"].append({
            "from": {"id": assoc["expertise_id"]},
            "to": {"id": assoc["event_id"]},
            "types": [{
                "associationCategory": "USER_DEFINED",
                "associationTypeId": 144
            }]
        })
    
    response = requests.post(
        f"https://api.hubapi.com/crm/v4/associations/{EXPERTISE_OBJECT_ID}/{EVENT_OBJECT_ID}/batch/create",
        headers=headers,
        json=batch_data
    )
    
    return response


def process_batch(batch_data, debug_first=False):
    headers = {'Authorization': f'Bearer {HUBSPOT_API_KEY}', 'Content-Type': 'application/json'}
    results = []
    
    #extraire les valeurs uniques pour les recherches batch
    unique_expertises = list(set(str(row['expertise_key']) for _, row in batch_data.iterrows()))
    unique_events = list(set(str(row['event_key']) for _, row in batch_data.iterrows()))
    
    if debug_first:
        print(f"Recherche de {len(unique_expertises)} expertises et {len(unique_events)} events uniques")
    
    #rechercher toutes les expertises
    expertise_response = search_objects_batch(EXPERTISE_OBJECT_ID, "pk_expertise", unique_expertises, headers)
    
    if expertise_response.status_code != 200:
        print(f"Erreur recherche expertises: {expertise_response.status_code}")
        #retourner des résultats avec la structure correcte
        return [{
            'index': index,
            'pk_expertise': str(row['expertise_key']),
            'pk_event': str(row['event_key']),
            'result': 'erreur batch expertises'
        } for index, row in batch_data.iterrows()]
    
    #créer un mapping pk_expertise -> id
    expertise_mapping = {}
    for expertise in expertise_response.json().get('results', []):
        pk_expertise = expertise['properties']['pk_expertise']
        expertise_mapping[pk_expertise] = expertise['id']
    
    if debug_first:
        print(f"Expertises trouvées: {len(expertise_mapping)}/{len(unique_expertises)}")
    
    #rechercher tous les events 
    event_response = search_objects_batch(EVENT_OBJECT_ID, "pk_event", unique_events, headers)
    
    if event_response.status_code != 200:
        print(f"Erreur recherche events: {event_response.status_code}")
        #retourner des résultats avec la structure correcte
        return [{
            'index': index,
            'pk_expertise': str(row['expertise_key']),
            'pk_event': str(row['event_key']),
            'result': 'erreur batch events'
        } for index, row in batch_data.iterrows()]
    
    #créer un mapping pk_event -> id
    event_mapping = {}
    for event in event_response.json().get('results', []):
        pk_event = event['properties']['pk_event']
        event_mapping[pk_event] = event['id']
    
    if debug_first:
        print(f"Events trouvés: {len(event_mapping)}/{len(unique_events)}")
    
    #préparer les associations valides
    valid_associations = []
    
    for index, row in batch_data.iterrows():
        pk_expertise = str(row['expertise_key'])
        pk_event = str(row['event_key'])
        
        expertise_id = expertise_mapping.get(pk_expertise)
        event_id = event_mapping.get(pk_event)
        
        if not expertise_id:
            results.append({
                'index': index,
                'pk_expertise': pk_expertise,
                'pk_event': pk_event,
                'result': 'expertise introuvable'
            })
        elif not event_id:
            results.append({
                'index': index,
                'pk_expertise': pk_expertise,
                'pk_event': pk_event,
                'result': 'event introuvable'
            })
        else:
            valid_associations.append({
                'index': index,
                'pk_expertise': pk_expertise,
                'pk_event': pk_event,
                'expertise_id': expertise_id,
                'event_id': event_id
            })
    
    #créer les associations en batch 
    batch_size = 100
    for i in range(0, len(valid_associations), batch_size):
        batch_assocs = valid_associations[i:i+batch_size]
        
        if debug_first and i == 0:
            print(f"Création de {len(batch_assocs)} associations en batch")
        
        assoc_response = create_associations_batch(batch_assocs, headers)
        
        if debug_first:
            print(f"Réponse associations: {assoc_response.status_code}")
            if assoc_response.status_code != 201:
                print(f"Détail erreur: {assoc_response.text}")
        
        if assoc_response.status_code == 201:
            #succès pour toutes les associations du batch
            for assoc in batch_assocs:
                results.append({
                    'index': assoc['index'],
                    'pk_expertise': assoc['pk_expertise'],
                    'pk_event': assoc['pk_event'],
                    'result': 'ok'
                })
        elif assoc_response.status_code == 207:
            batch_results = assoc_response.json().get('results', [])
            for j, assoc in enumerate(batch_assocs):
                if j < len(batch_results):
                    batch_result = batch_results[j]
                    if 'id' in batch_result:
                        result = 'ok'
                    elif 'status' in batch_result and batch_result['status'] == 'COMPLETE':
                        result = 'ok'
                    else:
                        result = 'erreur association'
                else:
                    result = 'erreur association'
                
                results.append({
                    'index': assoc['index'],
                    'pk_expertise': assoc['pk_expertise'],
                    'pk_event': assoc['pk_event'],
                    'result': result
                })
        else:
            error_msg = f"erreur batch associations ({assoc_response.status_code})"
            for assoc in batch_assocs:
                results.append({
                    'index': assoc['index'],
                    'pk_expertise': assoc['pk_expertise'],
                    'pk_event': assoc['pk_event'],
                    'result': error_msg
                })
        
        #pause entre les batchs d'associations pour respecter les limites
        if i + batch_size < len(valid_associations):
            time.sleep(0.2)
    
    return results

def main():
    #lire le CSV
    df = pd.read_csv(CSV_FILE, low_memory=False)
    total = len(df)
    print(f"Traitement de {total} lignes")
    
    #config du traitement par batch
    batch_size = 100
    
    success = 0
    exists = 0
    errors = 0
    
    #traiter par batch
    for i in range(0, total, batch_size):
        batch_end = min(i + batch_size, total)
        batch_data = df.iloc[i:batch_end]
        
        print(f"Traitement du batch {i//batch_size + 1} (lignes {i+1} à {batch_end})")
        
        # Debug pour le premier batch seulement
        debug_first = (i == 0)
        
        # Traiter le batch
        batch_results = process_batch(batch_data, debug_first=debug_first)
        
        # Compter les résultats du batch
        for result in batch_results:
            if result['result'] == "ok":
                success += 1
                status = "réussi"
            elif result['result'] == "existe déjà":
                exists += 1
                status = "existe déjà"
            else:
                errors += 1
                status = "erreur"
                    
        #pause entre les batch
        if batch_end < total:
            print(f"Pause entre les batchs...")
            time.sleep(1)
    
    print(f"\nTerminé:")
    print(f"Créées: {success}")
    print(f"Existantes: {exists}")
    print(f"Erreurs: {errors}")
    print(f"Total traité: {success + exists + errors}")

if __name__ == "__main__":
    main()
