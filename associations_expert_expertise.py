import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

#config
HUBSPOT_API_KEY = os.getenv("PROD_KEY")
CSV_FILE = "/root/apm/infocentre/apm-export-tables-back/filtered/dwh.expert_expertise.csv"  

#id des objets custom
EXPERT_OBJECT_ID = "0-1"  
EXPERTISE_OBJECT_ID = "2-140990150"  


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
            "from": {"id": assoc["expert_id"]},
            "to": {"id": assoc["expertise_id"]},
            "types": [{
                "associationCategory": "USER_DEFINED",
                "associationTypeId": 143
            }]
        })
    
    response = requests.post(
        f"https://api.hubapi.com/crm/v4/associations/{EXPERT_OBJECT_ID}/{EXPERTISE_OBJECT_ID}/batch/create",
        headers=headers,
        json=batch_data
    )
    
    return response


def process_batch(batch_data, debug_first=False):
    headers = {'Authorization': f'Bearer {HUBSPOT_API_KEY}', 'Content-Type': 'application/json'}
    results = []
    
    # INVERSION : expert_key contient les IDs d'expertise, expertise_key contient les IDs d'expert
    unique_experts = list(set(str(row['expertise_key']) for _, row in batch_data.iterrows()))  # INVERSÉ
    unique_expertises = list(set(str(row['expert_key']) for _, row in batch_data.iterrows()))  # INVERSÉ
    
    if debug_first:
        print(f"Recherche de {len(unique_experts)} experts et {len(unique_expertises)} expertises uniques")
    
    #rechercher tous les experts (avec les valeurs de la colonne expertise_key)
    expert_response = search_objects_batch(EXPERT_OBJECT_ID, "pk_expert", unique_experts, headers)
    
    if expert_response.status_code != 200:
        print(f"Erreur recherche experts: {expert_response.status_code}")
        #retourner des résultats avec la structure correcte
        return [{
            'index': index,
            'pk_expert': str(row['expertise_key']),  # INVERSÉ
            'pk_expertise': str(row['expert_key']),  # INVERSÉ
            'result': 'erreur batch experts'
        } for index, row in batch_data.iterrows()]
    
    #créer un mapping pk_expert -> id
    expert_mapping = {}
    for expert in expert_response.json().get('results', []):
        pk_expert = expert['properties']['pk_expert']
        expert_mapping[pk_expert] = expert['id']
    
    if debug_first:
        print(f"Experts trouvés: {len(expert_mapping)}/{len(unique_experts)}")
    
    #rechercher toutes les expertises (avec les valeurs de la colonne expert_key)
    expertise_response = search_objects_batch(EXPERTISE_OBJECT_ID, "pk_expertise", unique_expertises, headers)
    
    if expertise_response.status_code != 200:
        print(f"Erreur recherche expertises: {expertise_response.status_code}")
        #retourner des résultats avec la structure correcte
        return [{
            'index': index,
            'pk_expert': str(row['expertise_key']),  # INVERSÉ
            'pk_expertise': str(row['expert_key']),  # INVERSÉ
            'result': 'erreur batch expertises'
        } for index, row in batch_data.iterrows()]
    
    #créer un mapping pk_expertise -> id
    expertise_mapping = {}
    for expertise in expertise_response.json().get('results', []):
        pk_expertise = expertise['properties']['pk_expertise']
        expertise_mapping[pk_expertise] = expertise['id']
    
    if debug_first:
        print(f"Expertises trouvées: {len(expertise_mapping)}/{len(unique_expertises)}")
    
    #préparer les associations valides
    valid_associations = []
    
    for index, row in batch_data.iterrows():
        # INVERSION : expert_key contient l'ID d'expertise, expertise_key contient l'ID d'expert
        pk_expert = str(row['expertise_key'])      # INVERSÉ
        pk_expertise = str(row['expert_key'])      # INVERSÉ
        
        expert_id = expert_mapping.get(pk_expert)
        expertise_id = expertise_mapping.get(pk_expertise)
        
        if not expert_id:
            results.append({
                'index': index,
                'pk_expert': pk_expert,
                'pk_expertise': pk_expertise,
                'result': 'expert introuvable'
            })
        elif not expertise_id:
            results.append({
                'index': index,
                'pk_expert': pk_expert,
                'pk_expertise': pk_expertise,
                'result': 'expertise introuvable'
            })
        else:
            valid_associations.append({
                'index': index,
                'pk_expert': pk_expert,
                'pk_expertise': pk_expertise,
                'expert_id': expert_id,
                'expertise_id': expertise_id
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
                    'pk_expert': assoc['pk_expert'],
                    'pk_expertise': assoc['pk_expertise'],
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
                    'pk_expert': assoc['pk_expert'],
                    'pk_expertise': assoc['pk_expertise'],
                    'result': result
                })
        else:
            error_msg = f"erreur batch associations ({assoc_response.status_code})"
            for assoc in batch_assocs:
                results.append({
                    'index': assoc['index'],
                    'pk_expert': assoc['pk_expert'],
                    'pk_expertise': assoc['pk_expertise'],
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
    
    # Afficher les premières lignes pour vérifier l'inversion
    print("Premières lignes du CSV (avec inversion) :")
    for i in range(min(3, len(df))):
        print(f"  Ligne {i+1}: expert_key={df.iloc[i]['expert_key']} -> expertise_key={df.iloc[i]['expertise_key']}")
    print()
    
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
