import pandas as pd
import os

CONFIGURATIONS = [
    #event-club
    {
        'nom': 'Event-Club',
        'table_association': 'dwh.event_club.csv',
        'cle_gauche': 'event_key',
        'cle_droite': 'club_key',
        'table_gauche': 'dwh.mv_evt.csv',
        'colonne_gauche': 'pk_evt',
        'table_droite': 'dwh.mv_club.csv',
        'colonne_droite': 'PKClub'
    },
    
    #event-expert
    {
        'nom': 'Event-Expert',
        'table_association': 'dwh.event_expert.csv',
        'cle_gauche': 'event_key',
        'cle_droite': 'expert_key',
        'table_gauche': 'dwh.mv_evt.csv',
        'colonne_gauche': 'pk_evt',
        'table_droite': 'dwh.mv_expert.csv',
        'colonne_droite': 'PKExpert'  
    },
    
    #event-expertise
    {
        'nom': 'Event-Expertise',
        'table_association': 'dwh.event_expertise.csv',
        'cle_gauche': 'event_key',
        'cle_droite': 'expertise_key',
        'table_gauche': 'dwh.mv_evt.csv',
        'colonne_gauche': 'pk_evt',
        'table_droite': 'dwh.mv_expertise.csv',
        'colonne_droite': 'PKExpertise'
    },
    
    #expert-expertise
    {
        'nom': 'Expert-Expertise',
        'table_association': 'dwh.expert_expertise.csv',
        'cle_gauche': 'expertise_key',
        'cle_droite': 'expert_key',
        'table_gauche': 'dwh.mv_expert.csv',
        'colonne_gauche': 'PKExpert',  
        'table_droite': 'dwh.mv_expertise.csv',
        'colonne_droite': 'PKExpertise'
    }
]

BASE_PATH = '/root/apm/infocentre/apm-export-tables-back/exports/'
FILTERED_PATH = '/root/apm/infocentre/apm-export-tables-back/filtered/'

def check_associations():
    #créer le dossier filtered s'il n'existe pas
    os.makedirs(FILTERED_PATH, exist_ok=True)
    
    resultats_globaux = []
    
    for i, config in enumerate(CONFIGURATIONS, 1):
        print(f"\n{config['nom']}")
        print("-" * 50)
        
        try:
            #charger les fichiers
            association = pd.read_csv(os.path.join(BASE_PATH, config['table_association']))
            table_gauche = pd.read_csv(os.path.join(BASE_PATH, config['table_gauche']))
            table_droite = pd.read_csv(os.path.join(BASE_PATH, config['table_droite']))
            
            print(f"Fichiers chargés:")
            print(f"  - Association: {len(association)} lignes")
            print(f"  - Table gauche: {len(table_gauche)} lignes")
            print(f"  - Table droite: {len(table_droite)} lignes")
            
            #créer les sets de clés existantes
            cles_gauche_existantes = set(table_gauche[config['colonne_gauche']].astype(str))
            cles_droite_existantes = set(table_droite[config['colonne_droite']].astype(str))
            
            #récupérer les clés des associations
            cles_gauche_associations = set(association[config['cle_gauche']].astype(str))
            cles_droite_associations = set(association[config['cle_droite']].astype(str))
            
            #calculer les clés manquantes
            gauche_manquantes = cles_gauche_associations.difference(cles_gauche_existantes)
            droite_manquantes = cles_droite_associations.difference(cles_droite_existantes)
            
            # calculer les pourcentages
            pct_gauche_manquant = (len(gauche_manquantes) / len(cles_gauche_associations)) * 100 if len(cles_gauche_associations) > 0 else 0
            pct_droite_manquant = (len(droite_manquantes) / len(cles_droite_associations)) * 100 if len(cles_droite_associations) > 0 else 0
            
            # Afficher les résultats
            print(f"résultats:")
            print(f"  - Clés gauche ({config['cle_gauche']}) manquantes: {pct_gauche_manquant:.1f}%")
            print(f"    ({len(gauche_manquantes)} sur {len(cles_gauche_associations)})")
            print(f"  - Clés droite ({config['cle_droite']}) manquantes: {pct_droite_manquant:.1f}%")
            print(f"    ({len(droite_manquantes)} sur {len(cles_droite_associations)})")
            
            #filtrer les associations pour ne garder que les clés existantes
            association_str = association.copy()
            association_str[config['cle_gauche']] = association_str[config['cle_gauche']].astype(str)
            association_str[config['cle_droite']] = association_str[config['cle_droite']].astype(str)
            
            association_filtree = association_str[
                (association_str[config['cle_gauche']].isin(cles_gauche_existantes)) &
                (association_str[config['cle_droite']].isin(cles_droite_existantes))
            ]
            
            #sauvegarder le fichier filtré
            nom_fichier_filtre = config['table_association']
            chemin_filtre = os.path.join(FILTERED_PATH, nom_fichier_filtre)
            
            print(f"Sauvegarde vers: {chemin_filtre}")
            association_filtree.to_csv(chemin_filtre, index=False)
            
            
            print(f"  - Lignes avant filtrage: {len(association)}")
            print(f"  - Lignes après filtrage: {len(association_filtree)}")
            print(f"  - Lignes supprimées: {len(association) - len(association_filtree)}")
            
            #stocker les résultats
            resultats_globaux.append({
                'nom': config['nom'],
                'cle_gauche': config['cle_gauche'],
                'pct_gauche_manquant': pct_gauche_manquant,
                'nb_gauche_manquant': len(gauche_manquantes),
                'total_gauche': len(cles_gauche_associations),
                'cle_droite': config['cle_droite'],
                'pct_droite_manquant': pct_droite_manquant,
                'nb_droite_manquant': len(droite_manquantes),
                'total_droite': len(cles_droite_associations),
                'lignes_avant': len(association),
                'lignes_apres': len(association_filtree),
                'statut': 'OK'
            })
            
        except Exception as e:
            print(f"erreur: {e}")
            import traceback
            traceback.print_exc()
            resultats_globaux.append({
                'nom': config['nom'],
                'erreur': str(e),
                'statut': 'ERREUR'
            })
    
    #résumé global
    print("\n" + "="*70)
    print("Résumé :")
    print("="*70)
    
    for resultat in resultats_globaux:
        if resultat['statut'] == 'OK':
            print(f"\n{resultat['nom']}:")
            print(f"   {resultat['cle_gauche']}: {resultat['pct_gauche_manquant']:.1f}% manquant")
            print(f"   {resultat['cle_droite']}: {resultat['pct_droite_manquant']:.1f}% manquant")
            print(f"   Filtrage: {resultat['lignes_avant']} -> {resultat['lignes_apres']} lignes")
        else:
            print(f"\n{resultat['nom']}: {resultat['erreur']}")

if __name__ == "__main__":
    check_associations()
