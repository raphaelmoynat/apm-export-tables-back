import time
from dotenv import load_dotenv
import psycopg2
import os
import csv
from datetime import datetime
import yaml
import subprocess
import sys
import shutil
import requests

load_dotenv()

#bdd
SERVER = os.getenv("SERVER")
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))
DATABASE = os.getenv("DATABASE")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

#config
with open("/root/apm/infocentre/apm-export-tables-back/tables.yaml") as f:
    config = yaml.safe_load(f)

databases = config["tables"]

#postgresql
conn = psycopg2.connect(
    host=HOST,
    port=PORT,
    database=DATABASE,
    user=USERNAME,
    password=PASSWORD
)

chemin_export = '/root/apm/infocentre/apm-export-tables-back/exports'

IMPORT_SCRIPTS = {
    'dwh.mv_club': ['/root/apm/infocentre/apm-export-tables-back/import_club_hubdb.py', '/root/apm/infocentre/apm-export-tables-back/import_club_object.py'],
    'dwh.mv_region': ['/root/apm/infocentre/apm-export-tables-back/import_region.py'],
    'dwh.mv_expert': ['/root/apm/infocentre/apm-export-tables-back/import_contact.py expert'],
    'dwh.mv_permanent': ['/root/apm/infocentre/apm-export-tables-back/import_contact.py permanent'], 
    'dwh.mv_referent': ['/root/apm/infocentre/apm-export-tables-back/import_contact.py referent'],
    'dwh.mv_adherent_actif': ['/root/apm/infocentre/apm-export-tables-back/import_contact.py adherent_actif'],
    'dwh.mv_societe': ['/root/apm/infocentre/apm-export-tables-back/import_societe.py'],
    'dwh.mv_evt': ["/root/apm/infocentre/apm-export-tables-back/import_event_custom.py", "/root/apm/infocentre/apm-export-tables-back/import_event_marketing.py"],
    'dwh.mv_sollicitation': ["/root/apm/infocentre/apm-export-tables-back/import_sollicitation.py"],
    #'dwh.mv_participation': ["/root/apm/infocentre/apm-export-tables-back/import_participation.py"],
}

def clean_exports_directory():
    if os.path.exists(chemin_export):
        shutil.rmtree(chemin_export)
    
    os.makedirs(chemin_export, exist_ok=True)
    
    filtered_dir = 'apm/infocentre/apm-export-tables-back/filtered'
    if os.path.exists(filtered_dir):
        shutil.rmtree(filtered_dir)
    
    os.makedirs(filtered_dir, exist_ok=True)

def export(database):
    fichier_csv = os.path.join(chemin_export, f'{database}.csv')  
    # Créer une nouvelle connexion pour chaque export
    try:
        conn_local = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DATABASE,
            user=USERNAME,
            password=PASSWORD
        )
        
        cur = conn_local.cursor()
        copy_query = f"COPY (SELECT * FROM {database}) TO STDOUT WITH CSV HEADER"
        
        with open(fichier_csv, 'w', encoding='utf-8') as f:
            cur.copy_expert(copy_query, f)
        
        cur.execute(f"SELECT COUNT(*) FROM {database}")
        total_rows = cur.fetchone()[0]
        
        print(f"export : {fichier_csv} ({total_rows} lignes)")
        
        conn_local.close()  
        return True

    except Exception as e:
        print(f"erreur lors de l'export de {database} : {e}")
        if 'conn_local' in locals():
            conn_local.close()
        return False

def export_all_tables():
    print("début des exports...")
    success_count = 0
    failed_tables = []
    
    for name, table in databases.items():
        if export(table):
            success_count += 1
        else:
            failed_tables.append(table)
    
    print(f"exports terminés : {success_count}/{len(databases)} réussis")
    
    if failed_tables:
        print(f"tables échouées : {', '.join(failed_tables)}")
    
    # Continuer même si certains exports échouent
    return success_count, failed_tables

def run_import_script(script_command, table_name):
    #exécute un script d'import
    try:
        print(f"import de {table_name} en cours...")
        
        #séparer la commande et les arguments
        parts = script_command.split()
        script_file = parts[0]
        args = parts[1:] if len(parts) > 1 else []
        
        result = subprocess.run([sys.executable, script_file] + args, 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"import {table_name} réussi")
            return True
        else:
            print(f"erreur import {table_name}: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"erreur lors de l'exécution de l'import {table_name}: {e}")
        return False

def run_all_imports():
    print("début des imports...")
    success_count = 0    
    total_scripts = sum(len(scripts) if isinstance(scripts, list) else 1 for scripts in IMPORT_SCRIPTS.values())
    current_script = 0
    skipped_count = 0
    
    for table_name, scripts in IMPORT_SCRIPTS.items():
        csv_file = os.path.join(chemin_export, f'{table_name}.csv')
        if not os.path.exists(csv_file):
            print(f"fichier CSV manquant : {csv_file} - IGNORÉ")
            #compter les scripts ignorés
            scripts_count = len(scripts) if isinstance(scripts, list) else 1
            skipped_count += scripts_count
            current_script += scripts_count
            continue
        
        if isinstance(scripts, list):
            for script_command in scripts:
                current_script += 1
                if run_import_script(script_command, f"{table_name} ({script_command})"):
                    success_count += 1
                
                if current_script < total_scripts:
                    print(f"attente de 1 mn avant le prochain import... ")
                    time.sleep(60)
        else:
            current_script += 1
            if run_import_script(scripts, table_name):
                success_count += 1
            
            if current_script < total_scripts:
                print(f"attente de 1 mn avant le prochain import...")
                time.sleep(60)
    
    print(f"scripts ignorés (fichiers manquants) : {skipped_count}")
    return success_count, total_scripts - skipped_count

def send_log_slack(message):
    try:
        data = {"text": message}
        requests.post(SLACK_WEBHOOK_URL, json=data)
    except:
        print("Erreur envoi Slack")

def main():
    start_time = time.time()  
    print("start : ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    clean_exports_directory()
    
    #exporter toutes les tables 
    success_exports, failed_tables = export_all_tables()
    
    #lancer tous les imports possibles
    success_imports, total_possible_imports = run_all_imports()
    
    #calculer la durée
    end_time = time.time()
    duration_seconds = int(end_time - start_time)
    duration_minutes = duration_seconds // 60
    duration_remaining_seconds = duration_seconds % 60
    
    #résumé 
    print(f"tables exportées : {success_exports}/{len(databases)}")
    print(f"imports réussis : {success_imports}/{total_possible_imports}")
    print("end : ", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    #log slack simple
    if success_exports == len(databases) and success_imports == total_possible_imports:
        message = f"tables exportées : {len(databases)}\nimports réussis : {success_imports}/{total_possible_imports}\ndurée : {duration_minutes}m {duration_remaining_seconds}s"
    elif success_exports == 0:
        message = "APM: Échec des exports"
    else:
        message = f"tables exportées : {success_exports}/{len(databases)}\nimports réussis : {success_imports}/{total_possible_imports}\ndurée : {duration_minutes}m {duration_remaining_seconds}s"
    
    send_log_slack(message)
    
    print("terminé")

if __name__ == "__main__":
    main()
