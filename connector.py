from dotenv import load_dotenv
import psycopg2
import os
import csv
from datetime import datetime
import yaml
import subprocess
import sys
import shutil

load_dotenv()

#bdd
SERVER = os.getenv("SERVER")
HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))
DATABASE = os.getenv("DATABASE")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

#config
with open("tables.yaml") as f:
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

chemin_export = './exports'

IMPORT_SCRIPTS = {
    'dwh.mv_expert': ['import_contact.py expert'],
    'dwh.mv_permanent': ['import_contact.py permanent'], 
    'dwh.mv_referent': ['import_contact.py referent'],
    'dwh.mv_adherent': ['import_contact.py adherent'],
    'dwh.mv_societe': ['import_societe.py'],
    'dwh.mv_evt': ["import_event_custom.py", "import_event_marketing.py"],
}

def clean_exports_directory():
    if os.path.exists(chemin_export):
        shutil.rmtree(chemin_export)
    
    os.makedirs(chemin_export, exist_ok=True)
    
    filtered_dir = './filtered'
    if os.path.exists(filtered_dir):
        shutil.rmtree(filtered_dir)
    
    os.makedirs(filtered_dir, exist_ok=True)

def export(database):
    #exporte une table vers un fichier CSV
    fichier_csv = os.path.join(chemin_export, f'{database}.csv')  
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {database};")
        colonnes = [desc[0] for desc in cur.description]
        lignes = cur.fetchall()

        with open(fichier_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(colonnes)
            writer.writerows(lignes)

        print(f"export : {fichier_csv} ({len(lignes)} lignes)")
        return True

    except Exception as e:
        print(f"erreur lors de l'export de {database} : {e}")
        return False

def export_all_tables():
    #exporte toutes les tables configurées
    print("Début des exports...")
    success_count = 0
    
    for name, table in databases.items():
        if export(table):
            success_count += 1
    
    print(f"exports terminés : {success_count}/{len(databases)} réussis")
    return success_count == len(databases)

def run_import_script(script_command, table_name):
    #exécute un script d'import
    try:
        print(f"import de {table_name} en cours...")
        
        #séparer la commande et les arguments
        parts = script_command.split()
        script_file = parts[0]
        args = parts[1:] if len(parts) > 1 else []
        
        result = subprocess.run([sys.executable, script_file] + args, 
                              capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print(f"import {table_name} réussi")
            return True
        else:
            print(f"Erreur import {table_name}: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"Timeout pour l'import de {table_name}")
        return False
    except Exception as e:
        print(f"Erreur lors de l'exécution de l'import {table_name}: {e}")
        return False

def run_all_imports():
    print("début des imports...")
    success_count = 0    
    
    for table_name, scripts in IMPORT_SCRIPTS.items():
        csv_file = os.path.join(chemin_export, f'{table_name}.csv')
        if not os.path.exists(csv_file):
            print(f"Fichier CSV manquant : {csv_file}")
            continue
        
        if isinstance(scripts, list):
            for script_command in scripts:
                if run_import_script(script_command, f"{table_name} ({script_command})"):
                    success_count += 1
        else:
            if run_import_script(scripts, table_name):
                success_count += 1
    
    return success_count

def main():
    print("start")
    
    #nettoyer les dossiers
    clean_exports_directory()
    
    #exporter toutes les tables
    if not export_all_tables():
        print("échec")
        return
    
    #lancer tous les imports
    success_count = run_all_imports()
    
    #résumé final
    print(f"tables exportées : {len(databases)}")
    print(f"imports réussis : {success_count}/{len(IMPORT_SCRIPTS)}")
    
    if success_count == len(IMPORT_SCRIPTS):
        print("terminé")

if __name__ == "__main__":
    main()
