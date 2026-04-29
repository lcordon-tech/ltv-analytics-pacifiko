# ============================================================================
# FILE: Run/Services/credential_migration.py
# NUEVO - VERIFICACIÓN DE MIGRACIÓN CSV → JSON
# ============================================================================
"""
Verifica la integridad de la migración de credenciales.
"""

import json
import csv
from pathlib import Path
from typing import Dict, List, Tuple


class CredentialMigrationVerifier:
    """Verifica que la migración CSV → JSON sea correcta."""
    
    @classmethod
    def get_csv_path(cls) -> Path:
        """Retorna ruta al CSV legacy."""
        from Run.Config.paths import Paths
        return Paths.get_data_xlsx_folder() / "credentials_vault.csv"
    
    @classmethod
    def get_json_path(cls) -> Path:
        """Retorna ruta al JSON de credenciales."""
        from Run.Security.user_manager import UserManager
        return UserManager._config_path
    
    @classmethod
    def read_csv_data(cls) -> Dict:
        """Lee datos del CSV."""
        csv_path = cls.get_csv_path()
        if not csv_path.exists():
            return {}
        
        data = {}
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            for row in rows:
                country = row.get('country', '').upper()
                if country:
                    data[country] = {
                        'db_user': row.get('db_user', ''),
                        'db_pass': row.get('db_pass', ''),
                        'ssh_cmd': row.get('ssh_cmd', ''),
                        'host': row.get('host', ''),
                        'db_name': row.get('db_name', '')
                    }
        except Exception as e:
            print(f"⚠️ Error leyendo CSV: {e}")
        
        return data
    
    @classmethod
    def read_json_data(cls) -> Dict:
        """Lee datos del JSON (CredentialStore)."""
        from Run.Config.credential_store import CredentialStore
        store_data = CredentialStore._load_json()
        
        data = {}
        countries = store_data.get('countries', {})
        for country, country_data in countries.items():
            data[country] = {
                'db_user': store_data.get('db', {}).get('user', ''),
                'db_pass': store_data.get('db', {}).get('password', ''),
                'ssh_cmd': country_data.get('ssh', ''),
                'host': country_data.get('host', ''),
                'db_name': country_data.get('database', '')
            }
        
        return data
    
    @classmethod
    def compare(cls) -> Tuple[bool, List[str]]:
        """
        Compara datos CSV vs JSON.
        
        Returns:
            (is_consistent, differences)
        """
        csv_data = cls.read_csv_data()
        json_data = cls.read_json_data()
        
        differences = []
        
        # Verificar países en CSV que no están en JSON
        for country in csv_data:
            if country not in json_data:
                differences.append(f"País {country} en CSV pero no en JSON")
        
        # Verificar países en JSON que no están en CSV
        for country in json_data:
            if country not in csv_data and json_data[country].get('db_user'):
                differences.append(f"País {country} en JSON pero no en CSV (puede ser normal)")
        
        # Comparar valores para países comunes
        for country in set(csv_data.keys()) & set(json_data.keys()):
            csv_row = csv_data[country]
            json_row = json_data[country]
            
            if csv_row.get('db_user') != json_row.get('db_user'):
                differences.append(f"{country}: db_user difiere (CSV={csv_row.get('db_user')}, JSON={json_row.get('db_user')})")
            
            if csv_row.get('ssh_cmd') != json_row.get('ssh_cmd'):
                if csv_row.get('ssh_cmd') and json_row.get('ssh_cmd'):
                    differences.append(f"{country}: ssh_cmd difiere")
        
        is_consistent = len(differences) == 0
        return is_consistent, differences
    
    @classmethod
    def print_summary(cls):
        """Imprime resumen de la verificación."""
        print("\n" + "=" * 60)
        print("   VERIFICACIÓN DE MIGRACIÓN CSV → JSON".center(60))
        print("=" * 60)
        
        csv_path = cls.get_csv_path()
        json_path = cls.get_json_path()
        
        print(f"\n📁 CSV source: {csv_path}")
        print(f"   Existe: {csv_path.exists()}")
        print(f"\n📁 JSON source: {json_path}")
        print(f"   Existe: {json_path.exists()}")
        
        is_consistent, differences = cls.compare()
        
        print("\n" + "-" * 40)
        if is_consistent:
            print("✅ Migración CONSISTENTE: CSV y JSON están sincronizados")
        else:
            print("⚠️ Migración INCONSISTENTE:")
            for diff in differences[:10]:
                print(f"   • {diff}")
            if len(differences) > 10:
                print(f"   ... y {len(differences) - 10} más")
            
            print("\n💡 Sugerencia: Ejecuta migrate_from_csv() para sincronizar")
        
        print("-" * 40)