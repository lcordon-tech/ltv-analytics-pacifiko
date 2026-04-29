# ============================================================================
# FILE: Run/Config/vault_manager.py
# LEGACY - CON DATA_XLSX COMO ORIGEN
# ============================================================================
"""
Manejo seguro de credenciales - VERSIÓN LEGACY (DEPRECATED)

⚠️ DEPRECATED: Este archivo se mantiene por compatibilidad.
   Priorizar el uso de CredentialStore (JSON cifrado).
   Los CSV se leen/escriben en data_xlsx/credentials_vault.csv
"""

import csv
import subprocess
import warnings
from pathlib import Path
from typing import Optional, Dict, List

# Deprecation warning
warnings.warn(
    "VaultManager está deprecated. Usar CredentialStore en su lugar.",
    DeprecationWarning,
    stacklevel=2
)


class VaultManager:
    """
    Manejo seguro de credenciales - DEPRECATED.
    AHORA: Los CSV se almacenan en data_xlsx/credentials_vault.csv
    """
    
    def __init__(self):
        # Usar data_xlsx como ubicación principal
        from Run.Config.paths import Paths
        self.vault_path = Paths.get_data_xlsx_folder() / "credentials_vault.csv"
        self._ensure_vault_exists()
        
        # Importar CredentialStore para dual-write
        try:
            from .credential_store import CredentialStore
            self._cred_store = CredentialStore
        except ImportError:
            self._cred_store = None
    
    def _ensure_vault_exists(self):
        self.vault_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.vault_path.exists():
            with open(self.vault_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['country', 'db_user', 'db_pass', 'alias_user', 'alias_pass', 
                               'ssh_cmd', 'host', 'db_name'])
    
    def validate_db_connection(self, user: str, password: str, host: str, database: str) -> bool:
        """Valida conexión a BD."""
        try:
            import socket
            socket.setdefaulttimeout(5)
            
            import pymysql
            connection = pymysql.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                connect_timeout=5
            )
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            connection.close()
            return True
        except ImportError:
            print("⚠️ pymysql no instalado. Saltando validación BD.")
            return True
        except Exception as e:
            print(f"⚠️ BD no disponible: {e}")
            respuesta = input("¿Continuar de todas formas? (s/n): ").strip().lower()
            return respuesta in ['s', 'si', 'sí', 'yes', 'y']
    
    def validate_ssh_connection(self, ssh_cmd: str) -> bool:
        """Valida conexión SSH."""
        if not ssh_cmd or ssh_cmd == "":
            return True
        
        try:
            result = subprocess.run(
                ['ssh', '-q', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', ssh_cmd, 'exit'],
                timeout=6,
                capture_output=True
            )
            return result.returncode == 0
        except Exception:
            print("⚠️ SSH no disponible. Continuando en modo local.")
            return True
    
    def save_credentials(self, credentials: Dict) -> bool:
        """Guarda credenciales (dual-write: CSV + JSON)."""
        # Escribir en CSV
        try:
            rows = []
            exists = False
            
            if self.vault_path.exists():
                with open(self.vault_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
            
            country = credentials.get('country', 'CR')
            alias = credentials.get('alias_user', '')
            
            for i, row in enumerate(rows):
                if row.get('country') == country and row.get('alias_user') == alias:
                    rows[i] = credentials
                    exists = True
                    break
            
            if not exists:
                rows.append(credentials)
            
            with open(self.vault_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['country', 'db_user', 'db_pass', 'alias_user', 
                                                       'alias_pass', 'ssh_cmd', 'host', 'db_name'])
                writer.writeheader()
                writer.writerows(rows)
            
            print(f"✅ Credenciales guardadas en CSV para: {country} - {alias}")
            print(f"   📁 Ubicación: {self.vault_path}")
            
            # Dual-write a JSON si está disponible
            if self._cred_store:
                self._cred_store.update_country_config(
                    country,
                    credentials.get('host', ''),
                    credentials.get('db_name', ''),
                    credentials.get('ssh_cmd', '')
                )
                print(f"   ✅ También guardado en JSON")
            
            return True
        except Exception as e:
            print(f"❌ Error guardando vault: {e}")
            return False
    
    def get_credentials(self, country: str, user: str, password: str) -> Optional[Dict]:
        """Obtiene credenciales por país y usuario."""
        if not self.vault_path.exists():
            return None
        
        with open(self.vault_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('country') != country:
                    continue
                if (row['db_user'] == user and row['db_pass'] == password) or \
                   (row['alias_user'] == user and row['alias_pass'] == password):
                    return row
        return None
    
    def get_first_credentials(self, country: Optional[str] = None) -> Optional[Dict]:
        """
        Obtiene el primer registro del vault para un país.
        PRIORIDAD: JSON primero, CSV como fallback.
        """
        # Primero intentar desde JSON
        if self._cred_store:
            db_creds = self._cred_store.get_db_credentials()
            if db_creds and db_creds.get('user'):
                # Construir dict compatible con el formato esperado
                target_country = country or 'GT'
                ssh_cmd = self._cred_store.get_ssh_command(target_country) if country else ""
                host = self._cred_store.get_host(target_country) if country else "localhost"
                database = self._cred_store.get_database(target_country) if country else ""
                
                return {
                    'country': target_country,
                    'db_user': db_creds.get('user', ''),
                    'db_pass': db_creds.get('password', ''),
                    'alias_user': '',
                    'alias_pass': '',
                    'ssh_cmd': ssh_cmd,
                    'host': host,
                    'db_name': database
                }
        
        # Fallback a CSV
        if not self.vault_path.exists():
            return None
        
        with open(self.vault_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            if country:
                country_upper = country.upper()
                for row in rows:
                    if row.get('country', '').upper() == country_upper:
                        return {
                            'country': row.get('country', country),
                            'db_user': row.get('db_user', ''),
                            'db_pass': row.get('db_pass', ''),
                            'alias_user': row.get('alias_user', ''),
                            'alias_pass': row.get('alias_pass', ''),
                            'ssh_cmd': row.get('ssh_cmd', ''),
                            'host': row.get('host', 'localhost'),
                            'db_name': row.get('db_name', ''),
                        }
                return None
            
            if rows:
                row = rows[0]
                return {
                    'country': row.get('country', 'GT'),
                    'db_user': row.get('db_user', ''),
                    'db_pass': row.get('db_pass', ''),
                    'alias_user': row.get('alias_user', ''),
                    'alias_pass': row.get('alias_pass', ''),
                    'ssh_cmd': row.get('ssh_cmd', ''),
                    'host': row.get('host', 'localhost'),
                    'db_name': row.get('db_name', ''),
                }
            
            return None
    
    def get_all_countries(self) -> List[str]:
        """Retorna lista de países con credenciales configuradas."""
        # Intentar desde JSON primero
        if self._cred_store:
            countries = self._cred_store.get_all_countries()
            if countries:
                return countries
        
        # Fallback a CSV
        if not self.vault_path.exists():
            return []
        
        countries = set()
        with open(self.vault_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('country'):
                    countries.add(row['country'])
        return sorted(list(countries))