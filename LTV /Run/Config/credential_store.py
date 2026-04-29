# ============================================================================
# FILE: Run/Config/credential_store.py
# COMPLETO - CON DATA_XLSX COMO ORIGEN PARA CSV
# ============================================================================
"""
Almacenamiento persistente de credenciales - VERSIÓN HÍBRIDA
Estructura:
{
  "version": "2.0",
  "db": {"user": "...", "password": "..."},
  "countries": {
    "GT": {"ssh": "...", "host": "", "database": ""},
    "CR": {"ssh": "...", "host": "", "database": ""}
  },
  "users": {
    "alias": {
      "password_hash": "...",
      "created_at": "..."
    }
  }
}
"""

import json
import os
import csv
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List
from cryptography.fernet import Fernet


class CredentialStore:
    """Almacena credenciales de forma persistente y segura - HÍBRIDO."""
    
    STORE_DIR = Path(__file__).parent / "secure"
    STORE_FILE = STORE_DIR / "credentials.enc"
    KEY_FILE = STORE_DIR / ".key"
    BACKUP_DIR = STORE_DIR / "backups"
    
    # CSV legacy AHORA en data_xlsx
    @classmethod
    def _get_csv_path(cls) -> Path:
        from Run.Config.paths import Paths
        return Paths.get_data_xlsx_folder() / "credentials_vault.csv"
    
    @classmethod
    def _get_key(cls) -> bytes:
        """Obtiene o crea clave de cifrado."""
        cls.STORE_DIR.mkdir(parents=True, exist_ok=True)
        
        if cls.KEY_FILE.exists():
            with open(cls.KEY_FILE, 'rb') as f:
                return f.read()
        else:
            key = Fernet.generate_key()
            with open(cls.KEY_FILE, 'wb') as f:
                f.write(key)
            return key
    
    @classmethod
    def _get_cipher(cls) -> Fernet:
        return Fernet(cls._get_key())
    
    @classmethod
    def _get_default_structure(cls) -> Dict:
        """Retorna estructura por defecto."""
        return {
            "version": "2.0",
            "db": {"user": "", "password": ""},
            "countries": {
                "GT": {"ssh": "credential_store", "host": "127.0.0.1:3336", "database": "db_pacifiko"},
                "CR": {"ssh": "credential_store", "host": "127.0.0.1:3337", "database": "CRProdDb"}
            },
            "users": {}
        }
    
    @classmethod
    def _load_json(cls) -> Dict:
        """Carga credenciales desde JSON cifrado."""
        if not cls.STORE_FILE.exists():
            return cls._get_default_structure()
        
        try:
            cipher = cls._get_cipher()
            encrypted = cls.STORE_FILE.read_bytes()
            decrypted = cipher.decrypt(encrypted)
            data = json.loads(decrypted.decode())
            
            # Migrar si es necesario
            if data.get('version') != '2.0':
                data = cls._migrate_to_v2(data)
            
            return data
        except Exception as e:
            print(f"⚠️ Error cargando JSON: {e}")
            return cls._get_default_structure()
    
    @classmethod
    def _save_json(cls, data: Dict) -> bool:
        """Guarda credenciales en JSON cifrado."""
        try:
            cls.BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            
            # Backup antes de sobrescribir
            if cls.STORE_FILE.exists():
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_path = cls.BACKUP_DIR / f"credentials_{timestamp}.enc"
                shutil.copy2(cls.STORE_FILE, backup_path)
                # Limpiar backups viejos (más de 30 días)
                cls._cleanup_old_backups()
            
            cipher = cls._get_cipher()
            encrypted = cipher.encrypt(json.dumps(data, indent=2).encode())
            cls.STORE_FILE.write_bytes(encrypted)
            return True
        except Exception as e:
            print(f"❌ Error guardando JSON: {e}")
            return False
    
    @classmethod
    def _load_csv(cls) -> Optional[Dict]:
        """Carga credenciales desde CSV legacy (ahora en data_xlsx)."""
        csv_path = cls._get_csv_path()
        if not csv_path.exists():
            return None
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if not rows:
                return None
            
            # Convertir CSV a estructura nueva
            data = cls._get_default_structure()
            
            for row in rows:
                country = row.get('country', '').upper()
                if country in ['GT', 'CR']:
                    data['countries'][country]['ssh'] = row.get('ssh_cmd', '')
                    data['countries'][country]['host'] = row.get('host', f"127.0.0.1:333{6 if country == 'GT' else 7}")
                    data['countries'][country]['database'] = row.get('db_name', f"db_{country.lower()}")
                    
                    # Extraer DB creds del primer registro
                    if not data['db']['user'] and row.get('db_user'):
                        data['db']['user'] = row.get('db_user', '')
                        data['db']['password'] = row.get('db_pass', '')
            
            return data
        except Exception as e:
            print(f"⚠️ Error cargando CSV: {e}")
            return None
    
    @classmethod
    def _save_csv_legacy(cls, data: Dict):
        """Guarda credenciales en CSV legacy (dual-write) en data_xlsx."""
        csv_path = cls._get_csv_path()
        if not csv_path.parent.exists():
            csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            rows = []
            for country, country_data in data.get('countries', {}).items():
                rows.append({
                    'country': country,
                    'db_user': data.get('db', {}).get('user', ''),
                    'db_pass': data.get('db', {}).get('password', ''),
                    'alias_user': '',
                    'alias_pass': '',
                    'ssh_cmd': country_data.get('ssh', ''),
                    'host': country_data.get('host', ''),
                    'db_name': country_data.get('database', '')
                })
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['country', 'db_user', 'db_pass', 'alias_user', 
                                                       'alias_pass', 'ssh_cmd', 'host', 'db_name'])
                writer.writeheader()
                writer.writerows(rows)
            
            print(f"   📝 Dual-write: CSV actualizado en {csv_path}")
            return True
        except Exception as e:
            print(f"⚠️ Error guardando CSV legacy: {e}")
            return False
    
    @classmethod
    def _migrate_to_v2(cls, old_data: Dict) -> Dict:
        """Migra datos de versión anterior a v2."""
        print("🔧 Migrando credenciales a versión 2.0...")
        new_data = cls._get_default_structure()
        
        # Migrar DB creds
        if 'db' in old_data:
            new_data['db'] = old_data['db']
        
        # Migrar países
        if 'countries' in old_data:
            for country, values in old_data['countries'].items():
                if country in new_data['countries']:
                    new_data['countries'][country].update(values)
        
        # Migrar usuarios
        if 'users' in old_data:
            new_data['users'] = old_data['users']
        
        new_data['version'] = '2.0'
        return new_data
    
    @classmethod
    def _cleanup_old_backups(cls, days: int = 30):
        """Limpia backups más antiguos que 'days' días."""
        try:
            cutoff = datetime.now().timestamp() - (days * 86400)
            for backup in cls.BACKUP_DIR.glob("credentials_*.enc"):
                if backup.stat().st_mtime < cutoff:
                    backup.unlink()
        except Exception:
            pass
    
    @classmethod
    def migrate_from_csv(cls) -> bool:
        """Migra datos desde CSV a JSON si es necesario."""
        # Si JSON ya existe y tiene datos, no migrar
        if cls.STORE_FILE.exists():
            try:
                data = cls._load_json()
                if data.get('db', {}).get('user'):
                    print("✅ JSON ya tiene credenciales, no se necesita migración")
                    return True
            except:
                pass
        
        # Intentar migrar desde CSV
        csv_data = cls._load_csv()
        if csv_data and csv_data.get('db', {}).get('user'):
            print("🔄 Migrando credenciales desde CSV legacy...")
            # Crear backup del CSV antes de migrar
            csv_path = cls._get_csv_path()
            if csv_path.exists():
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_csv = csv_path.parent / f"credentials_vault_backup_{timestamp}.csv"
                shutil.copy2(csv_path, backup_csv)
                print(f"   📁 Backup CSV creado: {backup_csv.name}")
            
            if cls._save_json(csv_data):
                print("✅ Migración completada exitosamente")
                return True
            else:
                print("❌ Error durante migración")
        
        return False
    
    # ========== MÉTODOS PRINCIPALES ==========
    
    @classmethod
    def get_db_credentials(cls) -> Dict:
        """Obtiene credenciales de base de datos."""
        # Si falla JSON, intentar CSV
        data = cls._load_json()
        if not data.get('db', {}).get('user'):
            csv_data = cls._load_csv()
            if csv_data and csv_data.get('db', {}).get('user'):
                cls._save_json(csv_data)
                data = csv_data
        
        return data.get('db', {})
    
    @classmethod
    def save_db_credentials(cls, user: str, password: str) -> bool:
        """Guarda credenciales de base de datos (dual-write)."""
        data = cls._load_json()
        data['db']['user'] = user
        data['db']['password'] = password
        
        success_json = cls._save_json(data)
        success_csv = cls._save_csv_legacy(data)
        
        return success_json or success_csv
    
    @classmethod
    def get_ssh_command(cls, country_code: str) -> str:
        """Obtiene comando SSH para un país."""
        data = cls._load_json()
        countries = data.get('countries', {})
        country_data = countries.get(country_code.upper(), {})
        return country_data.get('ssh', '')
    
    @classmethod
    def save_ssh_command(cls, country_code: str, ssh_command: str) -> bool:
        """Guarda comando SSH para un país (dual-write)."""
        country_code = country_code.upper()
        data = cls._load_json()
        
        if 'countries' not in data:
            data['countries'] = {}
        if country_code not in data['countries']:
            data['countries'][country_code] = {}
        
        data['countries'][country_code]['ssh'] = ssh_command
        
        success_json = cls._save_json(data)
        success_csv = cls._save_csv_legacy(data)
        
        return success_json or success_csv
    
    @classmethod
    def get_host(cls, country_code: str) -> str:
        """Obtiene host para un país."""
        data = cls._load_json()
        countries = data.get('countries', {})
        country_data = countries.get(country_code.upper(), {})
        return country_data.get('host', f"127.0.0.1:333{6 if country_code == 'GT' else 7}")
    
    @classmethod
    def save_host(cls, country_code: str, host: str) -> bool:
        """Guarda host para un país."""
        country_code = country_code.upper()
        data = cls._load_json()
        
        if 'countries' not in data:
            data['countries'] = {}
        if country_code not in data['countries']:
            data['countries'][country_code] = {}
        
        data['countries'][country_code]['host'] = host
        
        return cls._save_json(data)
    
    @classmethod
    def get_database(cls, country_code: str) -> str:
        """Obtiene nombre de base de datos para un país."""
        data = cls._load_json()
        countries = data.get('countries', {})
        country_data = countries.get(country_code.upper(), {})
        return country_data.get('database', f"db_{country_code.lower()}")
    
    @classmethod
    def save_database(cls, country_code: str, database: str) -> bool:
        """Guarda nombre de base de datos para un país."""
        country_code = country_code.upper()
        data = cls._load_json()
        
        if 'countries' not in data:
            data['countries'] = {}
        if country_code not in data['countries']:
            data['countries'][country_code] = {}
        
        data['countries'][country_code]['database'] = database
        
        return cls._save_json(data)
    
    @classmethod
    def get_all_countries(cls) -> List[str]:
        """Retorna lista de países configurados."""
        data = cls._load_json()
        return list(data.get('countries', {}).keys())
    
    @classmethod
    def has_credentials(cls) -> bool:
        """Verifica si hay credenciales configuradas."""
        db_creds = cls.get_db_credentials()
        return bool(db_creds.get('user'))
    
    @classmethod
    def clear(cls):
        """Elimina todas las credenciales almacenadas."""
        if cls.STORE_FILE.exists():
            cls.STORE_FILE.unlink()
        print("🗑️ Credenciales eliminadas (JSON)")
        print("   CSV legacy conservado como backup en data_xlsx")
    
    @classmethod
    def get_user(cls, alias: str) -> Optional[Dict]:
        """Obtiene datos de un usuario."""
        data = cls._load_json()
        return data.get('users', {}).get(alias)
    
    @classmethod
    def save_user(cls, alias: str, user_data: Dict) -> bool:
        """Guarda datos de un usuario."""
        data = cls._load_json()
        if 'users' not in data:
            data['users'] = {}
        data['users'][alias] = user_data
        return cls._save_json(data)
    
    @classmethod
    def list_users(cls) -> List[str]:
        """Lista todos los usuarios."""
        data = cls._load_json()
        return list(data.get('users', {}).keys())
    
    @classmethod
    def delete_user(cls, alias: str) -> bool:
        """Elimina un usuario."""
        data = cls._load_json()
        if alias in data.get('users', {}):
            del data['users'][alias]
            return cls._save_json(data)
        return False
    
    @classmethod
    def get_country_config(cls, country_code: str) -> Dict:
        """Obtiene configuración completa de un país."""
        data = cls._load_json()
        countries = data.get('countries', {})
        return countries.get(country_code.upper(), {})
    
    @classmethod
    def update_country_config(cls, country_code: str, host: str, database: str, ssh_command: str) -> bool:
        """Actualiza configuración completa de un país."""
        country_code = country_code.upper()
        data = cls._load_json()
        
        if 'countries' not in data:
            data['countries'] = {}
        if country_code not in data['countries']:
            data['countries'][country_code] = {}
        
        data['countries'][country_code]['host'] = host
        data['countries'][country_code]['database'] = database
        data['countries'][country_code]['ssh'] = ssh_command
        
        success_json = cls._save_json(data)
        success_csv = cls._save_csv_legacy(data)
        
        return success_json or success_csv