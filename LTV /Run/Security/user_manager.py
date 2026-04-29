# ============================================================================
# FILE: Run/Security/user_manager.py
# COMPLETO - credentials.json EN Config/
# ============================================================================
"""
Gestión de usuarios - VERSIÓN POR PAÍS
Almacena credenciales DB y SSH separadas por país para cada usuario.
AHORA: credentials.json en Run/Config/credentials.json
"""

import json
import bcrypt
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime


class UserManager:
    """Gestiona usuarios - credentials.json en Config/"""
    
    _instance = None
    # NUEVA RUTA: Run/Config/credentials.json
    # Cambiar la ruta de credentials.json
    _config_path = Path(__file__).parent.parent / "Config" / "data_json" / "credentials.json"
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance
    
    def _load(self):
        if not self._config_path.exists():
            self._create_default()
        
        with open(self._config_path, 'r', encoding='utf-8') as f:
            self._data = json.load(f)
        
        # Migrar formato antiguo si es necesario
        self._migrate_if_needed()
        
        # Asegurar estructura nueva
        if "users" not in self._data:
            self._data["users"] = {}
        if "db" not in self._data:
            self._data["db"] = {"user": "", "password": ""}  # Legacy fallback
        if "db_by_country" not in self._data:
            self._data["db_by_country"] = {}  # Credenciales por país (referencia)
        if "countries" not in self._data:
            self._data["countries"] = {}
        
        self._save()
    
    def _migrate_if_needed(self):
        """Migra formato antiguo a nuevo (credenciales por país)."""
        users = self._data.get("users", {})
        migration_needed = False
        
        for alias, user_data in users.items():
            # Verificar si el usuario ya tiene la nueva estructura
            if 'db_by_country' in user_data:
                continue
            
            # Migrar desde formato antiguo
            user_data['db_by_country'] = {}
            
            # Si había DB global, aplicarla a ambos países
            db_creds = self._data.get("db", {})
            if db_creds.get('user'):
                user_data['db_by_country']['GT'] = {
                    'user': db_creds.get('user', ''),
                    'password': db_creds.get('password', '')
                }
                user_data['db_by_country']['CR'] = {
                    'user': db_creds.get('user', ''),
                    'password': db_creds.get('password', '')
                }
                migration_needed = True
            
            # Migrar SSH desde countries global
            countries = self._data.get("countries", {})
            if 'ssh_by_country' not in user_data:
                user_data['ssh_by_country'] = {}
            for country, country_data in countries.items():
                if country_data.get('ssh'):
                    user_data['ssh_by_country'][country] = country_data.get('ssh', '')
                    migration_needed = True
        
        # Limpiar datos globales antiguos después de migración
        if migration_needed:
            print("🔧 Migrando credenciales a formato por país...")
            self._save()
    
    def _create_default(self):
        """Crea archivo credentials.json por defecto en Config/"""
        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        default = {
            "db": {"user": "", "password": ""},
            "db_by_country": {},
            "countries": {},
            "users": {}
        }
        with open(self._config_path, 'w', encoding='utf-8') as f:
            json.dump(default, f, indent=2)
        self._data = default
        print(f"📁 Archivo credentials.json creado en: {self._config_path}")
    
    def _save(self):
        with open(self._config_path, 'w', encoding='utf-8') as f:
            json.dump(self._data, f, indent=2)
    
    def _hash_password(self, password: str) -> str:
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
    
    def _verify_password(self, password: str, hashed: str) -> bool:
        return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
    
    # ========== MÉTODO PRINCIPAL: CREAR USUARIO CON CREDENCIALES POR PAÍS ==========
    
    def create_user_with_country_creds(self, alias: str, password: str, 
                                        db_credentials: Dict[str, dict], 
                                        ssh_commands: Dict[str, str]) -> bool:
        """
        Crea usuario con credenciales DB por país y SSH por país.
        
        Args:
            alias: Nombre de usuario
            password: Contraseña de usuario
            db_credentials: Dict con credenciales por país
                Ej: {"GT": {"user": "user_gt", "password": "pass_gt"}, "CR": {...}}
            ssh_commands: Dict con comandos SSH por país
                Ej: {"GT": "ssh -N -L 3336:...", "CR": "ssh -N -L 3337:..."}
        """
        alias = alias.strip().lower()
        if alias in self._data["users"]:
            return False
        
        # Crear estructura de usuario
        user_data = {
            "alias": alias,
            "password_hash": self._hash_password(password),
            "created_at": datetime.now().isoformat(),
            "db_by_country": {},      # Credenciales DB por país
            "ssh_by_country": {}      # Comandos SSH por país
        }
        
        # Guardar credenciales DB por país
        for country, creds in db_credentials.items():
            country_upper = country.upper()
            user_data["db_by_country"][country_upper] = {
                "user": creds.get('user', ''),
                "password": creds.get('password', '')
            }
            # También guardar en db_by_country global para referencia rápida
            if country_upper not in self._data["db_by_country"]:
                self._data["db_by_country"][country_upper] = {
                    "user": creds.get('user', ''),
                    "password": creds.get('password', '')
                }
        
        # Guardar comandos SSH por país
        for country, ssh_cmd in ssh_commands.items():
            country_upper = country.upper()
            if ssh_cmd:
                user_data["ssh_by_country"][country_upper] = ssh_cmd
                # También guardar en countries global para referencia rápida
                if country_upper not in self._data["countries"]:
                    self._data["countries"][country_upper] = {}
                self._data["countries"][country_upper]["ssh"] = ssh_cmd
        
        # Guardar usuario
        self._data["users"][alias] = user_data
        
        # Legacy: mantener db global para compatibilidad (usar GT como default)
        gt_creds = db_credentials.get("GT", {})
        if gt_creds.get('user'):
            self._data["db"]["user"] = gt_creds.get('user', '')
            self._data["db"]["password"] = gt_creds.get('password', '')
        
        self._save()
        return True
    
    # ========== MÉTODO LEGACY (compatibilidad) ==========
    
    def create_user(self, alias: str, password: str, db_user: str, db_password: str,
                    ssh_gt: str = "", ssh_cr: str = "") -> bool:
        """
        LEGACY: Crea usuario con DB global y SSH por país.
        Prefiere usar create_user_with_country_creds.
        """
        db_credentials = {
            "GT": {"user": db_user, "password": db_password},
            "CR": {"user": db_user, "password": db_password}
        }
        ssh_commands = {}
        if ssh_gt:
            ssh_commands["GT"] = ssh_gt
        if ssh_cr:
            ssh_commands["CR"] = ssh_cr
        
        return self.create_user_with_country_creds(alias, password, db_credentials, ssh_commands)
    
    def authenticate(self, alias: str, password: str) -> Optional[Dict]:
        """Autentica usuario, retorna datos si éxito."""
        alias = alias.strip().lower()
        user = self._data["users"].get(alias)
        if not user:
            return None
        
        if self._verify_password(password, user["password_hash"]):
            return user
        return None
    
    # ========== MÉTODOS PARA OBTENER CREDENCIALES ==========
    
    def get_db_credentials(self, country_code: str = None) -> Dict:
        """
        Obtiene credenciales DB.
        PRIORIDAD: usuario actual > global
        """
        return self._data.get("db", {})
    
    def get_db_credentials_for_user(self, alias: str, country_code: str) -> Optional[Dict]:
        """Obtiene credenciales DB de un usuario para un país específico."""
        alias = alias.strip().lower()
        user = self._data["users"].get(alias)
        if not user:
            return None
        
        country_upper = country_code.upper()
        db_by_country = user.get("db_by_country", {})
        return db_by_country.get(country_upper)
    
    def get_ssh_command(self, country_code: str) -> str:
        """LEGACY: Obtiene comando SSH global para un país."""
        countries = self._data.get("countries", {})
        country_data = countries.get(country_code.upper(), {})
        return country_data.get("ssh", "")
    
    def get_ssh_command_for_user(self, alias: str, country_code: str) -> str:
        """Obtiene comando SSH de un usuario para un país específico."""
        alias = alias.strip().lower()
        user = self._data["users"].get(alias)
        if not user:
            return ""
        
        country_upper = country_code.upper()
        ssh_by_country = user.get("ssh_by_country", {})
        return ssh_by_country.get(country_upper, "")
    
    # ========== MÉTODOS DE ACTUALIZACIÓN ==========
    
    def update_user_password(self, alias: str, new_password: str) -> bool:
        alias = alias.strip().lower()
        if alias not in self._data["users"]:
            return False
        self._data["users"][alias]["password_hash"] = self._hash_password(new_password)
        self._save()
        return True
    
    def update_db_credentials(self, alias: str, country_code: str, user: str, password: str) -> bool:
        """Actualiza credenciales DB de un usuario para un país."""
        alias = alias.strip().lower()
        if alias not in self._data["users"]:
            return False
        
        country_upper = country_code.upper()
        if "db_by_country" not in self._data["users"][alias]:
            self._data["users"][alias]["db_by_country"] = {}
        
        self._data["users"][alias]["db_by_country"][country_upper] = {
            "user": user,
            "password": password
        }
        
        # Actualizar también global para referencia rápida
        if country_upper not in self._data["db_by_country"]:
            self._data["db_by_country"][country_upper] = {}
        self._data["db_by_country"][country_upper]["user"] = user
        self._data["db_by_country"][country_upper]["password"] = password
        
        # Si es GT, actualizar legacy db global
        if country_upper == "GT":
            self._data["db"]["user"] = user
            self._data["db"]["password"] = password
        
        self._save()
        return True
    
    def update_ssh_command(self, country_code: str, ssh_command: str) -> bool:
        """LEGACY: Actualiza comando SSH global para un país."""
        country_code = country_code.upper()
        if "countries" not in self._data:
            self._data["countries"] = {}
        if country_code not in self._data["countries"]:
            self._data["countries"][country_code] = {}
        self._data["countries"][country_code]["ssh"] = ssh_command
        self._save()
        return True
    
    def update_ssh_command_for_user(self, alias: str, country_code: str, ssh_command: str) -> bool:
        """Actualiza comando SSH de un usuario para un país."""
        alias = alias.strip().lower()
        if alias not in self._data["users"]:
            return False
        
        country_upper = country_code.upper()
        if "ssh_by_country" not in self._data["users"][alias]:
            self._data["users"][alias]["ssh_by_country"] = {}
        
        self._data["users"][alias]["ssh_by_country"][country_upper] = ssh_command
        
        # Actualizar también global para referencia rápida
        if country_upper not in self._data["countries"]:
            self._data["countries"][country_upper] = {}
        self._data["countries"][country_upper]["ssh"] = ssh_command
        
        self._save()
        return True
    
    # ========== MÉTODOS DE ADMINISTRACIÓN ==========
    
    def delete_user(self, alias: str) -> bool:
        alias = alias.strip().lower()
        if alias not in self._data["users"]:
            return False
        del self._data["users"][alias]
        self._save()
        return True
    
    def list_users(self) -> List[str]:
        return list(self._data["users"].keys())
    
    def user_exists(self, alias: str) -> bool:
        return alias.strip().lower() in self._data["users"]
    
    # ========== MÉTODOS LEGACY ==========
    
    def get_db_user(self) -> str:
        """LEGACY: Retorna usuario DB global (GT como referencia)."""
        return self._data.get("db", {}).get("user", "")
    
    def get_db_password(self) -> str:
        """LEGACY: Retorna contraseña DB global."""
        return self._data.get("db", {}).get("password", "")