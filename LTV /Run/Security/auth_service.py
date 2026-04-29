# ============================================================================
# FILE: Run/Security/auth_service.py
# COMPLETO - CREDENCIALES DB POR PAÍS
# ============================================================================
"""
Servicio de autenticación unificado - VERSIÓN POR PAÍS
Soporta diferentes credenciales DB para GT y CR.
"""

from typing import Optional, Dict
from pathlib import Path
import json
from .user_manager import UserManager
from Run.Config.dev_mode_manager import DevModeManager


class AuthService:
    """Unifica autenticación y proporciona credenciales finales POR PAÍS."""
    
    _instance = None
    _current_user: Optional[str] = None
    _current_country: Optional[str] = None
    _session_active: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._user_mgr = UserManager()
            cls._instance._dev_mode = DevModeManager()
            cls._instance._ensure_credentials_file()
        return cls._instance
    
    def _ensure_credentials_file(self):
        """Asegura que credentials.json existe con estructura base."""
        creds_file = Path(__file__).parent.parent.parent / "config" / "credentials.json"
        if not creds_file.exists():
            creds_file.parent.mkdir(parents=True, exist_ok=True)
            default = {
                "db": {"user": "", "password": ""},  # Legacy, no se usa
                "db_by_country": {},  # NUEVO: credenciales por país
                "countries": {"GT": {"ssh": ""}, "CR": {"ssh": ""}},
                "users": {}
            }
            with open(creds_file, 'w', encoding='utf-8') as f:
                json.dump(default, f, indent=2)
    
    @property
    def is_authenticated(self) -> bool:
        return self._current_user is not None and self._session_active
    
    def get_current_user(self) -> Optional[str]:
        return self._current_user
    
    def set_country(self, country_code: str):
        self._current_country = country_code.upper()
    
    def authenticate(self, alias: str, password: str) -> bool:
        """Autentica usuario y mantiene sesión."""
        user = self._user_mgr.authenticate(alias, password)
        if user:
            self._current_user = alias
            self._session_active = True
            return True
        return False
    
    def logout(self):
        self._current_user = None
        self._session_active = False
    
    def get_db_credentials(self, country_code: str = None) -> Optional[Dict]:
        """
        Retorna credenciales DB para un país específico.
        PRIORIDAD: credenciales por país del usuario > credenciales globales legacy
        """
        target_country = (country_code or self._current_country or "GT").upper()
        
        if not self.is_authenticated:
            print(f"⚠️ No autenticado")
            return None
        
        # 1. Intentar obtener credenciales DB por país para el usuario actual
        db_creds = self._user_mgr.get_db_credentials_for_user(self._current_user, target_country)
        
        if db_creds and db_creds.get('user'):
            print(f"   ✅ DB creds para {target_country}: {db_creds.get('user')}")
            # Obtener SSH específico del país
            ssh_command = self._user_mgr.get_ssh_command_for_user(self._current_user, target_country)
            
            # Host por país
            host_map = {"GT": "127.0.0.1:3336", "CR": "127.0.0.1:3337"}
            
            return {
                "host": host_map.get(target_country, "127.0.0.1:3306"),
                "database": self._get_database_name(target_country),
                "db_user": db_creds.get('user', ''),
                "db_password": db_creds.get('password', ''),
                "ssh_command": ssh_command,
                "country": target_country
            }
        
        # 2. Fallback a credenciales globales legacy
        global_creds = self._user_mgr.get_db_credentials()
        if global_creds and global_creds.get('user'):
            print(f"   ⚠️ Usando DB creds GLOBALES para {target_country} (legacy)")
            ssh_command = self._user_mgr.get_ssh_command(target_country)
            
            host_map = {"GT": "127.0.0.1:3336", "CR": "127.0.0.1:3337"}
            
            return {
                "host": host_map.get(target_country, "127.0.0.1:3306"),
                "database": self._get_database_name(target_country),
                "db_user": global_creds.get('user', ''),
                "db_password": global_creds.get('password', ''),
                "ssh_command": ssh_command,
                "country": target_country
            }
        
        print(f"⚠️ No hay credenciales DB para {target_country}")
        return None
    
    def _get_database_name(self, country_code: str) -> str:
        """Retorna nombre de base de datos por país."""
        db_names = {
            "GT": "db_pacifiko",
            "CR": "CRProdDb"
        }
        return db_names.get(country_code.upper(), f"db_{country_code.lower()}")
    
    def get_current_db_credentials(self) -> Optional[Dict]:
        return self.get_db_credentials(self._current_country)
    
    def should_ask_login(self) -> bool:
        """Determina si debe pedir login."""
        if self.is_authenticated:
            return False
        
        if not self._dev_mode.is_enabled():
            return True
        
        users = self._user_mgr.list_users()
        if not users:
            return True
        
        return True
    
    # ========== MÉTODOS LEGACY (compatibilidad) ==========
    
    def create_user(self, alias: str, password: str, db_user: str, db_password: str,
                    ssh_gt: str = "", ssh_cr: str = "") -> bool:
        """
        LEGACY: Crea usuario con DB global y SSH por país.
        Prefiere usar create_user_with_country_creds para nuevo código.
        """
        # Convertir a formato por país
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
    
    def create_user_with_country_creds(self, alias: str, password: str, 
                                        db_credentials: Dict[str, dict], 
                                        ssh_commands: Dict[str, str]) -> bool:
        """Crea usuario con credenciales DB por país."""
        result = self._user_mgr.create_user_with_country_creds(alias, password, db_credentials, ssh_commands)
        if result:
            self._current_user = alias
            self._session_active = True
        return result
    
    def update_user_password(self, alias: str, new_password: str) -> bool:
        return self._user_mgr.update_user_password(alias, new_password)
    
    def update_ssh_command(self, country_code: str, ssh_command: str) -> bool:
        """ACTUALIZA SSH para un país (global, sin usuario específico)."""
        return self._user_mgr.update_ssh_command(country_code, ssh_command)
    
    def update_ssh_command_for_user(self, alias: str, country_code: str, ssh_command: str) -> bool:
        """Actualiza SSH para un usuario y país específico."""
        return self._user_mgr.update_ssh_command_for_user(alias, country_code, ssh_command)
    
    def update_db_credentials(self, alias: str, country_code: str, user: str, password: str) -> bool:
        """Actualiza credenciales DB para un usuario y país."""
        return self._user_mgr.update_db_credentials(alias, country_code, user, password)
    
    def delete_user(self, alias: str) -> bool:
        if alias == self._current_user:
            self.logout()
        return self._user_mgr.delete_user(alias)
    
    def list_users(self) -> list:
        return self._user_mgr.list_users()
    
    def user_exists(self, alias: str) -> bool:
        return self._user_mgr.user_exists(alias)
    
    # Legacy methods
    def get_db_user(self) -> str:
        """LEGACY: Retorna usuario DB global."""
        return self._user_mgr.get_db_user()
    
    def get_db_password(self) -> str:
        """LEGACY: Retorna contraseña DB global."""
        return self._user_mgr.get_db_password()
    
    def get_db_credentials_for_user(self, alias: str, country: str) -> Optional[dict]:
        """Obtiene credenciales DB de un usuario para un país."""
        return self._user_mgr.get_db_credentials_for_user(alias, country)
    
    def get_ssh_command_for_user(self, alias: str, country: str) -> str:
        """Obtiene comando SSH de un usuario para un país."""
        return self._user_mgr.get_ssh_command_for_user(alias, country)