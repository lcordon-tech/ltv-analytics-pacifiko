# ============================================================================
# FILE: Run/Config/credentials.py
# COMPLETO - VERSIÓN HÍBRIDA (USA CREDENTIALSTORE)
# ============================================================================
"""
Centraliza todas las credenciales del sistema - VERSIÓN HÍBRIDA
Prioridad: CredentialStore (JSON cifrado) con fallback a VaultManager (CSV)
"""

from dataclasses import dataclass
from typing import Optional
from pathlib import Path

from .credential_store import CredentialStore
from .vault_manager import VaultManager


@dataclass
class DBCredentials:
    user: str
    password: str
    host: str
    database: str
    country: str = "GT"
    
    def to_env_dict(self) -> dict:
        return {
            "DB_USER": self.user or "",
            "DB_PASSWORD": self.password or "",
            "DB_HOST": self.host or "localhost",
            "DB_NAME": self.database or "",
            "LTV_COUNTRY": self.country or "GT"
        }


@dataclass
class SSHCredentials:
    command: str
    enabled: bool = True
    wait_seconds: int = 5
    
    def get_command(self) -> Optional[str]:
        return self.command if self.enabled else None


class Credentials:
    """
    Centraliza credenciales - HÍBRIDO.
    Prioridad: CredentialStore (JSON cifrado)
    Fallback: VaultManager (CSV legacy)
    """
    
    _store = CredentialStore
    _vault = VaultManager()
    _cached_db: Optional[DBCredentials] = None
    _cached_ssh: Optional[SSHCredentials] = None
    _current_country: Optional[str] = None
    
    @classmethod
    def reload_from_vault(cls, country: Optional[str] = None):
        """Recarga credenciales - prioridad JSON."""
        target_country = (country or cls._current_country or "GT").upper()
        
        print(f"\n🔐 [Credentials] Cargando credenciales para país: {target_country}")
        
        # Intentar desde CredentialStore (JSON cifrado)
        db_creds = cls._store.get_db_credentials()
        ssh_cmd = cls._store.get_ssh_command(target_country)
        host = cls._store.get_host(target_country)
        database = cls._store.get_database(target_country)
        
        if db_creds and db_creds.get('user'):
            print(f"   ✅ Credenciales desde JSON cifrado")
            print(f"   País: {target_country}")
            print(f"   DB User: {db_creds.get('user', 'N/A')}")
            print(f"   Host: {host}")
            print(f"   Database: {database}")
            
            cls._current_country = target_country
            cls._cached_db = DBCredentials(
                user=db_creds.get('user', ''),
                password=db_creds.get('password', ''),
                host=host,
                database=database,
                country=target_country
            )
            
            cls._cached_ssh = SSHCredentials(
                command=ssh_cmd,
                enabled=bool(ssh_cmd),
                wait_seconds=5
            )
            return True
        
        # Fallback a VaultManager (CSV legacy)
        print(f"   ⚠️ JSON sin datos, intentando CSV legacy...")
        creds = cls._vault.get_first_credentials(target_country)
        
        if creds:
            cls._current_country = creds.get('country', 'GT')
            print(f"   ✅ Credenciales desde CSV legacy")
            print(f"   País: {cls._current_country}")
            print(f"   DB User: {creds.get('db_user', 'N/A')}")
            print(f"   Host: {creds.get('host', 'N/A')}")
            print(f"   Database: {creds.get('db_name', 'N/A')}")
            
            # Migrar a JSON para futuras lecturas
            cls._store.save_db_credentials(
                creds.get('db_user', ''),
                creds.get('db_pass', '')
            )
            cls._store.update_country_config(
                cls._current_country,
                creds.get('host', ''),
                creds.get('db_name', ''),
                creds.get('ssh_cmd', '')
            )
            print(f"   📝 Migrado a JSON cifrado")
            
            cls._cached_db = DBCredentials(
                user=creds.get('db_user', '') or '',
                password=creds.get('db_pass', '') or '',
                host=creds.get('host', 'localhost') or 'localhost',
                database=creds.get('db_name', '') or f"db_{cls._current_country.lower()}",
                country=cls._current_country
            )
            
            ssh_cmd = creds.get('ssh_cmd', '') or ''
            cls._cached_ssh = SSHCredentials(
                command=ssh_cmd,
                enabled=bool(ssh_cmd),
                wait_seconds=5
            )
            return True
        
        print(f"❌ No hay credenciales en JSON ni CSV para {target_country}")
        return False
    
    @classmethod
    def load_for_country(cls, country: str) -> bool:
        """Carga credenciales específicas para un país."""
        print(f"\n🔍 [Credentials] load_for_country('{country}')")
        cls._current_country = country.upper()
        return cls.reload_from_vault(country)
    
    @classmethod
    def get_db_credentials(cls, force_country: str = None) -> DBCredentials:
        """Obtiene credenciales DB."""
        print(f"\n🔍 [Credentials] get_db_credentials()")
        
        if force_country:
            print(f"   Forzando país: {force_country}")
            cls._current_country = force_country.upper()
            cls._cached_db = None
        
        if cls._cached_db is None:
            if not cls.reload_from_vault(cls._current_country):
                current_country = cls._current_country or "GT"
                raise Exception(f"No hay credenciales configuradas para {current_country}")
        
        return cls._cached_db
    
    @classmethod
    def get_ssh_credentials(cls) -> SSHCredentials:
        if cls._cached_ssh is None:
            cls.reload_from_vault(cls._current_country)
        return cls._cached_ssh if cls._cached_ssh else SSHCredentials(command="", enabled=False)
    
    @classmethod
    def get_current_country(cls) -> Optional[str]:
        return cls._current_country
    
    @classmethod
    def get_backup_credentials(cls) -> dict:
        return {
            "backup_path": str(Path(__file__).parent.parent / "backups"),
            "retention_days": 30
        }
    
    @classmethod
    def force_reload(cls):
        """Fuerza recarga de credenciales."""
        cls._cached_db = None
        cls._cached_ssh = None
        return cls.reload_from_vault(cls._current_country)
    
    @classmethod
    def migrate_from_csv(cls) -> bool:
        """Forzar migración desde CSV a JSON."""
        return cls._store.migrate_from_csv()