"""
Gestión de credenciales globales por país (host, database, ssh_command)
"""
import json
from pathlib import Path
from typing import Dict, Optional


class CredentialManager:
    """Gestiona credenciales globales por país (no por usuario)."""
    
    _instance = None
    _config_path = Path(__file__).parent.parent.parent / "config" / "credentials.json"
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance
    
    def _load(self):
        """Carga el archivo credentials.json"""
        if not self._config_path.exists():
            self._create_default()
        
        with open(self._config_path, 'r', encoding='utf-8') as f:
            self._data = json.load(f)
    
    def _create_default(self):
        """Crea archivo por defecto"""
        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        default = {
            "countries": {
                "GT": {"host": "", "database": "", "ssh_command": ""},
                "CR": {"host": "", "database": "", "ssh_command": ""}
            },
            "users": {}
        }
        with open(self._config_path, 'w', encoding='utf-8') as f:
            json.dump(default, f, indent=2)
        self._data = default
    
    def _save(self):
        """Guarda cambios en disco"""
        with open(self._config_path, 'w', encoding='utf-8') as f:
            json.dump(self._data, f, indent=2)
    
    def get_country_config(self, country_code: str) -> Dict:
        """Retorna host, database, ssh_command para un país"""
        return self._data["countries"].get(country_code.upper(), {})
    
    def update_country_config(self, country_code: str, host: str, database: str, ssh_command: str) -> bool:
        """Actualiza configuración global de un país"""
        country_code = country_code.upper()
        self._data["countries"][country_code] = {
            "host": host,
            "database": database,
            "ssh_command": ssh_command
        }
        self._save()
        return True
    
    def get_all_countries(self) -> Dict:
        return self._data.get("countries", {})
    
    def get_available_countries(self) -> list:
        return list(self._data.get("countries", {}).keys())