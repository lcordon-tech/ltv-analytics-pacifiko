"""
Gestión del modo desarrollador - CONFIGURABLE desde JSON
"""
from Run.Config.config_loader import ConfigLoader


class DevModeManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance
    
    def _load(self):
        config = ConfigLoader.load("dev_mode", {"enabled": False})
        self._enabled = config.get("enabled", False)
    
    def _save(self):
        ConfigLoader.save("dev_mode", {"enabled": self._enabled})
    
    def is_enabled(self) -> bool:
        """Retorna True si modo desarrollador está activo."""
        return self._enabled
    
    def set_enabled(self, enabled: bool):
        self._enabled = enabled
        self._save()
    
    def is_locked(self) -> bool:
        """Mantiene compatibilidad con código existente (locked = NOT enabled)."""
        return not self._enabled