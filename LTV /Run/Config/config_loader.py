"""
Carga centralizada de configuraciones JSON
"""
import json
from pathlib import Path
from typing import Any, Optional


class ConfigLoader:
    """Carga y guarda configuraciones JSON."""
    
    _config_dir = Path(__file__).parent.parent.parent / "config"
    
    @classmethod
    def _get_path(cls, name: str) -> Path:
        cls._config_dir.mkdir(parents=True, exist_ok=True)
        return cls._config_dir / f"{name}.json"
    
    @classmethod
    def load(cls, name: str, default: Any = None) -> Any:
        path = cls._get_path(name)
        if not path.exists():
            return default
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            return default
    
    @classmethod
    def save(cls, name: str, data: Any):
        path = cls._get_path(name)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    
    @classmethod
    def exists(cls, name: str) -> bool:
        return cls._get_path(name).exists()