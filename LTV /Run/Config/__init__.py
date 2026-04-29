"""Módulo de configuración del sistema LTV"""
import os
from pathlib import Path

CONFIG_ROOT = Path(__file__).parent
DATA_XLSX_PATH = CONFIG_ROOT / "data_xlsx"

def ensure_config_structure():
    os.makedirs(DATA_XLSX_PATH, exist_ok=True)
    return True

from .vault_manager import VaultManager
from .dev_mode_manager import DevModeManager

__all__ = ['VaultManager', 'DevModeManager', 'ensure_config_structure', 'DATA_XLSX_PATH']