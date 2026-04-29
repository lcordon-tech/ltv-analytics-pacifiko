"""
Carga configuración de países desde archivos JSON externos.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class CountryConfig:
    """Configuración de país desde JSON."""
    code: str
    name: str
    currency: str
    default_fx_rate: float
    cohort_start_year: int
    cohort_end_year: int
    input_files: Dict[str, str] = field(default_factory=dict)
    output: Dict[str, str] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'CountryConfig':
        return cls(
            code=data['code'],
            name=data['name'],
            currency=data['currency'],
            default_fx_rate=data['default_fx_rate'],
            cohort_start_year=data['cohort_start_year'],
            cohort_end_year=data['cohort_end_year'],
            input_files=data.get('input_files', {}),
            output=data.get('output', {})
        )
    
    def get_input_file(self, file_type: str, default: str) -> str:
        """Retorna nombre de archivo para un tipo."""
        return self.input_files.get(file_type, default)
    
    def get_excel_sheet(self, file_type: str) -> str:
        """Retorna nombre de hoja (por ahora igual al código)."""
        return self.code


class CountryLoader:
    """Carga configuraciones de países desde archivos JSON."""
    
    DEFINITIONS_DIR = Path(__file__).parent / "definitions"
    
    @classmethod
    def get_available_countries(cls) -> List[str]:
        """Retorna lista de códigos de países disponibles."""
        if not cls.DEFINITIONS_DIR.exists():
            return []
        
        countries = []
        for json_file in cls.DEFINITIONS_DIR.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'code' in data:
                        countries.append(data['code'])
            except Exception:
                continue
        return sorted(countries)
    
    @classmethod
    def load_country(cls, country_code: str) -> Optional[CountryConfig]:
        """Carga configuración de un país por código."""
        country_code = country_code.upper().strip()
        
        # Buscar archivo JSON
        json_path = cls.DEFINITIONS_DIR / f"{country_code.lower()}.json"
        if not json_path.exists():
            # Fallback: buscar cualquier JSON que contenga el código
            for json_file in cls.DEFINITIONS_DIR.glob("*.json"):
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if data.get('code', '').upper() == country_code:
                            return CountryConfig.from_dict(data)
                except Exception:
                    continue
            return None
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return CountryConfig.from_dict(data)
        except Exception as e:
            print(f"❌ Error cargando configuración de {country_code}: {e}")
            return None
    
    @classmethod
    def get_country_display_name(cls, country_code: str) -> str:
        """Retorna nombre legible del país."""
        config = cls.load_country(country_code)
        return config.name if config else country_code