"""
Contexto completo por país.
Centraliza toda la configuración específica de un país.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, Optional


@dataclass
class CountryContext:
    """Configuración completa para un país."""
    
    code: str                           # GT, CR
    name: str                           # Guatemala, Costa Rica
    currency: str                       # GTQ, CRC
    default_fx_rate: float              # 7.66 para GT, 1.0 para CR
    cohort_start_year: int              # 2021 para GT, 2022 para CR
    cohort_end_year: int = 2030
    
    # Archivos Excel (nombres unificados)
    catalog_file: str = "catalogLTV.xlsx"
    sois_file: str = "SOIS.xlsx"
    supuestos_file: str = "SUPUESTOS.xlsx"
    fx_file: str = "TIPO_DE_CAMBIO.xlsx"
    cac_file: str = "CAC.xlsx"
    
    # Business units soportadas
    business_units: list = field(default_factory=lambda: ["1P", "3P", "FBP", "TM", "DS"])
    
    # Mapeo de archivo -> nombre de hoja
    _sheet_mapping: Dict[str, str] = field(default_factory=dict, repr=False)
    
    def __post_init__(self):
        """Inicializa mapeo de hojas por país."""
        self._sheet_mapping = {
            "catalog": self.code,
            "sois": self.code,
            "supuestos": self.code,
            "fx": self.code,
            "cac": self.code
        }
    
    def get_excel_sheet(self, file_type: str) -> str:
        """
        Retorna el nombre de la hoja para un tipo de archivo.
        
        Args:
            file_type: 'catalog', 'sois', 'supuestos', 'fx', 'cac'
        """
        return self._sheet_mapping.get(file_type, self.code)
    
    def get_input_file_path(self, inputs_dir: Path, file_type: str) -> Path:
        """Retorna la ruta completa del archivo Excel."""
        file_name = getattr(self, f"{file_type}_file", None)
        if not file_name:
            raise ValueError(f"Tipo de archivo desconocido: {file_type}")
        return inputs_dir / file_name
    
    def to_dict(self) -> Dict[str, Any]:
        """Convierte a diccionario para logging/debug."""
        return {
            "code": self.code,
            "name": self.name,
            "currency": self.currency,
            "default_fx_rate": self.default_fx_rate,
            "cohort_start_year": self.cohort_start_year,
            "cohort_end_year": self.cohort_end_year
        }
    
    def __str__(self) -> str:
        return f"CountryContext({self.code}: {self.name})"


class CountryContextFactory:
    """Fábrica para crear CountryContext por código de país."""
    
    _instances: Dict[str, CountryContext] = {}
    
    @classmethod
    def create(cls, country_code: str) -> CountryContext:
        """Crea o recupera un CountryContext para el código dado."""
        country_code = country_code.upper().strip()
        
        if country_code in cls._instances:
            return cls._instances[country_code]
        
        configs = {
            "GT": {
                "code": "GT",
                "name": "Guatemala",
                "currency": "GTQ",
                "default_fx_rate": 7.66,
                "cohort_start_year": 2020
            },
            "CR": {
                "code": "CR",
                "name": "Costa Rica",
                "currency": "CRC",
                "default_fx_rate": 1.0,
                "cohort_start_year": 2022
            }
        }
        
        if country_code not in configs:
            raise ValueError(f"País no soportado: {country_code}. Soportados: {list(configs.keys())}")
        
        context = CountryContext(**configs[country_code])
        cls._instances[country_code] = context
        return context
    
    @classmethod
    def get_default(cls) -> CountryContext:
        """Retorna contexto por defecto (Guatemala)."""
        return cls.create("CR")