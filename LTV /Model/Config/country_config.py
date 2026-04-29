"""
Configuración multi-país para el sistema LTV.
SOLO DATOS - sin lógica de negocio.
"""

from dataclasses import dataclass
from typing import Dict, Optional
from datetime import datetime


@dataclass
class CountryConfig:
    """Configuración estática por país."""
    code: str
    name: str
    cohort_start_date: str      # Formato YYYY-MM-DD
    cohort_end_date: str = "2030-12-31"
    cac_sheet: str = "GT"       # Nombre de la hoja en CAC.xlsx
    currency: str = "USD"
    default_fx_rate: float = 1.0


# Registro central de países
COUNTRY_REGISTRY: Dict[str, CountryConfig] = {
    "GT": CountryConfig(
        code="GT",
        name="Guatemala",
        cohort_start_date="2020-01-01",
        cac_sheet="GT",
        currency="GTQ",
        default_fx_rate=7.66
    ),
    "CR": CountryConfig(
        code="CR",
        name="Costa Rica",
        cohort_start_date="2030-12-31",
        cac_sheet="CR",
        currency="CRC",
        default_fx_rate=550.0
    ),
}


def get_country_config(country_code: str) -> CountryConfig:
    """Retorna configuración por código de país."""
    country_code = country_code.upper().strip()
    if country_code not in COUNTRY_REGISTRY:
        raise ValueError(f"País no soportado: {country_code}. Soportados: {list(COUNTRY_REGISTRY.keys())}")
    return COUNTRY_REGISTRY[country_code]


def select_country_interactive() -> CountryConfig:
    """Menú interactivo para seleccionar país."""
    print("\n" + "=" * 50)
    print("   SELECCIÓN DE PAÍS".center(50))
    print("=" * 50)
    
    countries = list(COUNTRY_REGISTRY.keys())
    for i, code in enumerate(countries, 1):
        config = COUNTRY_REGISTRY[code]
        print(f"   {i}. {config.name} ({code})")
    
    print("-" * 50)
    
    while True:
        try:
            option = input("\n👉 Selecciona un país (número o código): ").strip().upper()
            
            if option.isdigit():
                idx = int(option) - 1
                if 0 <= idx < len(countries):
                    return COUNTRY_REGISTRY[countries[idx]]
            
            if option in COUNTRY_REGISTRY:
                return COUNTRY_REGISTRY[option]
            
            print(f"❌ Opción inválida. Selecciona: {', '.join(countries)} o 1-{len(countries)}")
        except KeyboardInterrupt:
            print("\n\n👋 Cancelado por el usuario.")
            raise