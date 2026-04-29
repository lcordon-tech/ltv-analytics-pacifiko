# ============================================================================
# FILE 8: Run/Country/country_selector.py - VERSIÓN CORREGIDA
# ============================================================================
"""
Selector de país al inicio del sistema - CON NAVEGACIÓN MEJORADA
"""
import os
from typing import Optional, Dict

from Run.Country.country_loader import CountryLoader


class CountrySelector:
    """Menú interactivo para seleccionar país - CON OPCIÓN DE VOLVER A AUTH"""
    
    def __init__(self):
        self._selected_country: Optional[str] = None
        self._env_var = "LTV_COUNTRY"
        self._available_countries: Dict[str, Dict] = {}
        self._load_available_countries()
    
    def _load_available_countries(self):
        """Carga países disponibles desde archivos JSON."""
        available_codes = CountryLoader.get_available_countries()
        
        for code in available_codes:
            config = CountryLoader.load_country(code)
            if config:
                self._available_countries[code] = {
                    "name": config.name,
                    "currency": config.currency,
                    "default_fx": config.default_fx_rate,
                    "cohort_start_year": config.cohort_start_year,
                    "cohort_end_year": config.cohort_end_year
                }
    
    def get_available_countries(self) -> Dict[str, Dict]:
        """Retorna diccionario de países disponibles."""
        return self._available_countries
    
    def select_with_back(self) -> str:
        """
        Selecciona país con opción de volver a auth.
        Retorna:
        - código de país: éxito
        - "BACK_TO_AUTH": volver a autenticación
        - "EXIT": salir del sistema
        """
        # 🔧 OPCIÓN: Preguntar si usar variable de entorno
        env_country = os.environ.get(self._env_var, "").upper().strip()
        if env_country in self._available_countries:
            print(f"\n⚠️ Se detectó variable de entorno LTV_COUNTRY={env_country}")
            usar = input(f"¿Usar {self._get_country_name(env_country)}? (s/n) [s]: ").strip().lower()
            if usar in ['s', 'si', 'sí', 'yes', 'y', '']:
                print(f"🌎 País seleccionado por variable de entorno: {self._get_country_name(env_country)}")
                self._selected_country = env_country
                return env_country
            else:
                print("   Ignorando variable de entorno. Selecciona manualmente.")
                # Limpiar variable de entorno para esta sesión
                os.environ.pop(self._env_var, None)
        
        # Un solo país
        if len(self._available_countries) == 1:
            only_country = list(self._available_countries.keys())[0]
            print(f"🌎 Único país disponible: {self._get_country_name(only_country)}")
            self._selected_country = only_country
            return only_country
        
        # Menú interactivo
        while True:
            print("\n" + "=" * 50)
            print("   SELECCIÓN DE PAÍS".center(50))
            print("=" * 50)
            
            for code, info in self._available_countries.items():
                print(f"   {code}. {info['name']}")
            print("-" * 50)
            print("   b. 🔙 Volver al menú de autenticación")
            print("   q. ❌ Salir")
            print("=" * 50)
            
            option = input("\n👉 Selecciona un país: ").strip().upper()
            
            # Easter egg
            if option.lower() == "pacifiko.com":
                print("\n🔔✨ Ding Dong! 🅿️")
                print("   (Easter egg activado - volviendo al selector)\n")
                continue
            
            if option == 'B':
                return "BACK_TO_AUTH"
            
            if option == 'Q':
                return "EXIT"
            
            if option in self._available_countries:
                self._selected_country = option
                # Guardar en variable de entorno para futuras ejecuciones
                os.environ[self._env_var] = option
                print(f"\n✅ País seleccionado: {self._get_country_name(option)}")
                print(f"   📅 Cohortes desde: {self._available_countries[option]['cohort_start_year']}")
                print(f"   💱 Moneda: {self._available_countries[option]['currency']}")
                return option
            else:
                valid_codes = ', '.join(self._available_countries.keys())
                print(f"❌ Opción inválida. Selecciona: {valid_codes}, b o q")
    
    def select(self) -> str:
        """Método legacy para compatibilidad."""
        return self.select_with_back()
    
    def get_selected(self) -> Optional[str]:
        """Retorna el país seleccionado."""
        return self._selected_country
    
    def _get_country_name(self, code: str) -> str:
        """Retorna nombre legible del país."""
        return self._available_countries.get(code, {}).get("name", code)
    
    def get_country_info(self, code: str) -> Optional[Dict]:
        """Retorna información completa de un país."""
        return self._available_countries.get(code)
    
    def has_countries(self) -> bool:
        """Verifica si hay países configurados."""
        return len(self._available_countries) > 0