# ============================================================================
# FILE: Run/Menu/menu_config.py
# COMPLETO - CON DEFAULT FLAGS Y ORDENAMIENTO DE COHORTES
# VERSIÓN MEJORADA: Persistencia de ordenamiento global
# ============================================================================
# Archivo: Run/Menu/menu_config.py
# Responsabilidad: Configuraciones, modos, persistencia, gestión de cohortes (CRUD completo)

import json
import re
from pathlib import Path
from typing import List, Optional, Tuple, Dict

from Run.Config.paths import Paths, PathsConfig
from Run.Services.cohort_supuestos_manager import CohortSupuestosManager
from Run.Utils.logger import SystemLogger
from Run.Config.config_loader import ConfigLoader


class MenuConfig:
    """Gestiona configuraciones del menú (modos, persistencia, paths, cohortes)"""
    
    # Constantes de modos
    GROUPING_BEHAVIORAL = "behavioral"
    GROUPING_ENTRY_BASED = "entry_based"
    
    BRAND_MODE_FLAT = "flat"
    BRAND_MODE_HIERARCHICAL = "hierarchical"
    BRAND_MODE_DUAL = "dual"
    
    BRAND_MODE_TO_DIMENSION = {
        BRAND_MODE_FLAT: 3,
        BRAND_MODE_HIERARCHICAL: 5,
        BRAND_MODE_DUAL: 6,
    }
    
    CONVERSION_CUMULATIVE = "cumulative"
    CONVERSION_INCREMENTAL = "incremental"
    
    # Granularidades soportadas
    GRANULARITY_QUARTERLY = 'quarterly'
    GRANULARITY_MONTHLY = 'monthly'
    GRANULARITY_WEEKLY = 'weekly'
    GRANULARITY_SEMIANNUAL = 'semiannual'
    GRANULARITY_YEARLY = 'yearly'
    
    GRANULARITIES = [
        GRANULARITY_QUARTERLY,
        GRANULARITY_MONTHLY,
        GRANULARITY_WEEKLY,
        GRANULARITY_SEMIANNUAL,
        GRANULARITY_YEARLY
    ]
    
    # Constantes para ordenamiento global
    SORT_BY_CLIENTS = "clients"
    SORT_BY_LTV = "ltv"
    SORT_BY_SCORE = "score"
    
    SORT_OPTIONS = {
        SORT_BY_CLIENTS: "👥 Número de Clientes - DEFAULT",
        SORT_BY_LTV: "💰 LTV Promedio por Cliente",
        SORT_BY_SCORE: "🏆 Final Score (relevancia estratégica)"
    }
    
    def __init__(self, paths: PathsConfig, logger: SystemLogger):
        self.paths = paths
        self.logger = logger
        
        self.current_grouping_mode = self.GROUPING_ENTRY_BASED
        self.current_brand_mode = self.BRAND_MODE_HIERARCHICAL
        self.current_conversion_mode = self.CONVERSION_CUMULATIVE
        self.current_granularity = self.GRANULARITY_QUARTERLY  # DEFAULT
        
        # Ordenamiento global (persistente)
        self.global_sort_criteria = self._load_global_sort_preference()
        
        self.config_file = Path(__file__).parent.parent / "Config" / "data_json" / "user_config.json"
        self.paths_file = Path(__file__).parent.parent / "Config" / "data_json" / "user_paths.json"
        
        self._load_saved_paths()
        self._load_config()
    
    # ==================================================================
    # NUEVO: ORDENAMIENTO GLOBAL PERSISTENTE
    # ==================================================================
    
    def _load_global_sort_preference(self) -> str:
        """Carga preferencia de ordenamiento global desde JSON."""
        try:
            saved = ConfigLoader.load("user_sort_preference")
            if saved and saved.get("sort_criteria") in self.SORT_OPTIONS:
                return saved.get("sort_criteria")
        except Exception:
            pass
        return self.SORT_BY_CLIENTS
    
    def save_global_sort_preference(self, criteria: str):
        """Persiste preferencia de ordenamiento global."""
        if criteria not in self.SORT_OPTIONS:
            return
        try:
            ConfigLoader.save("user_sort_preference", {"sort_criteria": criteria})
            self.global_sort_criteria = criteria
            print(f"✅ Ordenamiento global guardado: {self.get_global_sort_display()}")
        except Exception as e:
            print(f"⚠️ No se pudo guardar preferencia: {e}")
    
    def get_global_sort_display(self) -> str:
        """Retorna display del criterio de ordenamiento global."""
        return self.SORT_OPTIONS.get(self.global_sort_criteria, "👥 Número de Clientes - DEFAULT")
    
    def get_global_sort_criteria(self) -> str:
        """Retorna el criterio actual de ordenamiento global."""
        return self.global_sort_criteria
    
    def select_global_sort(self):
        """Menú interactivo para ordenamiento global."""
        print("\n" + "=" * 50)
        print("   ORDENAMIENTO GLOBAL".center(50))
        print("=" * 50)
        print(f"\n📊 Criterio actual: {self.get_global_sort_display()}")
        print("\nEste ordenamiento se aplicará a TODOS los resultados de búsqueda")
        print("\nOpciones:")
        print(f"   1. {self.SORT_OPTIONS[self.SORT_BY_CLIENTS]}")
        print(f"   2. {self.SORT_OPTIONS[self.SORT_BY_LTV]}")
        print(f"   3. {self.SORT_OPTIONS[self.SORT_BY_SCORE]}")
        print("   q. 🔙 Volver")
        print("-" * 50)
        
        option = input("\n👉 Opción (1/2/3/q): ").strip().lower()
        
        if option == '1':
            self.save_global_sort_preference(self.SORT_BY_CLIENTS)
        elif option == '2':
            self.save_global_sort_preference(self.SORT_BY_LTV)
        elif option == '3':
            self.save_global_sort_preference(self.SORT_BY_SCORE)
        elif option == 'q':
            return
        else:
            print("❌ Opción inválida")
    
    # ==================================================================
    # MÉTODOS EXISTENTES (sin cambios)
    # ==================================================================
    
    def _load_saved_paths(self):
        """Carga carpetas guardadas, pero valida que pertenezcan a la versión actual."""
        try:
            if self.paths_file.exists():
                with open(self.paths_file, 'r') as f:
                    saved = json.load(f)
                
                # Obtener la versión actual (usa la carpeta base actual)
                current_base = str(self.paths.base_path)
                saved_base = saved.get("base_path", "")
                
                # Si la carpeta base cambió, ignorar rutas guardadas
                if saved_base != current_base:
                    print(f"🔄 Detectado cambio de versión/carpeta base")
                    print(f"   Anterior: {saved_base}")
                    print(f"   Actual:   {current_base}")
                    print(f"   Re-inicializando configuraciones...")
                    return  # No cargar rutas antiguas
                
                if "results_base" in saved:
                    results_path = Path(saved["results_base"])
                    if results_path.exists():
                        self.paths.results_base = results_path
                if "inputs_dir" in saved:
                    inputs_path = Path(saved["inputs_dir"])
                    if inputs_path.exists():
                        self.paths.inputs_dir = inputs_path
                    
        except Exception:
            pass
    
    def _save_paths(self):
        """Guarda las carpetas elegidas junto con la versión actual."""
        try:
            self.paths_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.paths_file, 'w') as f:
                json.dump({
                    "base_path": str(self.paths.base_path),
                    "results_base": str(self.paths.results_base),
                    "inputs_dir": str(self.paths.inputs_dir)
                }, f)
        except Exception:
            pass
    
    def _load_config(self):
        """Carga configuración guardada desde archivo JSON"""
        default_config = {
            "grouping_mode": self.GROUPING_ENTRY_BASED,
            "brand_mode": self.BRAND_MODE_HIERARCHICAL,
            "conversion_mode": self.CONVERSION_CUMULATIVE,
            "granularity": self.GRANULARITY_QUARTERLY
        }
        
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r') as f:
                    saved = json.load(f)
                    self.current_grouping_mode = saved.get("grouping_mode", default_config["grouping_mode"])
                    self.current_brand_mode = saved.get("brand_mode", default_config["brand_mode"])
                    self.current_conversion_mode = saved.get("conversion_mode", default_config["conversion_mode"])
                    self.current_granularity = saved.get("granularity", default_config["granularity"])
                    return
        except Exception as e:
            self.logger.warning(f"No se pudo cargar configuración: {e}")
        
        self.current_grouping_mode = default_config["grouping_mode"]
        self.current_brand_mode = default_config["brand_mode"]
        self.current_conversion_mode = default_config["conversion_mode"]
        self.current_granularity = default_config["granularity"]
    
    def _save_config(self):
        """Guarda configuración actual a archivo JSON"""
        try:
            self.config_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.config_file, 'w') as f:
                json.dump({
                    "grouping_mode": self.current_grouping_mode,
                    "brand_mode": self.current_brand_mode,
                    "conversion_mode": self.current_conversion_mode,
                    "granularity": self.current_granularity
                }, f, indent=2)
        except Exception as e:
            self.logger.warning(f"No se pudo guardar configuración: {e}")
    
    # ==================================================================
    # GETTERS PARA DISPLAYS (CON DEFAULT FLAGS)
    # ==================================================================
    
    def get_grouping_mode_display(self) -> str:
        if self.current_grouping_mode == self.GROUPING_BEHAVIORAL:
            return "Comportamental (behavioral)"
        return "Basado en entrada (entry_based) - DEFAULT"
    
    def get_brand_mode_display(self) -> str:
        if self.current_brand_mode == self.BRAND_MODE_FLAT:
            return "Plano (todas las compras)"
        elif self.current_brand_mode == self.BRAND_MODE_HIERARCHICAL:
            return "Jerárquico (marca dentro de subcategoría) - DEFAULT"
        return "Dual (Subcategoría + Marca separadas)"
    
    def get_conversion_mode_display(self) -> str:
        if self.current_conversion_mode == self.CONVERSION_CUMULATIVE:
            return "Acumulativa (creciente)"
        return "Incremental (distribución)"
    
    def get_granularity_display(self) -> str:
        displays = {
            self.GRANULARITY_QUARTERLY: "Trimestral (quarterly) - DEFAULT",
            self.GRANULARITY_MONTHLY: "Mensual (monthly)",
            self.GRANULARITY_WEEKLY: "Semanal (weekly)",
            self.GRANULARITY_SEMIANNUAL: "Semestral (semiannual)",
            self.GRANULARITY_YEARLY: "Anual (yearly)"
        }
        return displays.get(self.current_granularity, "Trimestral (quarterly) - DEFAULT")
    
    # ==================================================================
    # SELECTORES INTERACTIVOS
    # ==================================================================
    
    def select_grouping_mode(self):
        print("\n" + "=" * 50)
        print("      MODO DE AGRUPACIÓN".center(50))
        print("=" * 50)
        print("\n1. 🔄 Comportamental (behavioral)")
        print("2. 🎯 Basado en entrada (entry_based) - DEFAULT")
        print("\nq. 🔙 Volver")
        
        while True:
            option = input("\n👉 Opción (1/2/q): ").strip().lower()
            if option == '1':
                self.current_grouping_mode = self.GROUPING_BEHAVIORAL
                self._save_config()
                print("✅ Modo cambiado")
                break
            elif option == '2':
                self.current_grouping_mode = self.GROUPING_ENTRY_BASED
                self._save_config()
                print("✅ Modo cambiado")
                break
            elif option == 'q':
                break
            else:
                print("❌ Opción inválida")
    
    def select_brand_mode(self):
        print("\n" + "=" * 50)
        print("      MODO DE MARCA".center(50))
        print("=" * 50)
        print("\n1. 🏷️ Plano")
        print("2. 🔗 Jerárquico - DEFAULT")
        print("3. 📊 Dual")
        print("\nq. 🔙 Volver")
        
        while True:
            option = input("\n👉 Opción (1/2/3/q): ").strip().lower()
            if option == '1':
                self.current_brand_mode = self.BRAND_MODE_FLAT
                self._save_config()
                print("✅ Modo cambiado")
                break
            elif option == '2':
                self.current_brand_mode = self.BRAND_MODE_HIERARCHICAL
                self._save_config()
                print("✅ Modo cambiado")
                break
            elif option == '3':
                self.current_brand_mode = self.BRAND_MODE_DUAL
                self._save_config()
                print("✅ Modo cambiado")
                break
            elif option == 'q':
                break
            else:
                print("❌ Opción inválida")
    
    def select_granularity(self):
        """Selecciona la granularidad de cohortes con orden correcto."""
        print("\n" + "=" * 50)
        print("      GRANULARIDAD DE COHORTES".center(50))
        print("=" * 50)
        print(f"\n📊 Granularidad actual: {self.get_granularity_display()}")
        print("\nSelecciona la granularidad temporal:")
        print("   1. Anual (yearly)")
        print("   2. Semestral (semiannual)")
        print("   3. Trimestral (quarterly) - DEFAULT")
        print("   4. Mensual (monthly)")
        print("   5. Semanal (weekly)")
        print("\n💡 Los supuestos se transforman automáticamente")
        print("\nb. 🔙 Volver")
        
        while True:
            option = input("\n👉 Opción (1/2/3/4/5/b): ").strip().lower()
            if option == '1':
                self.current_granularity = self.GRANULARITY_YEARLY
                self._save_config()
                print("✅ Granularidad cambiada a: Anual (yearly)")
                break
            elif option == '2':
                self.current_granularity = self.GRANULARITY_SEMIANNUAL
                self._save_config()
                print("✅ Granularidad cambiada a: Semestral (semiannual)")
                break
            elif option == '3':
                self.current_granularity = self.GRANULARITY_QUARTERLY
                self._save_config()
                print("✅ Granularidad cambiada a: Trimestral (quarterly) - DEFAULT")
                break
            elif option == '4':
                self.current_granularity = self.GRANULARITY_MONTHLY
                self._save_config()
                print("✅ Granularidad cambiada a: Mensual (monthly)")
                break
            elif option == '5':
                self.current_granularity = self.GRANULARITY_WEEKLY
                self._save_config()
                print("✅ Granularidad cambiada a: Semanal (weekly)")
                break
            elif option == 'b':
                break
            else:
                print("❌ Opción inválida")
    
    # ==================================================================
    # MÉTODOS DE SELECCIÓN DE CARPETAS
    # ==================================================================
    
    @staticmethod
    def select_input_folder(country_code: str = "CR") -> Optional[Path]:
        saved_path = Paths._load_saved_input_folder(country_code)
        if saved_path and saved_path.exists():
            print(f"\n📂 Carpeta de entrada guardada: {saved_path}")
            usar = input("¿Usar esta carpeta? (s/n): ").strip().lower()
            if usar in ['s', 'si', 'sí', 'yes', 'y', '']:
                return saved_path
        
        print("\n" + "=" * 50)
        print("   SELECCIONAR CARPETA DE ENTRADA".center(50))
        print("=" * 50)
        print("📁 Opciones:")
        print("   1. 📂 Seleccionar con explorador gráfico (recomendado)")
        print("   2. ⌨️  Ingresar ruta manualmente")
        print("   3. 📁 Usar carpeta DEFAULT (data_xlsx)")
        print("   4. ❌ Cancelar")
        print("-" * 50)
        
        option = input("👉 Opción (1/2/3/4): ").strip()
        
        if option == '1':
            try:
                import tkinter as tk
                from tkinter import filedialog
                root = tk.Tk()
                root.withdraw()
                folder = filedialog.askdirectory(title="Selecciona carpeta de datos LTV")
                root.destroy()
                if folder:
                    path = Path(folder)
                    Paths._save_input_folder(path, country_code)
                    print(f"✅ Carpeta seleccionada: {path}")
                    return path
                else:
                    print("⚠️ No se seleccionó ninguna carpeta")
            except Exception as e:
                print(f"⚠️ Error al abrir selector gráfico: {e}")
                return MenuConfig._manual_input_folder(country_code)
        elif option == '2':
            return MenuConfig._manual_input_folder(country_code)
        elif option == '3':
            default = Paths.get_data_xlsx_folder()
            default.mkdir(parents=True, exist_ok=True)
            Paths._save_input_folder(default, country_code)
            print(f"✅ Carpeta DEFAULT seleccionada: {default}")
            return default
        else:
            print("⚠️ Cancelado. Usando carpeta actual.")
            return None
    
    @staticmethod
    def _manual_input_folder(country_code: str = "CR") -> Optional[Path]:
        print("\n📝 Ingresa la ruta completa de la carpeta:")
        ruta = input("👉 ").strip()
        if ruta:
            path = Path(ruta)
            if path.exists():
                Paths._save_input_folder(path, country_code)
                print(f"✅ Carpeta guardada: {path}")
                return path
            else:
                crear = input("¿Crear la carpeta? (s/n): ").strip().lower()
                if crear in ['s', 'si', 'sí', 'yes', 'y']:
                    path.mkdir(parents=True, exist_ok=True)
                    Paths._save_input_folder(path, country_code)
                    print(f"✅ Carpeta creada y guardada: {path}")
                    return path
        return None
    
    @staticmethod
    def select_output_folder(country_code: str = "CR") -> Optional[Path]:
        saved_path = Paths._load_saved_output_folder(country_code)
        if saved_path and saved_path.exists():
            print(f"\n📂 Carpeta de salida guardada: {saved_path}")
            usar = input("¿Usar esta carpeta? (s/n): ").strip().lower()
            if usar in ['s', 'si', 'sí', 'yes', 'y', '']:
                return saved_path
        
        print("\n" + "=" * 50)
        print("   SELECCIONAR CARPETA DE SALIDA".center(50))
        print("=" * 50)
        print("📁 Opciones:")
        print("   1. 📂 Seleccionar con explorador gráfico (recomendado)")
        print("   2. ⌨️  Ingresar ruta manualmente")
        print("   3. 📁 Usar carpeta DEFAULT (Results_LTV)")
        print("   4. ❌ Cancelar")
        print("-" * 50)
        
        option = input("👉 Opción (1/2/3/4): ").strip()
        
        if option == '1':
            try:
                import tkinter as tk
                from tkinter import filedialog
                root = tk.Tk()
                root.withdraw()
                folder = filedialog.askdirectory(title="Selecciona carpeta para RESULTADOS LTV")
                root.destroy()
                if folder:
                    path = Path(folder)
                    Paths._save_output_folder(path, country_code)
                    print(f"✅ Carpeta seleccionada: {path}")
                    return path
                else:
                    print("⚠️ No se seleccionó ninguna carpeta")
            except Exception as e:
                print(f"⚠️ Error al abrir selector gráfico: {e}")
                return MenuConfig._manual_output_folder(country_code)
        elif option == '2':
            return MenuConfig._manual_output_folder(country_code)
        elif option == '3':
            from Run.Config.paths import Paths as PathsUtil
            root = PathsUtil.get_project_root()
            default = root / f"Data_LTV_{country_code}" / "Results_LTV"
            default.mkdir(parents=True, exist_ok=True)
            Paths._save_output_folder(default, country_code)
            print(f"✅ Carpeta DEFAULT seleccionada: {default}")
            return default
        else:
            print("⚠️ Cancelado. Usando carpeta actual.")
            return None
    
    @staticmethod
    def _manual_output_folder(country_code: str = "CR") -> Optional[Path]:
        print("\n📝 Ingresa la ruta completa de la carpeta:")
        ruta = input("👉 ").strip()
        if ruta:
            path = Path(ruta)
            path.mkdir(parents=True, exist_ok=True)
            Paths._save_output_folder(path, country_code)
            print(f"✅ Carpeta guardada: {path}")
            return path
        return None
    
    # ==================================================================
    # MÉTODOS DE ORDENAMIENTO DE COHORTES
    # ==================================================================
    
    def _sort_cohorts_chronologically(self, cohorts: List[str]) -> List[str]:
        """
        Ordena cohortes cronológicamente por año y trimestre.
        Soporta formatos: Q1, Q2, 2021 Q1, 2021-Q1, Q-1, Q-2
        """
        def cohort_sort_key(cohort: str) -> tuple:
            cohort_clean = cohort.upper().strip()
            
            # Buscar año (4 dígitos)
            year_match = re.search(r'(\d{4})', cohort_clean)
            year = int(year_match.group(1)) if year_match else 0
            
            # Buscar trimestre (Q1, Q2, Q3, Q4, Q-1, Q-2, etc.)
            quarter_match = re.search(r'[Qq](\d+|\-\d+)', cohort_clean)
            if quarter_match:
                q_str = quarter_match.group(1)
                if q_str.startswith('-'):
                    quarter = int(q_str)
                else:
                    quarter = int(q_str)
            else:
                quarter = 0
            
            return (year, quarter)
        
        return sorted(cohorts, key=cohort_sort_key)
    
    # ==================================================================
    # GESTIÓN DE COHORTES (CRUD COMPLETO)
    # ==================================================================
    
    def _get_supuestos_manager(self) -> Optional[CohortSupuestosManager]:
        """Obtiene una instancia del manager de supuestos"""
        supuestos_path = self.paths.inputs_dir / self.paths.supuestos_file
        if not supuestos_path.exists():
            print(f"❌ No se encuentra SUPUESTOS.xlsx en {supuestos_path}")
            print("   Ejecuta el pipeline al menos una vez para generarlo")
            return None
        from Run.Services.cohort_supuestos_manager import CohortSupuestosManager
        return CohortSupuestosManager(str(supuestos_path), self.paths.country)
    
    def _display_cohorts_summary(self, manager):
        """Muestra resumen de cohortes existentes ordenadas cronológicamente."""
        print("\n" + "-" * 50)
        print("📊 COHORTES EXISTENTES".center(50))
        print("-" * 50)
        
        for sheet_name in manager.EXPECTED_SHEETS:
            cohorts = manager.get_existing_cohorts(sheet_name)
            if cohorts:
                sorted_cohorts = self._sort_cohorts_chronologically(list(cohorts))
                print(f"\n📋 {sheet_name}: {len(cohorts)} cohortes")
                display = sorted_cohorts[:10]
                if len(sorted_cohorts) > 15:
                    display.append("...")
                    display.extend(sorted_cohorts[-5:])
                print(f"   {', '.join(display)}")
    
    def _view_cohort_supuestos(self, manager: CohortSupuestosManager):
        """Ver supuestos detallados de una cohorte - VERSIÓN CORREGIDA"""
        print("\n" + "=" * 60)
        print("   VER SUPUESTOS DE COHORTE".center(60))
        print("=" * 60)
        
        # Mostrar pestañas disponibles con nombres reales de hojas
        print("\n📋 Pestañas disponibles:")
        for i, bu in enumerate(manager.EXPECTED_SHEETS, 1):
            cohort_count = len(manager.get_existing_cohorts(bu))
            real_sheet_name = manager._get_sheet_name_for_bu(bu)
            print(f"   {i}. {real_sheet_name} ({bu}) - {cohort_count} cohortes")
        
        try:
            sheet_idx = int(input("\n👉 Selecciona pestaña (número): ")) - 1
            if sheet_idx < 0 or sheet_idx >= len(manager.EXPECTED_SHEETS):
                raise ValueError
            bu = manager.EXPECTED_SHEETS[sheet_idx]
            sheet_name = manager._get_sheet_name_for_bu(bu)
        except:
            print("❌ Selección inválida")
            return
        
        # Obtener cohortes ordenadas
        cohorts = self._sort_cohorts_chronologically(list(manager.get_existing_cohorts(bu)))
        if not cohorts:
            print(f"⚠️ No hay cohortes en {sheet_name}")
            return
        
        print(f"\n📊 Cohortes en {sheet_name}:")
        for i, cohort in enumerate(cohorts, 1):
            print(f"   {i}. {cohort}")
        
        try:
            cohort_idx = int(input("\n👉 Selecciona cohorte (número): ")) - 1
            if cohort_idx < 0 or cohort_idx >= len(cohorts):
                raise ValueError
            cohort_id = cohorts[cohort_idx]
        except:
            print("❌ Selección inválida")
            return
        
        # Obtener valores usando el manager
        values = manager.get_cohort_supuestos(cohort_id, bu)
        if values:
            print(f"\n📋 SUPUESTOS PARA {cohort_id} en {sheet_name}:")
            print("-" * 50)
            
            # Mostrar todos los campos de forma ordenada
            fields_display = [
                ('cogs', 'COGS (Costo de ventas)'),
                ('retention', 'Retention (Retención)'),
                ('cac', 'CAC (Costo adquisición)'),
                ('shipping_cost', 'Shipping Cost (Costo envío)'),
                ('shipping_revenue', 'Shipping Revenue (Ingreso envío)'),
                ('credit_card_payment', 'Credit Card Payment (Comisión TC)'),
                ('cash_on_delivery_comision', 'Cash on Delivery Comisión'),
                ('fc_variable_headcount', 'FC Variable Headcount'),
                ('cs_variable_headcount', 'CS Variable Headcount'),
                ('fraud', 'Fraud (Fraude)'),
                ('infrastructure', 'Infrastructure (Infraestructura)'),
            ]
            
            for field, display_name in fields_display:
                val = values.get(field, 0)
                print(f"   {display_name}: {val}")
            print("-" * 50)
        else:
            print(f"❌ No se encontraron valores para {cohort_id}")
    
    # Reemplazar el método _edit_cohort_values con esta versión mejorada

    def _edit_cohort_values(self, manager: CohortSupuestosManager):
        """Edita valores de una cohorte existente - VERSIÓN ROBUSTA CON CONVERSIÓN A FLOAT"""
        print("\n" + "=" * 60)
        print("   EDITAR SUPUESTOS DE COHORTE".center(60))
        print("=" * 60)
        
        # Mostrar pestañas disponibles
        print("\n📋 Pestañas disponibles:")
        for i, bu in enumerate(manager.EXPECTED_SHEETS, 1):
            cohort_count = len(manager.get_existing_cohorts(bu))
            real_sheet_name = manager._get_sheet_name_for_bu(bu)
            print(f"   {i}. {real_sheet_name} ({bu}) - {cohort_count} cohortes")
        
        sheet_choice = input("\n👉 Selecciona pestaña (número): ").strip()
        try:
            sheet_idx = int(sheet_choice) - 1
            if sheet_idx < 0 or sheet_idx >= len(manager.EXPECTED_SHEETS):
                raise ValueError
            bu = manager.EXPECTED_SHEETS[sheet_idx]
            sheet_name = manager._get_sheet_name_for_bu(bu)
        except:
            print("❌ Selección inválida")
            return
        
        # Mostrar cohortes de esa pestaña ordenadas
        cohorts = self._sort_cohorts_chronologically(list(manager.get_existing_cohorts(bu)))
        if not cohorts:
            print(f"⚠️ No hay cohortes en {sheet_name}")
            return
        
        print(f"\n📊 Cohortes en {sheet_name}:")
        for i, cohort in enumerate(cohorts[:30], 1):
            print(f"   {i}. {cohort}")
        if len(cohorts) > 30:
            print(f"   ... y {len(cohorts) - 30} más")
        
        cohort_choice = input("\n👉 Selecciona cohorte (nombre o número): ").strip().upper()
        
        selected_cohort = None
        if cohort_choice.isdigit():
            idx = int(cohort_choice) - 1
            if 0 <= idx < len(cohorts):
                selected_cohort = cohorts[idx]
        else:
            if cohort_choice in cohorts:
                selected_cohort = cohort_choice
        
        if not selected_cohort:
            print(f"❌ Cohorte '{cohort_choice}' no encontrada")
            return
        
        # Obtener valores actuales
        current_values = manager.get_cohort_supuestos(selected_cohort, bu)
        if not current_values:
            print(f"❌ No se encontraron valores para {selected_cohort}")
            return
        
        # Mostrar valores actuales
        print("\n" + "=" * 60)
        print(f"📝 EDITANDO COHORTE: {selected_cohort} en {sheet_name}".center(60))
        print("=" * 60)
        
        fields_config = [
            ('cogs', 'COGS (Costo de ventas)', 0.7 if bu == 'TM' else 0),
            ('retention', 'Retention (Retención)', 0),
            ('cac', 'CAC (Costo adquisición)', 0),
            ('shipping_cost', 'Shipping Cost (Costo envío)', 0),
            ('shipping_revenue', 'Shipping Revenue (Ingreso envío)', 0),
            ('credit_card_payment', 'Credit Card Payment (Comisión TC)', 0),
            ('cash_on_delivery_comision', 'Cash on Delivery Comisión', 0),
            ('fc_variable_headcount', 'FC Variable Headcount', 0),
            ('cs_variable_headcount', 'CS Variable Headcount', 0),
            ('fraud', 'Fraud (Fraude)', 0),
            ('infrastructure', 'Infrastructure (Infraestructura)', 0),
        ]
        
        new_values = {}
        
        print("\n📊 VALORES ACTUALES:")
        print("-" * 50)
        for field, display_name, default in fields_config:
            current_val = current_values.get(field, default)
            print(f"   {display_name}: {current_val}")
        
        print("\n" + "-" * 50)
        print("💡 INSTRUCCIONES:")
        print("   - Presiona Enter para mantener el valor actual")
        print("   - Escribe un número para cambiar el valor")
        print("   - Escribe 'skip' para saltar este campo")
        print("   - Escribe 'done' para terminar la edición")
        print("-" * 50)
        
        for field, display_name, default in fields_config:
            current_val = current_values.get(field, default)
            
            while True:
                prompt = f"\n👉 {display_name} [{current_val}]: "
                user_input = input(prompt).strip().lower()
                
                if user_input == '':
                    new_values[field] = current_val
                    break
                elif user_input == 'skip':
                    new_values[field] = current_val
                    print(f"   ⏭️ Saltado: {display_name}")
                    break
                elif user_input == 'done':
                    print(f"   ✅ Edición finalizada")
                    break
                else:
                    try:
                        new_val = float(user_input)
                        new_values[field] = new_val
                        print(f"   ✅ {display_name}: {current_val} → {new_val}")
                        break
                    except ValueError:
                        print(f"   ❌ Valor inválido. Ingresa un número, 'skip' o 'done'")
            
            if user_input == 'done':
                for f, _, d in fields_config:
                    if f not in new_values:
                        new_values[f] = current_values.get(f, d)
                break
        
        for field, _, default in fields_config:
            if field not in new_values:
                new_values[field] = current_values.get(field, default)
        
        print("\n" + "=" * 60)
        print("   RESUMEN DE CAMBIOS".center(60))
        print("=" * 60)
        
        changes_made = False
        for field, display_name, _ in fields_config:
            old_val = current_values.get(field, 0)
            new_val = new_values.get(field, old_val)
            if old_val != new_val:
                print(f"   {display_name}: {old_val} → {new_val}")
                changes_made = True
        
        if not changes_made:
            print("   ⚠️ No se realizaron cambios")
            return
        
        confirm = input("\n👉 ¿Guardar estos cambios? (s/n): ").strip().lower()
        if confirm not in ['s', 'si', 'sí', 'yes', 'y']:
            print("   ❌ Cambios descartados")
            return
        
        # Guardar cambios - CONVERSIÓN A FLOAT
        try:
            import pandas as pd
            
            df = pd.read_excel(manager.supuestos_path, sheet_name=sheet_name)
            df['cohort'] = df['cohort'].astype(str).str.strip().str.upper()
            
            # 🔧 CONVERTIR TODAS LAS COLUMNAS NUMÉRICAS A FLOAT
            numeric_cols = [f[0] for f in fields_config]
            for col in numeric_cols:
                if col in df.columns:
                    # Convertir a float, errores → NaN, luego NaN → 0
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float)
            
            mask = df['cohort'] == selected_cohort
            
            for field, _, _ in fields_config:
                if field in new_values:
                    df.loc[mask, field] = float(new_values[field])
            
            with pd.ExcelWriter(manager.supuestos_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                excel_file = pd.ExcelFile(manager.supuestos_path)
                for sheet in excel_file.sheet_names:
                    if sheet != sheet_name:
                        df_sheet = pd.read_excel(manager.supuestos_path, sheet_name=sheet)
                        df_sheet.to_excel(writer, sheet_name=sheet, index=False)
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"✅ Cohorte {selected_cohort} actualizada exitosamente")
            manager._load_existing_cohorts()
            
        except Exception as e:
            print(f"❌ Error al actualizar: {e}")
            import traceback
            traceback.print_exc()
    
    def _add_new_cohorts_interactive(self, manager: CohortSupuestosManager):
        """
        Agrega nuevas cohortes al archivo Excel de forma interactiva.
        VERSIÓN CORREGIDA: Usa nombres reales de hojas (con sufijo de país)
        """
        print("\n" + "=" * 50)
        print("   AGREGAR NUEVAS COHORTES".center(50))
        print("=" * 50)
        
        # Detectar cohortes que ya existen
        all_existing = set()
        for bu in manager.EXPECTED_SHEETS:
            all_existing.update(manager.get_existing_cohorts(bu))
        
        existing_sorted = self._sort_cohorts_chronologically(list(all_existing))
        print(f"📊 Cohortes existentes: {existing_sorted[:20]}{'...' if len(existing_sorted) > 20 else ''}")
        print("\n📝 Ingresa las cohortes que quieres agregar (ej: Q22, Q23, 2024-01)")
        print("   Puedes ingresar múltiples separadas por coma o espacio")
        print("   Deja vacío para cancelar")
        
        cohort_input = input("\n👉 Cohortes a agregar: ").strip()
        if not cohort_input:
            return
        
        # Parsear input
        new_cohorts_raw = cohort_input.replace(',', ' ').split()
        new_cohorts = [c.strip().upper() for c in new_cohorts_raw if c.strip()]
        
        # Filtrar las que ya existen
        really_new = [c for c in new_cohorts if c not in all_existing]
        
        if not really_new:
            print("⚠️ Todas las cohortes ya existen")
            return
        
        print(f"\n📌 Nuevas cohortes a agregar: {really_new}")
        
        # Preguntar por pestañas - MOSTRAR NOMBRES REALES
        print("\n📋 ¿En qué pestañas quieres agregarlas?")
        print("   1. Todas (1P, 3P, FBP, TM, DS)")
        print("   2. Seleccionar manualmente")
        print("   3. Cancelar")
        
        option = input("\n👉 Opción (1/2/3): ").strip()
        
        if option == '3':
            return
        
        sheets_to_update = []
        if option == '1':
            sheets_to_update = manager.EXPECTED_SHEETS
        else:
            print("\nSelecciona pestañas (separadas por número):")
            for i, bu in enumerate(manager.EXPECTED_SHEETS, 1):
                real_name = manager._get_sheet_name_for_bu(bu)
                print(f"   {i}. {real_name} ({bu})")
            sheet_input = input("\n👉 Números (ej: 1,3,5): ").strip()
            try:
                indices = [int(x.strip()) - 1 for x in sheet_input.split(',')]
                sheets_to_update = [manager.EXPECTED_SHEETS[i] for i in indices if 0 <= i < len(manager.EXPECTED_SHEETS)]
            except:
                print("❌ Selección inválida")
                return
        
        if not sheets_to_update:
            print("❌ No se seleccionaron pestañas")
            return
        
        # Preguntar por valores
        use_defaults = input("\n¿Usar valores por defecto para retention y cogs? (s/n): ").strip().lower()
        auto_defaults = use_defaults in ['s', 'si', 'sí', 'yes', 'y']
        
        # Agregar a cada pestaña usando el nombre REAL de la hoja
        for bu in sheets_to_update:
            sheet_name = manager._get_sheet_name_for_bu(bu)  # ← CLAVE: usar nombre real
            print(f"\n📝 Procesando pestaña: {sheet_name}")
            
            new_rows = []
            for cohort in really_new:
                row = self._prompt_for_values(cohort, bu, use_defaults=auto_defaults)
                new_rows.append(row)
            
            if new_rows:
                manager._append_to_excel(sheet_name, new_rows)
        
        print("\n✅ Cohortes agregadas exitosamente")
        manager._load_existing_cohorts()
    
    def _prompt_for_values(self, cohort: str, bu: str, use_defaults: bool = False) -> Dict:
        """Solicita valores para una nueva cohorte - PREGUNTA TODOS LOS CAMPOS"""
        
        if use_defaults:
            return {
                'cohort': cohort,
                'shipping_cost': 0.0,
                'shipping_revenue': 0.0,
                'credit_card_payment': 0.0,
                'cash_on_delivery_comision': 0.0,
                'fc_variable_headcount': 0.0,
                'cs_variable_headcount': 0.0,
                'fraud': 0.0,
                'infrastructure': 0.0,
                'cogs': 0.7 if bu == 'TM' else 0.0,
                'retention': 0.0,
                'cac': 0.0
            }
        
        print(f"\n📝 Configurando cohorte {cohort} para BU {bu}:")
        print("   (Presiona Enter para usar valor por defecto 0)")
        print("-" * 40)
        
        # TODOS LOS CAMPOS
        shipping_cost = input(f"   Shipping Cost (Costo envío) [0]: ").strip()
        shipping_revenue = input(f"   Shipping Revenue (Ingreso envío) [0]: ").strip()
        credit_card = input(f"   Credit Card Payment (Comisión TC) [0]: ").strip()
        cod = input(f"   Cash on Delivery Comisión [0]: ").strip()
        fc_hc = input(f"   FC Variable Headcount [0]: ").strip()
        cs_hc = input(f"   CS Variable Headcount [0]: ").strip()
        fraud = input(f"   Fraud (Fraude) [0]: ").strip()
        infrastructure = input(f"   Infrastructure (Infraestructura) [0]: ").strip()
        
        cogs_default = 0.7 if bu == 'TM' else 0
        cogs = input(f"   COGS (Costo de ventas) [{cogs_default}]: ").strip()
        
        retention = input(f"   Retention (Retención) [0]: ").strip()
        cac = input(f"   CAC (Costo adquisición) [0]: ").strip()
        
        return {
            'cohort': cohort,
            'shipping_cost': float(shipping_cost) if shipping_cost else 0.0,
            'shipping_revenue': float(shipping_revenue) if shipping_revenue else 0.0,
            'credit_card_payment': float(credit_card) if credit_card else 0.0,
            'cash_on_delivery_comision': float(cod) if cod else 0.0,
            'fc_variable_headcount': float(fc_hc) if fc_hc else 0.0,
            'cs_variable_headcount': float(cs_hc) if cs_hc else 0.0,
            'fraud': float(fraud) if fraud else 0.0,
            'infrastructure': float(infrastructure) if infrastructure else 0.0,
            'cogs': float(cogs) if cogs else float(cogs_default),
            'retention': float(retention) if retention else 0.0,
            'cac': float(cac) if cac else 0.0
        }
    
    def _delete_cohort(self, manager: CohortSupuestosManager):
        """Elimina una cohorte del archivo Excel - VERSIÓN CORREGIDA"""
        print("\n" + "=" * 60)
        print("   ELIMINAR COHORTE".center(60))
        print("=" * 60)
        
        # Mostrar pestañas disponibles con nombres reales de hojas
        print("\n📋 Pestañas disponibles:")
        for i, bu in enumerate(manager.EXPECTED_SHEETS, 1):
            cohort_count = len(manager.get_existing_cohorts(bu))
            real_sheet_name = manager._get_sheet_name_for_bu(bu)
            print(f"   {i}. {real_sheet_name} ({bu}) - {cohort_count} cohortes")
        
        try:
            sheet_idx = int(input("\n👉 Selecciona pestaña (número): ")) - 1
            if sheet_idx < 0 or sheet_idx >= len(manager.EXPECTED_SHEETS):
                raise ValueError
            bu = manager.EXPECTED_SHEETS[sheet_idx]
            sheet_name = manager._get_sheet_name_for_bu(bu)  # ← CLAVE: nombre real
        except:
            print("❌ Selección inválida")
            return
        
        # Obtener cohortes ordenadas
        cohorts = self._sort_cohorts_chronologically(list(manager.get_existing_cohorts(bu)))
        if not cohorts:
            print(f"⚠️ No hay cohortes en {sheet_name}")
            return
        
        print(f"\n📊 Cohortes en {sheet_name}:")
        for i, cohort in enumerate(cohorts, 1):
            print(f"   {i}. {cohort}")
        
        try:
            cohort_idx = int(input("\n👉 Selecciona cohorte a eliminar (número): ")) - 1
            if cohort_idx < 0 or cohort_idx >= len(cohorts):
                raise ValueError
            cohort_id = cohorts[cohort_idx]
        except:
            print("❌ Selección inválida")
            return
        
        confirm = input(f"\n⚠️ ¿Eliminar cohorte '{cohort_id}' de {sheet_name}? (s/n): ").strip().lower()
        if confirm not in ['s', 'si', 'sí', 'yes', 'y']:
            print("   Eliminación cancelada")
            return
        
        try:
            import pandas as pd
            
            # Usar sheet_name REAL (con sufijo)
            df = pd.read_excel(manager.supuestos_path, sheet_name=sheet_name)
            df['cohort'] = df['cohort'].astype(str).str.strip().str.upper()
            
            df = df[df['cohort'] != cohort_id.upper()]
            
            with pd.ExcelWriter(manager.supuestos_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                excel_file = pd.ExcelFile(manager.supuestos_path)
                for sheet in excel_file.sheet_names:
                    if sheet != sheet_name:
                        df_sheet = pd.read_excel(manager.supuestos_path, sheet_name=sheet)
                        df_sheet.to_excel(writer, sheet_name=sheet, index=False)
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"✅ Cohorte '{cohort_id}' eliminada de {sheet_name}")
            manager._load_existing_cohorts()
            
        except Exception as e:
            print(f"❌ Error al eliminar: {e}")
            import traceback
            traceback.print_exc()

    def manage_cohorts_menu(self):
        """Menú principal de gestión de cohortes (CRUD completo)"""
        manager = self._get_supuestos_manager()
        if not manager:
            return
        
        while True:
            print("\n" + "=" * 50)
            print("   GESTIÓN DE COHORTES".center(50))
            print("=" * 50)
            print("\n1. 📋 Ver cohortes existentes")
            print("2. 📊 Ver supuestos por cohorte")
            print("3. ✏️ Editar valores de cohorte")
            print("4. ➕ Agregar nuevas cohortes")
            print("5. 🗑️ Eliminar cohorte")
            print("6. 🔄 Validar estructura del archivo")
            print("\nq. 🔙 Volver")
            print("=" * 50)
            
            option = input("\n👉 Opción: ").strip().lower()
            
            if option == '1':
                self._display_cohorts_summary(manager)
                input("\nPresiona Enter para continuar...")
            elif option == '2':
                self._view_cohort_supuestos(manager)
                input("\nPresiona Enter para continuar...")
            elif option == '3':
                self._edit_cohort_values(manager)
                input("\nPresiona Enter para continuar...")
            elif option == '4':
                self._add_new_cohorts_interactive(manager)
                input("\nPresiona Enter para continuar...")
            elif option == '5':
                self._delete_cohort(manager)
                input("\nPresiona Enter para continuar...")
            elif option == '6':
                warnings = manager.validate_supuestos_file()
                for w in warnings:
                    print(w)
                input("\nPresiona Enter para continuar...")
            elif option == 'q':
                break
            else:
                print("❌ Opción inválida")