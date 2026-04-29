# ============================================================================
# FILE: Run/Menu/menu_executor.py
# COMPLETO - CON CORRECCIÓN PARA CR (FORZAR CARGA DE CREDENCIALES POR PAÍS)
# VERSIÓN MEJORADA: Fallback a DataRepository cuando no hay datos
# ============================================================================
# Archivo: Run/Menu/menu_executor.py
# Versión refactorizada - MULTI-PAÍS CON CREDENCIALES CORREGIDAS

import os
import sys
import time
import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Tuple

import pandas as pd

from Run.Config.paths import PathsConfig
from Run.Config.credentials import Credentials
from Run.Services.script_runner import ScriptRunner
from Run.Services.cohort_supuestos_manager import CohortSupuestosManager
from Run.Core.ssh_manager import SSHManager
from Run.Core.cohort_context_manager import CohortContextManager
from Run.Country.country_context import CountryContext
from Run.FX.fx_engine import FXEngine
from Run.Utils.logger import SystemLogger
from Run.Utils.retry import retry, RetryError


class MenuExecutor:
    """Ejecuta los pipelines (DR, MD) - MULTI-PAÍS CON CREDENCIALES CORREGIDAS"""
    
    def __init__(self, paths: PathsConfig, logger: SystemLogger, 
                 country_context: CountryContext, fx_engine: FXEngine):
        self.paths = paths
        self.logger = logger
        self.country_context = country_context
        self.fx_engine = fx_engine
        self.script_runner = ScriptRunner()
        self.ssh_manager = SSHManager()
        self.current_process_running = False
        self._cancel_requested = False
        
        self.dr_script = os.path.join(paths.code_path, "DataRepository", "Output", "mainDR.py")
        self.md_script = os.path.join(paths.code_path, "Model", "Output", "mainMD.py")
        
        self._cohort_context = None
    
    def request_cancel(self):
        """Solicita cancelación del proceso actual."""
        self._cancel_requested = True
    
    def ensure_runtime_environment(self) -> bool:
        print("\n" + "🔧 VERIFICANDO ENTORNO RUNTIME".center(60, "-"))
        
        self.paths.resolve()
        print(f"✅ Paths normalizados: {self.paths.inputs_dir}")
        print(f"🌎 País: {self.country_context.name} ({self.country_context.code})")
        print(f"💱 FX Rate default: {self.country_context.default_fx_rate}")
        
        if not self.ssh_manager.start():
            print("⚠️ No se pudo establecer SSH. Continuando en modo local...")
        
        print("-" * 60)
        return True
    
    def get_cohort_context(self) -> CohortContextManager:
        if self._cohort_context is None:
            supuestos_path = self.paths.inputs_dir / self.paths.supuestos_file
            self._cohort_context = CohortContextManager(supuestos_path, self.country_context)
        return self._cohort_context
    
    def start_ssh_tunnel(self) -> bool:
        return self.ssh_manager.start()
    
    def stop_ssh_tunnel(self):
        self.ssh_manager.stop()
    
    def get_date_range_from_user(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        print("\n" + "=" * 50)
        print(f"   RANGO TEMPORAL - {self.country_context.name}".center(50))
        print("=" * 50)
        print(f"📅 Año inicio cohortes: {self.country_context.cohort_start_year}")
        print("-" * 50)
        
        start_input = input("📌 Fecha inicio (YYYY-MM) [default: todo]: ").strip()
        end_input = input("📌 Fecha fin (YYYY-MM) [default: todo]: ").strip()
        
        if not start_input and not end_input:
            print("\n✅ Usando dataset COMPLETO")
            return (None, None)
        
        pattern = r'^\d{4}-\d{2}$'
        start_date = None
        end_date = None
        
        if start_input and re.match(pattern, start_input):
            try:
                start_date = datetime.strptime(start_input, "%Y-%m")
                print(f"✅ Fecha inicio: {start_date.strftime('%Y-%m-%d')}")
            except ValueError:
                print(f"⚠️ Formato inválido: '{start_input}'")
        
        if end_input and re.match(pattern, end_input):
            try:
                end_date = datetime.strptime(end_input, "%Y-%m")
                if end_date.month == 12:
                    end_date = end_date.replace(year=end_date.year + 1, month=1, day=1)
                else:
                    end_date = end_date.replace(month=end_date.month + 1, day=1)
                print(f"✅ Fecha fin: {end_date.strftime('%Y-%m-%d')}")
            except ValueError:
                print(f"⚠️ Formato inválido: '{end_input}'")
        
        if not start_date and not end_date:
            return (None, None)
        
        if start_date and end_date and start_date >= end_date:
            print(f"\n⚠️ ERROR: Fecha inicio >= fecha fin. Usando dataset COMPLETO.")
            return (None, None)
        
        return (start_date, end_date)
    
    def check_and_setup_cohort_supuestos(self, cohorts_in_data: list) -> bool:
        supuestos_path = self.paths.inputs_dir / self.paths.supuestos_file
        
        if not supuestos_path.exists():
            print(f"⚠️ No se encontró SUPUESTOS.xlsx en {supuestos_path}")
            return True
        
        # Pasar country_code al manager
        manager = CohortSupuestosManager(str(supuestos_path), self.country_context.code)
        
        warnings = manager.validate_supuestos_file()
        for w in warnings:
            print(w)
        
        if not cohorts_in_data:
            return True
        
        return manager.interactive_setup(cohorts_in_data)
    
    def _get_cohorts_from_data_ltv(self) -> list:
        data_ltv_files = list(self.paths.data_ltv.glob("Resultado_Unit_Economics_*.csv"))
        if not data_ltv_files:
            return []
        
        latest_file = max(data_ltv_files, key=os.path.getctime)
        
        try:
            df_cohorts = pd.read_csv(latest_file, usecols=['cohort'])
            unique_cohorts = df_cohorts['cohort'].dropna().unique().tolist()
            return sorted(set(unique_cohorts))
        except Exception as e:
            print(f"⚠️ Error leyendo cohortes: {e}")
            return []
    
    def _get_base_env(self) -> dict:
        """Obtiene el entorno base con credenciales."""
        # 🔧 Pasar el país actual explícitamente
        db_creds = Credentials.get_db_credentials(self.country_context.code)
        env = db_creds.to_env_dict()
        env["LTV_COUNTRY"] = self.country_context.code
        env["LTV_FX_DEFAULT_RATE"] = str(self.country_context.default_fx_rate)
        
        # 🔍 DIAGNÓSTICO - Ver qué tiene db_creds
        print(f"\n🔍 [_get_base_env] db_creds:")
        print(f"   user: {db_creds.user}")
        print(f"   host: {db_creds.host}")
        print(f"   database: {db_creds.database}")
        print(f"   country: {db_creds.country}")
        
        print(f"\n🔍 [_get_base_env] env dict:")
        print(f"   DB_USER: {env.get('DB_USER', 'N/A')}")
        print(f"   DB_HOST: {env.get('DB_HOST', 'N/A')}")
        print(f"   DB_NAME: {env.get('DB_NAME', 'N/A')}")
        print(f"   LTV_COUNTRY: {env.get('LTV_COUNTRY', 'N/A')}")
        
        return env
    
    # Agregar parámetro dimension_filter a get_env_for_md
    def get_env_for_md(self, run_folder: str, dimensions: List[int], only_category: bool = False,
                    date_range=None, extra_env: Dict[str, str] = None,
                    grouping_mode: str = "entry_based", conversion_mode: str = "cumulative",
                    granularity: str = "quarterly", dimension_filter: Optional[Dict] = None) -> dict:
        env = self._get_base_env()
        env["LTV_PATH_CONTROL"] = str(self.paths.data_ltv)
        env["LTV_INPUT_DIR"] = str(self.paths.inputs_dir)
        env["LTV_SOIS_FILE"] = self.paths.sois_file
        env["LTV_SUPUESTOS_FILE"] = self.paths.supuestos_file
        env["LTV_CATALOGO_FILE"] = self.paths.catalogo_file
        env["LTV_CAC_FILE"] = self.paths.cac_file
        env["LTV_FX_FILE"] = self.paths.fx_file
        env["LTV_OUTPUT_DIR"] = run_folder
        env["LTV_GROUPING_MODE"] = grouping_mode
        env["LTV_CONVERSION_MODE"] = conversion_mode
        env["LTV_GRANULARITY"] = granularity
        
        # NUEVO: Pasar filtro de dimensiones si existe
        if dimension_filter:
            import json
            env["LTV_DIMENSION_FILTER"] = json.dumps(dimension_filter)
        
        if date_range:
            start_date, end_date = date_range
            if start_date:
                env["LTV_START_DATE"] = start_date.strftime("%Y-%m-%d")
            if end_date:
                env["LTV_END_DATE"] = end_date.strftime("%Y-%m-%d")
        
        if only_category:
            env["LTV_ONLY_CATEGORY"] = "TRUE"
            env["LTV_DIMENSION_MODE"] = "0"
        else:
            env["LTV_ONLY_CATEGORY"] = "FALSE"
            env["LTV_DIMENSION_MODE"] = ",".join(str(d) for d in dimensions)
        
        if extra_env:
            env.update(extra_env)
        
        return env
    
    def get_env_for_dr(self, date_range=None) -> dict:
        env = self._get_base_env()
        env["LTV_INPUT_DIR"] = str(self.paths.inputs_dir)
        env["LTV_SOIS_FILE"] = self.paths.sois_file
        env["LTV_SUPUESTOS_FILE"] = self.paths.supuestos_file
        env["LTV_CATALOGO_FILE"] = self.paths.catalogo_file
        env["LTV_CAC_FILE"] = self.paths.cac_file
        env["LTV_FX_FILE"] = self.paths.fx_file
        env["LTV_OUTPUT_DIR"] = str(self.paths.data_ltv)
        env["LTV_PATH_CONTROL"] = str(self.paths.data_ltv)
        
        # 🔧 FORZAR CONVERSIÓN A STRING SEGURO
        if date_range:
            start_date = None
            end_date = None
            
            # Manejar tupla
            if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
                start_date, end_date = date_range
            # Manejar diccionario
            elif isinstance(date_range, dict):
                start_date = date_range.get('start_date')
                end_date = date_range.get('end_date')
            else:
                start_date = date_range
            
            # Convertir a string para la variable de entorno
            if start_date:
                if hasattr(start_date, 'strftime'):
                    env["LTV_START_DATE"] = start_date.strftime("%Y-%m-%d")
                else:
                    env["LTV_START_DATE"] = str(start_date)
            
            if end_date:
                if hasattr(end_date, 'strftime'):
                    env["LTV_END_DATE"] = end_date.strftime("%Y-%m-%d")
                else:
                    env["LTV_END_DATE"] = str(end_date)
        
        return env
    
    def create_run_folder(self, dimension_name: str = "", grouping_mode: str = "entry_based",
                          granularity: str = "quarterly") -> str:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        mode_suffix = "beh" if grouping_mode == "behavioral" else "ent"
        gran_suffix = granularity[:3]
        country_suffix = self.country_context.code.lower()
        
        if dimension_name:
            folder_name = f"Resultados_LTV_{country_suffix}_{dimension_name}_{mode_suffix}_{gran_suffix}_{timestamp}"
        else:
            folder_name = f"Resultados_LTV_{country_suffix}_{mode_suffix}_{gran_suffix}_{timestamp}"
        
        run_folder = self.paths.results_base / folder_name
        run_folder.mkdir(parents=True, exist_ok=True)
        return str(run_folder)
    
    def data_ltv_has_files(self) -> bool:
        if not self.paths.data_ltv.exists():
            return False
        try:
            return any(f.is_file() for f in self.paths.data_ltv.iterdir())
        except:
            return False
    
    def validate_scripts(self) -> bool:
        for script in [self.dr_script, self.md_script]:
            if not os.path.exists(script):
                print(f"❌ ERROR: No existe script en: {script}")
                return False
        return True
    
    @retry(max_attempts=2, delay=1)
    def _run_dr_with_retry(self, date_range=None) -> bool:
        return self.script_runner.run_script(self.dr_script, self.get_env_for_dr(date_range))
    
    # Modificar _run_md_with_retry
    @retry(max_attempts=2, delay=1)
    def _run_md_with_retry(self, run_folder: str, dimensions: List[int], only_category: bool = False,
                        date_range=None, grouping_mode: str = "entry_based",
                        conversion_mode: str = "cumulative", granularity: str = "quarterly",
                        dimension_filter: Optional[Dict] = None) -> bool:
        return self.script_runner.run_script(self.md_script, 
            self.get_env_for_md(run_folder, dimensions, only_category, date_range, None,
                            grouping_mode, conversion_mode, granularity, dimension_filter))
    
    def run_dr(self, date_range=None) -> bool:
        """Ejecuta Data Repository con credenciales FORZADAS para el país actual."""
        # 🔧 CRITICAL: Asegurar credenciales correctas antes de ejecutar DR
        print(f"\n🔐 [run_dr] Verificando credenciales para: {self.country_context.code}")
        Credentials.load_for_country(self.country_context.code)
        
        print("\n" + "=" * 80)
        print(f" EJECUTANDO DATA REPOSITORY - {self.country_context.name} ".center(80))
        print("=" * 80)
        if date_range and (date_range[0] or date_range[1]):
            start_str = date_range[0].strftime('%Y-%m-%d') if date_range[0] else 'TODO'
            end_str = date_range[1].strftime('%Y-%m-%d') if date_range[1] else 'TODO'
            print(f"📅 Rango aplicado: {start_str} → {end_str}")
        
        # Verificar credenciales antes de ejecutar
        try:
            db_creds = Credentials.get_db_credentials()
            print(f"✅ Credenciales DB cargadas: {db_creds.user}@{db_creds.host}/{db_creds.database}")
        except Exception as e:
            print(f"❌ Error cargando credenciales DB: {e}")
            return False
        
        try:
            return self._run_dr_with_retry(date_range)
        except RetryError as e:
            self.logger.error(f"DR falló después de reintentos: {e}")
            return False
    
    def run_full_pipeline(self, date_range=None, grouping_mode: str = "entry_based",
                          conversion_mode: str = "cumulative", granularity: str = "quarterly") -> bool:
        print(f"\n📊 Granularidad seleccionada: {granularity}")
        print(f"🌎 País: {self.country_context.name} ({self.country_context.code})")
        
        # 🔧 Asegurar credenciales correctas
        Credentials.load_for_country(self.country_context.code)
        
        if not self.ensure_runtime_environment():
            print("❌ Error en entorno runtime")
            return False
        
        run_folder = self.create_run_folder("Full", grouping_mode, granularity)
        
        try:
            self.current_process_running = True
            self._cancel_requested = False
            time.sleep(2)
            
            if not self.run_dr(date_range):
                print("❌ DataRepository falló.")
                return False
            
            if self._cancel_requested:
                print("⏹️ Proceso cancelado por el usuario")
                return False
            
            cohorts = self._get_cohorts_from_data_ltv()
            if cohorts:
                print(f"📊 Cohortes detectadas: {len(cohorts)}")
                if not self.check_and_setup_cohort_supuestos(cohorts):
                    print("❌ Configuración de supuestos cancelada.")
                    return False
            
            if self._cancel_requested:
                print("⏹️ Proceso cancelado por el usuario")
                return False
            
            dimensions = [1, 2, 3, 4, 5, 6]
            if not self._run_md_with_retry(run_folder, dimensions, only_category=False,
                                           date_range=date_range, grouping_mode=grouping_mode,
                                           conversion_mode=conversion_mode, granularity=granularity):
                print("❌ Model falló.")
                return False
            
            print("\n✨ PIPELINE COMPLETO FINALIZADO ✨")
            print(f"📂 Resultados: {run_folder}")
            return True
            
        except Exception as e:
            print(f"❌ Error en pipeline completo: {e}")
            return False
        finally:
            self.current_process_running = False
            self.ssh_manager.stop()
    
    def run_dr_only(self, date_range=None) -> bool:
        """Ejecuta solo Data Repository con credenciales FORZADAS."""
        # 🔧 Asegurar credenciales correctas
        Credentials.load_for_country(self.country_context.code)
        
        if not self.ensure_runtime_environment():
            print("❌ Error en entorno runtime")
            return False
        
        try:
            self.current_process_running = True
            self._cancel_requested = False
            return self.run_dr(date_range)
        finally:
            self.current_process_running = False
            self.ssh_manager.stop()
    
    # ==========================================================================
    # NUEVO: MÉTODO CON FALLBACK MEJORADO
    # ==========================================================================
    
    def _check_and_prompt_dr_fallback(self, date_range=None) -> bool:
        """
        Verifica si hay datos en Data_LTV.
        Si no hay, pregunta al usuario si desea ejecutar DataRepository.
        
        Returns:
            True: datos disponibles o DR ejecutado exitosamente
            False: usuario cancela o DR falla
        """
        if self.data_ltv_has_files():
            print("✅ Datos existentes en Data_LTV")
            return True
        
        print("\n" + "=" * 60)
        print("   ⚠️ NO HAY DATOS DISPONIBLES".center(60))
        print("=" * 60)
        print("\nLa carpeta Data_LTV está vacía o no existe.")
        print("Para ejecutar análisis, necesitas primero obtener datos.")
        print("\n📦 Opciones:")
        print("   1. 🔄 Ejecutar DataRepository ahora")
        print("   2. ❌ Cancelar y volver al menú")
        print("-" * 50)
        
        option = input("\n👉 Opción (1/2): ").strip()
        
        if option == '1':
            print("\n📦 Ejecutando DataRepository...")
            if self.run_dr(date_range):
                print("✅ DataRepository completado exitosamente")
                return True
            else:
                print("❌ DataRepository falló. No se puede continuar.")
                return False
        else:
            print("❌ Operación cancelada por el usuario")
            return False
    
    # ==========================================================================
    # MÉTODO run_model_analysis MODIFICADO CON FALLBACK
    # ==========================================================================
    
    def run_model_analysis(self, dimensions: List[int], display_name: str, only_category: bool = False,
                        date_range=None, grouping_mode: str = "entry_based",
                        conversion_mode: str = "cumulative", granularity: str = "quarterly",
                        dimension_filter: Optional[Dict] = None) -> bool:
        """Ejecuta análisis de modelo con credenciales FORZADAS para el país actual.
        
        VERSIÓN MEJORADA:
        - Fallback a DataRepository cuando no hay datos
        - Soporte para filtros jerárquicos (dimension_filter)
        """
        print(f"\n📊 Granularidad seleccionada: {granularity}")
        print(f"🌎 País: {self.country_context.name} ({self.country_context.code})")
        
        if dimension_filter:
            print(f"🔍 Filtro aplicado: {dimension_filter}")
        
        # 🔧 CRITICAL: Asegurar credenciales correctas
        Credentials.load_for_country(self.country_context.code)
        
        if not self.ensure_runtime_environment():
            print("❌ Error en entorno runtime")
            return False
        
        # ========== NUEVO: FALLBACK A DATAREPOSITORY ==========
        if not self._check_and_prompt_dr_fallback(date_range):
            return False
        
        if self._cancel_requested:
            print("⏹️ Proceso cancelado por el usuario")
            return False
        
        # ========== VERIFICAR SUPUESTOS DE COHORTES ==========
        cohorts = self._get_cohorts_from_data_ltv()
        if cohorts:
            print(f"📊 Cohortes detectadas: {len(cohorts)}")
            if not self.check_and_setup_cohort_supuestos(cohorts):
                print("❌ Configuración de supuestos cancelada.")
                return False
        
        run_folder = self.create_run_folder(display_name.replace(" ", "_"), grouping_mode, granularity)
        
        try:
            self.current_process_running = True
            self._cancel_requested = False
            if not self._run_md_with_retry(run_folder, dimensions, only_category, date_range,
                                        grouping_mode, conversion_mode, granularity,
                                        dimension_filter=dimension_filter):
                print("❌ Model falló.")
                return False
            
            print(f"\n✨ {display_name} FINALIZADO ✨")
            print(f"📂 Resultados: {run_folder}")
            return True
        finally:
            self.current_process_running = False
            self.ssh_manager.stop()
    
    def run_heavy_analysis_only(self, date_range=None, grouping_mode: str = "entry_based",
                                conversion_mode: str = "cumulative", granularity: str = "quarterly") -> bool:
        """Ejecuta solo análisis pesados con credenciales FORZADAS.
        
        VERSIÓN MEJORADA: Fallback a DataRepository cuando no hay datos.
        """
        print("\n" + "=" * 80)
        print(f" EJECUTANDO ANÁLISIS PESADOS - {self.country_context.name} ".center(80))
        print("=" * 80)
        
        # 🔧 Asegurar credenciales correctas
        Credentials.load_for_country(self.country_context.code)
        
        if not self.ensure_runtime_environment():
            print("❌ Error en entorno runtime")
            return False
        
        # ========== NUEVO: FALLBACK A DATAREPOSITORY ==========
        if not self._check_and_prompt_dr_fallback(date_range):
            return False
        
        if self._cancel_requested:
            print("⏹️ Proceso cancelado por el usuario")
            return False
        
        # ========== VERIFICAR SUPUESTOS DE COHORTES ==========
        cohorts = self._get_cohorts_from_data_ltv()
        if cohorts:
            print(f"📊 Cohortes detectadas: {len(cohorts)}")
            if not self.check_and_setup_cohort_supuestos(cohorts):
                print("❌ Configuración de supuestos cancelada.")
                return False
        
        run_folder = self.create_run_folder("HeavyAnalysis", grouping_mode, granularity)
        
        try:
            self.current_process_running = True
            self._cancel_requested = False
            
            env_overrides = {
                "LTV_SKIP_DIMENSIONS": "TRUE",
                "LTV_DIMENSION_MODE": "0"
            }
            
            env = self.get_env_for_md(run_folder, [0], only_category=False, date_range=date_range,
                                      extra_env=env_overrides, grouping_mode=grouping_mode,
                                      conversion_mode=conversion_mode, granularity=granularity)
            
            if not self.script_runner.run_script(self.md_script, env):
                print("❌ Model falló.")
                return False
            
            print("\n✨ ANÁLISIS PESADOS FINALIZADO ✨")
            print(f"📂 Resultados: {run_folder}")
            return True
        finally:
            self.current_process_running = False
            self.ssh_manager.stop()