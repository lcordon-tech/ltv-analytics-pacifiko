"""
Pipeline principal del Modelo LTV.
VERSIÓN MULTI-PAÍS: Soporta Guatemala y Costa Rica con configuración dinámica.
"""

import sys
import os
import io
import traceback
import time
from datetime import datetime

# Agregar la carpeta Model al path
MODEL_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, MODEL_PATH)

# Imports con nueva estructura
from Model.Data.real_data_repository import RealDataRepository
from Model.Data.cac_repository import CACRepository
from Model.Domain.controller import LTVController
from Model.Analytics.Cohort.cohort_analyzer import CohortAnalyzer
from Model.Analytics.Cohort.cohort_retention_matrix import CohortRetentionMatrix
from Model.Analytics.unit_economics import UnitEconomicsAnalyzer
from Model.Analytics.Cohort.cohort_behavior_calculator import CohortBehaviorCalculator
from Model.Analytics.category_value_analyzer import CategoryValueAnalyzer
from Model.Analytics.dashboard_analyzer import DashboardAnalyzer
from Model.Output.data_exporter import DataExporter
from Model.Config.country_config import get_country_config

# Importar módulos de Category
lib_path = os.environ.get("LTV_LIB_PATH")
if lib_path and lib_path not in sys.path:
    sys.path.append(lib_path)

# Importar sistema de cohortes dinámicas
from Category.Cohort.cohort_config import CohortConfig, TimeGranularity
from Category.Cohort.cohort_manager import CohortManager
from Category.Reporting.global_exporter import GlobalLTVOrchestrator

# Obtiene la carpeta donde está guardado el archivo
directorio_actual = os.path.dirname(os.path.abspath(__file__))
sys.path.append(directorio_actual)


class Logger:
    """Redirige stdout a un buffer para capturar logs."""
    def __init__(self, terminal, buffer):
        self.terminal = terminal
        self.buffer = buffer

    def write(self, message):
        self.terminal.write(message)
        self.buffer.write(message)

    def flush(self):
        self.terminal.flush()


def print_banner(text: str, width: int = 110, char: str = "="):
    """Imprime un banner formateado."""
    print("\n" + text.center(width, char))


def _parse_date_range(start_str: str, end_str: str):
    """Parsea rango de fechas desde variables de entorno."""
    start_date = None
    end_date = None
    
    if start_str:
        try:
            start_date = datetime.strptime(start_str, "%Y-%m-%d")
            print(f"📅 Fecha inicio: {start_date.date()}")
        except ValueError:
            try:
                start_date = datetime.strptime(start_str, "%Y-%m")
                print(f"📅 Fecha inicio: {start_date.date()}")
            except ValueError:
                print(f"⚠️ Fecha inicio inválida: {start_str}")
    
    if end_str:
        try:
            end_date = datetime.strptime(end_str, "%Y-%m-%d")
            print(f"📅 Fecha fin: {end_date.date()}")
        except ValueError:
            try:
                end_date = datetime.strptime(end_str, "%Y-%m")
                if end_date.month == 12:
                    end_date = end_date.replace(year=end_date.year + 1, month=1, day=1)
                else:
                    end_date = end_date.replace(month=end_date.month + 1, day=1)
                print(f"📅 Fecha fin: {end_date.date()}")
            except ValueError:
                print(f"⚠️ Fecha fin inválida: {end_str}")
    
    return start_date, end_date


def _create_cohort_config_from_env(country_config=None) -> CohortConfig:
    """
    Crea configuración de cohortes desde variables de entorno y país.
    
    Args:
        country_config: Configuración del país (para fechas default)
    
    Returns:
        CohortConfig con granularidad y rango definido
    """
    granularity_str = os.environ.get("LTV_GRANULARITY", "quarterly")
    
    # Prioridad: variable de entorno > país > default
    start_date_str = os.environ.get("LTV_START_DATE")
    end_date_str = os.environ.get("LTV_END_DATE")
    
    # Si no hay fecha en variable de entorno, usar la del país
    if not start_date_str and country_config and country_config.cohort_start_date:
        start_date_str = country_config.cohort_start_date
        print(f"🌎 Usando start_date desde configuración de país: {start_date_str}")
    
    if not end_date_str and country_config and country_config.cohort_end_date:
        end_date_str = country_config.cohort_end_date
        print(f"🌎 Usando end_date desde configuración de país: {end_date_str}")
    
    start_date, end_date = _parse_date_range(start_date_str, end_date_str)
    
    # Mapeo de strings a TimeGranularity
    granularity_map = {
        "daily": TimeGranularity.DAILY,
        "weekly": TimeGranularity.WEEKLY,
        "monthly": TimeGranularity.MONTHLY,
        "quarterly": TimeGranularity.QUARTERLY,
        "semiannual": TimeGranularity.SEMIANNUAL,
        "yearly": TimeGranularity.YEARLY,
    }
    
    granularity = granularity_map.get(granularity_str.lower(), TimeGranularity.QUARTERLY)
    
    # Leer ventanas de conversión (opcional)
    conv_windows_str = os.environ.get("LTV_CONVERSION_WINDOWS", "30,60,90,180,360")
    conversion_windows = [int(w.strip()) for w in conv_windows_str.split(",")]
    
    return CohortConfig(
        granularity=granularity,
        start_date=start_date,
        end_date=end_date,
        conversion_windows=conversion_windows
    )


def run_analysis(input_path=None, country_code=None):
    """
    Función principal del pipeline LTV - MULTI-PAÍS.
    
    Args:
        input_path: Ruta de entrada (opcional)
        country_code: Código de país (GT, CR). Si None, usa variable de entorno.
    
    Variables de entorno soportadas:
    - LTV_PATH_CONTROL: Ruta a los datos de entrada
    - LTV_OUTPUT_DIR: Directorio de salida (opcional)
    - LTV_ONLY_CATEGORY: "TRUE" para solo category/subcategory (legacy)
    - LTV_DIMENSION_MODE: Modo de dimensión (0=todas, 1=category, etc.)
    - LTV_GROUPING_MODE: "behavioral" o "entry_based" (default: "entry_based")
    - LTV_SKIP_DIMENSIONS: "TRUE" para saltar el orquestador multi-dimensión
    - LTV_GRANULARITY: "quarterly", "monthly", "weekly", "semiannual", "yearly"
    - LTV_START_DATE: Fecha inicio para cohortes (YYYY-MM-DD o YYYY-MM)
    - LTV_END_DATE: Fecha fin para cohortes (YYYY-MM-DD o YYYY-MM)
    - LTV_CONVERSION_WINDOWS: Ventanas de conversión (ej: "30,60,90,180,360")
    - LTV_COUNTRY: Código de país (GT, CR)
    """
    
    # ==========================================================================
    # 0. OBTENER CONFIGURACIÓN DE PAÍS
    # ==========================================================================
    if country_code is None:
        country_code = os.environ.get("LTV_COUNTRY", "GT").upper().strip()
    
    country_config = get_country_config(country_code)
    
    print("\n" + "=" * 110)
    print(f"   SISTEMA LTV v6.0 - {country_config.name} ({country_config.code})".center(110))
    print("=" * 110)
    
    # ==========================================================================
    # 1. CONFIGURACIÓN Y VARIABLES DE ENTORNO
    # ==========================================================================
    PATH_DIR = os.environ.get("LTV_PATH_CONTROL")
    target_dir = os.environ.get("LTV_OUTPUT_DIR", PATH_DIR)
    only_category_mode = os.environ.get("LTV_ONLY_CATEGORY") == "TRUE"
    grouping_mode = os.environ.get("LTV_GROUPING_MODE", "entry_based")
    
    print(f"📌 LTV_GROUPING_MODE = {grouping_mode}")
    print(f"📌 LTV_COUNTRY = {country_config.code}")
    
    # Leer si debemos saltar el análisis multi-dimensión
    skip_dimensions = os.environ.get("LTV_SKIP_DIMENSIONS", "FALSE").upper() == "TRUE"
    print(f"📌 LTV_SKIP_DIMENSIONS = {skip_dimensions}")
    
    # ==========================================================================
    # 2. CONFIGURACIÓN DE COHORTES DINÁMICAS (con fechas del país)
    # ==========================================================================
    cohort_config = _create_cohort_config_from_env(country_config)
    cohort_manager = CohortManager(cohort_config)
    
    print(f"\n📊 CONFIGURACIÓN DE COHORTES:")
    print(f"   País: {country_config.name} ({country_config.code})")
    print(f"   Granularidad: {cohort_config.granularity.value}")
    print(f"   Ventanas conversión: {cohort_config.conversion_windows}")
    print(f"   Total cohortes: {cohort_config.num_periods or 'auto'}")
    if cohort_config.start_date:
        print(f"   Rango: {cohort_config.start_date.date()} → {cohort_config.end_date.date()}")
    print("-" * 60)
    
    # ==========================================================================
    # 3. CONFIGURACIÓN DE DIMENSIONES
    # ==========================================================================
    dimension_mode_str = os.environ.get("LTV_DIMENSION_MODE", "0")
    print(f"📌 LTV_DIMENSION_MODE raw = '{dimension_mode_str}'")
    
    # Detectar si es una lista separada por comas
    if ',' in dimension_mode_str:
        dimensions_list = [int(d.strip()) for d in dimension_mode_str.split(',') if d.strip()]
        print(f"📌 Lista de dimensiones detectada: {dimensions_list}")
        dimensions_to_process = dimensions_list
        if len(dimensions_list) == 1:
            dimension_mode = dimensions_list[0]
        else:
            dimension_mode = 0
        if only_category_mode:
            pipeline_title = "MODO LEGACY: CATEGORY + SUBCATEGORY"
        elif len(dimensions_list) == 1:
            from Category.Utils.dimension_config import get_dimension_config
            dim_config = get_dimension_config(dimensions_list[0])
            pipeline_title = f"MODO ESPECÍFICO: {dim_config['output_key'].upper()}"
        else:
            pipeline_title = f"MODO MULTI-DIMENSIÓN: {len(dimensions_list)} dimensiones"
    else:
        try:
            dimension_mode = int(dimension_mode_str)
        except ValueError:
            dimension_mode = 0
        
        if only_category_mode:
            dimensions_to_process = [1, 2]  # Category y Subcategory
            pipeline_title = "MODO LEGACY: CATEGORY + SUBCATEGORY"
        elif dimension_mode == 0:
            from Category.Utils.dimension_config import get_all_dimension_modes
            dimensions_to_process = get_all_dimension_modes()
            pipeline_title = f"MODO COMPLETO: {len(dimensions_to_process)} DIMENSIONES"
        else:
            dimensions_to_process = [dimension_mode]
            from Category.Utils.dimension_config import get_dimension_config
            dim_config = get_dimension_config(dimension_mode)
            pipeline_title = f"MODO ESPECÍFICO: {dim_config['output_key'].upper()}"
    
    print(f"📌 Dimensiones a procesar: {dimensions_to_process}")
    
    # ==========================================================================
    # 4. CARGA DE CAC (con país)
    # ==========================================================================
    print("\n" + "-" * 60)
    print(" CARGANDO CAC DESDE EXCEL ".center(60))
    print("-" * 60)
    
    cac_path = os.environ.get("LTV_CAC_PATH")
    ad_spend = CACRepository.get_cac_mapping(
        country_config=country_config,
        cac_path=cac_path,
        granularity=cohort_config.granularity.value
    )
    
    if not ad_spend:
        print(f"⚠️ ADVERTENCIA: No hay datos de CAC para {country_config.name}")
        print("   El análisis de LTV neto no incluirá CAC")
    else:
        print(f"✅ CAC cargado para {country_config.name}: {len(ad_spend)} cohortes")
        sample = list(ad_spend.items())[:5]
        for cohort, cac_val in sample:
            print(f"   {cohort}: ${cac_val:.2f}")
        if len(ad_spend) > 5:
            print(f"   ... y {len(ad_spend) - 5} más")
    
    # ==========================================================================
    # 5. INICIALIZACIÓN Y BANNER
    # ==========================================================================
    start_total = time.time()
    output_buffer = io.StringIO()
    original_stdout = sys.stdout
    sys.stdout = Logger(original_stdout, output_buffer)

    print("=" * 110)
    print(f"   SISTEMA LTV PACIFIKO v6.0 - MULTI-PAÍS".center(110))
    print(f"   {pipeline_title}".center(110))
    print(f"   PAÍS: {country_config.name} ({country_config.code})".center(110))
    print(f"   PATH: {PATH_DIR}".center(110))
    print(f"   GROUPING MODE: {grouping_mode.upper()}".center(110))
    print(f"   GRANULARITY: {cohort_config.granularity.value.upper()}".center(110))
    if skip_dimensions:
        print(f"   ⏭️  SKIP DIMENSIONS: TRUE (sin reportes multi-dimensión)".center(110))
    if not ad_spend:
        print(f"   ⚠️  CAC NO DISPONIBLE".center(110))
    print("=" * 110)
    print()
    
    ltv_engine = LTVController()
    
    try:
        # ==========================================================================
        # 6. FASE 1: INGESTA DE DATOS (con país)
        # ==========================================================================
        print_banner(" [FASE 1] INGESTA Y PROCESAMIENTO DE DATOS ", 110, "-")
        t_f1 = time.time()
        
        real_repo = RealDataRepository()
        raw_data = real_repo.get_orders_from_excel(
            path_or_dir=PATH_DIR,
            country_config=country_config  # ← filtrar por país
        )
        ltv_engine.process_raw_data(raw_data)
        customers = ltv_engine.get_customers()
        
        print(f"✅ Clientes procesados para {country_config.name}: {len(customers)}")
        print(f"⏱️  Fase 1 completada en: {time.time() - t_f1:.2f} segundos")
        
        # ==========================================================================
        # 7. FASE ESTRATÉGICA: UNIT ECONOMICS Y PIPELINE MULTI-DIMENSIÓN
        # ==========================================================================
        print_banner(" [FASE ESTRATÉGICA] MOTORES DE CÁLCULO Y PRODUCTO ", 110, "-")
        t_fc = time.time()

        # --- A. CÁLCULO DE UNIT ECONOMICS GLOBAL ---
        print(" > Calculando Unit Economics (CAC/LTV) por cohorte...")
        cohort_engine = CohortAnalyzer(customers, granularity=cohort_config.granularity.value)
        cohort_table = cohort_engine.build_cohort_table()
        
        ue_engine = UnitEconomicsAnalyzer(
            cohort_data=cohort_table,
            ad_spend=ad_spend,
            customers=customers,
            granularity=cohort_config.granularity.value
        )
        ue_results = ue_engine.run_analysis()
        print(f"   ✅ Unit Economics calculado para {len(ue_results)} cohortes")

        # --- B. ORQUESTACIÓN MULTI-DIMENSIÓN (SALTAR SI SE SOLICITA) ---
        if not skip_dimensions:
            print(f"\n > Ejecutando Orquestador Multi-Dimensión para {len(dimensions_to_process)} dimensión(es)...")
            print(f"   Dimensiones: {dimensions_to_process}")
            
            try:
                orchestrator = GlobalLTVOrchestrator(
                    customers=customers,
                    ue_results=ue_results,
                    grouping_mode=grouping_mode,
                    output_dir=target_dir,
                    dimensions=dimensions_to_process,
                    cohort_config=cohort_config  # ← pasar configuración de cohortes
                )
                orchestrator.run_pipeline_completo()
                
            except Exception as e:
                print(f"❌ Error en el Orquestador Global: {e}")
                traceback.print_exc()
        else:
            print("\n > ⏭️ SALTANDO análisis multi-dimensión (LTV_SKIP_DIMENSIONS=TRUE)")
            print("   No se generarán reportes por categoría, subcategoría, marca o producto")

        print(f"\n⏱️  Fase Estratégica completada en: {time.time() - t_fc:.2f} segundos")

        # ==========================================================================
        # 8. VERIFICACIÓN DE MODO - SALIDA TEMPRANA SI ES NECESARIO
        # ==========================================================================
        is_single_dimension = (only_category_mode or 
                               (dimension_mode != 0 and len(dimensions_to_process) == 1))
        
        if not skip_dimensions and is_single_dimension:
            print_banner(" 🛑 MODO DIMENSIONES FINALIZADO CON ÉXITO ", 110, "!")
            duration_total = time.time() - start_total
            sys.stdout = original_stdout
            print(f"\n✅ PROCESO COMPLETADO EN {duration_total:.2f} SEGUNDOS")
            print(f"📂 Resultados en: {orchestrator.output_dir if 'orchestrator' in dir() else target_dir}")
            return

        # ==========================================================================
        # 9. FASE 2: MOTORES DE ANÁLISIS PESADOS
        # ==========================================================================
        print_banner(" [FASE 2] MOTORES DE ANÁLISIS Y CÁLCULO (LTV/UE) ", 110, "-")
        t_f2 = time.time()

        # Modo para análisis legacy
        DIMENSION_MODE = 2  # Default subcategory
        
        # Matrices de retención (usando granularidad)
        retention_mode = "monthly" if cohort_config.granularity == TimeGranularity.MONTHLY else "quarterly"
        engine = CohortRetentionMatrix(customers, mode=retention_mode)
        ret_abs = engine.get_tabular_format(as_percentage=False)
        ret_pct = engine.get_tabular_format(as_percentage=True)
        
        # Análisis por dimensión (legacy)
        category_engine_old = CategoryValueAnalyzer(customers, mode=DIMENSION_MODE)
        
        dashboard = DashboardAnalyzer(
            customers=customers,
            unit_econ_results=ue_results,
            cohort_data=cohort_table,
            mode=DIMENSION_MODE,
            granularity=cohort_config.granularity.value
        )
        
        print(f"⏱️  Fase 2 completada en: {time.time() - t_f2:.2f} segundos")

        # ==========================================================================
        # 10. FASE 3: REPORTES Y VISUALIZACIONES EN CONSOLA
        # ==========================================================================
        print_banner(" [FASE 3] GENERACIÓN DE REPORTES EN CONSOLA ", 110, "-")
        t_f3 = time.time()
        
        behavior_engine = CohortBehaviorCalculator(customers, granularity=cohort_config.granularity.value)
        behavior_data = {
            'frequency': behavior_engine.get_purchase_frequency_stats(),
            'time': behavior_engine.get_time_to_reorder_stats(),
            'conversion': behavior_engine.get_conversion_windows_stats()
        }
        
        dashboard.print_global_summary()
        cohort_engine.print_frequency_report()
        category_engine_old.print_category_strategic_report()
        
        segments = dashboard.print_customer_segments()
        dashboard.print_segment_deep_dive(segments)
        
        print_banner(" DESGLOSE DE UNIT ECONOMICS (CAC VARIABLE) ", 110, "-")
        ue_engine.print_unit_economics()
        
        print(f"⏱️  Fase 3 completada en: {time.time() - t_f3:.2f} segundos")

        # ==========================================================================
        # 11. FASE 4: EXPORTACIÓN DE ARCHIVOS FINALES
        # ==========================================================================
        print_banner(" [FASE 4] EXPORTACIÓN DE ARCHIVOS FINALES ", 110, "-")
        t_f4 = time.time()
        
        exporter = DataExporter(
            customers, 
            ue_results, 
            cohort_table, 
            behavior_data, 
            behavior_data, 
            retention_abs_data=ret_abs, 
            retention_pct_data=ret_pct, 
            mode=DIMENSION_MODE,
            granularity=cohort_config.granularity.value,
            country_config=country_config  # ← pasar país para prefijo en archivos
        )
        
        try:
            exporter.export_to_excel()
            print("✅ Excel principal exportado correctamente")
        except PermissionError:
            print("⚠️ ERROR: El Excel principal está abierto. Ciérralo para guardar.")
        except Exception as e:
            print(f"⚠️ Error exportando Excel: {e}")
        
        print(f"⏱️  Fase 4 completada en: {time.time() - t_f4:.2f} segundos")
        
        # ==========================================================================
        # 12. FINALIZACIÓN
        # ==========================================================================
        duration_total = time.time() - start_total
        sys.stdout = original_stdout
        exporter.export_summary_text(output_buffer.getvalue())

        print("\n" + "=" * 110)
        print(f"✅ PROCESO TOTAL COMPLETADO EN {duration_total:.2f} SEGUNDOS".center(110))
        print("=" * 110)
        
        # Mostrar resumen de outputs generados
        output_dir = target_dir or os.getcwd()
        print(f"\n📂 OUTPUTS GENERADOS PARA {country_config.name} ({country_config.code}):")
        if not skip_dimensions:
            print(f"   • Reportes multi-dimensión: {output_dir}")
        print(f"   • Excel principal: {os.path.join(output_dir, f'{country_config.code}_Analisis_LTV_Pacifiko_*.xlsx')}")
        print(f"   • Logs: {os.path.join(output_dir, 'summary_health.txt')}")

    except Exception as e:
        sys.stdout = original_stdout
        print(f"\n{'='*60}")
        print(f"🚨 FALLO CRÍTICO EN EL PIPELINE")
        print(f"{'='*60}")
        print(f"Error: {e}")
        print(f"\nDetalles del error:")
        traceback.print_exc()
        
        # Guardar el error en un archivo de log
        error_log_path = os.path.join(target_dir or os.getcwd(), f"error_log_{time.strftime('%Y%m%d_%H%M%S')}.txt")
        try:
            with open(error_log_path, 'w', encoding='utf-8') as f:
                f.write(f"Error: {e}\n\n")
                f.write(traceback.format_exc())
            print(f"\n📝 Error log guardado en: {error_log_path}")
        except:
            pass
        
        sys.exit(1)


if __name__ == "__main__":
    run_analysis()