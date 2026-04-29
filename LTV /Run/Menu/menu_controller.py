# ============================================================================
# FILE: Run/Menu/menu_controller.py
# COMPLETO - MULTI-PAÍS CON NAVEGACIÓN MEJORADA + FILTRO POR DIMENSIÓN
# VERSIÓN MEJORADA: Integración con ordenamiento global y fallback DR
# ============================================================================
# Archivo: Run/Menu/menu_controller.py
# Versión v13.5 - MULTI-PAÍS CON NAVEGACIÓN MEJORADA + FILTRO + ORDENAMIENTO GLOBAL

import os
import sys
import signal
import json
from pathlib import Path
from typing import List, Optional, Tuple, Dict

RUN_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if RUN_PATH not in sys.path:
    sys.path.insert(0, RUN_PATH)

from Run.Config.paths import Paths, PathsConfig
from Run.Config.credentials import Credentials
from Run.Config.dev_mode_manager import DevModeManager
from Run.Country.country_context import CountryContext
from Run.FX.fx_engine import FXEngine
from Run.Menu.menu_auth import MenuAuth
from Run.Menu.menu_config import MenuConfig
from Run.Menu.menu_executor import MenuExecutor
from Run.Core.cohort_context_manager import CohortContextManager
from Run.Utils.logger import SystemLogger


class MenuController:
    """Orquestador principal - MULTI-PAÍS CON NAVEGACIÓN MEJORADA + FILTRO"""
    
    # Constantes de modos
    MODE_FULL = '1'
    MODE_DR_ONLY = '2'
    MODE_MODEL = '3'
    MODE_QUERY = '4'
    MODE_CONFIG = '5'
    MODE_CHANGE_COUNTRY = '0'
    MODE_BACK = 'b'
    MODE_QUIT = 'q'
    
    # Señales de retorno
    RETURN_EXIT = "EXIT"
    RETURN_BACK_TO_COUNTRY = "BACK_TO_COUNTRY"
    
    SUBMODE_MODEL_COMPLETE = '1'
    SUBMODE_MODEL_GENERAL = '2'
    SUBMODE_CATEGORY = '3'
    SUBMODE_SUBCATEGORY = '4'
    SUBMODE_BRAND = '5'
    SUBMODE_PRODUCT = '6'
    SUBMODE_SPECIAL = '7'
    SUBMODE_HEAVY_ONLY = '8'
    SUBMODE_FILTER = '9'  # NUEVO
    SUBMODE_GLOBAL_SORT = 's'  # NUEVO: ordenamiento global
    
    DIM_CATEGORY = '1'
    DIM_SUBCATEGORY = '2'
    DIM_BRAND = '3'
    DIM_PRODUCT = '4'
    
    def __init__(self, paths: PathsConfig, country_context: CountryContext, fx_engine: FXEngine):
        self.paths = paths
        self.country_context = country_context
        self.fx_engine = fx_engine
        self.logger = SystemLogger()
        
        self.dev_mode = DevModeManager()
        self.auth = None
        
        self.config = MenuConfig(paths, self.logger)
        self.executor = MenuExecutor(paths, self.logger, country_context, fx_engine)
        
        self._cohort_context = None
        
        # Configurar manejador de señales para cancelación
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def set_auth(self, auth):
        """Inyecta el objeto de autenticación después de la creación."""
        self.auth = auth
        if self.auth and self.country_context:
            self.auth.set_country(self.country_context.code)
    
    def _signal_handler(self, signum, frame):
        """Maneja Ctrl+C durante ejecución"""
        if hasattr(self, 'executor') and self.executor.current_process_running:
            print("\n\n⚠️ Proceso en ejecución. ¿Cancelar? (s/n): ", end="")
            resp = input().strip().lower()
            if resp in ['s', 'si', 'sí', 'yes', 'y']:
                self.executor.request_cancel()
                print("⏹️ Cancelación solicitada...")
        else:
            self._graceful_shutdown_handler(signum, frame)
    
    def _graceful_shutdown_handler(self, signum, frame):
        print("\n\n⚠️ Interrupción detectada. Cerrando conexiones...")
        self.logger.info("Ctrl+C detectado - iniciando shutdown graceful")
        self.executor.ssh_manager.stop()
    
    def _get_cohort_context(self) -> CohortContextManager:
        if self._cohort_context is None:
            supuestos_path = self.paths.inputs_dir / self.paths.supuestos_file
            self._cohort_context = CohortContextManager(supuestos_path, self.country_context)
        return self._cohort_context
    
    def _validate_input_files(self) -> bool:
        input_dir = self.paths.inputs_dir
        required_files = [self.paths.sois_file, self.paths.supuestos_file, self.paths.catalogo_file]
        
        missing = []
        for file in required_files:
            full_path = input_dir / file
            if not full_path.exists():
                missing.append(file)
        
        if missing:
            print(f"\n❌ Archivos faltantes en {input_dir}:")
            for f in missing:
                print(f"   • {f}")
            print("\n📌 Opciones:")
            print("   1. Colocar los archivos en la carpeta indicada")
            print("   2. Cambiar carpeta de entrada (opción en Configuraciones)")
            return False
        
        print(f"✅ Archivos Excel encontrados ({len(required_files)}/{len(required_files)})")
        return True
    
    def _validate_pre_conditions(self) -> bool:
        print("\n" + "🔍 VALIDACIÓN PRE-OPERACIONAL".center(60, "-"))
        
        if not self._validate_input_files():
            return False
        
        print(f"🌎 País activo: {self.country_context.name} ({self.country_context.code})")
        print(f"💱 Tipo de cambio base: {self.country_context.default_fx_rate}")
        
        test_file = self.paths.results_base / ".write_test"
        try:
            test_file.write_text("test")
            test_file.unlink()
            print("✅ Permisos de escritura OK")
        except Exception:
            print(f"❌ No se puede escribir en {self.paths.results_base}")
            return False
        
        print("-" * 60)
        return True
    
    # ==========================================================================
    # NUEVO MÉTODO: BÚSQUEDA CON QUERY ENGINE MEJORADO
    # ==========================================================================
    
    def _open_query_engine_with_global_sort(self):
        """Abre el motor de búsqueda con ordenamiento global - VERSIÓN CORREGIDA"""
        from Model.Domain.controller import LTVController
        from Model.Data.real_data_repository import RealDataRepository
        from Model.Data.cac_repository import CACRepository
        from Category.Utils.query_engine import DimensionQueryEngine
        from Category.Cohort.cohort_config import CohortConfig, TimeGranularity
        from types import SimpleNamespace
        
        print("\n" + "=" * 60)
        print(f"      BUSCADOR INTERACTIVO LTV - {self.country_context.name}".center(60))
        print("=" * 60)
        
        # 🔧 Fallback a DataRepository si no hay datos
        if not self.executor.data_ltv_has_files():
            print("\n⚠️ No se encontraron datos en Data_LTV")
            respuesta = input("¿Deseas ejecutar DataRepository primero? (s/n): ").strip().lower()
            if respuesta in ['s', 'si', 'sí', 'yes', 'y']:
                date_range = self.executor.get_date_range_from_user()
                if not self.executor.run_dr(date_range):
                    print("❌ DataRepository falló.")
                    return
            else:
                return
        
        try:
            real_repo = RealDataRepository()
            raw_data = real_repo.get_orders_from_excel(
                path_or_dir=str(self.paths.data_ltv),
                country_config=self.country_context
            )
            
            ltv_engine = LTVController()
            ltv_engine.process_raw_data(raw_data)
            customers = ltv_engine.get_customers()
            
            print(f"   ✅ {len(customers)} clientes cargados en memoria")
            
            granularity = self.config.current_granularity
            cac_path = self.paths.inputs_dir / self.paths.cac_file
            
            temp_country_config = SimpleNamespace(
                code=self.country_context.code,
                cac_sheet=self.country_context.code,
                name=self.country_context.name
            )
            
            ad_spend = CACRepository.get_cac_mapping(
                country_config=temp_country_config,
                cac_path=str(cac_path),
                granularity=granularity,
                transform=True
            )
            
            granularity_map = {
                "quarterly": TimeGranularity.QUARTERLY,
                "monthly": TimeGranularity.MONTHLY,
                "weekly": TimeGranularity.WEEKLY,
                "semiannual": TimeGranularity.SEMIANNUAL,
                "yearly": TimeGranularity.YEARLY,
            }
            time_granularity = granularity_map.get(granularity, TimeGranularity.QUARTERLY)
            cohort_config = CohortConfig(granularity=time_granularity)
            
            engine = DimensionQueryEngine(
                customers,
                grouping_mode=self.config.current_grouping_mode,
                conversion_mode=self.config.current_conversion_mode,
                ue_results=None,
                cohort_config=cohort_config,
                cac_map=ad_spend
            )
            
            # Aplicar ordenamiento global desde config
            global_sort = self.config.get_global_sort_criteria()
            engine.set_sort_criteria(global_sort)
            print(f"   📊 Ordenamiento global aplicado: {self.config.get_global_sort_display()}")
            
            # Sincronizar modo de conversión
            engine.set_conversion_mode(self.config.current_conversion_mode)
            
            # 🔧 IMPORTANTE: Usar el interactive_search del engine (tiene todo: PID, keywords, etc.)
            engine.interactive_search()
            
        except ImportError as e:
            print(f"\n❌ Error importando módulos: {e}")
            import traceback
            traceback.print_exc()
            input("\nPresiona Enter para continuar...")
        except Exception as e:
            print(f"\n❌ Error al cargar datos: {e}")
            import traceback
            traceback.print_exc()
            input("\nPresiona Enter para continuar...")
    
    def _change_granularity_in_query(self, engine):
        """Cambia granularidad y actualiza engine."""
        from Category.Cohort.cohort_config import CohortConfig, TimeGranularity
        from Category.Cohort.cohort_manager import CohortManager
        
        print("\n" + "=" * 50)
        print("   CAMBIAR GRANULARIDAD DE COHORTES".center(50))
        print("=" * 50)
        print(f"Granularidad actual: {self.config.current_granularity}")
        print("\nOpciones:")
        print("   1. Anual (yearly)")
        print("   2. Semestral (semiannual)")
        print("   3. Trimestral (quarterly) - DEFAULT")
        print("   4. Mensual (monthly)")
        print("   5. Semanal (weekly)")
        print("   b. Cancelar")
        
        option = input("\n👉 Opción: ").strip()
        
        granularity_map = {
            '1': 'yearly',
            '2': 'semiannual',
            '3': 'quarterly',
            '4': 'monthly',
            '5': 'weekly',
        }
        
        if option in granularity_map:
            new_granularity = granularity_map[option]
            self.config.current_granularity = new_granularity
            self.config._save_config()
            
            time_map = {
                'quarterly': TimeGranularity.QUARTERLY,
                'monthly': TimeGranularity.MONTHLY,
                'weekly': TimeGranularity.WEEKLY,
                'semiannual': TimeGranularity.SEMIANNUAL,
                'yearly': TimeGranularity.YEARLY,
            }
            
            new_config = CohortConfig(granularity=time_map.get(new_granularity, TimeGranularity.QUARTERLY))
            
            cohort_context = self._get_cohort_context()
            new_cac_map = cohort_context.get_cac_map(granularity=new_granularity)
            
            engine.cohort_config = new_config
            engine.cohort_manager = CohortManager(new_config)
            engine.cac_map = new_cac_map
            
            print(f"✅ Granularidad cambiada a: {new_granularity}")
            input("\nPresiona Enter para continuar...")
        elif option == 'b':
            return
        
    def _run_query_mode(self):
        """Abre el motor de búsqueda - CON LOOP INFINITO Y DIAGNÓSTICO"""
        from Model.Domain.controller import LTVController
        from Model.Data.real_data_repository import RealDataRepository
        from Model.Data.cac_repository import CACRepository
        from Category.Utils.query_engine import DimensionQueryEngine
        from Category.Cohort.cohort_config import CohortConfig, TimeGranularity
        from types import SimpleNamespace
        import sys
        
        print("\n" + "=" * 60)
        print(f"      BUSCADOR INTERACTIVO LTV - {self.country_context.name}".center(60))
        print("=" * 60)
        
        # 🔧 Verificar datos primero
        if not self.executor.data_ltv_has_files():
            print("\n❌ No se encontraron datos en Data_LTV")
            respuesta = input("¿Deseas ejecutar DataRepository primero? (s/n): ").strip().lower()
            if respuesta in ['s', 'si', 'sí', 'yes', 'y']:
                date_range = self.executor.get_date_range_from_user()
                if not self.executor.run_dr(date_range):
                    print("❌ DataRepository falló.")
                    return
            else:
                return
        
        try:
            print("\n⏳ Cargando datos desde Data_LTV...")
            sys.stdout.flush()
            
            real_repo = RealDataRepository()
            raw_data = real_repo.get_orders_from_excel(
                path_or_dir=str(self.paths.data_ltv),
                country_config=self.country_context
            )
            
            print("⏳ Procesando datos de clientes...")
            sys.stdout.flush()
            
            ltv_engine = LTVController()
            ltv_engine.process_raw_data(raw_data)
            customers = ltv_engine.get_customers()
            
            print(f"   ✅ {len(customers)} clientes cargados en memoria")
            
            # ========== 🔍 DEBUG: VERIFICAR DATOS DE CLIENTES ==========
            print("\n" + "=" * 60)
            print("   🔍 DIAGNÓSTICO DE DATOS".center(60))
            print("=" * 60)
            
            if customers:
                # Mostrar información del primer cliente
                first_customer = customers[0]
                orders = first_customer.get_orders_sorted()
                print(f"\n📊 Cliente muestra (ID: {getattr(first_customer, 'customer_id', 'N/A')}):")
                print(f"   • Total órdenes: {len(orders)}")
                print(f"   • Revenue total: ${first_customer.total_revenue():,.2f}")
                print(f"   • Contribution Profit (CP): ${first_customer.total_cp():,.2f}")
                print(f"   • Órdenes promedio por cliente (debería ser 1+): {len(orders)}")
                
                # Mostrar primeras 3 órdenes
                print(f"\n   📋 Primeras 3 órdenes:")
                for i, order in enumerate(orders[:3]):
                    revenue = getattr(order, 'revenue', 0)
                    cp = getattr(order, 'contribution_profit', 0)
                    category = getattr(order, 'category', 'N/A')
                    subcategory = getattr(order, 'subcategory', 'N/A')
                    print(f"      {i+1}. Revenue: ${revenue:.2f} | CP: ${cp:.2f} | Cat: {category} | Sub: {subcategory}")
                
                # Calcular métricas agregadas para validación
                total_revenue_all = sum(c.total_revenue() for c in customers)
                total_cp_all = sum(c.total_cp() for c in customers)
                total_orders_all = sum(len(c.get_orders_sorted()) for c in customers)
                
                print(f"\n📊 AGREGADOS GENERALES:")
                print(f"   • Total clientes: {len(customers):,}")
                print(f"   • Total órdenes: {total_orders_all:,}")
                print(f"   • Revenue total: ${total_revenue_all:,.2f}")
                print(f"   • CP total: ${total_cp_all:,.2f}")
                print(f"   • LTV promedio: ${total_cp_all / len(customers):,.2f}")
            else:
                print("⚠️ No hay clientes cargados")
            
            print("=" * 60)
            # ========== FIN DEBUG ==========
            
            granularity = self.config.current_granularity
            cac_path = self.paths.inputs_dir / self.paths.cac_file
            supuestos_path = self.paths.inputs_dir / self.paths.supuestos_file
            
            temp_country_config = SimpleNamespace(
                code=self.country_context.code,
                cac_sheet=self.country_context.code,
                name=self.country_context.name
            )
            
            print(f"\n⏳ Cargando CAC desde: {cac_path}")
            print(f"   (Fallback a SUPUESTOS.xlsx si no existe)")
            sys.stdout.flush()
            
            ad_spend = CACRepository.get_cac_mapping(
                country_config=temp_country_config,
                cac_path=str(cac_path),
                granularity=granularity,
                transform=True
            )
            
            granularity_map = {
                "quarterly": TimeGranularity.QUARTERLY,
                "monthly": TimeGranularity.MONTHLY,
                "weekly": TimeGranularity.WEEKLY,
                "semiannual": TimeGranularity.SEMIANNUAL,
                "yearly": TimeGranularity.YEARLY,
            }
            time_granularity = granularity_map.get(granularity, TimeGranularity.QUARTERLY)
            cohort_config = CohortConfig(granularity=time_granularity)
            
            print("⏳ Inicializando motor de búsqueda...")
            sys.stdout.flush()
            
            engine = DimensionQueryEngine(
                customers,
                grouping_mode=self.config.current_grouping_mode,
                conversion_mode=self.config.current_conversion_mode,
                ue_results=None,
                cohort_config=cohort_config,
                cac_map=ad_spend
            )
            
            # 🔧 DIAGNÓSTICO CAC
            if not ad_spend:
                print("\n   ⚠️ ADVERTENCIA: Mapa de CAC vacío.")
                print("      LTV/CAC ratio no estará disponible en los resultados.")
                print("      Verifica que:")
                print(f"         - {cac_path.name} exista y tenga hoja '{self.country_context.code}'")
                print(f"         - O SUPUESTOS.xlsx tenga columna 'cac'")
            else:
                print(f"   ✅ CAC cargado: {len(ad_spend)} cohortes")
                sample = list(ad_spend.items())[:3]
                for cohort, value in sample:
                    print(f"      {cohort}: ${value:.2f}")
                if len(ad_spend) > 3:
                    print(f"      ... y {len(ad_spend) - 3} más")
            
            # Aplicar ordenamiento global desde config
            if hasattr(self.config, 'get_global_sort_criteria'):
                global_sort = self.config.get_global_sort_criteria()
                engine.set_sort_criteria(global_sort)
                print(f"   📊 Ordenamiento global aplicado: {self.config.get_global_sort_display()}")
            
            # Sincronizar modo de conversión
            engine.set_conversion_mode(self.config.current_conversion_mode)
            
            print("✅ Buscador listo.")
            print("-" * 60)
            
            # 🔧 LOOP INFINITO DEL BUSCADOR
            while True:
                print("\n" + "=" * 50)
                print("   🔍 BUSCADOR LTV".center(50))
                print("=" * 50)
                print("   Presiona 'b' dentro del buscador para volver aquí")
                print("   Presiona 'q' para salir del buscador")
                print("=" * 50)
                
                engine.interactive_search()
                
                print("\n" + "-" * 40)
                continuar = input("👉 ¿Realizar otra búsqueda? (s/n): ").strip().lower()
                if continuar not in ['s', 'si', 'sí', 'yes', 'y', '']:
                    print("\n✅ Volviendo al menú principal...")
                    break
            
        except ImportError as e:
            print(f"\n❌ Error importando módulos: {e}")
            import traceback
            traceback.print_exc()
            input("\nPresiona Enter para continuar...")
        except Exception as e:
            print(f"\n❌ Error al cargar datos: {e}")
            import traceback
            traceback.print_exc()
            input("\nPresiona Enter para continuar...")
    
    # ==========================================================================
    # NUEVO MÉTODO: FILTRO POR DIMENSIÓN
    # ==========================================================================

    def _get_dimension_filter(self) -> Optional[Dict[str, List[str]]]:
        """
        Menú jerárquico para seleccionar dimensiones.
        Presionar Enter en CATEGORÍA = sin filtro de categoría (permite seguir)
        Presionar Enter en niveles posteriores salta ese nivel.
        """
        print("\n" + "=" * 60)
        print("   SELECCIÓN JERÁRQUICA DE DIMENSIONES".center(60))
        print("=" * 60)
        print("\nPuedes navegar por la jerarquía para filtrar el análisis.")
        print("⚠️ Presionar Enter en CATEGORÍA = sin filtro de categoría (continúa)")
        print("   Presionar Enter en niveles posteriores salta ese nivel.\n")
        
        # Cargar datos para obtener valores disponibles
        try:
            from Model.Domain.controller import LTVController
            from Model.Data.real_data_repository import RealDataRepository
            
            if not self.executor.data_ltv_has_files():
                print("\n⚠️ No hay datos disponibles. Ejecuta DataRepository primero.")
                return None
            
            real_repo = RealDataRepository()
            raw_data = real_repo.get_orders_from_excel(
                path_or_dir=str(self.paths.data_ltv),
                country_config=self.country_context
            )
            
            ltv_engine = LTVController()
            ltv_engine.process_raw_data(raw_data)
            customers = ltv_engine.get_customers()
            
            # Extraer todos los valores únicos y sus relaciones
            all_categories = set()
            category_to_subcategories = {}
            category_to_brands = {}
            subcategory_to_brands = {}
            brand_to_products = {}
            
            for customer in customers:
                for order in customer.get_orders_sorted():
                    cat = getattr(order, 'category', None)
                    sub = getattr(order, 'subcategory', None)
                    brand = getattr(order, 'brand', None)
                    product = getattr(order, 'name', None)
                    
                    if cat and str(cat).strip().lower() not in ['', 'nan', 'none', 'n/a']:
                        cat_clean = str(cat).strip()
                        all_categories.add(cat_clean)
                        
                        if brand and str(brand).strip().lower() not in ['', 'nan', 'none', 'n/a']:
                            brand_clean = str(brand).strip()
                            if cat_clean not in category_to_brands:
                                category_to_brands[cat_clean] = set()
                            category_to_brands[cat_clean].add(brand_clean)
                        
                        if sub and str(sub).strip().lower() not in ['', 'nan', 'none', 'n/a']:
                            sub_clean = str(sub).strip()
                            if cat_clean not in category_to_subcategories:
                                category_to_subcategories[cat_clean] = set()
                            category_to_subcategories[cat_clean].add(sub_clean)
                            
                            if brand and str(brand).strip().lower() not in ['', 'nan', 'none', 'n/a']:
                                brand_clean = str(brand).strip()
                                key = f"{cat_clean}|{sub_clean}"
                                if key not in subcategory_to_brands:
                                    subcategory_to_brands[key] = set()
                                subcategory_to_brands[key].add(brand_clean)
                                
                                if product and str(product).strip().lower() not in ['', 'nan', 'none', 'n/a']:
                                    prod_clean = str(product).strip()
                                    key2 = f"{cat_clean}|{sub_clean}|{brand_clean}"
                                    if key2 not in brand_to_products:
                                        brand_to_products[key2] = set()
                                    brand_to_products[key2].add(prod_clean)
            
            all_categories = sorted(all_categories)
            
            if not all_categories:
                print("⚠️ No se encontraron categorías en los datos.")
                return None
            
            selected = {}
            selected_category = None  # Inicializar como None (sin filtro de categoría)
            
            # ========== NIVEL 1: CATEGORÍA ==========
            print("\n" + "-" * 50)
            print(f"📂 CATEGORÍAS disponibles ({len(all_categories)}):")
            print("-" * 50)
            
            for i, cat in enumerate(all_categories[:30], 1):
                display_cat = cat[:60] if len(cat) > 60 else cat
                print(f"   {i:2}. {display_cat}")
            
            if len(all_categories) > 30:
                print(f"   ... y {len(all_categories) - 30} más")
            
            cat_input = input("\n👉 Selecciona una categoría (número o nombre) [Enter = sin filtro]: ").strip()
            
            # 🔧 Enter = sin filtro de categoría (NO cancelar)
            if not cat_input:
                print("\n   ⏭️ Sin filtro de categoría - Se pueden seleccionar subcategorías y marcas de TODAS las categorías")
                selected_category = None  # Sin filtro de categoría
            else:
                # Procesar selección normal
                if cat_input.isdigit() and 1 <= int(cat_input) <= len(all_categories):
                    selected_category = all_categories[int(cat_input) - 1]
                else:
                    matches = [c for c in all_categories if cat_input.lower() in c.lower()]
                    if len(matches) == 1:
                        selected_category = matches[0]
                    elif len(matches) > 1:
                        print(f"\n   🔍 Múltiples coincidencias:")
                        for i, m in enumerate(matches[:10], 1):
                            print(f"      {i}. {m}")
                        sub_choice = input("\n   👉 Selecciona el número exacto (o Enter para cancelar): ").strip()
                        if not sub_choice:
                            return None
                        if sub_choice.isdigit() and 1 <= int(sub_choice) <= len(matches):
                            selected_category = matches[int(sub_choice) - 1]
                
                if selected_category:
                    selected['category'] = [selected_category]
                    print(f"   ✅ Categoría seleccionada: {selected_category}")
                else:
                    print("\n❌ Selección inválida. Filtro cancelado.")
                    return None
            
            # ========== NIVEL 2: SUBCATEGORÍA ==========
            # Si hay categoría seleccionada, usar sus subcategorías
            # Si no hay categoría (None), juntar TODAS las subcategorías de todas las categorías
            if selected_category:
                subcategories = sorted(category_to_subcategories.get(selected_category, set()))
            else:
                # Sin filtro de categoría: juntar todas las subcategorías únicas
                all_subcategories = set()
                for subs in category_to_subcategories.values():
                    all_subcategories.update(subs)
                subcategories = sorted(all_subcategories)
            
            if subcategories:
                nivel_texto = f"de '{selected_category}'" if selected_category else "de TODAS las categorías"
                print("\n" + "-" * 50)
                print(f"📁 SUBCATEGORÍAS {nivel_texto} ({len(subcategories)}):")
                print("-" * 50)
                
                for i, sub in enumerate(subcategories[:30], 1):
                    display_sub = sub[:60] if len(sub) > 60 else sub
                    print(f"   {i:2}. {display_sub}")
                
                if len(subcategories) > 30:
                    print(f"   ... y {len(subcategories) - 30} más")
                
                sub_input = input("\n👉 Selecciona una subcategoría (número o nombre), o Enter para saltar: ").strip()
                
                if sub_input:
                    selected_subcategory = None
                    if sub_input.isdigit() and 1 <= int(sub_input) <= len(subcategories):
                        selected_subcategory = subcategories[int(sub_input) - 1]
                    else:
                        matches = [s for s in subcategories if sub_input.lower() in s.lower()]
                        if len(matches) == 1:
                            selected_subcategory = matches[0]
                        elif len(matches) > 1:
                            print(f"\n   🔍 Múltiples coincidencias:")
                            for i, m in enumerate(matches[:10], 1):
                                print(f"      {i}. {m}")
                            sub_choice = input("\n   👉 Selecciona el número exacto (o Enter para saltar): ").strip()
                            if sub_choice.isdigit() and 1 <= int(sub_choice) <= len(matches):
                                selected_subcategory = matches[int(sub_choice) - 1]
                    
                    if selected_subcategory:
                        selected['subcategory'] = [selected_subcategory]
                        print(f"   ✅ Subcategoría seleccionada: {selected_subcategory}")
                    else:
                        print("   ❌ Selección inválida. Saltando subcategoría...")
                else:
                    print("   ⏭️ Saltando subcategoría...")
            else:
                print(f"\n   ℹ️ No hay subcategorías para esta selección.")
            
            # ========== NIVEL 3: MARCA ==========
            # Construir marcas disponibles según filtros actuales
            brands = set()
            
            if 'subcategory' in selected:
                # Hay subcategoría seleccionada
                subcat = selected['subcategory'][0]
                if selected_category:
                    key = f"{selected_category}|{subcat}"
                    brands = subcategory_to_brands.get(key, set())
                else:
                    # Sin categoría, buscar en todas las categorías esa subcategoría
                    for cat, subs in category_to_subcategories.items():
                        if subcat in subs:
                            key = f"{cat}|{subcat}"
                            brands.update(subcategory_to_brands.get(key, set()))
            elif selected_category:
                # Solo categoría seleccionada
                brands = category_to_brands.get(selected_category, set())
            else:
                # Sin categoría ni subcategoría: todas las marcas
                for cat_brands in category_to_brands.values():
                    brands.update(cat_brands)
            
            brands = sorted(brands)
            
            if brands:
                nivel_texto = ""
                if 'subcategory' in selected:
                    nivel_texto = f"en subcategoría '{selected['subcategory'][0]}'"
                elif selected_category:
                    nivel_texto = f"en categoría '{selected_category}'"
                else:
                    nivel_texto = "en TODAS las categorías"
                
                print("\n" + "-" * 50)
                print(f"🏷️ MARCAS disponibles {nivel_texto} ({len(brands)}):")
                print("-" * 50)
                
                for i, brand in enumerate(brands[:30], 1):
                    display_brand = brand[:60] if len(brand) > 60 else brand
                    print(f"   {i:2}. {display_brand}")
                
                if len(brands) > 30:
                    print(f"   ... y {len(brands) - 30} más")
                
                brand_input = input("\n👉 Selecciona una marca (número o nombre), o Enter para saltar: ").strip()
                
                if brand_input:
                    selected_brand = None
                    if brand_input.isdigit() and 1 <= int(brand_input) <= len(brands):
                        selected_brand = brands[int(brand_input) - 1]
                    else:
                        matches = [b for b in brands if brand_input.lower() in b.lower()]
                        if len(matches) == 1:
                            selected_brand = matches[0]
                        elif len(matches) > 1:
                            print(f"\n   🔍 Múltiples coincidencias:")
                            for i, m in enumerate(matches[:10], 1):
                                print(f"      {i}. {m}")
                            sub_choice = input("\n   👉 Selecciona el número exacto (o Enter para saltar): ").strip()
                            if sub_choice.isdigit() and 1 <= int(sub_choice) <= len(matches):
                                selected_brand = matches[int(sub_choice) - 1]
                    
                    if selected_brand:
                        selected['brand'] = [selected_brand]
                        print(f"   ✅ Marca seleccionada: {selected_brand}")
                    else:
                        print("   ❌ Selección inválida. Saltando marca...")
                else:
                    print("   ⏭️ Saltando marca...")
            else:
                print("\n   ℹ️ No hay marcas disponibles para esta selección.")
            
            # ========== RESUMEN FINAL ==========
            if not selected:
                print("\n⚠️ No se seleccionó ningún filtro. Se ejecutará análisis COMPLETO.")
                confirm = input("\n👉 ¿Ejecutar análisis completo? (s/n): ").strip().lower()
                if confirm in ['s', 'si', 'sí', 'yes', 'y']:
                    return None  # None = análisis completo
                else:
                    print("   ❌ Operación cancelada.")
                    return {}
            
            print("\n" + "-" * 50)
            print("📋 RESUMEN DEL FILTRO:")
            for key, values in selected.items():
                print(f"   • {key}: {values[0]}")
            
            confirm = input("\n👉 ¿Ejecutar análisis con este filtro? (s/n): ").strip().lower()
            if confirm not in ['s', 'si', 'sí', 'yes', 'y']:
                print("   ❌ Filtro cancelado.")
                return {}
            
            return selected
            
        except Exception as e:
            print(f"⚠️ Error cargando datos para filtro: {e}")
            import traceback
            traceback.print_exc()
            return {}
        
    def _run_filtered_analysis(self, date_range=None):
        """
        Ejecuta análisis con filtro por dimensiones.
        - {} = usuario canceló → volver al menú
        - None = análisis completo (sin filtro)
        - dict con valores = análisis con filtro
        """
        dimension_filter = self._get_dimension_filter()
        
        # Diccionario vacío = usuario canceló explícitamente
        if dimension_filter == {}:
            print("\n❌ Análisis cancelado. Volviendo al menú...")
            return
        
        # None = análisis completo sin filtro
        if dimension_filter is None:
            print("\n✅ Ejecutando análisis COMPLETO (sin filtro)")
            self._run_model_complete(date_range)
            self._wait_for_user()
            return
        
        # Si el filtro está vacío (por cualquier otro motivo)
        if not dimension_filter:
            print("\n❌ No se seleccionó ningún filtro válido. Volviendo al menú...")
            return
        
        print("\n" + "=" * 60)
        print("   EJECUTANDO CON FILTRO ACTIVO".center(60))
        print("=" * 60)
        
        # Detectar nivel de filtro
        has_category = 'category' in dimension_filter and dimension_filter['category']
        has_subcategory = 'subcategory' in dimension_filter and dimension_filter['subcategory']
        has_brand = 'brand' in dimension_filter and dimension_filter['brand']
        has_product = 'product' in dimension_filter and dimension_filter['product']
        
        # Mostrar resumen del filtro aplicado
        print("\n📋 FILTRO APLICADO:")
        if has_category:
            print(f"   • Categoría: {dimension_filter['category'][0]}")
        if has_subcategory:
            print(f"   • Subcategoría: {dimension_filter['subcategory'][0]}")
        if has_brand:
            print(f"   • Marca: {dimension_filter['brand'][0]}")
        if has_product:
            print(f"   • Producto: {dimension_filter['product'][0]}")
        
        # ========== CONSTRUIR OPCIONES SEGÚN MAMUSHKA ==========
        print("\n📊 ¿Qué análisis quieres ejecutar con este filtro?")
        
        options = []
        
        # Si hay producto → solo producto
        if has_product:
            options.append(('1', "🎯 Análisis de PRODUCTO (solo este producto)"))
        
        # Si hay marca (y no producto) → análisis de marca
        elif has_brand:
            options.append(('1', "🏷️ Análisis de MARCA (solo esta marca)"))
        
        # Si hay subcategoría (y no marca ni producto) → análisis de MARCAS y PRODUCTOS
        elif has_subcategory:
            options.append(('1', "🏷️ Análisis de MARCAS (todas las marcas dentro de esta subcategoría)"))
            options.append(('2', "🎯 Análisis de PRODUCTOS (todos los productos dentro de esta subcategoría)"))
        
        # Si solo hay categoría → análisis de SUBCATEGORÍAS, MARCAS y PRODUCTOS
        elif has_category:
            options.append(('1', "📁 Análisis de SUBCATEGORÍAS (todas dentro de esta categoría)"))
            options.append(('2', "🏷️ Análisis de MARCAS (todas dentro de esta categoría)"))
            options.append(('3', "🎯 Análisis de PRODUCTOS (todos dentro de esta categoría)"))
        
        # Opción de ordenamiento global (siempre disponible)
        options.append(('S', "⚙️ Cambiar ordenamiento global"))
        options.append(('0', "🔙 Cancelar"))
        
        # Mostrar opciones
        for key, desc in options:
            print(f"   {key}) {desc}")
        
        analysis_option = input("\n👉 Opción: ").strip().upper()
        
        # Cambiar ordenamiento global
        if analysis_option == 'S':
            self._select_global_sort()
            return self._run_filtered_analysis(date_range)
        
        # Cancelar
        if analysis_option == '0':
            print("❌ Análisis cancelado.")
            return
        
        # ========== EJECUTAR SEGÚN OPCIÓN ==========
        
        # PRODUCTO
        if analysis_option == '1' and has_product:
            self.executor.run_model_analysis(
                [4], 
                f"Filtrado_Producto_{self.country_context.code}",
                only_category=False,
                date_range=date_range,
                grouping_mode=self.config.current_grouping_mode,
                conversion_mode=self.config.current_conversion_mode,
                granularity=self.config.current_granularity,
                dimension_filter=dimension_filter
            )
            self._wait_for_user()
            return
        
        # MARCA (cuando hay marca directamente)
        if analysis_option == '1' and has_brand and not has_product:
            dim_code = self.config.BRAND_MODE_TO_DIMENSION[self.config.current_brand_mode]
            self.executor.run_model_analysis(
                [dim_code], 
                f"Filtrado_Marca_{self.country_context.code}",
                only_category=False,
                date_range=date_range,
                grouping_mode=self.config.current_grouping_mode,
                conversion_mode=self.config.current_conversion_mode,
                granularity=self.config.current_granularity,
                dimension_filter=dimension_filter
            )
            self._wait_for_user()
            return
        
        # MARCAS dentro de subcategoría
        if analysis_option == '1' and has_subcategory and not has_brand and not has_product:
            dim_code = self.config.BRAND_MODE_TO_DIMENSION[self.config.current_brand_mode]
            self.executor.run_model_analysis(
                [dim_code], 
                f"Filtrado_Marcas_En_Subcategoria_{self.country_context.code}",
                only_category=False,
                date_range=date_range,
                grouping_mode=self.config.current_grouping_mode,
                conversion_mode=self.config.current_conversion_mode,
                granularity=self.config.current_granularity,
                dimension_filter=dimension_filter
            )
            self._wait_for_user()
            return
        
        # PRODUCTOS dentro de subcategoría
        if analysis_option == '2' and has_subcategory and not has_brand and not has_product:
            self.executor.run_model_analysis(
                [4], 
                f"Filtrado_Productos_En_Subcategoria_{self.country_context.code}",
                only_category=False,
                date_range=date_range,
                grouping_mode=self.config.current_grouping_mode,
                conversion_mode=self.config.current_conversion_mode,
                granularity=self.config.current_granularity,
                dimension_filter=dimension_filter
            )
            self._wait_for_user()
            return
        
        # SUBCATEGORÍAS dentro de categoría
        if analysis_option == '1' and has_category and not has_subcategory:
            self.executor.run_model_analysis(
                [2], 
                f"Filtrado_Subcategorias_En_Categoria_{self.country_context.code}",
                only_category=False,
                date_range=date_range,
                grouping_mode=self.config.current_grouping_mode,
                conversion_mode=self.config.current_conversion_mode,
                granularity=self.config.current_granularity,
                dimension_filter=dimension_filter
            )
            self._wait_for_user()
            return
        
        # MARCAS dentro de categoría
        if analysis_option == '2' and has_category and not has_subcategory:
            dim_code = self.config.BRAND_MODE_TO_DIMENSION[self.config.current_brand_mode]
            self.executor.run_model_analysis(
                [dim_code], 
                f"Filtrado_Marcas_En_Categoria_{self.country_context.code}",
                only_category=False,
                date_range=date_range,
                grouping_mode=self.config.current_grouping_mode,
                conversion_mode=self.config.current_conversion_mode,
                granularity=self.config.current_granularity,
                dimension_filter=dimension_filter
            )
            self._wait_for_user()
            return
        
        # PRODUCTOS dentro de categoría
        if analysis_option == '3' and has_category and not has_subcategory:
            self.executor.run_model_analysis(
                [4], 
                f"Filtrado_Productos_En_Categoria_{self.country_context.code}",
                only_category=False,
                date_range=date_range,
                grouping_mode=self.config.current_grouping_mode,
                conversion_mode=self.config.current_conversion_mode,
                granularity=self.config.current_granularity,
                dimension_filter=dimension_filter
            )
            self._wait_for_user()
            return
        
        print("❌ Opción inválida para el nivel de filtro seleccionado.")

    # ==========================================================================
    # MÉTODOS DE MENÚ
    # ==========================================================================
    
    def display_main_menu(self):
        """Menú principal - SIN opción de cambiar país (usar 'b' para volver)"""
        print("\n" + "=" * 60)
        print(f"      SISTEMA LTV - {self.country_context.name}".center(60))
        print("=" * 60)
        print(f"\n⚙️ CONFIGURACIÓN ACTUAL:")
        print(f"   🌎 País: {self.country_context.name} ({self.country_context.currency})")
        print(f"   📊 Agrupación: {self.config.get_grouping_mode_display()}")
        print(f"   🏷️  Modo marca: {self.config.get_brand_mode_display()}")
        print(f"   📅 Granularidad: {self.config.get_granularity_display()}")
        print(f"   📊 Ordenamiento global: {self.config.get_global_sort_display()}")
        print(f"   📂 Input dir: {self.paths.inputs_dir}")
        print("\n" + "-" * 40)
        print("1. 🚀 PIPELINE COMPLETO")
        print("2. 💾 SOLO DATA REPOSITORY")
        print("3. 📊 MODELO")
        print("4. 🔍 BUSCADOR")
        print("5. ⚙️ CONFIGURACIONES")
        print("b. 🔙 Cambiar país / Volver")
        print("q. ❌ SALIR")
        print("=" * 60)
        return input("\n👉 Selecciona una opción: ").strip().lower()

    def display_model_submenu(self):
        """Menú de modelo con números y opción 'b' para volver."""
        print("\n" + "=" * 60)
        print(f"      MODELO - {self.country_context.name}".center(60))
        print("=" * 60)
        print(f"\n⚙️ Modo marca actual: {self.config.get_brand_mode_display()}")
        print(f"📅 Granularidad actual: {self.config.get_granularity_display()}")
        print(f"📊 Ordenamiento global: {self.config.get_global_sort_display()}")
        
        print("\n📂 ANÁLISIS POR DIMENSIÓN:")
        print("   1. Categoría (Category)")
        print("   2. Subcategoría (Subcategory)")
        print("   3. Marca (Brand)")
        print("   4. Producto")
        print("   5. Subcategoría + Marca")
        
        print("\n🔍 FILTROS:")
        print("   6. Filtrar análisis por dimensión")
        
        print("\n📊 ANÁLISIS COMPLETO:")
        print("   7. Modelo COMPLETO (Todas las dimensiones)")
        print("   8. Modelo GENERAL (Category + Subcategory)")
        print("   9. Especial (Seleccionar dimensiones específicas)")
        
        print("\n🔬 ANÁLISIS PESADO:")
        print("   10. SOLO ANÁLISIS PESADOS (sin reportes multi-dimensión)")
        
        print("\n⚙️ CONFIGURACIÓN:")
        print("   S. Cambiar ordenamiento global")
        
        print("\n   b. 🔙 Volver al menú principal")
        print("=" * 60)
        return input("\n👉 Selecciona una opción: ").strip().lower()
    
    def display_config_submenu(self):
        print("\n" + "=" * 60)
        print("      CONFIGURACIONES".center(60))
        print("=" * 60)
        print(f"\n1. 🔄 Modo de agrupación: {self.config.get_grouping_mode_display()}")
        print(f"2. 🏷️  Modo de análisis de marca: {self.config.get_brand_mode_display()}")
        print(f"3. 📅 Granularidad de cohortes: {self.config.get_granularity_display()}")
        print(f"4. 📊 Ordenamiento global: {self.config.get_global_sort_display()}")
        print(f"5. 📂 Cambiar carpeta de ENTRADA (inputs)")
        print(f"6. 💾 Cambiar carpeta de SALIDA (resultados)")
        print(f"7. 📊 GESTIÓN DE COHORTES (agregar/editar/ver/eliminar)")
        print(f"8. 🗑️ LIMPIEZA DEL SISTEMA (eliminar logs, cache, temporales)")  # ← NUEVO
        print("\nb. 🔙 Volver")
        print("=" * 60)
        return input("\n👉 Selecciona una opción: ").strip().lower()
    
    def display_special_dimensions_menu(self, selected: List[str]) -> str:
        print("\n" + "=" * 60)
        print("      SELECCIÓN DE DIMENSIONES".center(60))
        print("=" * 60)
        print(f"\n✅ Dimensiones seleccionadas: {', '.join(selected) if selected else 'NINGUNA'}")
        print("\n📂 Dimensiones disponibles:")
        print("   1. Categoría (Category)")
        print("   2. Subcategoría (Subcategory)")
        print("   3. Marca (Brand)")
        print("   4. Producto (Product)")
        print("\n   q. ✅ Ejecutar análisis con las dimensiones seleccionadas")
        print("   r. 🔄 Reiniciar selección")
        print("   b. 🔙 Volver al menú de modelo")
        print("=" * 60)
        return input("\n👉 Selecciona una opción: ").strip().lower()
    
    # ==================================================================
    # MÉTODOS DE CONFIGURACIÓN
    # ==================================================================
    
    def _select_grouping_mode(self):
        self.config.select_grouping_mode()
    
    def _select_brand_mode(self):
        self.config.select_brand_mode()
    
    def _select_granularity(self):
        self.config.select_granularity()
    
    def _select_global_sort(self):
        self.config.select_global_sort()
    
    def _select_input_folder(self):
        new_path = Paths.select_input_folder(self.country_context.code)
        if new_path:
            self.paths.inputs_dir = new_path
    
    def _select_output_folder(self):
        new_path = Paths.select_output_folder(self.country_context.code)
        if new_path:
            self.paths.results_base = new_path
    
    def _manage_cohorts(self):
        self.config.manage_cohorts_menu()
    
    # ==================================================================
    # MÉTODOS DE EJECUCIÓN
    # ==================================================================
    
    def _run_full_pipeline(self):
        Credentials.load_for_country(self.country_context.code)
        
        if not self._validate_pre_conditions():
            return
        date_range = self.executor.get_date_range_from_user()
        self.executor.run_full_pipeline(
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_dr_only(self):
        Credentials.load_for_country(self.country_context.code)
        
        if not self._validate_pre_conditions():
            return
        date_range = self.executor.get_date_range_from_user()
        self.executor.run_dr_only(date_range)
    
    def _run_model_complete(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_model_analysis(
            [1, 2, 3, 4, 5, 6], "Modelo Completo",
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_model_general(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_model_analysis(
            [1, 2], "Modelo General", only_category=True,
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_model_category(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_model_analysis(
            [1], "Categoría",
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_model_subcategory(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_model_analysis(
            [2], "Subcategoría",
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_model_brand(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        dim_code = self.config.BRAND_MODE_TO_DIMENSION[self.config.current_brand_mode]
        dim_name = self.config.get_brand_mode_display()
        self.executor.run_model_analysis(
            [dim_code], dim_name,
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_model_product(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_model_analysis(
            [4], "Producto",
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _run_special_mode(self, date_range=None, dimension_filter=None):
        Credentials.load_for_country(self.country_context.code)
        
        selected = []
        dim_map = {
            "Categoría": 1,
            "Subcategoría": 2,
            "Marca": self.config.BRAND_MODE_TO_DIMENSION[self.config.current_brand_mode],
            "Producto": 4,
        }
        
        while True:
            option = self.display_special_dimensions_menu(selected)
            
            if option == self.DIM_CATEGORY:
                if "Categoría" not in selected:
                    selected.append("Categoría")
            elif option == self.DIM_SUBCATEGORY:
                if "Subcategoría" not in selected:
                    selected.append("Subcategoría")
            elif option == self.DIM_BRAND:
                if "Marca" not in selected:
                    selected.append("Marca")
            elif option == self.DIM_PRODUCT:
                if "Producto" not in selected:
                    selected.append("Producto")
            elif option == 'r':
                selected = []
                print("\n🔄 Selección reiniciada.")
            elif option == 'q':
                if not selected:
                    print("❌ No has seleccionado ninguna dimensión.")
                    continue
                dimensions = [dim_map[name] for name in selected]
                display_name = " + ".join(selected)
                self.executor.run_model_analysis(
                    dimensions, display_name, only_category=False, date_range=date_range,
                    grouping_mode=self.config.current_grouping_mode,
                    conversion_mode=self.config.current_conversion_mode,
                    granularity=self.config.current_granularity,
                    dimension_filter=dimension_filter
                )
                return
            elif option == 'b':
                return
            else:
                print("❌ Opción inválida.")
    
    def _run_heavy_analysis_only(self, date_range=None):
        Credentials.load_for_country(self.country_context.code)
        
        self.executor.run_heavy_analysis_only(
            date_range=date_range,
            grouping_mode=self.config.current_grouping_mode,
            conversion_mode=self.config.current_conversion_mode,
            granularity=self.config.current_granularity
        )
    
    def _wait_for_user(self):
        input("\n👉 Presiona Enter para volver al menú principal...")
    
    # ==================================================================
    # MÉTODO PRINCIPAL RUN - RETORNA SEÑALES
    # ==================================================================

    def run(self):
        """Ejecuta el menú principal. Retorna señal para el main loop."""
        
        Credentials.load_for_country(self.country_context.code)
        
        while True:
            main_option = self.display_main_menu()
            
            if main_option == self.MODE_BACK:
                self.executor.ssh_manager.stop()
                return self.RETURN_BACK_TO_COUNTRY
            
            elif main_option == self.MODE_FULL:
                self._run_full_pipeline()
                self._wait_for_user()
                
            elif main_option == self.MODE_DR_ONLY:
                self._run_dr_only()
                self._wait_for_user()
                
            elif main_option == self.MODE_MODEL:
                date_range = self.executor.get_date_range_from_user()
                while True:
                    model_option = self.display_model_submenu()
                    
                    # 🔧 MAPEO DE NÚMEROS A ACCIONES
                    if model_option == '1':
                        self._run_model_category(date_range)
                        self._wait_for_user()
                    elif model_option == '2':
                        self._run_model_subcategory(date_range)
                        self._wait_for_user()
                    elif model_option == '3':
                        self._run_model_brand(date_range)
                        self._wait_for_user()
                    elif model_option == '4':
                        self._run_model_product(date_range)
                        self._wait_for_user()
                    elif model_option == '5':
                        self.executor.run_model_analysis(
                            [5], "Subcategoría + Marca",
                            date_range=date_range,
                            grouping_mode=self.config.current_grouping_mode,
                            conversion_mode=self.config.current_conversion_mode,
                            granularity=self.config.current_granularity
                        )
                        self._wait_for_user()
                    elif model_option == '6':
                        self._run_filtered_analysis(date_range)
                        self._wait_for_user()
                    elif model_option == '7':
                        self._run_model_complete(date_range)
                        self._wait_for_user()
                    elif model_option == '8':
                        self._run_model_general(date_range)
                        self._wait_for_user()
                    elif model_option == '9':
                        self._run_special_mode(date_range)
                        self._wait_for_user()
                    elif model_option == '10':
                        self._run_heavy_analysis_only(date_range)
                        self._wait_for_user()
                    elif model_option == 's':
                        self._select_global_sort()
                    # 🔧 CORREGIDO: acepta '0' O 'b'
                    elif model_option == '0' or model_option == 'b':
                        break
                    else:
                        print("❌ Opción inválida. Usa 1-10, S o 0/b.")
                        
            elif main_option == self.MODE_QUERY:
                self._run_query_mode()
                
            elif main_option == self.MODE_CONFIG:
                while True:
                    config_option = self.display_config_submenu()
                    if config_option == '1':
                        self._select_grouping_mode()
                    elif config_option == '2':
                        self._select_brand_mode()
                    elif config_option == '3':
                        self._select_granularity()
                    elif config_option == '4':
                        self._select_global_sort()
                    elif config_option == '5':
                        self._select_input_folder()
                    elif config_option == '6':
                        self._select_output_folder()
                    elif config_option == '7':
                        self._manage_cohorts()
                    elif config_option == '8':
                        from Run.Services.system_cleaner import SystemCleaner
                        SystemCleaner.clean_interactive()
                    elif config_option == self.MODE_BACK:
                        break
                    else:
                        print("❌ Opción inválida")
                        
            elif main_option == self.MODE_QUIT:
                self.executor.ssh_manager.stop()
                print("\n👋 ¡Hasta luego!")
                return self.RETURN_EXIT
            else:
                print("❌ Opción inválida")
    