"""
Motor de consultas para buscar dimensiones específicas (Category, Subcategory, Brand, Product, Subcategory+Brand)
sin necesidad de regenerar todo el pipeline.

VERSIÓN DINÁMICA v5.0 - Soporta cohortes dinámicos vía CohortManager.
VERSIÓN MEJORADA: Resultados ordenados por relevancia + PID en productos + ordenamiento configurable.
VERSIÓN CON PID: Búsqueda automática por PID exacto.
VERSIÓN 6.0: Buscador unificado (keyword + PID exacto) + ordenamiento global.
"""
 
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime
import os
import re

from Category.Cohort.cohort_manager import CohortManager
from Category.Cohort.cohort_config import CohortConfig, TimeGranularity
from Model.Data.cac_repository import CACRepository
from Run.Config.config_loader import ConfigLoader


class DimensionQueryEngine:
    """
    Motor para consultar métricas de dimensiones específicas.
    
    Uso:
        # Con configuración por defecto (quarterly)
        engine = DimensionQueryEngine(customers, grouping_mode="entry_based")
        
        # Con configuración personalizada (mensual)
        config = CohortConfig(granularity=TimeGranularity.MONTHLY)
        engine = DimensionQueryEngine(customers, grouping_mode="entry_based", cohort_config=config)
        
        # Buscar categoría específica
        result = engine.query(category="Electrónica")
    """
    
    # Modos de agrupación
    GROUPING_BEHAVIORAL = "behavioral"
    GROUPING_ENTRY_BASED = "entry_based"
    
    # Modos de conversión
    CONVERSION_CUMULATIVE = "cumulative"    # Acumulativa (creciente) - DEFAULT
    CONVERSION_INCREMENTAL = "incremental"  # Incremental (distribución)
    
    # Constantes para ordenamiento
    SORT_BY_FINAL_SCORE = "score"
    SORT_BY_LTV = "ltv"
    SORT_BY_CLIENTS = "clients"
    
    SORT_OPTIONS = {
        SORT_BY_CLIENTS: "👥 Número de Clientes (default)",
        SORT_BY_LTV: "💰 LTV Promedio por Cliente",
        SORT_BY_FINAL_SCORE: "🏆 Final Score (relevancia estratégica)"
    }

    def __init__(self, customers: List[Any], grouping_mode: str = "entry_based",
                conversion_mode: str = "cumulative", ue_results: dict = None,
                cohort_config: Optional[CohortConfig] = None,
                cac_map: Optional[Dict[str, float]] = None):
        """
        Args:
            customers: Lista de objetos Customer
            grouping_mode: "entry_based" o "behavioral"
            conversion_mode: "cumulative" o "incremental"
            ue_results: Resultados de UnitEconomicsAnalyzer (opcional)
            cohort_config: Configuración de cohortes
            cac_map: Mapa de CAC ya transformado (opcional, priority sobre ue_results)
        """
        self.customers = customers
        self.grouping_mode = grouping_mode
        self.conversion_mode = conversion_mode
        self._cache = {}
        self._product_groups = None
        
        # CohortManager
        self.cohort_config = cohort_config or CohortConfig()
        self.cohort_manager = CohortManager(self.cohort_config)
        print(f"   📊 CohortManager granularidad: {self.cohort_config.granularity.value}")
        
        # Cargar preferencia de ordenamiento global
        self._load_global_sort_preference()
        
        # ========== CAC MAP: prioridad a cac_map si se pasa ==========
        if cac_map is not None:
            self.cac_map = cac_map
            print(f"   📊 CAC map recibido directamente: {len(self.cac_map)} cohortes")
        elif ue_results:
            # Extraer desde ue_results (compatibilidad)
            if "cohorts" in ue_results:
                cohorts_data = ue_results["cohorts"]
            else:
                cohorts_data = ue_results
            
            self.cac_map = {}
            for cohort_id, data in cohorts_data.items():
                if isinstance(data, dict):
                    self.cac_map[cohort_id] = data.get("cac", 0)
                else:
                    self.cac_map[cohort_id] = data
            print(f"   📊 CAC map desde ue_results: {len(self.cac_map)} cohortes")
        else:
            # Intentar cargar desde archivo
            cac_path = os.environ.get("LTV_CAC_PATH")
            if cac_path:
                self.cac_map = CACRepository.get_cac_mapping(cac_path, self.cohort_config.granularity.value, transform=True)
            else:
                self.cac_map = {}
            print(f"   📊 CAC map desde archivo: {len(self.cac_map)} cohortes")
        
        if not self.cac_map:
            print(f"   ⚠️ Sin mapa de CAC - LTV/CAC ratio no disponible")
        
        mode_display = "Comportamental" if grouping_mode == self.GROUPING_BEHAVIORAL else "Basado en entrada"
        conv_display = "Acumulativa" if conversion_mode == self.CONVERSION_CUMULATIVE else "Incremental"
        sort_display = self.get_sort_criteria_display()
        print(f"🔧 DimensionQueryEngine inicializado con grouping_mode={mode_display}, conversion_mode={conv_display}, sort={sort_display}")
        
        # Diagnóstico de PID
        self._diagnose_pid()
    
    def _load_global_sort_preference(self):
        """Carga preferencia de ordenamiento global desde configuración."""
        try:
            saved = ConfigLoader.load("user_sort_preference")
            if saved and saved.get("sort_criteria") in self.SORT_OPTIONS:
                self.sort_criteria = saved.get("sort_criteria")
            else:
                self.sort_criteria = self.SORT_BY_CLIENTS
        except Exception:
            self.sort_criteria = self.SORT_BY_CLIENTS
    
    def _save_global_sort_preference(self):
        """Persiste preferencia de ordenamiento global."""
        try:
            ConfigLoader.save("user_sort_preference", {"sort_criteria": self.sort_criteria})
        except Exception:
            pass
    
    def _diagnose_pid(self):
        """Diagnóstico para verificar si hay PID en los datos."""
        sample_pids = []
        for customer in self.customers[:20]:
            for order in customer.get_orders_sorted()[:5]:
                pid = getattr(order, 'prod_pid', None)
                if pid and str(pid).strip() not in ['', 'N/A', 'nan', 'none', 'null']:
                    sample_pids.append(str(pid).strip())
                    if len(sample_pids) >= 5:
                        break
            if len(sample_pids) >= 5:
                break
        
        if sample_pids:
            print(f"   ✅ PID encontrados en datos: {sample_pids[:3]}...")
        else:
            print(f"   ⚠️ No se encontraron PID. Verificar que prod_pid existe en los datos.")
    
    # ========== MÉTODOS DE ORDENAMIENTO ==========
    
    def set_sort_criteria(self, criteria: str):
        """Cambia el criterio de ordenamiento."""
        if criteria in self.SORT_OPTIONS:
            self.sort_criteria = criteria
            self._save_global_sort_preference()
            print(f"✅ Criterio de ordenamiento cambiado a: {self.SORT_OPTIONS[criteria]}")
        else:
            print(f"❌ Criterio inválido. Opciones: {list(self.SORT_OPTIONS.keys())}")
    
    def get_sort_criteria_display(self) -> str:
        """Retorna display del criterio actual."""
        return self.SORT_OPTIONS.get(self.sort_criteria, "👥 Número de Clientes (default)")
    
    # ========== NUEVO: BÚSQUEDA UNIFICADA (KEYWORD + PID EXACTO) ==========
    
    def _is_likely_pid(self, term: str) -> bool:
        """Detecta si un término parece un PID (formato: letras + números, 8-20 caracteres)."""
        term_clean = term.upper().replace(" ", "").strip()
        # Patrón típico de PID: alfanumérico, 6-20 caracteres
        if len(term_clean) >= 6 and len(term_clean) <= 30:
            if re.match(r'^[A-Z0-9\-_]+$', term_clean):
                return True
        return False
    
    def _search_by_pid_exact(self, pid: str) -> Optional[Tuple[str, str]]:
        pid_clean = pid.upper().replace(" ", "").strip()
        
        # 🔧 Si no parece un PID, salir rápido
        if len(pid_clean) < 5 or not any(c.isdigit() for c in pid_clean):
            return None
        
        print(f"   🔍 Buscando PID exacto: '{pid_clean}'...")
        
        count = 0
        for customer in self.customers:
            for order in customer.get_orders_sorted():
                order_pid = getattr(order, 'prod_pid', None)
                if order_pid:
                    order_pid_clean = str(order_pid).upper().replace(" ", "").strip()
                    if order_pid_clean == pid_clean:
                        product_name = getattr(order, 'name', None)
                        if product_name:
                            print(f"   ✅ PID encontrado en producto: {product_name}")
                            return (str(product_name).strip(), order_pid_clean)
                count += 1
                if count % 1000 == 0:
                    print(f"      Revisando órdenes... ({count})")
        
        print(f"   ❌ PID '{pid_clean}' no encontrado después de revisar {count} órdenes")
        return None
    
    def unified_search(self, dimension: str, search_term: str) -> List[Tuple[str, float, Optional[str]]]:
        """
        Busca por keyword y exact match PID.
        VERSIÓN OPTIMIZADA: Filtra PRIMERO por keyword, luego calcula relevancia.
        """
        print(f"   🔍 unified_search: dimension={dimension}, term='{search_term}'")
        
        search_lower = search_term.lower().strip()
        
        # 1. Verificar si es PID exacto SOLO para productos
        if dimension == 'name' and len(search_lower) >= 5 and any(c.isdigit() for c in search_lower):
            print("   🔍 Paso 1: verificando si es PID...")
            pid_result = self._search_by_pid_exact(search_term)
            if pid_result:
                product_name, pid = pid_result
                print(f"   ✅ PID exacto encontrado: {product_name}")
                return [(product_name, 100.0, pid)]
        
        # 2. Búsqueda normal por keyword - PRIMERO FILTRAR (rápido)
        print(f"   🔍 Paso 2: listando valores disponibles para '{dimension}'...")
        values = self.list_available_values(dimension)
        print(f"   📋 {len(values)} valores disponibles")
        
        # 🔧 FILTRO RÁPIDO: solo valores que contienen la keyword
        print("   🔍 Paso 3: filtrando por keyword...")
        filtered_values = [v for v in values if search_lower in v.lower()]
        print(f"   📋 Filtrados {len(filtered_values)} valores que contienen '{search_term}'")
        
        # 3. Calcular relevancia SOLO para los filtrados
        print("   🔍 Paso 4: calculando relevancia y ordenando...")
        matches = []
        total = len(filtered_values)
        
        for i, val in enumerate(filtered_values):
            if i % 100 == 0 and i > 0:
                print(f"      Procesando {i}/{total}...")
            relevance = self._score_relevance(val, search_term)
            if relevance > 0:
                pid_value = None
                if dimension == 'name':
                    pid_value = self._get_pid_for_product(val)
                matches.append((val, relevance, pid_value))
        
        print(f"   🔍 Encontradas {len(matches)} coincidencias con relevancia > 0")
        matches.sort(key=lambda x: x[1], reverse=True)
        return matches
    
    # ========== MÉTODOS DE ORDENAMIENTO POR CRITERIO GLOBAL ==========
    
    def _sort_by_clients(self, matches: List[str], dimension: str) -> List[str]:
        """Ordena matches por número de clientes."""
        matches_with_metrics = []
        for match in matches:
            customers = self._get_dimension_customers(dimension, match)
            if customers:
                metrics = self._calculate_metrics_for_customers(customers, dimension, match)
                matches_with_metrics.append((match, metrics.get('total_clientes', 0)))
            else:
                matches_with_metrics.append((match, 0))
        matches_with_metrics.sort(key=lambda x: x[1], reverse=True)
        return [m[0] for m in matches_with_metrics]
    
    def _sort_by_ltv(self, matches: List[str], dimension: str) -> List[str]:
        """Ordena matches por LTV promedio."""
        matches_with_metrics = []
        for match in matches:
            customers = self._get_dimension_customers(dimension, match)
            if customers:
                metrics = self._calculate_metrics_for_customers(customers, dimension, match)
                matches_with_metrics.append((match, metrics.get('ltv_promedio', 0)))
            else:
                matches_with_metrics.append((match, 0))
        matches_with_metrics.sort(key=lambda x: x[1], reverse=True)
        return [m[0] for m in matches_with_metrics]
    
    def _sort_by_score(self, matches: List[str], dimension: str) -> List[str]:
        """Ordena matches por LTV/CAC ratio."""
        matches_with_metrics = []
        for match in matches:
            customers = self._get_dimension_customers(dimension, match)
            if customers:
                metrics = self._calculate_metrics_for_customers(customers, dimension, match)
                matches_with_metrics.append((match, metrics.get('ltv_cac_ratio', 0)))
            else:
                matches_with_metrics.append((match, 0))
        matches_with_metrics.sort(key=lambda x: x[1], reverse=True)
        return [m[0] for m in matches_with_metrics]
    
    def _apply_global_sort(self, matches: List[str], dimension: str) -> List[str]:
        """Aplica ordenamiento global a cualquier dimensión."""
        if not matches:
            return matches
        
        if self.sort_criteria == self.SORT_BY_CLIENTS:
            return self._sort_by_clients(matches, dimension)
        elif self.sort_criteria == self.SORT_BY_LTV:
            return self._sort_by_ltv(matches, dimension)
        elif self.sort_criteria == self.SORT_BY_FINAL_SCORE:
            return self._sort_by_score(matches, dimension)
        return matches  # default
    
    # ========== MÉTODOS PRINCIPALES ==========
    
    def _get_cac_for_customer(self, customer) -> float:
        """
        Obtiene el CAC para un cliente basado en su cohorte (fecha de primera compra).
        Usa CohortManager para obtener el cohort_id dinámicamente.
        """
        orders = customer.get_orders_sorted()
        if not orders:
            return 0
        
        first_order = orders[0]
        first_date = first_order.order_date
        
        # Usar CohortManager para obtener cohort_id dinámico
        cohort_id = self.cohort_manager.get_cohort_id(first_date)
        
        return self.cac_map.get(cohort_id, 0)
    
    def _get_dimension_customers(self, dimension: str, value: str) -> List[Any]:
        from copy import copy
        
        filtered_customers = []
        search_value_lower = str(value).strip().lower()
        
        for customer in self.customers:
            orders = customer.get_orders_sorted()
            if not orders:
                continue
            
            if self.grouping_mode == self.GROUPING_ENTRY_BASED:
                # 🔧 ENTRY_BASED: Solo importa la PRIMERA orden
                first_order = orders[0]
                attr_value = str(getattr(first_order, dimension, "")).strip().lower()
                
                if attr_value == search_value_lower:
                    # ✅ El cliente ENTRÓ con esta dimensión
                    # Conservar TODAS sus órdenes (sin filtrar)
                    customer_copy = copy(customer)
                    customer_copy._orders = orders  # ← TODAS las órdenes
                    customer_copy._timeline_cache = None
                    filtered_customers.append(customer_copy)
            else:
                # BEHAVIORAL: Filtrar órdenes que cumplen
                filtered_orders = []
                for order in orders:
                    attr_value = str(getattr(order, dimension, "")).strip().lower()
                    if attr_value == search_value_lower:
                        filtered_orders.append(order)
                
                if filtered_orders:
                    customer_copy = copy(customer)
                    customer_copy._orders = filtered_orders
                    customer_copy._timeline_cache = None
                    filtered_customers.append(customer_copy)
        
        return filtered_customers
    
    def _get_dimension_customers_with_parent_filters(self, dimension: str, value: str,
                                                       parent_filters: Dict[str, str] = None) -> List[Any]:
        """
        Filtra clientes respetando jerarquía padre → hijo.
        
        Args:
            dimension: "category", "subcategory", "brand", "name"
            value: Valor a buscar
            parent_filters: {'category': 'Perros', 'subcategory': 'Alimento'}
        
        Returns:
            Lista de clientes que cumplen con el criterio y los filtros padre
        """
        if not parent_filters:
            return self._get_dimension_customers(dimension, value)
        
        filtered = []
        search_value_lower = str(value).strip().lower()
        
        for customer in self.customers:
            orders = customer.get_orders_sorted()
            if not orders:
                continue
            
            # Verificar filtros padre
            passes_parents = True
            for parent_dim, parent_val in parent_filters.items():
                parent_val_clean = str(parent_val).strip().lower()
                if self.grouping_mode == self.GROUPING_ENTRY_BASED:
                    first_order = orders[0]
                    attr_parent = str(getattr(first_order, parent_dim, "")).strip().lower()
                    if attr_parent != parent_val_clean:
                        passes_parents = False
                        break
                else:
                    if not any(str(getattr(o, parent_dim, "")).strip().lower() == parent_val_clean for o in orders):
                        passes_parents = False
                        break
            
            if not passes_parents:
                continue
            
            # Verificar dimensión actual
            if self.grouping_mode == self.GROUPING_ENTRY_BASED:
                first_order = orders[0]
                attr_value = str(getattr(first_order, dimension, "")).strip().lower()
                if attr_value == search_value_lower:
                    filtered.append(customer)
            else:
                if any(str(getattr(o, dimension, "")).strip().lower() == search_value_lower for o in orders):
                    filtered.append(customer)
        
        return filtered
    
    def _calculate_conversion_rates(self, customers: List[Any], total_clientes: int) -> Dict[str, float]:
        """
        Calcula las tasas de conversión según el modo seleccionado.
        Usa las ventanas de conversión del CohortConfig.
        """
        windows = self.cohort_config.conversion_windows
        base = total_clientes if total_clientes > 0 else 1
        
        if self.conversion_mode == self.CONVERSION_INCREMENTAL:
            # Modo INCREMENTAL: cada cliente cuenta SOLO en la primera ventana que cumple
            conv_counts = {w: 0 for w in windows}
            
            for customer in customers:
                orders = customer.get_orders_sorted()
                if len(orders) >= 2:
                    d1 = orders[0].order_date
                    d2 = orders[1].order_date
                    diff = (d2 - d1).days
                    
                    for w in windows:
                        if diff <= w:
                            conv_counts[w] += 1
                            break  # Solo la PRIMERA ventana
            
            return {
                f"Pct_Conv_{w}d": round((conv_counts[w] / base) * 100, 2)
                for w in windows
            }
        
        else:  # Modo CUMULATIVE
            conv_counts = {w: 0 for w in windows}
            
            for customer in customers:
                orders = customer.get_orders_sorted()
                if len(orders) >= 2:
                    d1 = orders[0].order_date
                    d2 = orders[1].order_date
                    diff = (d2 - d1).days
                    
                    # Acumulativo: cuenta en TODAS las ventanas que cumple
                    for w in windows:
                        if diff <= w:
                            conv_counts[w] += 1
            
            return {
                f"Pct_Conv_{w}d": round((conv_counts[w] / base) * 100, 2)
                for w in windows
            }
    
    def _calculate_metrics_for_customers(self, customers: List[Any], 
                                          dimension_name: str,
                                          dimension_value: str) -> Dict[str, Any]:
        """
        Calcula métricas para un conjunto de clientes filtrados por dimensión.
        """
        if not customers:
            return {
                "dimension": dimension_name,
                "value": dimension_value,
                "grouping_mode": self.grouping_mode,
                "conversion_mode": self.conversion_mode,
                "granularity": self.cohort_config.granularity.value,
                "found": False,
                "error": "No se encontraron clientes con este valor"
            }
        
        # Métricas básicas
        total_clientes = len(customers)
        
        # Contar órdenes por cliente
        order_counts = [len(c.get_orders_sorted()) for c in customers]
        total_ordenes = sum(order_counts)
        pedidos_promedio = round(total_ordenes / total_clientes, 2) if total_clientes > 0 else 0
        
        # Frecuencia de compras
        n_c2 = sum(1 for c in order_counts if c >= 2)
        n_c3 = sum(1 for c in order_counts if c >= 3)
        n_c4 = sum(1 for c in order_counts if c >= 4)
        
        pct_2da = round((n_c2 / total_clientes) * 100, 2) if total_clientes > 0 else 0
        pct_3ra = round((n_c3 / total_clientes) * 100, 2) if total_clientes > 0 else 0
        pct_4ta = round((n_c4 / total_clientes) * 100, 2) if total_clientes > 0 else 0
        
        # Revenue y LTV
        total_revenue = sum(c.total_revenue() for c in customers)
        total_cp = sum(c.total_cp() for c in customers)  # LTV BRUTO (sin CAC)
        aov = round(total_revenue / total_ordenes, 2) if total_ordenes > 0 else 0
        ltv_promedio = round(total_cp / total_clientes, 2) if total_clientes > 0 else 0
        
        # Calcular CAC y LTV/CAC Ratio (usando CohortManager)
        total_cac = 0
        for customer in customers:
            total_cac += self._get_cac_for_customer(customer)
        
        cac_promedio = round(total_cac / total_clientes, 2) if total_clientes > 0 else 0
        ltv_cac_ratio = round(ltv_promedio / cac_promedio, 2) if cac_promedio > 0 else 0
        
        # Tiempo entre compras (mediana)
        tiempos_1a2 = []
        tiempos_2a3 = []
        tiempos_3a4 = []
        
        for customer in customers:
            orders = customer.get_orders_sorted()
            if len(orders) >= 2:
                diff = (orders[1].order_date - orders[0].order_date).days
                if diff > 0:
                    tiempos_1a2.append(diff)
            if len(orders) >= 3:
                diff = (orders[2].order_date - orders[1].order_date).days
                if diff > 0:
                    tiempos_2a3.append(diff)
            if len(orders) >= 4:
                diff = (orders[3].order_date - orders[2].order_date).days
                if diff > 0:
                    tiempos_3a4.append(diff)
        
        mediana_1a2 = round(np.median(tiempos_1a2), 0) if tiempos_1a2 else 0
        mediana_2a3 = round(np.median(tiempos_2a3), 0) if tiempos_2a3 else 0
        mediana_3a4 = round(np.median(tiempos_3a4), 0) if tiempos_3a4 else 0
        
        # Tasas de conversión (usando ventanas del config)
        pct_conv = self._calculate_conversion_rates(customers, total_clientes)
        
        return {
            "dimension": dimension_name,
            "value": dimension_value,
            "grouping_mode": self.grouping_mode,
            "conversion_mode": self.conversion_mode,
            "granularity": self.cohort_config.granularity.value,
            "found": True,
            "total_clientes": total_clientes,
            "total_ordenes": total_ordenes,
            "pedidos_promedio": pedidos_promedio,
            "pct_2da_compra": pct_2da,
            "pct_3ra_compra": pct_3ra,
            "pct_4ta_compra": pct_4ta,
            "abs_2da_compra": n_c2,
            "abs_3ra_compra": n_c3,
            "abs_4ta_compra": n_c4,
            "aov": aov,
            "ltv_promedio": ltv_promedio,
            "cac_promedio": cac_promedio,
            "ltv_cac_ratio": ltv_cac_ratio,
            "revenue_total": round(total_revenue, 2),
            "cp_total": round(total_cp, 2),
            "mediana_dias_1a2": mediana_1a2,
            "mediana_dias_2a3": mediana_2a3,
            "mediana_dias_3a4": mediana_3a4,
            **pct_conv
        }
    
    def _add_pid_to_result(self, result: Dict[str, Any], product_name: str) -> Dict[str, Any]:
        """
        Agrega PID al resultado cuando se busca por producto.
        """
        if not result.get("found", False):
            return result
        
        if not product_name:
            return result
        
        pid_value = None
        product_name_clean = str(product_name).strip()
        
        for customer in self.customers:
            orders = customer.get_orders_sorted()
            for order in orders:
                order_product = getattr(order, 'name', None)
                if order_product and str(order_product).strip() == product_name_clean:
                    pid = getattr(order, 'prod_pid', None)
                    if pid and str(pid).strip().lower() not in ['', 'nan', 'none', 'n/a', 'null']:
                        pid_value = str(pid).strip()
                        break
            if pid_value:
                break
        
        if pid_value:
            result["PID"] = pid_value
            print(f"   📦 PID encontrado: {pid_value}")
        else:
            result["PID"] = "N/A"
            print(f"   ⚠️ PID no disponible para {product_name}")
        
        return result
    
    def _get_pid_for_product(self, product_name: str) -> Optional[str]:
        """
        Obtiene el PID de un producto por su nombre.
        
        Args:
            product_name: Nombre del producto
        
        Returns:
            PID como string o None si no se encuentra
        """
        product_clean = str(product_name).strip()
        
        for customer in self.customers:
            for order in customer.get_orders_sorted():
                order_product = getattr(order, 'name', None)
                if order_product and str(order_product).strip() == product_clean:
                    pid = getattr(order, 'prod_pid', None)
                    if pid and str(pid).strip().lower() not in ['', 'nan', 'none', 'n/a', 'null']:
                        return str(pid).strip()
        return None
    
    def _rank_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ordena el resultado por relevancia de negocio según el criterio seleccionado.
        """
        if not result.get("found", False):
            return result
        
        return result
    
    def query(self, category: str = None, subcategory: str = None, 
              brand: str = None, product: str = None,
              subcategory_brand: str = None,
              parent_filters: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Busca una dimensión específica.
        
        Ejemplos:
            query(category="Electrónica")
            query(subcategory="Laptops")
            query(brand="Samsung")
            query(product="Galaxy S21")
            query(subcategory_brand="Televisores | Samsung")
            query(product="Galaxy S21", parent_filters={"category": "Electrónica", "subcategory": "Celulares"})
        """
        if category:
            if parent_filters:
                customers = self._get_dimension_customers_with_parent_filters("category", category, parent_filters)
            else:
                customers = self._get_dimension_customers("category", category)
            result = self._calculate_metrics_for_customers(customers, "Categoria", category)
            result["conversion_mode"] = self.conversion_mode
            return self._rank_result(result)
        
        if subcategory:
            if parent_filters:
                customers = self._get_dimension_customers_with_parent_filters("subcategory", subcategory, parent_filters)
            else:
                customers = self._get_dimension_customers("subcategory", subcategory)
            result = self._calculate_metrics_for_customers(customers, "Subcategoria", subcategory)
            result["conversion_mode"] = self.conversion_mode
            return self._rank_result(result)
        
        if brand:
            if parent_filters:
                customers = self._get_dimension_customers_with_parent_filters("brand", brand, parent_filters)
            else:
                customers = self._get_dimension_customers("brand", brand)
            result = self._calculate_metrics_for_customers(customers, "Brand", brand)
            result["conversion_mode"] = self.conversion_mode
            return self._rank_result(result)
        
        if product:
            if parent_filters:
                customers = self._get_dimension_customers_with_parent_filters("name", product, parent_filters)
            else:
                customers = self._get_dimension_customers("name", product)
            result = self._calculate_metrics_for_customers(customers, "Producto", product)
            result["conversion_mode"] = self.conversion_mode
            result = self._add_pid_to_result(result, product)
            return self._rank_result(result)
        
        if subcategory_brand:
            customers = self._get_dimension_customers("subcategory_brand", subcategory_brand)
            result = self._calculate_metrics_for_customers(customers, "Subcategoria_Marca", subcategory_brand)
            result["conversion_mode"] = self.conversion_mode
            return self._rank_result(result)
        
        return {"error": "Debes especificar category, subcategory, brand, product o subcategory_brand"}
    
    def list_available_values(self, dimension: str) -> List[str]:
        """
        Lista todos los valores disponibles para una dimensión.
        AHORA: Mira TODAS las órdenes de TODOS los clientes (entry_based también).
        """
        values = set()
        for customer in self.customers:
            # 🔧 CAMBIADO: Ahora mira TODAS las órdenes, no solo la primera
            for order in customer.get_orders_sorted():
                if dimension == "subcategory_brand":
                    val = getattr(order, "subcategory_brand", None)
                else:
                    val = getattr(order, dimension, None)
                
                if val and str(val).strip().lower() not in ["", "n/a", "nan", "none"]:
                    values.add(str(val).strip())
        
        return sorted(values)
    
    def _score_relevance(self, product_name: str, search_term: str) -> float:
        # 🔧 Fast path: si el término está vacío
        if not search_term:
            return 0
        
        search_lower = search_term.lower().strip()
        product_lower = product_name.lower().strip()
        
        # 🔧 Fast path: si el término es más largo que el producto
        if len(search_lower) > len(product_lower):
            return 0
        
        # ... resto del código igual ...
        
        if not search_lower:
            return 0
        
        score = 0
        
        # 1. Coincidencia exacta
        if product_lower == search_lower:
            score += 100
        
        # 2. Coincidencia al inicio del nombre
        elif product_lower.startswith(search_lower):
            score += 50
        
        # 3. Coincidencia de palabras completas
        product_words = product_lower.split()
        search_words = search_lower.split()
        
        matched_words = 0
        for sw in search_words:
            if sw in product_words:
                matched_words += 1
                score += 10
                if product_words and product_words[0] == sw:
                    score += 5
        
        # 4. Proporción de palabras coincidentes
        if search_words:
            word_match_ratio = matched_words / len(search_words)
            score += word_match_ratio * 20
        
        # 5. Coincidencia parcial
        if search_lower in product_lower:
            score += 1
        
        # 6. Bonus por longitud
        score += min(len(product_lower) * 0.01, 5)
        
        return round(score, 2)
    
    def _group_similar_products(self, products: List[str]) -> Dict[str, List[str]]:
        """Agrupa productos similares."""
        grouped = {}
        
        for product in products:
            base = product
            
            patterns = [
                r',\s*Color\s+\w+',
                r',\s*Colour\s+\w+',
                r',\s*Talla\s+\w+',
                r',\s*Size\s+\w+',
                r',\s*[\w]+\s+Incluido',
                r'\s*\([^)]*\)',
                r',\s*[A-Z][a-z]+$',
            ]
            
            for pattern in patterns:
                base = re.sub(pattern, '', base, flags=re.IGNORECASE)
            
            words = base.split()
            if len(words) > 7:
                base = ' '.join(words[:7])
            
            base = re.sub(r'\s+', ' ', base).strip().rstrip(',').strip()
            
            if base not in grouped:
                grouped[base] = []
            grouped[base].append(product)
        
        return grouped
    
    def _extract_variant_detail(self, variant: str, base_name: str) -> str:
        """Extrae la parte única de una variante."""
        detail = variant.replace(base_name, "").strip()
        
        if not detail:
            return variant[:40] + "..." if len(variant) > 40 else variant
        
        detail = detail.lstrip(',').lstrip('-').lstrip(';').strip()
        
        detail = re.sub(r'^Color\s+', '', detail, flags=re.IGNORECASE)
        detail = re.sub(r'^Colour\s+', '', detail, flags=re.IGNORECASE)
        detail = re.sub(r'^Talla\s+', '', detail, flags=re.IGNORECASE)
        detail = re.sub(r'^Size\s+', '', detail, flags=re.IGNORECASE)
        
        if len(detail) > 35:
            detail = detail[:32] + "..."
        
        return detail if detail else "Estándar"
    
    # ========== MÉTODOS DE MENÚ INTERACTIVO ==========
    
    def _search_by_pid_standalone(self):
        """
        Búsqueda de producto por PID exacto (ahora integrada en búsqueda unificada,
        pero mantenida por compatibilidad).
        """
        print("\n" + "=" * 50)
        print("   BÚSQUEDA POR PID (EXACTO)".center(50))
        print("=" * 50)
        print("💡 El PID debe coincidir exactamente con el identificador del producto")
        print("-" * 50)
        
        pid_input = input("\n👉 Ingresa el PID a buscar: ").strip().upper()
        
        if not pid_input:
            print("❌ Búsqueda cancelada.")
            return None
        
        print(f"\n🔍 Buscando producto con PID exacto: '{pid_input}'...")
        
        result = self._search_by_pid_exact(pid_input)
        
        if not result:
            print(f"❌ No se encontró ningún producto con PID '{pid_input}'")
            return None
        
        product_name, pid = result
        print(f"✅ Producto encontrado: {product_name} (PID: {pid})")
        
        return product_name
    
    def _change_granularity(self):
        """Permite cambiar la granularidad de cohortes interactivamente."""
        print("\n" + "=" * 50)
        print("   CAMBIAR GRANULARIDAD DE COHORTES".center(50))
        print("=" * 50)
        print(f"Granularidad actual: {self.cohort_config.granularity.value}")
        print("\nOpciones:")
        print("   1. Diaria (daily)")
        print("   2. Semanal (weekly)")
        print("   3. Mensual (monthly)")
        print("   4. Trimestral (quarterly) - DEFAULT")
        print("   5. Semestral (semiannual)")
        print("   6. Anual (yearly)")
        print("   q. Cancelar")
        
        option = input("\n👉 Opción: ").strip()
        
        granularity_map = {
            '1': TimeGranularity.DAILY,
            '2': TimeGranularity.WEEKLY,
            '3': TimeGranularity.MONTHLY,
            '4': TimeGranularity.QUARTERLY,
            '5': TimeGranularity.SEMIANNUAL,
            '6': TimeGranularity.YEARLY,
        }
        
        if option in granularity_map:
            new_granularity = granularity_map[option]
            self.cohort_config = CohortConfig(granularity=new_granularity)
            self.cohort_manager = CohortManager(self.cohort_config)
            print(f"✅ Granularidad cambiada a: {new_granularity.value}")
            
            cac_path = os.environ.get("LTV_CAC_PATH")
            if cac_path:
                self.cac_map = CACRepository.get_cac_mapping(cac_path, new_granularity.value)
                if self.cac_map:
                    print(f"✅ CAC recargado: {len(self.cac_map)} cohortes")
                else:
                    print(f"⚠️ No se pudo recargar CAC para granularidad {new_granularity.value}")
        else:
            print("❌ Cancelado")
    
    def _get_menu_options_for_dimension(self, dimension: str) -> dict:
        """Retorna opciones de menú específicas para la dimensión."""
        if dimension == 'brand':
            return {
                'show_sort': True,
                'show_conversion': False,
                'show_brand_mode': True,
                'brand_mode_options': ['flat', 'hierarchical', 'dual'],
                'hint': "Puedes cambiar entre modo plano, jerárquico o dual"
            }
        elif dimension == 'name':
            return {
                'show_sort': True,
                'show_conversion': True,
                'show_brand_mode': False,
                'hint': "Puedes cambiar ordenamiento y tipo de conversión"
            }
        else:
            return {
                'show_sort': True,
                'show_conversion': False,
                'show_brand_mode': False,
                'hint': ""
            }
    
    def interactive_search(self, dimension: str = None):
        """
        Modo interactivo para buscar dimensiones.
        VERSIÓN MEJORADA: PID solo para productos.
        """
        mode_display = "Comportamental" if self.grouping_mode == self.GROUPING_BEHAVIORAL else "Basado en entrada"
        conv_display = "Acumulativa" if self.conversion_mode == self.CONVERSION_CUMULATIVE else "Incremental"
        sort_display = self.get_sort_criteria_display()
        
        if dimension is None:
            # ========== MENÚ PRINCIPAL ==========
            print("\n" + "=" * 60)
            print("      BUSCADOR DE DIMENSIONES LTV".center(60))
            print("=" * 60)
            print(f"\n⚙️ Modo de agrupación: {mode_display}")
            print(f"⚙️ Modo de conversión: {conv_display}")
            print(f"📊 Ordenamiento: {sort_display}")
            print(f"📅 Granularidad de cohortes: {self.cohort_config.granularity.value}")
            print("\n¿Qué quieres buscar?")
            print("1. 📂 Categoría")
            print("2. 📁 Subcategoría")
            print("3. 🏷️  Marca (plano - todas las compras)")
            print("4. 🎯 Producto")
            print("5. 🔗 Subcategoría + Marca (jerárquico)")
            
            # 🔧 PID SOLO PARA PRODUCTO (opción 4)
            # No mostrar 'p' en el menú principal, solo se activa cuando se selecciona producto
            
            print("c. 🔄 Cambiar modo de conversión")
            print("s. 📊 Cambiar ordenamiento")
            print("g. 🔄 Cambiar granularidad (temporal)")
            print("b. 🔙 Volver al menú principal del buscador")
            print("=" * 60)
            
            option = input("\n👉 Selecciona una opción: ").strip().lower()
            
            if option == 'c':
                if self.conversion_mode == self.CONVERSION_CUMULATIVE:
                    self.conversion_mode = self.CONVERSION_INCREMENTAL
                    print("\n✅ Modo de conversión cambiado a: INCREMENTAL (distribución)")
                else:
                    self.conversion_mode = self.CONVERSION_CUMULATIVE
                    print("\n✅ Modo de conversión cambiado a: ACUMULATIVA (creciente)")
                input("\nPresiona Enter para continuar...")
                return self.interactive_search(dimension)
            
            if option == 's':
                self._change_sort_criteria()
                input("\nPresiona Enter para continuar...")
                return self.interactive_search(dimension)
            
            if option == 'g':
                self._change_granularity()
                return self.interactive_search(dimension)
            
            if option == 'b':
                print("\n🔙 Volviendo al menú principal del buscador...")
                return
            
            dim_map = {
                '1': ('category', 'Categorías'),
                '2': ('subcategory', 'Subcategorías'),
                '3': ('brand', 'Marcas'),
                '4': ('name', 'Productos'),
                '5': ('subcategory_brand', 'Combinaciones (Subcategoría + Marca)')
            }
            
            if option not in dim_map:
                return
            
            dimension, display_name = dim_map[option]
            
            # 🔧 Si es producto, ofrecer búsqueda por PID después de mostrar valores
            if dimension == 'name':
                print("\n💡 TIP: También puedes buscar por PID exacto después de ver la lista")
            
        else:
            display_name = {
                'category': 'Categorías',
                'subcategory': 'Subcategorías',
                'brand': 'Marcas',
                'name': 'Productos',
                'subcategory_brand': 'Combinaciones (Subcategoría + Marca)'
            }.get(dimension, 'Valores')
    
        
        # ========== MOSTRAR VALORES DISPONIBLES ==========
        print(f"\n📋 {display_name} disponibles (modo: {mode_display}):")
        
        try:
            terminal_width = os.get_terminal_size().columns
            display_width = min(terminal_width - 10, 120)
        except:
            display_width = 100
        
        print("-" * min(display_width, 100))
        
        values = self.list_available_values(dimension)
        
        if not values:
            print("⚠️ No hay datos disponibles para esta dimensión.")
            return
        
        # Agrupar productos
        if dimension == 'name':
            grouped = self._group_similar_products(values)
            display_values = []
            self._product_groups = {}
            
            for base_name, variants in grouped.items():
                if len(variants) == 1:
                    display_values.append(variants[0])
                    self._product_groups[variants[0]] = {'type': 'single', 'variants': variants}
                else:
                    display_name_group = f"{base_name} [{len(variants)} variantes]"
                    display_values.append(display_name_group)
                    self._product_groups[display_name_group] = {'type': 'group', 'variants': variants, 'base': base_name}
            
            values = display_values
        else:
            self._product_groups = None
        
        # Mostrar primeros 30 valores
        for i, val in enumerate(values[:30], 1):
            if len(val) > 80:
                display_val = val[:77] + "..."
            else:
                display_val = val
            print(f"   {i:2}. {display_val}")
        
        if len(values) > 30:
            print(f"   ... y {len(values) - 30} más")
        
        print("-" * min(display_width, 100))
        
        # ========== NUEVO: BUSCADOR UNIFICADO ==========
        # ========== NUEVO: BUSCADOR UNIFICADO ==========
        print("\n💡 TIP: Puedes buscar por KEYWORD")

        # 🔧 Mostrar hint de PID SOLO si es producto
        if dimension == 'name':
            print("   Para buscar por PID, ingréselo exactamente (ej: 'ABC123XYZ')")
        elif dimension == 'subcategory_brand':
            print("   Busca combinaciones como 'Laptops (Dell)'")
        else:
            print(f"   Escribe el nombre de la {display_name.lower()} que buscas")

        print("-" * 50)
        
        search_term = input("\n🔍 Escribe el nombre a buscar (o parte de él): ").strip()

        if not search_term:
            print("❌ Búsqueda cancelada.")
            return

        # 🔧 NUEVO: Mensaje de progreso
        print("\n⏳ Buscando... (esto puede tomar unos segundos)")
        import sys
        sys.stdout.flush()
        
        # Usar búsqueda unificada
        unified_results = self.unified_search(dimension, search_term)
        
        if not unified_results:
            print(f"❌ No se encontraron coincidencias para '{search_term}'")
            return
        
        # Extraer matches y ordenar según criterio global
        matches = [r[0] for r in unified_results]
        pids = {r[0]: r[2] for r in unified_results if r[2]}
        
        # Aplicar ordenamiento global
        if dimension != 'name':  # Para productos ya está ordenado por relevance
            matches = self._apply_global_sort(matches, dimension)
        
        print(f"\n🔍 Se encontraron {len(matches)} coincidencias (ordenadas por {self.get_sort_criteria_display()}):")
        
        for i, match in enumerate(matches[:15], 1):
            display_match = match[:70] + "..." if len(match) > 70 else match
            pid_info = f" [PID: {pids[match]}]" if match in pids else ""
            print(f"   {i:2}. {display_match}{pid_info}")
        
        if len(matches) > 15:
            print(f"   ... y {len(matches) - 15} más")
        
        # ========== SUBMENÚ DE OPCIONES ==========
        if dimension == 'name':
            print("\n" + "-" * 40)
            print("📊 OPCIONES:")
            print("   c. 🔄 Cambiar modo de conversión")
            print(f"   s. 📊 Cambiar ordenamiento (actual: {self.get_sort_criteria_display()})")
            print("   g. 🔄 Cambiar granularidad")
            print("   q. 🔙 Volver")
            print("-" * 40)
            
            sub_option = input("\n👉 Opción (número, c, s, g, q): ").strip().lower()
            
            if sub_option == 'c':
                if self.conversion_mode == self.CONVERSION_CUMULATIVE:
                    self.conversion_mode = self.CONVERSION_INCREMENTAL
                    print("\n✅ Modo de conversión cambiado a: INCREMENTAL")
                else:
                    self.conversion_mode = self.CONVERSION_CUMULATIVE
                    print("\n✅ Modo de conversión cambiado a: ACUMULATIVA")
                input("\nPresiona Enter para continuar...")
                return self.interactive_search(dimension)
            
            if sub_option == 's':
                self._change_sort_criteria()
                input("\nPresiona Enter para continuar...")
                return self.interactive_search(dimension)
            
            if sub_option == 'g':
                self._change_granularity()
                return self.interactive_search(dimension)
            
            if sub_option == 'q':
                return
            
            selected = sub_option
        else:
            # Para otras dimensiones, mostrar hint de ordenamiento
            menu_options = self._get_menu_options_for_dimension(dimension)
            if menu_options.get('show_sort'):
                print(f"\n💡 Los resultados están ordenados por: {self.get_sort_criteria_display()}")
                print("   Puedes cambiar el ordenamiento con la opción 's' en el menú principal")
            
            selected = input("\n👉 Selecciona el número exacto o escribe el nombre: ").strip()
        
        # ========== PROCESAR SELECCIÓN ==========
        if selected.isdigit() and 1 <= int(selected) <= len(matches):
            selected_value = matches[int(selected) - 1]
        else:
            filtered = [m for m in matches if selected.lower() in m.lower()]
            if len(filtered) == 1:
                selected_value = filtered[0]
            else:
                selected_value = selected
        
        # Manejar selección de productos con variantes
        if dimension == 'name' and self._product_groups and selected_value in self._product_groups:
            group_info = self._product_groups[selected_value]
            
            if group_info['type'] == 'group':
                variants = group_info['variants']
                base_name = group_info['base']
                
                print(f"\n🔍 Producto '{base_name}' tiene {len(variants)} variantes:")
                
                for i, variant in enumerate(variants[:10], 1):
                    variant_detail = self._extract_variant_detail(variant, base_name)
                    print(f"   {i:2}. {variant_detail}")
                
                if len(variants) > 10:
                    print(f"   ... y {len(variants) - 10} más")
                
                print("\n📊 Opciones:")
                print("   0. Ver TODAS las variantes agrupadas (recomendado)")
                print(f"   1-{len(variants)}. Seleccionar una variante específica")
                
                choice = input("\n👉 Selecciona una opción: ").strip()
                
                if choice == '0' or choice == '':
                    print(f"\n🔍 Buscando {len(variants)} variantes de '{base_name}'...")
                    
                    all_customers = []
                    for variant in variants:
                        customers = self._get_dimension_customers("name", variant)
                        all_customers.extend(customers)
                    
                    unique_customers = list({c.customer_id: c for c in all_customers}.values())
                    
                    result = self._calculate_metrics_for_customers(unique_customers, "Producto", base_name)
                    result["conversion_mode"] = self.conversion_mode
                    result = self._add_pid_to_result(result, base_name)
                    self._print_result(result)
                    return
                
                elif choice.isdigit() and 1 <= int(choice) <= len(variants):
                    selected_variant = variants[int(choice) - 1]
                    print(f"\n🔍 Buscando variante específica...")
                    result = self.query(product=selected_variant)
                    result["conversion_mode"] = self.conversion_mode
                    self._print_result(result)
                    return
                else:
                    print("❌ Opción inválida. Mostrando todas las variantes agrupadas.")
                    all_customers = []
                    for variant in variants:
                        customers = self._get_dimension_customers("name", variant)
                        all_customers.extend(customers)
                    unique_customers = list({c.customer_id: c for c in all_customers}.values())
                    result = self._calculate_metrics_for_customers(unique_customers, "Producto", base_name)
                    result["conversion_mode"] = self.conversion_mode
                    result = self._add_pid_to_result(result, base_name)
                    self._print_result(result)
                    return
            
            else:
                result = self.query(product=selected_value)
                result["conversion_mode"] = self.conversion_mode
                self._print_result(result)
                return
        
        # Búsqueda normal
        if dimension == 'category':
            result = self.query(category=selected_value)
        elif dimension == 'subcategory':
            result = self.query(subcategory=selected_value)
        elif dimension == 'brand':
            result = self.query(brand=selected_value)
        elif dimension == 'name':
            result = self.query(product=selected_value)
        elif dimension == 'subcategory_brand':
            result = self.query(subcategory_brand=selected_value)
        else:
            result = {"error": f"Dimensión {dimension} no soportada"}
        
        result["conversion_mode"] = self.conversion_mode
        self._print_result(result)
    
    def _change_sort_criteria(self):
        """Permite cambiar el criterio de ordenamiento de resultados."""
        print("\n" + "=" * 50)
        print("   CAMBIAR ORDENAMIENTO DE RESULTADOS".center(50))
        print("=" * 50)
        print(f"Criterio actual: {self.get_sort_criteria_display()}")
        print("\nOpciones:")
        print("   1. 👥 Número de Clientes (default)")
        print("   2. 💰 LTV Promedio por Cliente")
        print("   3. 🏆 Final Score (relevancia estratégica)")
        print("   b. Cancelar")
        print("=" * 50)
        
        option = input("\n👉 Opción: ").strip()
        
        if option == '1':
            self.set_sort_criteria(self.SORT_BY_CLIENTS)
        elif option == '2':
            self.set_sort_criteria(self.SORT_BY_LTV)
        elif option == '3':
            self.set_sort_criteria(self.SORT_BY_FINAL_SCORE)
        elif option == 'b':
            return
        else:
            print("❌ Opción inválida")
    
    def _print_result(self, result: Dict[str, Any]):
        """Imprime los resultados de forma formateada."""
        if not result.get("found", False):
            print(f"\n❌ {result.get('error', 'No se encontraron resultados')}")
            return
        
        modo = "Comportamental" if result.get("grouping_mode") == self.GROUPING_BEHAVIORAL else "Basado en entrada"
        
        conv_mode = result.get("conversion_mode", self.conversion_mode)
        conv_modo = "Acumulativa" if conv_mode == self.CONVERSION_CUMULATIVE else "Incremental"
        
        sort_display = self.get_sort_criteria_display()
        
        if conv_mode == self.CONVERSION_INCREMENTAL:
            conv_note = "\n   📌 NOTA: Tasas INCREMENTALES (distribución - cada cliente cuenta una sola vez en la primera ventana que cumple)"
        else:
            conv_note = "\n   📌 NOTA: Tasas ACUMULATIVAS (crecientes - clientes cuentan en múltiples ventanas)"
        
        print("\n" + "=" * 70)
        print(f"📊 RESULTADOS PARA {result['dimension'].upper()}: {result['value']}".center(70))
        print(f"   (Agrupación: {modo} | Conversión: {conv_modo} | Orden: {sort_display})".center(70))
        print(conv_note)
        print("=" * 70)
        
        pid_display = ""
        if result.get('PID'):
            pid_display = f"\n   • PID:                   {result['PID']}"
        
        print(f"""
    📈 MÉTRICAS GENERALES:
    • Clientes únicos:     {result['total_clientes']:,}
    • Total de órdenes:    {result['total_ordenes']:,}
    • Pedidos por cliente: {result['pedidos_promedio']}
    {pid_display}
    🔄 FRECUENCIA DE COMPRA:
    • 2da compra:          {result['abs_2da_compra']:,} ({result['pct_2da_compra']}%)
    • 3ra compra:          {result['abs_3ra_compra']:,} ({result['pct_3ra_compra']}%)
    • 4ta compra:          {result['abs_4ta_compra']:,} ({result['pct_4ta_compra']}%)

    ⏱️ TIEMPO ENTRE COMPRAS (mediana):
    • 1ra → 2da:           {int(result['mediana_dias_1a2'])} días
    • 2da → 3ra:           {int(result['mediana_dias_2a3'])} días
    • 3ra → 4ta:           {int(result['mediana_dias_3a4'])} días

    📊 TASAS DE CONVERSIÓN (2da compra):
    • 30 días:             {result.get('Pct_Conv_30d', 0)}%
    • 60 días:             {result.get('Pct_Conv_60d', 0)}%
    • 90 días:             {result.get('Pct_Conv_90d', 0)}%
    • 180 días:            {result.get('Pct_Conv_180d', 0)}%
    • 360 días:            {result.get('Pct_Conv_360d', 0)}%

    💰 VALOR ECONÓMICO:
    • AOV (Ticket promedio): ${result['aov']:,.2f}
    • LTV promedio:          ${result['ltv_promedio']:,.2f}
    • CAC promedio:          ${result['cac_promedio']:,.2f}
    • LTV/CAC ratio:          {result['ltv_cac_ratio']:,.2f}x
    • Revenue total:         ${result['revenue_total']:,.2f}
    • Contribution Profit:   ${result['cp_total']:,.2f}
    """)
        print("=" * 70)
    
    def quick_search(self, dimension: str, value: str) -> None:
        """Búsqueda rápida sin interactividad."""
        if dimension == 'category':
            result = self.query(category=value)
        elif dimension == 'subcategory':
            result = self.query(subcategory=value)
        elif dimension == 'brand':
            result = self.query(brand=value)
        elif dimension == 'product':
            result = self.query(product=value)
        elif dimension == 'subcategory_brand':
            result = self.query(subcategory_brand=value)
        else:
            print(f"❌ Dimensión '{dimension}' no soportada. Usa: category, subcategory, brand, product, subcategory_brand")
            return
        
        result["conversion_mode"] = self.conversion_mode
        self._print_result(result)
    
    def set_conversion_mode(self, mode: str):
        """Cambia el modo de conversión."""
        if mode in [self.CONVERSION_CUMULATIVE, self.CONVERSION_INCREMENTAL]:
            self.conversion_mode = mode
            print(f"✅ Modo de conversión cambiado a: {mode}")
        else:
            print(f"❌ Modo inválido. Usa 'cumulative' o 'incremental'")
    
    def set_granularity(self, granularity: str):
        """Cambia la granularidad de cohortes."""
        try:
            new_granularity = TimeGranularity.from_string(granularity)
            self.cohort_config = CohortConfig(granularity=new_granularity)
            self.cohort_manager = CohortManager(self.cohort_config)
            print(f"✅ Granularidad cambiada a: {new_granularity.value}")
            
            cac_path = os.environ.get("LTV_CAC_PATH")
            if cac_path:
                self.cac_map = CACRepository.get_cac_mapping(cac_path, new_granularity.value)
                if self.cac_map:
                    print(f"✅ CAC recargado: {len(self.cac_map)} cohortes")
        except Exception as e:
            print(f"❌ Error cambiando granularidad: {e}")