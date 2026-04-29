# Category/Utils/utils_ue.py
"""
Utilidades para construir DataFrames de Unit Economics con soporte de cohortes dinámicos.
"""

import pandas as pd
from collections import defaultdict
from typing import List, Dict, Any, Tuple, Optional

from Category.Grouping.category_grouper import CategoryGrouper
from Category.Grouping.entry_grouper import EntryBasedBehaviorGrouper
from Category.Cohort.cohort_manager import CohortManager
from Category.Cohort.cohort_config import CohortConfig, TimeGranularity


def build_unit_economics_dataframe(
    customers: List[Any],
    mode: int,
    ue_results: Dict,
    grouping_mode: str = "entry_based",
    by_cohort: bool = False,
    cohort_manager: Optional[CohortManager] = None
) -> pd.DataFrame:
    """
    Construye DataFrame de Unit Economics por dimensión.
    
    Versión MEJORADA: Soporta cohortes dinámicos via CohortManager.
    
    Args:
        customers: Lista de objetos Customer
        mode: Modo de dimensión (1=Category, 2=Subcategory, 3=Brand, 4=Product, 5=Subcategory_Brand, 6=Dual)
        ue_results: Resultados de UnitEconomicsAnalyzer (CAC por cohorte)
        grouping_mode: "entry_based" o "behavioral"
        by_cohort: Si True, desglosa por cohorte dentro de cada dimensión
        cohort_manager: CohortManager opcional. Si es None, usa default quarterly.
    
    Returns:
        DataFrame con métricas de Unit Economics por dimensión/cohorte
    """
    
    # Mapeo de modos a atributos y nombres de columna
    MODE_CONFIG = {
        1: {'attr': 'category', 'dim_col': 'Categoria', 'group_by': 'category', 'dual': False},
        2: {'attr': 'subcategory', 'dim_col': 'Subcategoria', 'group_by': 'subcategory', 'dual': False},
        3: {'attr': 'brand', 'dim_col': 'Brand', 'group_by': 'brand', 'dual': False},
        4: {'attr': 'name', 'dim_col': 'Producto', 'group_by': 'name', 'dual': False},
        5: {'attr': 'subcategory_brand', 'dim_col': 'Subcategoria_Marca', 'group_by': 'subcategory_brand', 'dual': False},
        6: {'attr': 'subcategory', 'dim_col': 'Subcategoria', 'group_by': 'subcategory', 'dual': True, 'brand_col': 'Marca'},
    }
    
    # Validar modo
    if mode not in MODE_CONFIG:
        print(f"⚠️ Modo {mode} no soportado en Unit Economics. Usando modo por defecto (1).")
        mode = 1
    
    config = MODE_CONFIG[mode]
    dim_col = config['dim_col']
    group_by_attr = config['group_by']
    is_dual = config.get('dual', False)
    brand_col = config.get('brand_col', None)
    
    # ========== CREAR COHORT MANAGER SI NO SE PROPORCIONA ==========
    if cohort_manager is None:
        cohort_manager = CohortManager()  # default quarterly
    
    print(f"   📊 CohortManager granularidad: {cohort_manager.config.granularity.value}")
    # =================================================================
    
    # ========== EXTRAER CAC MAP CORRECTAMENTE ==========
    if ue_results is None:
        cac_map = {}
    elif "cohorts" in ue_results:
        cohorts_data = ue_results["cohorts"]
        cac_map = {}
        for cohort_id, data in cohorts_data.items():
            if isinstance(data, dict):
                cac_map[cohort_id] = data.get("cac", 0)
            else:
                cac_map[cohort_id] = data
    else:
        # Si ue_results ya es el diccionario de cohorts directamente
        cac_map = {}
        for cohort_id, data in ue_results.items():
            if isinstance(data, dict):
                cac_map[cohort_id] = data.get("cac", 0)
            else:
                cac_map[cohort_id] = data
    
    print(f"   📊 CAC Map construido: {len(cac_map)} cohortes")
    # ================================================================

    # 2. SELECCIÓN DE ESTRATEGIA DE AGRUPACIÓN
    if grouping_mode == "entry_based":
        print(f"DEBUG: Utils usando EntryBased para modo {mode} ({dim_col})")
        grouped_data, _ = EntryBasedBehaviorGrouper.group(customers, mode=mode)
    else:
        print(f"DEBUG: Utils usando CategoryGrouper (Behavioral) para modo {mode} ({dim_col})")
        grouped_data, _ = CategoryGrouper.group(customers, group_by=group_by_attr)

    rows = []

    # 3. PROCESAMIENTO
    for dim_name, customers_list in grouped_data.items():
        # Para modo dual, necesitamos también la marca del primer cliente
        brand_name = ""
        if is_dual and customers_list:
            # Obtener la marca del primer cliente (para asociar a la subcategoría)
            first_customer = customers_list[0]
            orders = first_customer.get_orders_sorted()
            if orders:
                first_order = orders[0]
                brand_name = getattr(first_order, 'brand', '')
                if brand_name and str(brand_name).lower() in ["nan", "none", "n/a", "", "null"]:
                    brand_name = ""
        
        if by_cohort:
            # Agrupación secundaria por cohorte (usando CohortManager)
            cohort_splits = _group_customers_by_cohort_dynamic(customers_list, cohort_manager)
            for cohort_id, sub_customers in cohort_splits.items():
                row = _calculate_ue_row(sub_customers, cac_map, dim_name, dim_col, cohort_id, cohort_manager)
                if is_dual and brand_name:
                    row[brand_col] = brand_name
                rows.append(row)
        else:
            row = _calculate_ue_row(customers_list, cac_map, dim_name, dim_col, cohort_manager=cohort_manager)
            if is_dual and brand_name:
                row[brand_col] = brand_name
            rows.append(row)

    # 4. FORMATEO FINAL
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Definición dinámica de nombres de columna según modo
    cac_name = 'CAC_Cohorte_$' if by_cohort else 'CAC_Promedio_$'
    
    # Columnas esperadas en el orden correcto
    if is_dual:
        base_cols = ['Subcategoria', 'Marca']
        if by_cohort:
            base_cols.append('Cohorte')
    else:
        base_cols = [dim_col] if not by_cohort else [dim_col, 'Cohorte']
    
    final_cols = base_cols + [
        'Total_Clientes', 'Total_Ordenes', 'GMV_Total_$',
        'LTV_Acumulado_Total_$', cac_name, 
        'LTV_Promedio_Cliente_$', 'LTV_Neto_Promedio_Cliente_$',
        'AOV_$', 'Ordenes_Promedio', 'LTV/CAC_Ratio', 'Payback_Proxy_Meses'
    ]
    
    # Asegurar que solo devolvemos las columnas que existen
    existing_cols = [c for c in final_cols if c in df.columns]
    
    # Si CAC_Promedio_$ no existe pero CAC_Cohorte_$ sí, renombrar
    if 'CAC_Promedio_$' not in df.columns and 'CAC_Cohorte_$' in df.columns and not by_cohort:
        df['CAC_Promedio_$'] = df['CAC_Cohorte_$']
        existing_cols.append('CAC_Promedio_$')
    
    df = df[existing_cols]
    
    # Ordenar
    if by_cohort and 'Cohorte' in df.columns:
        if is_dual:
            sort_by = ['Cohorte', 'Subcategoria', 'Marca']
        else:
            sort_by = ['Cohorte', dim_col]
        ascending = True
    elif 'GMV_Total_$' in df.columns:
        sort_by = 'GMV_Total_$'
        ascending = False
    else:
        if is_dual:
            sort_by = ['Subcategoria', 'Marca']
        else:
            sort_by = dim_col
        ascending = True
    
    return df.sort_values(by=sort_by, ascending=ascending)


def _group_customers_by_cohort_dynamic(customers_list: List[Any], cohort_manager: CohortManager) -> Dict[str, List[Any]]:
    """
    Agrupa customers por cohorte usando CohortManager dinámico.
    
    Args:
        customers_list: Lista de customers
        cohort_manager: CohortManager para generar cohort_ids
    
    Returns:
        Diccionario {cohort_label: [customers]}
    """
    cohort_map = defaultdict(list)
    
    for customer in customers_list:
        orders = customer.get_orders_sorted()
        if not orders:
            cohort_map["Unknown"].append(customer)
            continue
        
        # Buscar la fecha de la primera compra de ESTA categoría
        try:
            first_purchase_date = min(o.order_date for o in orders)
        except Exception:
            cohort_map["Unknown"].append(customer)
            continue
        
        # Obtener cohort_id dinámico
        cohort_id = cohort_manager.get_cohort_id(first_purchase_date)
        cohort_map[cohort_id].append(customer)
    
    return dict(cohort_map)


def _calculate_ue_row(
    customers_list: List[Any],
    cac_map: Dict[str, float],
    dim_name: str,
    dim_col: str,
    cohort_id: str = None,
    cohort_manager: Optional[CohortManager] = None
) -> Dict[str, Any]:
    """
    Calcula las métricas core para un grupo de customers.
    
    CORRECCIÓN v5.0:
    - Usa cohort_manager para obtener cohort_id de cada cliente
    - LTV_Acumulado_Total_$ (antes Margen_Operativo_Total_$)
    - LTV_Promedio_Cliente_$ = BRUTO (sin CAC)
    - LTV_Neto_Promedio_Cliente_$ = NETO (con CAC restado)
    - CAC_Promedio_$ = CAC independiente
    - LTV/CAC_Ratio usa LTV bruto
    - Payback_Proxy_Meses = (CAC * 12) / LTV_Bruto
    """
    t_clients = len(customers_list)
    t_orders = sum(c.total_orders() for c in customers_list)
    t_gmv = sum(c.total_revenue() for c in customers_list)
    t_margen = sum(c.total_cp() for c in customers_list)  # LTV acumulado total
    
    # Suma de CACs individuales basados en la cohorte real de cada cliente
    t_cac = 0
    
    # Usar cohort_manager si se proporciona, sino calcular trimestre manualmente
    if cohort_manager is None:
        from Category.Cohort.cohort_manager import CohortManager
        cohort_manager = CohortManager()  # default quarterly
    
    for customer in customers_list:
        # Obtener la cohorte del cliente usando CohortManager
        orders = customer.get_orders_sorted()
        if orders:
            first_date = orders[0].order_date
            c_origin = cohort_manager.get_cohort_id(first_date)
        else:
            c_origin = 'Desconocido'
        
        t_cac += cac_map.get(c_origin, 0)

    cac_avg = t_cac / t_clients if t_clients > 0 else 0
    
    # LTV BRUTO (sin CAC)
    ltv_bruto_total = t_margen
    ltv_bruto_prom = ltv_bruto_total / t_clients if t_clients > 0 else 0
    
    # LTV NETO (con CAC restado)
    ltv_neto_total = t_margen - t_cac
    ltv_neto_prom = ltv_neto_total / t_clients if t_clients > 0 else 0
    
    # LTV/CAC_Ratio: usa LTV BRUTO (sin CAC)
    ltv_cac_ratio = (ltv_bruto_prom / cac_avg) if cac_avg > 0 else 0
    
    # Payback_Proxy_Meses: meses para recuperar el CAC
    if ltv_bruto_prom > 0:
        payback_months = (cac_avg * 12) / ltv_bruto_prom
        payback_proxy = round(payback_months, 1)
    else:
        payback_proxy = 0

    # Diccionario base con TODAS las columnas
    res = {
        dim_col: dim_name,
        'Total_Clientes': t_clients,
        'Total_Ordenes': t_orders,
        'GMV_Total_$': round(t_gmv, 2),
        'LTV_Acumulado_Total_$': round(t_margen, 2),
        'CAC_Promedio_$': round(cac_avg, 2),  
        'LTV_Promedio_Cliente_$': round(ltv_bruto_prom, 2),
        'LTV_Neto_Promedio_Cliente_$': round(ltv_neto_prom, 2),
        'AOV_$': round(t_gmv / t_orders, 2) if t_orders > 0 else 0,
        'Ordenes_Promedio': round(t_orders / t_clients, 2) if t_clients > 0 else 0,
        'LTV/CAC_Ratio': round(ltv_cac_ratio, 2),
        'Payback_Proxy_Meses': payback_proxy,
    }
    
    # Si es cohorte, agregar identificador
    if cohort_id:
        res['Cohorte'] = cohort_id
    
    return res