from typing import List, Union, Dict, Optional, Set
from datetime import datetime
import numpy as np
from .order import Order
from Model.Utils.cohort_utils import CohortUtils, CustomerCohortCache


class Customer:
    """
    Representa un cliente y gestiona la evolución de su valor (LTV) nominal.
    Distingue entre ítems individuales y transacciones únicas (Order ID).
    
    MEJORAS v2:
    - Cache de cohort_id (evita recalcular)
    - Método get_cohort_id() centralizado
    - Propiedad lazy para unique_orders
    """
    
    def __init__(self, customer_id: Union[str, int]):
        self.customer_id = customer_id
        self._orders: List[Order] = []
        
        # Caches
        self._timeline_cache: Optional[List[Dict]] = None
        self._unique_orders_cache: Optional[List[Order]] = None
        self._unique_order_ids_cache: Optional[Set[str]] = None
    
    def add_order(self, order: Order) -> None:
        """Agrega un ítem de orden e invalida caches."""
        if str(order.customer_id) != str(self.customer_id):
            raise ValueError(f"ID {self.customer_id}: La orden no le pertenece.")
        self._orders.append(order)
        self._invalidate_caches()
    
    def _invalidate_caches(self):
        """Invalida todos los caches cuando se agrega una orden."""
        self._timeline_cache = None
        self._unique_orders_cache = None
        self._unique_order_ids_cache = None
    
    # --- COHORTE (NUEVO - CENTRALIZADO) ---
    
    def get_cohort_id(self, granularity: str = "quarterly") -> str:
        """
        Retorna la cohorte del cliente según su primera compra.
        Usa cache centralizado para evitar recalcular.
        """
        return CustomerCohortCache.get_cohort_id(self, granularity)
    
    # --- IDENTIFICACIÓN DE TRANSACCIONES ÚNICAS ---
    
    def get_unique_orders(self) -> List[Order]:
        """
        Retorna órdenes únicas (desduplicadas por order_id).
        Con cache local.
        """
        if self._unique_orders_cache is not None:
            return self._unique_orders_cache
        
        seen_order_ids = set()
        unique_orders = []
        
        for order in self.get_orders_sorted():
            if order.order_id not in seen_order_ids:
                unique_orders.append(order)
                seen_order_ids.add(order.order_id)
        
        self._unique_orders_cache = unique_orders
        self._unique_order_ids_cache = seen_order_ids
        return unique_orders
    
    def get_unique_order_ids(self) -> Set[str]:
        """Retorna el set de IDs de compra únicos."""
        if self._unique_order_ids_cache is not None:
            return self._unique_order_ids_cache
        
        self.get_unique_orders()  # Popula el cache
        return self._unique_order_ids_cache
    
    def get_unique_purchases(self) -> List[Dict]:
        """
        Agrupa los ítems por (order_id + order_date) para obtener compras reales agregadas.
        """
        purchases = {}
        for o in self._orders:
            key = (str(o.order_id), o.order_date)
            if key not in purchases:
                purchases[key] = {
                    'order_id': o.order_id,
                    'date': o.order_date,
                    'revenue': 0.0,
                    'cp': 0.0
                }
            purchases[key]['revenue'] += o.revenue
            purchases[key]['cp'] += o.calculate_cp()
        
        return sorted(purchases.values(), key=lambda x: x['date'])
    
    def total_orders(self) -> int:
        """Métrica de frecuencia real. Cuenta compras únicas."""
        return len(self.get_unique_orders())
    
    # --- SEGMENTACIÓN ---
    
    def get_entry_dimension(self, mode=2) -> str:
        """
        Retorna la dimensión de entrada según mode.
        mode=1: category | mode=2: subcategory | mode=3: brand | mode=4: product (name)
        mode=5: subcategory_brand (formato: "Subcategoria (Marca)")
        """
        unique_orders = self.get_unique_orders()
        if not unique_orders:
            return "N/A"
        
        first_order = unique_orders[0]
        
        if mode == 1:
            return getattr(first_order, 'category', 'N/A')
        elif mode == 2:
            subcat = getattr(first_order, 'subcategory', None)
            category = getattr(first_order, 'category', 'N/A')
            is_invalid = subcat is None or (isinstance(subcat, float) and np.isnan(subcat)) or \
                        str(subcat).strip() in ["", "Unknown", "nan", "None"]
            return category if is_invalid else str(subcat)
        elif mode == 3:
            return str(getattr(first_order, 'brand', 'N/A'))
        elif mode == 4:
            return str(getattr(first_order, 'name', 'N/A'))
        elif mode == 5:
            subcat = getattr(first_order, 'subcategory', None)
            brand = getattr(first_order, 'brand', None)
            
            subcat_clean = str(subcat).strip() if subcat and str(subcat).lower() not in ["nan", "none", "n/a", "", "null"] else ""
            brand_clean = str(brand).strip() if brand and str(brand).lower() not in ["nan", "none", "n/a", "", "null"] else ""
            
            if subcat_clean and brand_clean:
                return f"{subcat_clean} ({brand_clean})"
            elif brand_clean:
                return brand_clean
            elif subcat_clean:
                return subcat_clean
            else:
                return 'N/A'
        else:
            return 'N/A'
    
    def get_categories(self) -> Set[str]:
        """Categorías únicas que han pasado por las manos del cliente."""
        return {order.category for order in self._orders}
    
    def get_business_units(self) -> Set[str]:
        """Retorna las unidades de negocio únicas vinculadas al cliente."""
        return {order.business_unit for order in self._orders}
    
    def first_category(self) -> str:
        """
        Identifica la categoría 'ancla' de la primera compra.
        Soporta múltiples ítems en la primera transacción eligiendo el de mayor revenue.
        """
        unique_orders = self.get_unique_orders()
        if not unique_orders:
            return "N/A"
        
        first_order = unique_orders[0]
        return getattr(first_order, 'category', 'N/A')
    
    # --- MÉTODOS DE ACCESO ---
    
    def get_orders(self) -> List[Order]:
        """Retorna copia de todas las filas/ítems."""
        return self._orders.copy()
    
    def get_orders_sorted(self) -> List[Order]:
        """Retorna todas las filas ordenadas cronológicamente."""
        orders = self.get_orders()
        orders.sort(key=lambda x: x.order_date)
        return orders
    
    def total_revenue(self) -> float:
        """GMV histórico (suma de todos los ítems)."""
        return sum(order.revenue for order in self._orders)
    
    def total_cp(self) -> float:
        """Margen Neto acumulado (Contribution Profit total)."""
        return sum(order.calculate_cp() for order in self._orders)
    
    # --- LÓGICA DE LTV NOMINAL ---
    
    def ltv_timeline(self) -> List[Dict]:
        """
        Calcula la evolución del LTV agrupando por transacción única.
        """
        if self._timeline_cache is not None:
            return self._timeline_cache
        
        timeline = []
        cumulative_ltv = 0.0
        
        sorted_transactions = self.get_unique_purchases()
        
        for tx in sorted_transactions:
            cumulative_ltv += tx["cp"]
            timeline.append({
                "date": tx["date"],
                "cp": round(tx["cp"], 2),
                "ltv": round(cumulative_ltv, 2),
                "revenue": round(tx["revenue"], 2)
            })
        
        self._timeline_cache = timeline
        return timeline
    
    def final_ltv(self, cac_mapping: dict = None, include_cac: bool = False, 
                  granularity: str = "quarterly"):
        """
        Calcula el LTV del cliente.
        
        Args:
            cac_mapping: Diccionario de CAC por cohorte
            include_cac: Si True, resta CAC (LTV neto)
            granularity: Granularidad para obtener cohort_id
        
        Returns:
            LTV bruto (sin CAC) por defecto, o LTV neto si include_cac=True
        """
        total_margin = self.total_cp()
        
        if not include_cac or not cac_mapping:
            return total_margin
        
        # Usar el nuevo método get_cohort_id
        cohort_id = self.get_cohort_id(granularity)
        cac_variable = cac_mapping.get(cohort_id, 0)
        
        return total_margin - cac_variable
    
    def __repr__(self):
        return (f"<Customer {self.customer_id} | Purchases: {self.total_orders()} | "
                f"LTV: ${self.final_ltv():.2f} | Entry: {self.first_category()}>")