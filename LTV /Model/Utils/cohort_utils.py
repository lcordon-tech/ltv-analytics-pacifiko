"""
Utilidades centralizadas para manejo de cohortes.
Elimina duplicación de lógica en múltiples módulos.
"""

from datetime import datetime
from functools import lru_cache
from typing import Dict, Set, List, Optional
from collections import defaultdict


class CohortUtils:
    """Utilidades centralizadas para manejo de cohortes."""
    
    # Cache para cohort_ids (evita recalcular fechas repetidas)
    _cohort_id_cache: Dict[str, str] = {}
    _period_value_cache: Dict[str, int] = {}
    
    @classmethod
    @lru_cache(maxsize=2048)
    def get_cohort_id(cls, date: datetime, granularity: str = "quarterly") -> str:
        """
        Retorna el identificador de cohorte para una fecha y granularidad.
        
        Args:
            date: Fecha datetime
            granularity: 'quarterly', 'monthly', 'weekly', 'semiannual', 'yearly'
        
        Returns:
            String identificador (ej: '2024-Q1', '2024-01', '2024', etc.)
        """
        if granularity == "quarterly":
            quarter = (date.month - 1) // 3 + 1
            return f"{date.year}-Q{quarter}"
        elif granularity == "monthly":
            return date.strftime("%Y-%m")
        elif granularity == "weekly":
            return date.strftime("%Y-W%W")
        elif granularity == "semiannual":
            half = 1 if date.month <= 6 else 2
            return f"{date.year}-H{half}"
        elif granularity == "yearly":
            return str(date.year)
        else:
            # Default a quarterly
            quarter = (date.month - 1) // 3 + 1
            return f"{date.year}-Q{quarter}"
    
    @classmethod
    @lru_cache(maxsize=2048)
    def get_period_value(cls, date: datetime, granularity: str = "quarterly") -> int:
        """
        Retorna valor numérico para comparación de períodos.
        
        Args:
            date: Fecha datetime
            granularity: 'quarterly', 'monthly', 'weekly', 'semiannual', 'yearly'
        
        Returns:
            Valor numérico monótono (mayor = más reciente)
        """
        if granularity == "quarterly":
            return date.year * 4 + ((date.month - 1) // 3)
        elif granularity == "monthly":
            return date.year * 12 + (date.month - 1)
        elif granularity == "weekly":
            # Semana aproximada (no perfecta pero monótona)
            return date.year * 52 + (date.timetuple().tm_yday // 7)
        elif granularity == "semiannual":
            half = 0 if date.month <= 6 else 1
            return date.year * 2 + half
        elif granularity == "yearly":
            return date.year
        else:
            return date.year * 4 + ((date.month - 1) // 3)
    
    @classmethod
    def parse_cohort_id(cls, cohort_id: str) -> Optional[tuple]:
        """
        Parsea un cohort_id y retorna (year, period, granularity).
        
        Args:
            cohort_id: String como '2024-Q1', '2024-01', '2024', etc.
        
        Returns:
            Tuple (year, period_num, granularity) o None si no se puede parsear
        """
        if '-Q' in cohort_id:
            parts = cohort_id.split('-Q')
            return (int(parts[0]), int(parts[1]), 'quarterly')
        elif '-' in cohort_id and len(cohort_id) == 7:  # YYYY-MM
            parts = cohort_id.split('-')
            return (int(parts[0]), int(parts[1]), 'monthly')
        elif len(cohort_id) == 4 and cohort_id.isdigit():
            return (int(cohort_id), 0, 'yearly')
        elif '-H' in cohort_id:
            parts = cohort_id.split('-H')
            return (int(parts[0]), int(parts[1]), 'semiannual')
        elif '-W' in cohort_id:
            parts = cohort_id.split('-W')
            return (int(parts[0]), int(parts[1]), 'weekly')
        
        return None


class UniqueOrderMixin:
    """
    Mixin para centralizar la lógica de órdenes únicas.
    Elimina duplicación en CohortBehaviorCalculator, CohortRetentionMatrix, etc.
    """
    
    def __init__(self):
        self._unique_orders_cache: Dict[str, List] = {}
    
    def get_unique_orders(self, customer) -> List:
        """
        Retorna órdenes únicas de un customer (desduplicado por order_id).
        Con cache para evitar recalcular múltiples veces.
        """
        cid = str(customer.customer_id)
        
        if cid in self._unique_orders_cache:
            return self._unique_orders_cache[cid]
        
        seen_order_ids = set()
        unique_orders = []
        
        for order in customer.get_orders_sorted():
            if order.order_id not in seen_order_ids:
                unique_orders.append(order)
                seen_order_ids.add(order.order_id)
        
        self._unique_orders_cache[cid] = unique_orders
        return unique_orders
    
    def clear_cache(self):
        """Limpia el cache (útil si los datos cambian)."""
        self._unique_orders_cache.clear()


class CustomerCohortCache:
    """
    Cache centralizado para cohort_ids de customers.
    Evita recalcular cohort_id cada vez que se necesita.
    """
    
    _instance = None
    _cache: Dict[str, Dict[str, str]] = {}  # {customer_id: {granularity: cohort_id}}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def get_cohort_id(cls, customer, granularity: str = "quarterly") -> str:
        """Obtiene cohort_id del cache o lo calcula."""
        cid = str(customer.customer_id)
        
        if cid not in cls._cache:
            cls._cache[cid] = {}
        
        if granularity in cls._cache[cid]:
            return cls._cache[cid][granularity]
        
        # Calcular y cachear
        first_order = customer.get_orders_sorted()[0] if customer.get_orders() else None
        if first_order:
            cohort_id = CohortUtils.get_cohort_id(first_order.order_date, granularity)
        else:
            cohort_id = "Unknown"
        
        cls._cache[cid][granularity] = cohort_id
        return cohort_id
    
    @classmethod
    def clear(cls):
        """Limpia el cache."""
        cls._cache.clear()