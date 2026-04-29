# Category/Cohort/cohort_grouper.py
"""
Agrupador de customers por cohorte usando CohortManager.
Reemplaza la lógica fija de trimestres.
"""

from typing import List, Dict, Any, Optional
from collections import defaultdict

from .cohort_manager import CohortManager
from .cohort_config import CohortConfig, TimeGranularity


class CohortGrouper:
    """
    Agrupa clientes por cohorte de adquisición DENTRO de una dimensión.
    Versión dinámica que usa CohortManager.
    
    Uso:
        # Con configuración por defecto (quarterly, compatible con anterior)
        grouper = CohortGrouper()
        grouped = grouper.group(customers)
        
        # Con configuración mensual
        config = CohortConfig(granularity=TimeGranularity.MONTHLY)
        grouper = CohortGrouper(config)
        grouped = grouper.group(customers)
    """
    
    def __init__(self, config: Optional[CohortConfig] = None):
        """
        Args:
            config: Configuración de cohortes. Si es None, usa quarterly default.
        """
        self.manager = CohortManager(config)
    
    @classmethod
    def from_granularity(cls, granularity: str, **kwargs) -> 'CohortGrouper':
        """
        Crea un grouper desde una granularidad string.
        
        Args:
            granularity: 'daily', 'weekly', 'monthly', 'quarterly', 'semiannual', 'yearly'
            **kwargs: Argumentos adicionales para CohortConfig
        """
        config = CohortConfig(
            granularity=TimeGranularity.from_string(granularity),
            **kwargs
        )
        return cls(config)
    
    def group(self, customers: List[Any]) -> Dict[str, List[Any]]:
        """
        Agrupa clientes por cohorte de adquisición.
        
        Para cada cliente, determina su fecha de primera compra
        (dentro de las órdenes que tiene, que ya pueden estar filtradas por dimensión).
        
        Args:
            customers: Lista de objetos Customer (ya filtrados por dimensión)
        
        Returns:
            Diccionario {cohort_label: [customers]}
        """
        cohort_map = defaultdict(list)
        
        for customer in customers:
            # Obtener órdenes ordenadas (ya filtradas por dimensión)
            orders = customer.get_orders_sorted()
            
            if not orders:
                cohort_map["Unknown"].append(customer)
                continue
            
            # Buscar la fecha de la primera compra
            try:
                first_purchase_date = min(o.order_date for o in orders)
            except Exception:
                cohort_map["Unknown"].append(customer)
                continue
            
            # Obtener cohort_id dinámico
            cohort_id = self.manager.get_cohort_id(first_purchase_date)
            cohort_map[cohort_id].append(customer)
        
        return dict(cohort_map)
    
    def get_cohort_manager(self) -> CohortManager:
        """Retorna el manager de cohortes para uso externo."""
        return self.manager
    
    def print_summary(self):
        """Imprime resumen de la configuración."""
        self.manager.print_summary()