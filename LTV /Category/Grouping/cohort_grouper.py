# Category/Grouping/cohort_grouper.py
"""
Wrapper de compatibilidad para CohortGrouper.
Mantiene la misma interfaz que el código existente espera.
"""

from typing import List, Any, Dict
from Category.Cohort.cohort_grouper import CohortGrouper as DynamicCohortGrouper
from Category.Cohort.cohort_config import CohortConfig, TimeGranularity


class CohortGrouper:
    """
    Agrupador de cohortes (compatibilidad con código existente).
    Por defecto usa granularidad trimestral (comportamiento original).
    
    Para cambiar la granularidad, usar:
        grouper = CohortGrouper(granularity="monthly")
    """
    
    def __init__(self, granularity: str = "quarterly", **kwargs):
        """
        Args:
            granularity: 'daily', 'weekly', 'monthly', 'quarterly', 'semiannual', 'yearly'
            **kwargs: Argumentos adicionales para CohortConfig
        """
        self.granularity = granularity
        self._grouper = DynamicCohortGrouper.from_granularity(granularity, **kwargs)
    
    @staticmethod
    def group(customers: List[Any]) -> Dict[str, List[Any]]:
        """
        Método estático para compatibilidad con código existente.
        NOTA: Este método estático NO permite configurar granularidad.
        Se recomienda instanciar la clase y usar el método de instancia.
        
        Para compatibilidad, usa quarterly por defecto.
        """
        grouper = CohortGrouper()
        return grouper._grouper.group(customers)
    
    def group_instances(self, customers: List[Any]) -> Dict[str, List[Any]]:
        """Método de instancia para agrupar customers."""
        return self._grouper.group(customers)
    
    def get_cohort_manager(self):
        """Retorna el manager de cohortes."""
        return self._grouper.get_cohort_manager()
    
    def print_summary(self):
        """Imprime resumen de configuración."""
        print(f"📊 CohortGrouper (granularidad: {self.granularity})")
        self._grouper.print_summary()