# Category/Cohort/cohort_manager.py
"""
Gestor central de cohortes dinámicas.
Responsable de generar y gestionar cohortes a partir de fechas.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict

from .cohort_config import CohortConfig, TimeGranularity


class CohortManager:
    """
    Gestor central de cohortes.
    
    Uso:
        manager = CohortManager(config)
        
        # Obtener cohorte de una fecha
        cohort_id = manager.get_cohort_id(order_date)
        
        # Agrupar transacciones
        grouped = manager.group_by_cohort(transactions)
        
        # Obtener todas las cohortes
        all_cohorts = manager.get_all_cohorts()
    """
    
    def __init__(self, config: Optional[CohortConfig] = None):
        """
        Args:
            config: Configuración de cohortes. Si es None, usa quarterly default.
        """
        self.config = config or CohortConfig()
        self._all_labels = None
        self._cohort_cache = {}
    
    @classmethod
    def from_granularity(cls, granularity: str, **kwargs) -> 'CohortManager':
        """
        Crea un manager desde una granularidad string.
        
        Args:
            granularity: 'daily', 'weekly', 'monthly', 'quarterly', 'semiannual', 'yearly'
            **kwargs: Argumentos adicionales para CohortConfig
        """
        config = CohortConfig(
            granularity=TimeGranularity.from_string(granularity),
            **kwargs
        )
        return cls(config)
    
    def get_cohort_id(self, date: datetime) -> str:
        """
        Retorna el identificador de cohorte para una fecha.
        
        Args:
            date: Fecha de la transacción
        
        Returns:
            Etiqueta de cohorte (ej: "2024-Q1", "2024-01", etc.)
        """
        # Cache para evitar recalcular
        cache_key = date.isoformat()
        if cache_key in self._cohort_cache:
            return self._cohort_cache[cache_key]
        
        cohort_id = self.config.get_cohort_label(date)
        self._cohort_cache[cache_key] = cohort_id
        return cohort_id
    
    def get_cohort_index(self, date: datetime) -> int:
        """
        Retorna el índice numérico de cohorte para ordenamiento.
        """
        return self.config.get_period_index(date)
    
    def get_all_cohorts(self) -> List[str]:
        """Retorna todas las etiquetas de cohorte configuradas."""
        if self._all_labels is None:
            self._all_labels = self.config.get_all_labels()
        return self._all_labels
    
    def get_cohort_count(self) -> int:
        """Retorna el número total de cohortes."""
        return len(self.get_all_cohorts())
    
    def get_conversion_windows(self) -> List[int]:
        """Retorna las ventanas de tiempo para conversión."""
        return self.config.conversion_windows
    
    def group_by_cohort(self, items: List[Any], date_extractor=None) -> Dict[str, List[Any]]:
        """
        Agrupa items por cohorte.
        
        Args:
            items: Lista de items (órdenes, customers, etc.)
            date_extractor: Función para extraer fecha de cada item.
                           Si es None, asume que el item tiene atributo 'order_date'.
        
        Returns:
            Diccionario {cohort_label: [items]}
        """
        groups = defaultdict(list)
        
        for item in items:
            if date_extractor:
                date = date_extractor(item)
            elif hasattr(item, 'order_date'):
                date = item.order_date
            elif isinstance(item, dict) and 'order_date' in item:
                date = item['order_date']
            else:
                continue
            
            if date:
                cohort_id = self.get_cohort_id(date)
                groups[cohort_id].append(item)
        
        return dict(groups)
    
    def get_cohort_sequence(self, first_date: datetime, last_date: datetime) -> List[Tuple[str, int]]:
        """
        Retorna la secuencia de cohortes entre dos fechas.
        
        Returns:
            Lista de tuplas (cohort_label, cohort_index)
        """
        sequence = []
        current = first_date
        idx = 0
        
        while current <= last_date and idx < self.get_cohort_count():
            label = self.get_cohort_id(current)
            sequence.append((label, self.get_cohort_index(current)))
            current = self.config._add_period(current)
            idx += 1
        
        return sequence
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Retorna un resumen de la configuración."""
        return {
            'granularity': self.config.granularity.value,
            'start_date': self.config.start_date.isoformat() if self.config.start_date else None,
            'end_date': self.config.end_date.isoformat() if self.config.end_date else None,
            'total_cohorts': self.get_cohort_count(),
            'conversion_windows': self.config.conversion_windows,
            'all_cohorts': self.get_all_cohorts()[:10],  # primeras 10
            'cohort_sample': self.get_all_cohorts()[:5] if self.get_cohort_count() > 5 else self.get_all_cohorts()
        }
    
    def print_summary(self):
        """Imprime resumen de configuración."""
        summary = self.get_config_summary()
        print(f"\n📊 COHORT MANAGER SUMMARY")
        print(f"   Granularidad: {summary['granularity']}")
        print(f"   Total cohortes: {summary['total_cohorts']}")
        print(f"   Ventanas conversión: {summary['conversion_windows']}")
        print(f"   Ejemplo cohortes: {summary['cohort_sample']}")
        if summary['total_cohorts'] > 10:
            print(f"   ... y {summary['total_cohorts'] - 10} más")