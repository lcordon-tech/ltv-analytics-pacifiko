# Category/Cohort/cohort_config.py
"""
Configuración dinámica de cohortes.
Permite definir granularidad temporal, rangos y ventanas custom.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional, Tuple, Union
import re


class TimeGranularity(Enum):
    """Granularidades temporales soportadas."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    SEMIANNUAL = "semiannual"
    YEARLY = "yearly"
    CUSTOM = "custom"
    
    @classmethod
    def from_string(cls, value: str):
        """Convierte string a enum."""
        value_lower = value.lower()
        for member in cls:
            if member.value == value_lower:
                return member
        return cls.QUARTERLY  # default


@dataclass
class CohortConfig:
    """
    Configuración dinámica para generación de cohortes.
    
    Ejemplos:
        # Mensual
        config = CohortConfig(
            granularity=TimeGranularity.MONTHLY,
            start_date=datetime(2022, 1, 1),
            end_date=datetime(2024, 12, 31)
        )
        
        # Custom (Black Friday)
        config = CohortConfig(
            granularity=TimeGranularity.CUSTOM,
            custom_boundaries=[
                datetime(2023, 11, 24),  # Black Friday
                datetime(2023, 11, 25),
                datetime(2023, 11, 26),
            ]
        )
        
        # Trimestral (default, compatible con sistema anterior)
        config = CohortConfig()  # usa quarterly 2020-2026
    """
    
    granularity: TimeGranularity = TimeGranularity.QUARTERLY
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    num_periods: Optional[int] = None
    custom_boundaries: List[datetime] = field(default_factory=list)
    labels: List[str] = field(default_factory=list)
    
    # Ventanas de tiempo para conversión (días)
    conversion_windows: List[int] = field(default_factory=lambda: [30, 60, 90, 180, 360])
    
    # Valores por defecto (compatibilidad con sistema anterior)
    DEFAULT_START = datetime(2020, 1, 1)
    DEFAULT_END = datetime(2026, 12, 31)
    
    def __post_init__(self):
        """Valida y completa la configuración."""
        if self.granularity == TimeGranularity.CUSTOM:
            if not self.custom_boundaries and not self.labels:
                raise ValueError("CUSTOM granularity requires custom_boundaries or labels")
            return
        
        # Fechas por defecto si no se especifican
        if self.start_date is None:
            self.start_date = self.DEFAULT_START
        if self.end_date is None:
            self.end_date = self.DEFAULT_END
        
        # Calcular número de períodos si no se especifica
        if self.num_periods is None:
            self.num_periods = self._calculate_num_periods()
    
    def _calculate_num_periods(self) -> int:
        """Calcula el número de períodos entre start_date y end_date."""
        if self.start_date is None or self.end_date is None:
            return 25  # default quarterly
        
        if self.granularity == TimeGranularity.DAILY:
            return (self.end_date - self.start_date).days + 1
        elif self.granularity == TimeGranularity.WEEKLY:
            return ((self.end_date - self.start_date).days // 7) + 1
        elif self.granularity == TimeGranularity.MONTHLY:
            return (self.end_date.year - self.start_date.year) * 12 + (self.end_date.month - self.start_date.month) + 1
        elif self.granularity == TimeGranularity.QUARTERLY:
            return (self.end_date.year - self.start_date.year) * 4 + ((self.end_date.month - 1) // 3 - (self.start_date.month - 1) // 3) + 1
        elif self.granularity == TimeGranularity.SEMIANNUAL:
            return (self.end_date.year - self.start_date.year) * 2 + ((self.end_date.month - 1) // 6 - (self.start_date.month - 1) // 6) + 1
        elif self.granularity == TimeGranularity.YEARLY:
            return self.end_date.year - self.start_date.year + 1
        else:
            return 25
    
    def get_cohort_label(self, date: datetime, index: int = 0) -> str:
        """
        Genera la etiqueta para una fecha según la granularidad.
        
        Args:
            date: Fecha a formatear
            index: Índice opcional para granularidad CUSTOM
        
        Returns:
            Etiqueta de cohorte (ej: "2024-Q1", "2024-01", "2024-W01", etc.)
        """
        if self.granularity == TimeGranularity.CUSTOM:
            if self.labels and index < len(self.labels):
                return self.labels[index]
            elif self.custom_boundaries and index < len(self.custom_boundaries):
                return self.custom_boundaries[index].strftime("%Y-%m-%d")
            return f"Custom_{index}"
        
        if self.granularity == TimeGranularity.DAILY:
            return date.strftime("%Y-%m-%d")
        elif self.granularity == TimeGranularity.WEEKLY:
            return date.strftime("%Y-W%W")
        elif self.granularity == TimeGranularity.MONTHLY:
            return date.strftime("%Y-%m")
        elif self.granularity == TimeGranularity.QUARTERLY:
            quarter = (date.month - 1) // 3 + 1
            return f"{date.year}-Q{quarter}"
        elif self.granularity == TimeGranularity.SEMIANNUAL:
            half = 1 if date.month <= 6 else 2
            return f"{date.year}-H{half}"
        elif self.granularity == TimeGranularity.YEARLY:
            return str(date.year)
        else:
            quarter = (date.month - 1) // 3 + 1
            return f"{date.year}-Q{quarter}"
    
    def get_period_index(self, date: datetime) -> int:
        """
        Retorna el índice numérico del período para una fecha.
        Útil para ordenamiento y comparación.
        """
        if self.granularity == TimeGranularity.CUSTOM:
            # Buscar el índice más cercano
            for i, boundary in enumerate(self.custom_boundaries):
                if date >= boundary:
                    continue
                return max(0, i - 1)
            return len(self.custom_boundaries) - 1
        
        if self.granularity == TimeGranularity.DAILY:
            return (date - self.start_date).days
        elif self.granularity == TimeGranularity.WEEKLY:
            return ((date - self.start_date).days // 7)
        elif self.granularity == TimeGranularity.MONTHLY:
            return (date.year - self.start_date.year) * 12 + (date.month - self.start_date.month)
        elif self.granularity == TimeGranularity.QUARTERLY:
            start_q = (self.start_date.month - 1) // 3
            current_q = (date.month - 1) // 3
            return (date.year - self.start_date.year) * 4 + (current_q - start_q)
        elif self.granularity == TimeGranularity.SEMIANNUAL:
            start_h = 0 if self.start_date.month <= 6 else 1
            current_h = 0 if date.month <= 6 else 1
            return (date.year - self.start_date.year) * 2 + (current_h - start_h)
        elif self.granularity == TimeGranularity.YEARLY:
            return date.year - self.start_date.year
        else:
            start_q = (self.start_date.month - 1) // 3
            current_q = (date.month - 1) // 3
            return (date.year - self.start_date.year) * 4 + (current_q - start_q)
    
    def get_all_labels(self) -> List[str]:
        """Genera todas las etiquetas de cohorte."""
        if self.granularity == TimeGranularity.CUSTOM:
            if self.labels:
                return self.labels
            return [d.strftime("%Y-%m-%d") for d in self.custom_boundaries]
        
        labels = []
        current = self.start_date
        while current <= self.end_date:
            labels.append(self.get_cohort_label(current))
            current = self._add_period(current)
        
        return labels
    
    def _add_period(self, date: datetime) -> datetime:
        """Avanza una unidad según la granularidad."""
        if self.granularity == TimeGranularity.DAILY:
            return date + timedelta(days=1)
        elif self.granularity == TimeGranularity.WEEKLY:
            return date + timedelta(days=7)
        elif self.granularity == TimeGranularity.MONTHLY:
            if date.month == 12:
                return datetime(date.year + 1, 1, 1)
            return datetime(date.year, date.month + 1, 1)
        elif self.granularity == TimeGranularity.QUARTERLY:
            new_month = date.month + 3
            if new_month > 12:
                return datetime(date.year + 1, new_month - 12, 1)
            return datetime(date.year, new_month, 1)
        elif self.granularity == TimeGranularity.SEMIANNUAL:
            if date.month <= 6:
                return datetime(date.year, 7, 1)
            return datetime(date.year + 1, 1, 1)
        elif self.granularity == TimeGranularity.YEARLY:
            return datetime(date.year + 1, 1, 1)
        else:
            new_month = date.month + 3
            if new_month > 12:
                return datetime(date.year + 1, new_month - 12, 1)
            return datetime(date.year, new_month, 1)
    
    @classmethod
    def from_dict(cls, config_dict: dict) -> 'CohortConfig':
        """Crea configuración desde diccionario."""
        granularity = TimeGranularity.from_string(config_dict.get('granularity', 'quarterly'))
        
        start_date = config_dict.get('start_date')
        if start_date and isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        
        end_date = config_dict.get('end_date')
        if end_date and isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)
        
        custom_boundaries = config_dict.get('custom_boundaries', [])
        if custom_boundaries and isinstance(custom_boundaries[0], str):
            custom_boundaries = [datetime.fromisoformat(d) for d in custom_boundaries]
        
        return cls(
            granularity=granularity,
            start_date=start_date,
            end_date=end_date,
            num_periods=config_dict.get('num_periods'),
            custom_boundaries=custom_boundaries,
            labels=config_dict.get('labels', []),
            conversion_windows=config_dict.get('conversion_windows', [30, 60, 90, 180, 360])
        )
    
    def to_dict(self) -> dict:
        """Convierte a diccionario para serialización."""
        return {
            'granularity': self.granularity.value,
            'start_date': self.start_date.isoformat() if self.start_date else None,
            'end_date': self.end_date.isoformat() if self.end_date else None,
            'num_periods': self.num_periods,
            'custom_boundaries': [d.isoformat() for d in self.custom_boundaries],
            'labels': self.labels,
            'conversion_windows': self.conversion_windows
        }