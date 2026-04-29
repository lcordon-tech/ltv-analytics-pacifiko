# Category/Cohort/__init__.py
"""Módulo para gestión dinámica de cohortes."""

from .cohort_config import CohortConfig, TimeGranularity
from .cohort_manager import CohortManager
# NO importar cohort_grouper aquí

__all__ = ['CohortConfig', 'TimeGranularity', 'CohortManager']