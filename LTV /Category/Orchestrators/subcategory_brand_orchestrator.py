"""
Orquestador para la dimensión jerárquica Subcategoría + Marca.
"""

from Category.Orchestrators.base_dimension_orchestrator import BaseDimensionOrchestrator
from Category.Utils.dimension_config import DimensionMode


class SubcategoryBrandOrchestrator(BaseDimensionOrchestrator):
    """
    Orquestador para análisis jerárquico por Subcategoría + Marca.
    """
    
    def _get_dimension_mode(self) -> int:
        return DimensionMode.SUBCATEGORY_BRAND