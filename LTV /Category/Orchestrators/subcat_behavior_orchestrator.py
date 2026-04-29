# Category/Orchestrators/subcat_behavior_orchestrator.py

from Category.Orchestrators.base_dimension_orchestrator import BaseDimensionOrchestrator
from Category.Utils.dimension_config import DimensionMode


class SubcategoryBehaviorOrchestrator(BaseDimensionOrchestrator):
    """
    Orquestador para análisis por Subcategoría.
    """
    
    def _get_dimension_mode(self) -> int:
        return DimensionMode.SUBCATEGORY