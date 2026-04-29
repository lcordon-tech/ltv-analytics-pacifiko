# Category/Orchestrators/behavior_orchestrator.py

from Category.Orchestrators.base_dimension_orchestrator import BaseDimensionOrchestrator
from Category.Utils.dimension_config import DimensionMode


class CategoryBehaviorOrchestrator(BaseDimensionOrchestrator):
    """
    Orquestador para análisis por Categoría.
    """
    
    def _get_dimension_mode(self) -> int:
        return DimensionMode.CATEGORY
    
    # No necesitas __init__ porque hereda el de la clase base