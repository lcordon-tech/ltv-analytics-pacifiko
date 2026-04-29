# Category/Orchestrators/brand_behavior_orchestrator.py

from Category.Orchestrators.base_dimension_orchestrator import BaseDimensionOrchestrator
from Category.Utils.dimension_config import DimensionMode


class BrandBehaviorOrchestrator(BaseDimensionOrchestrator):
    """
    Orquestador para análisis por Marca (Brand).
    """
    
    def _get_dimension_mode(self) -> int:
        return DimensionMode.BRAND