# Category/Orchestrators/product_behavior_orchestrator.py

from Category.Orchestrators.base_dimension_orchestrator import BaseDimensionOrchestrator
from Category.Utils.dimension_config import DimensionMode


class ProductBehaviorOrchestrator(BaseDimensionOrchestrator):
    """
    Orquestador para análisis por Producto.
    """
    
    def _get_dimension_mode(self) -> int:
        return DimensionMode.PRODUCT