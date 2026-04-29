"""
Exporter para la dimensión Category (compatibilidad legacy).
VERSIÓN REFACTORIZADA: ahora hereda de BaseExporter.
"""

from Category.Reporting.base_exporter import BaseExporter
from Category.Utils.dimension_config import DimensionMode


class CategoryExporter(BaseExporter):
    """
    Exporter para análisis por Categoría.
    Mantiene compatibilidad con código existente.
    """
    
    def _get_dimension_mode(self) -> int:
        return DimensionMode.CATEGORY