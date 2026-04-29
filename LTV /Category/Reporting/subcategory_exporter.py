"""
Exporter para la dimensión Subcategory (compatibilidad legacy).
VERSIÓN REFACTORIZADA: ahora hereda de BaseExporter.
"""

from Category.Reporting.base_exporter import BaseExporter
from Category.Utils.dimension_config import DimensionMode


class SubcategoryExporter(BaseExporter):
    """
    Exporter para análisis por Subcategoría.
    Mantiene compatibilidad con código existente.
    """
    
    def _get_dimension_mode(self) -> int:
        return DimensionMode.SUBCATEGORY