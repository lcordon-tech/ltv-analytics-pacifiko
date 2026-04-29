"""
Exporter para la dimensión jerárquica Subcategoría + Marca.
"""

from Category.Reporting.base_exporter import BaseExporter
from Category.Utils.dimension_config import DimensionMode


class SubcategoryBrandExporter(BaseExporter):
    """
    Exporter para análisis jerárquico por Subcategoría + Marca.
    """
    
    def _get_dimension_mode(self) -> int:
        return DimensionMode.SUBCATEGORY_BRAND