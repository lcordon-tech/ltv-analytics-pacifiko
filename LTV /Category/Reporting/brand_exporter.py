"""
Exporter específico para la dimensión Brand (Marca).
"""

from Category.Reporting.base_exporter import BaseExporter
from Category.Utils.dimension_config import DimensionMode


class BrandExporter(BaseExporter):
    """
    Exporter para análisis por Marca.
    Hereda toda la lógica de BaseExporter.
    """
    
    def _get_dimension_mode(self) -> int:
        return DimensionMode.BRAND