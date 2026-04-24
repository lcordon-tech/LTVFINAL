"""
Exporter específico para la dimensión Producto (Product Name).
"""

from Category.Reporting.base_exporter import BaseExporter
from Category.Utils.dimension_config import DimensionMode


class ProductExporter(BaseExporter):
    """
    Exporter para análisis por Producto.
    Hereda toda la lógica de BaseExporter.
    """
    
    def _get_dimension_mode(self) -> int:
        return DimensionMode.PRODUCT