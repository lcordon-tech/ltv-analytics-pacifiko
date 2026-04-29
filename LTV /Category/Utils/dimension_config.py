"""
Configuración central para todas las dimensiones del sistema LTV.
VERSIÓN MEJORADA: Incluye metadata de jerarquía (parent_dimension).
"""

from enum import IntEnum
from typing import Dict, Any


class DimensionMode(IntEnum):
    CATEGORY = 1
    SUBCATEGORY = 2
    BRAND = 3
    PRODUCT = 4
    SUBCATEGORY_BRAND = 5
    SUBCATEGORY_MARCA_DUAL = 6


# Configuración completa para cada dimensión
DIMENSION_CONFIGS: Dict[int, Dict[str, Any]] = {
    DimensionMode.CATEGORY: {
        'mode_id': 1,
        'group_by_attr': 'category',
        'output_key': 'Categoria',
        'main_key': 'Categoria',
        'entry_dim_method': 'get_entry_dimension',
        'sheet_suffix': 'Cat',
        'sheet_suffix_long': 'Categoria',
        'folder_suffix': 'Categorias',
        'excel_filename': 'Analisis_Categorias_LTV',
        'txt_filename': 'Resumen_Estrategico_Categoria',
        'ue_dim_label': 'Categoria',
        'col_name': 'Categoria',
        'col_name_alt': 'category',
        'dashboard_label': 'Categoria',
        'has_brand_column': False,
        'parent_dimension': None,           # ← NUEVO: Sin padre
        'parent_col_name': None,
    },
    
    DimensionMode.SUBCATEGORY: {
        'mode_id': 2,
        'group_by_attr': 'subcategory',
        'output_key': 'Subcategoria',
        'main_key': 'Subcategoria',
        'entry_dim_method': 'get_entry_dimension',
        'sheet_suffix': 'Sub',
        'sheet_suffix_long': 'Subcategoria',
        'folder_suffix': 'Subcategorias',
        'excel_filename': 'Analisis_Subcategorias_LTV',
        'txt_filename': 'Resumen_Estrategico_Subcategory',
        'ue_dim_label': 'Subcat',
        'col_name': 'Subcategoria',
        'col_name_alt': 'subcategory',
        'dashboard_label': 'Subcategoria',
        'has_brand_column': False,
        'parent_dimension': 'category',      # ← NUEVO: El padre es category
        'parent_col_name': 'Categoria_Padre', # ← NUEVO
    },

    DimensionMode.BRAND: {
        'mode_id': 3,
        'group_by_attr': 'brand',
        'output_key': 'Brand',
        'main_key': 'Brand',
        'entry_dim_method': 'get_entry_dimension',
        'sheet_suffix': 'Brand',
        'sheet_suffix_long': 'Brand',
        'folder_suffix': 'Brands',
        'excel_filename': 'Analisis_Brands_LTV',
        'txt_filename': 'Resumen_Estrategico_Brand',
        'ue_dim_label': 'Brand',
        'col_name': 'Brand',
        'col_name_alt': 'brand',
        'dashboard_label': 'Brand',
        'has_brand_column': False,
        'parent_dimension': None,            # ← NUEVO: Sin padre
        'parent_col_name': None,
    },
    
    DimensionMode.PRODUCT: {
        'mode_id': 4,
        'group_by_attr': 'name',
        'output_key': 'Producto',
        'main_key': 'Producto',
        'entry_dim_method': 'get_entry_dimension',
        'sheet_suffix': 'Prod',
        'sheet_suffix_long': 'Producto',
        'folder_suffix': 'Productos',
        'excel_filename': 'Analisis_Productos_LTV',
        'txt_filename': 'Resumen_Estrategico_Product',
        'ue_dim_label': 'Product',
        'col_name': 'Producto',
        'col_name_alt': 'name',
        'dashboard_label': 'Producto',
        'has_brand_column': False,
        'parent_dimension': 'subcategory',   # ← NUEVO: El padre es subcategory
        'parent_col_name': 'Subcategoria_Padre', # ← NUEVO
    },

    DimensionMode.SUBCATEGORY_BRAND: {
        'mode_id': 5,
        'group_by_attr': 'subcategory_brand',
        'output_key': 'Subcategoria_Marca',
        'main_key': 'Subcategoria_Marca',
        'entry_dim_method': 'get_entry_dimension',
        'sheet_suffix': 'SubBrand',
        'sheet_suffix_long': 'Subcategoria_Marca',
        'folder_suffix': 'Subcategoria_Marca',
        'excel_filename': 'Analisis_Subcategoria_Marca_LTV',
        'txt_filename': 'Resumen_Estrategico_Subcategoria_Marca',
        'ue_dim_label': 'Subcategoria_Marca',
        'col_name': 'Subcategoria_Marca',
        'col_name_alt': 'subcategory_brand',
        'dashboard_label': 'Subcategoría + Marca',
        'has_brand_column': False,
        'parent_dimension': 'subcategory',   # ← NUEVO: El padre es subcategory
        'parent_col_name': 'Subcategoria_Padre', # ← NUEVO
    },

    DimensionMode.SUBCATEGORY_MARCA_DUAL: {
        'mode_id': 6,
        'group_by_attr': 'subcategory',
        'output_key': 'Subcategoria_Marca_Dual',
        'main_key': 'Subcategoria',
        'entry_dim_method': 'get_entry_dimension',
        'sheet_suffix': 'SubMarca',
        'sheet_suffix_long': 'Subcategoria_Marca_Dual',
        'folder_suffix': 'Subcategoria_Marca_Dual',
        'excel_filename': 'Analisis_Subcategoria_Marca_Dual_LTV',
        'txt_filename': 'Resumen_Estrategico_Subcategoria_Marca_Dual',
        'ue_dim_label': 'Subcategoria',
        'col_name': 'Subcategoria',
        'col_name_alt': 'subcategory',
        'dashboard_label': 'Subcategoría + Marca (Dual)',
        'has_brand_column': True,
        'brand_column': 'Marca',
        'parent_dimension': 'category',      # ← NUEVO: El padre es category
        'parent_col_name': 'Categoria_Padre', # ← NUEVO
    },
}


def get_dimension_config(mode: int) -> Dict[str, Any]:
    """Retorna la configuración para un modo de dimensión."""
    if mode not in DIMENSION_CONFIGS:
        raise ValueError(f"Modo de dimensión {mode} no soportado. "
                         f"Opciones: {list(DIMENSION_CONFIGS.keys())}")
    return DIMENSION_CONFIGS[mode].copy()


def get_all_dimension_modes() -> list:
    """Retorna lista de todos los modos de dimensión soportados."""
    return list(DIMENSION_CONFIGS.keys())


def get_dimension_name(mode: int) -> str:
    """Retorna el nombre legible de la dimensión."""
    config = get_dimension_config(mode)
    return config.get('output_key', f'Dimension_{mode}')
