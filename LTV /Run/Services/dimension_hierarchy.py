"""
Servicio de jerarquía progresiva para filtros.
Categoría → Subcategoría → Marca → Producto

VERSIÓN 1.0: Índices optimizados para búsqueda rápida.
"""

from typing import List, Dict, Optional, Any
from collections import defaultdict


class DimensionHierarchy:
    """
    Gestiona jerarquía progresiva de dimensiones.
    
    Uso:
        hierarchy = DimensionHierarchy(customers)
        
        # Obtener subcategorías de una categoría
        subcats = hierarchy.get_subcategories("Electrónica")
        
        # Obtener marcas de una categoría y subcategoría
        brands = hierarchy.get_brands(category="Electrónica", subcategory="Laptops")
        
        # Obtener productos con filtros progresivos
        products = hierarchy.get_products(category="Electrónica", brand="Dell")
    """
    
    def __init__(self, customers: List[Any]):
        """
        Args:
            customers: Lista de objetos Customer con órdenes
        """
        self.customers = customers
        self._cache = {}
        self._build_indexes()
    
    def _build_indexes(self):
        """Construye índices para búsqueda rápida."""
        # Índices
        self._categories = set()
        self._cat_to_subs = defaultdict(set)
        self._cat_sub_to_brands = defaultdict(set)
        self._cat_sub_brand_to_products = defaultdict(set)
        self._brands = set()
        self._products = set()
        
        for customer in self.customers:
            for order in customer.get_orders_sorted():
                cat = self._clean_value(getattr(order, 'category', None))
                sub = self._clean_value(getattr(order, 'subcategory', None))
                brand = self._clean_value(getattr(order, 'brand', None))
                product = self._clean_value(getattr(order, 'name', None))
                
                if cat:
                    self._categories.add(cat)
                    
                    if sub:
                        self._cat_to_subs[cat].add(sub)
                        
                        if brand:
                            key = f"{cat}|{sub}"
                            self._cat_sub_to_brands[key].add(brand)
                            self._brands.add(brand)
                            
                            if product:
                                key2 = f"{cat}|{sub}|{brand}"
                                self._cat_sub_brand_to_products[key2].add(product)
                                self._products.add(product)
    
    def _clean_value(self, value: Any) -> Optional[str]:
        """Limpia y normaliza un valor."""
        if value is None:
            return None
        val_str = str(value).strip()
        if val_str.lower() in ['', 'nan', 'none', 'n/a', 'null']:
            return None
        return val_str
    
    # ========== MÉTODOS DE CONSULTA ==========
    
    def get_all_categories(self) -> List[str]:
        """Retorna todas las categorías disponibles."""
        return sorted(self._categories)
    
    def get_subcategories(self, category: str) -> List[str]:
        """
        Retorna subcategorías de una categoría específica.
        
        Args:
            category: Nombre de la categoría
        
        Returns:
            Lista de subcategorías ordenadas
        """
        cache_key = f"subcats_{category}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        result = sorted(self._cat_to_subs.get(category, set()))
        self._cache[cache_key] = result
        return result
    
    def get_brands(self, category: str = None, subcategory: str = None) -> List[str]:
        """
        Retorna marcas filtradas por categoría y/o subcategoría.
        
        Args:
            category: Categoría (opcional)
            subcategory: Subcategoría (opcional)
        
        Returns:
            Lista de marcas ordenadas
        """
        cache_key = f"brands_{category}_{subcategory}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        brands = set()
        
        if category and subcategory:
            key = f"{category}|{subcategory}"
            brands = self._cat_sub_to_brands.get(key, set())
        elif category:
            # Todas las marcas de todas las subcategorías de esta categoría
            for sub in self._cat_to_subs.get(category, set()):
                key = f"{category}|{sub}"
                brands.update(self._cat_sub_to_brands.get(key, set()))
        elif subcategory:
            # Todas las categorías que tienen esta subcategoría
            for cat, subs in self._cat_to_subs.items():
                if subcategory in subs:
                    key = f"{cat}|{subcategory}"
                    brands.update(self._cat_sub_to_brands.get(key, set()))
        else:
            # Todas las marcas
            brands = self._brands
        
        result = sorted(brands)
        self._cache[cache_key] = result
        return result
    
    def get_products(self, category: str = None, subcategory: str = None, 
                     brand: str = None) -> List[str]:
        """
        Retorna productos filtrados por jerarquía completa.
        
        Args:
            category: Categoría (opcional)
            subcategory: Subcategoría (opcional)
            brand: Marca (opcional)
        
        Returns:
            Lista de productos ordenados
        """
        cache_key = f"products_{category}_{subcategory}_{brand}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        products = set()
        
        # Caso: categoría + subcategoría + marca
        if category and subcategory and brand:
            key = f"{category}|{subcategory}|{brand}"
            products = self._cat_sub_brand_to_products.get(key, set())
        
        # Caso: categoría + marca (sin subcategoría específica)
        elif category and brand:
            for sub in self._cat_to_subs.get(category, set()):
                key = f"{category}|{sub}|{brand}"
                products.update(self._cat_sub_brand_to_products.get(key, set()))
        
        # Caso: categoría + subcategoría (sin marca)
        elif category and subcategory:
            for br in self.get_brands(category, subcategory):
                key = f"{category}|{subcategory}|{br}"
                products.update(self._cat_sub_brand_to_products.get(key, set()))
        
        # Caso: solo marca (en todas las categorías)
        elif brand:
            for cat, subs in self._cat_to_subs.items():
                for sub in subs:
                    key = f"{cat}|{sub}|{brand}"
                    products.update(self._cat_sub_brand_to_products.get(key, set()))
        
        # Caso: solo categoría
        elif category:
            for sub in self._cat_to_subs.get(category, set()):
                for br in self.get_brands(category, sub):
                    key = f"{category}|{sub}|{br}"
                    products.update(self._cat_sub_brand_to_products.get(key, set()))
        
        # Caso: solo subcategoría (buscar en todas las categorías)
        elif subcategory:
            for cat, subs in self._cat_to_subs.items():
                if subcategory in subs:
                    for br in self.get_brands(cat, subcategory):
                        key = f"{cat}|{subcategory}|{br}"
                        products.update(self._cat_sub_brand_to_products.get(key, set()))
        
        result = sorted(products)
        self._cache[cache_key] = result
        return result
    
    def get_brand_count(self, category: str = None, subcategory: str = None) -> int:
        """Retorna el número de marcas que cumplen los filtros."""
        return len(self.get_brands(category, subcategory))
    
    def get_product_count(self, category: str = None, subcategory: str = None,
                          brand: str = None) -> int:
        """Retorna el número de productos que cumplen los filtros."""
        return len(self.get_products(category, subcategory, brand))
    
    def validate_hierarchy(self, category: str, subcategory: str = None, 
                           brand: str = None) -> bool:
        """
        Valida si una combinación jerárquica es válida.
        
        Returns:
            True si la combinación existe en los datos
        """
        if category not in self._categories:
            return False
        
        if subcategory:
            if subcategory not in self._cat_to_subs.get(category, set()):
                return False
        
        if brand:
            if subcategory:
                key = f"{category}|{subcategory}"
                if brand not in self._cat_sub_to_brands.get(key, set()):
                    return False
            else:
                # Verificar si la marca existe en alguna subcategoría de esta categoría
                for sub in self._cat_to_subs.get(category, set()):
                    key = f"{category}|{sub}"
                    if brand in self._cat_sub_to_brands.get(key, set()):
                        return True
                return False
        
        return True
    
    def print_summary(self):
        """Imprime resumen de la jerarquía."""
        print("\n" + "=" * 50)
        print("   JERARQUÍA DE DIMENSIONES".center(50))
        print("=" * 50)
        print(f"📁 Categorías: {len(self._categories)}")
        print(f"📁 Subcategorías: {sum(len(v) for v in self._cat_to_subs.values())}")
        print(f"🏷️ Marcas: {len(self._brands)}")
        print(f"🎯 Productos: {len(self._products)}")
        print("-" * 50)
        
        # Mostrar primeras categorías
        if self._categories:
            print("\n📋 Categorías disponibles:")
            for cat in sorted(self._categories)[:10]:
                sub_count = len(self._cat_to_subs.get(cat, set()))
                print(f"   • {cat} ({sub_count} subcategorías)")
            if len(self._categories) > 10:
                print(f"   ... y {len(self._categories) - 10} más")