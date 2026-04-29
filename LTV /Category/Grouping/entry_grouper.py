# Category/Grouping/entry_grouper.py

from collections import defaultdict
from typing import List, Dict, Any, Tuple

class EntryBasedBehaviorGrouper:
    """
    SINGLE-ANCHOR MODEL: Agrupa toda la actividad del cliente 
    bajo su categoría/subcategoría/marca/producto de entrada.
    """
    @staticmethod
    def group(customers: List[Any], mode: int = 1) -> Tuple[Dict[str, List[Any]], Dict[str, Any]]:
        """
        SINGLE-ANCHOR MODEL: Agrupa toda la actividad del cliente 
        bajo su categoría/subcategoría/marca/producto de entrada.
        
        mode: 1 para Categoria, 2 para Subcategoria, 3 para Brand, 4 para Producto, 
            5 para Subcategoria+Marca (formato: "Subcategoria (Marca)")
        """
        group_map = defaultdict(list)
        stats = {
            "total_customers": len(customers),
            "customers_without_entry": 0,
            "attribution_model": "entry_based"
        }

        # Mapeo de mode a nombre de dimensión para debug
        mode_names = {
            1: "category", 
            2: "subcategory", 
            3: "brand", 
            4: "product",
            5: "subcategory_brand (formato: Subcategoria (Marca))"
        }
        print(f"   📌 EntryBasedBehaviorGrouper: mode={mode} ({mode_names.get(mode, 'unknown')})")

        for customer in customers:
            # Para modo 5 (subcategory_brand) - NUEVO FORMATO
            if mode == 5:
                orders = customer.get_orders_sorted()
                if not orders:
                    stats["customers_without_entry"] += 1
                    continue
                
                first_order = orders[0]
                subcat = getattr(first_order, 'subcategory', 'N/A')
                brand = getattr(first_order, 'brand', 'N/A')
                
                # Limpiar valores
                subcat_clean = str(subcat).strip() if subcat and str(subcat).lower() not in ["nan", "none", "n/a", "", "null"] else ""
                brand_clean = str(brand).strip() if brand and str(brand).lower() not in ["nan", "none", "n/a", "", "null"] else ""
                
                # NUEVO FORMATO: "Subcategoría (Marca)"
                if subcat_clean and brand_clean:
                    entry_dim = f"{subcat_clean} ({brand_clean})"
                elif brand_clean:
                    entry_dim = brand_clean
                elif subcat_clean:
                    entry_dim = subcat_clean
                else:
                    entry_dim = "N/A"
            else:
                # Modos 1-4: usar get_entry_dimension
                entry_dim = customer.get_entry_dimension(mode=mode)
            
            if not entry_dim or str(entry_dim).strip() in ["", "N/A", "nan", "None"]:
                stats["customers_without_entry"] += 1
                continue

            # IMPORTANTE: NO clonamos ni filtramos órdenes.
            # Pasamos el objeto customer completo.
            group_map[entry_dim].append(customer)

        print(f"   📊 Grupos generados: {len(group_map)}")
        return dict(group_map), stats