import copy
from collections import defaultdict
from typing import List, Dict, Any, Tuple

class CategoryGrouper:
    @staticmethod
    def group(customers: List[Any], group_by: str = "category") -> Tuple[Dict[str, List[Any]], Dict[str, int]]:
        """
        Agrupador Universal: Funciona para 'category', 'subcategory', 'brand', 'name', 'subcategory_brand'.
        group_by: El nombre del atributo a extraer de la orden.
        
        Para 'subcategory_brand', el formato es: "Subcategoría (Marca)"
        """
        group_map = defaultdict(list)
        
        stats = {
            "filas_leidas_brutas": 0,
            "filas_sin_categoria": 0,
            "filas_duplicadas_bloqueadas": 0,
            "filas_procesadas_ok": 0
        }
        
        for customer in customers:
            # Diccionario temporal para este cliente (agrupado por el nivel elegido)
            purchases_by_level = defaultdict(list)
            seen_rows = set() 

            all_orders = customer.get_orders_sorted()
            
            for order in all_orders:
                stats["filas_leidas_brutas"] += 1
                
                # 1. Extraer el valor dinámicamente según group_by
                if group_by == "subcategory_brand":
                    # NUEVO FORMATO: "Subcategoría (Marca)"
                    subcat = getattr(order, 'subcategory', None)
                    brand = getattr(order, 'brand', None)
                    
                    subcat_clean = str(subcat).strip() if subcat and str(subcat).lower() not in ["nan", "none", "n/a", "null", "", "undefined"] else ""
                    brand_clean = str(brand).strip() if brand and str(brand).lower() not in ["nan", "none", "n/a", "null", "", "undefined"] else ""
                    
                    if subcat_clean and brand_clean:
                        val = f"{subcat_clean} ({brand_clean})"
                    elif brand_clean:
                        val = brand_clean
                    elif subcat_clean:
                        val = subcat_clean
                    else:
                        val = ""
                else:
                    # Modos normales: category, subcategory, brand, name
                    raw_val = getattr(order, group_by, None)
                    val = str(raw_val).strip() if raw_val else ""
                
                if not val or val.lower() in ["nan", "none", "n/a", "null", "undefined"]:
                    stats["filas_sin_categoria"] += 1
                    continue

                # 2. Control de Duplicados Reforzado
                pid = getattr(order, 'prod_pid', 'no_pid')
                oid = getattr(order, 'order_id', 'no_oid')
                rev = getattr(order, 'revenue', 0.0)
                
                # El UID incluye el valor del nivel para evitar colisiones
                uid = f"{oid}_{pid}_{val}_{rev}"
                
                if uid in seen_rows:
                    stats["filas_duplicadas_bloqueadas"] += 1
                    continue
                
                # 3. Éxito: La fila es válida para este grupo
                purchases_by_level[val].append(order)
                seen_rows.add(uid)
                stats["filas_procesadas_ok"] += 1

            # 4. Inyección en clones (Virtualización de Clientes)
            for val_key, items in purchases_by_level.items():
                customer_view = copy.copy(customer)
                customer_view._orders = items
                customer_view._timeline_cache = None
                
                group_map[val_key].append(customer_view)
                
        return dict(group_map), stats