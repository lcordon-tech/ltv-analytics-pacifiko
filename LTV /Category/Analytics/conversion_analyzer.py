from typing import List, Dict, Any
import numpy as np

class CategoryConversionAnalyzer:
    """
    Analiza la conversión acumulada e incremental a la 2da, 3ra y 4ta compra.
    Mantiene paridad con la lógica de posición de fila de FrequencyAnalyzer.
    """
    @staticmethod
    def analyze(grouped_data: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        report = []
        windows = [30, 60, 90, 180, 360]
        
        for cat, customers in grouped_data.items():
            if not customers: continue
            
            total_n = len(customers)
            total_filas_cat = 0
            
            # --- ESTRUCTURAS DE DATOS PARA CONTEO ---
            # lvl 2: 2da compra, lvl 3: 3ra, lvl 4: 4ta
            # counts[nivel][ventana]
            counts = {lvl: {w: 0 for w in windows} for lvl in [2, 3, 4]}
            counts_mas_360 = {2: 0, 3: 0, 4: 0}
            
            # Bases para tasas de conversión (Denominadores)
            llegaron_a_2da = 0
            llegaron_a_3ra = 0

            for c in customers:
                orders = getattr(c, 'orders', c.get_orders_sorted())
                n_orders = len(orders)
                total_filas_cat += n_orders
                
                if n_orders < 2: continue

                # Helper para extraer fechas
                def get_d(o):
                    return getattr(o, 'order_date', o.get('order_date') if isinstance(o, dict) else None)

                # --- Lógica 2da Compra (Nivel 2) ---
                d1, d2 = get_d(orders[0]), get_d(orders[1])
                if d1 and d2:
                    llegaron_a_2da += 1
                    diff = (d2 - d1).days
                    for w in windows:
                        if diff <= w: counts[2][w] += 1
                    if diff > 360: counts_mas_360[2] += 1

                # --- Lógica 3ra Compra (Nivel 3) ---
                if n_orders >= 3:
                    d2_3, d3 = get_d(orders[1]), get_d(orders[2])
                    if d2_3 and d3:
                        llegaron_a_3ra += 1
                        diff = (d3 - d2_3).days
                        for w in windows:
                            if diff <= w: counts[3][w] += 1
                        if diff > 360: counts_mas_360[3] += 1

                # --- Lógica 4ta Compra (Nivel 4) ---
                if n_orders >= 4:
                    d3_4, d4 = get_d(orders[2]), get_d(orders[3])
                    if d3_4 and d4:
                        diff = (d4 - d3_4).days
                        for w in windows:
                            if diff <= w: counts[4][w] += 1
                        if diff > 360: counts_mas_360[4] += 1

            # --- CONSTRUCCIÓN DEL DICCIONARIO DE RESULTADOS ---
            res = {
                "Categoria": cat,
                "Total_Clientes": total_n,
                "Total_Filas_CSV_Auditoria": total_filas_cat
            }

            # Configuración de mapeo para iterar niveles
            # nivel: (prefijo_columna, base_para_porcentaje)
            config = {
                2: ("Conv", total_n),       # 2da compra vs base total
                3: ("3ra", llegaron_a_2da), # 3ra compra vs los que compraron 2 veces
                4: ("4ta", llegaron_a_3ra)  # 4ta compra vs los que compraron 3 veces
            }

            for lvl, (prefix, base) in config.items():
                prev_w_val = 0
                for w in windows:
                    curr_val = counts[lvl][w]
                    # Nombres de llaves (Mantiene "Clientes_30d" para 2da compra por compatibilidad)
                    base_key = f"{w}d" if lvl == 2 else f"{prefix}_{w}d"
                    pct_key = f"Pct_Conv_{w}d" if lvl == 2 else f"Pct_{prefix}_{w}d"
                    
                    # 1. ACUMULADO
                    res[f"Clientes_{base_key}"] = curr_val
                    res[pct_key] = round((curr_val / base) * 100, 2) if base > 0 else 0
                    
                    # 2. INCREMENTAL
                    inc_val = curr_val - prev_w_val
                    res[f"Clientes_{base_key}_inc"] = inc_val
                    res[f"{pct_key}_inc"] = round((inc_val / base) * 100, 2) if base > 0 else 0
                    
                    prev_w_val = curr_val
                
                # Auditoría Mas_360
                m360_key = "Clientes_Mas_360d" if lvl == 2 else f"Clientes_{prefix}_Mas_360d"
                res[m360_key] = counts_mas_360[lvl]

            report.append(res)
            
        return sorted(report, key=lambda x: x["Total_Clientes"], reverse=True)