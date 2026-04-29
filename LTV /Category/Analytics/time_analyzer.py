import statistics
import numpy as np  # Usaremos np.nan para claridad
from typing import List, Dict, Any

class CategoryTimeAnalyzer:
    """
    Mide la velocidad de recompra basándose estrictamente en la posición de las filas.
    MODIFICACIÓN: Devuelve None (NaN) en lugar de 0 para muestras vacías, 
    permitiendo un scoring preciso.
    """
    @staticmethod
    def analyze(grouped_data: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        report = []
        
        for cat, customers in grouped_data.items():
            deltas_1a2 = []; deltas_2a3 = []; deltas_3a4 = []; deltas_5plus = []
            excl_1a2 = 0; excl_2a3 = 0; excl_3a4 = 0; excl_5plus = 0
            total_filas_cat = 0 

            for customer in customers:
                raw_items = getattr(customer, 'orders', customer.get_orders_sorted())
                total_filas_cat += len(raw_items)
                
                if len(raw_items) < 2:
                    continue
                
                def get_date(item):
                    return getattr(item, 'order_date', item.get('order_date') if isinstance(item, dict) else None)

                def get_decimal_days(d_final, d_inicial):
                    if not d_final or not d_inicial: return 0
                    return (d_final - d_inicial).total_seconds() / 86400

                min_gap = 0.0007 # Aprox 1 minuto

                # --- Evaluación de Intervalos ---
                # 1 a 2
                d12 = get_decimal_days(get_date(raw_items[1]), get_date(raw_items[0]))
                if d12 > min_gap: deltas_1a2.append(d12)
                else: excl_1a2 += 1
                
                # 2 a 3
                if len(raw_items) >= 3:
                    d23 = get_decimal_days(get_date(raw_items[2]), get_date(raw_items[1]))
                    if d23 > min_gap: deltas_2a3.append(d23)
                    else: excl_2a3 += 1
                
                # 3 a 4
                if len(raw_items) >= 4:
                    d34 = get_decimal_days(get_date(raw_items[3]), get_date(raw_items[2]))
                    if d34 > min_gap: deltas_3a4.append(d34)
                    else: excl_3a4 += 1

                # 5ta o Más
                if len(raw_items) >= 5:
                    for i in range(4, len(raw_items)):
                        d_extra = get_decimal_days(get_date(raw_items[i]), get_date(raw_items[i-1]))
                        if d_extra > min_gap: deltas_5plus.append(d_extra)
                        else: excl_5plus += 1
            
            # --- CÁLCULO DE MEDIANAS LIMPIAS ---
            # Si no hay deltas, usamos None. Pandas lo convertirá a NaN automáticamente.
            report.append({
                "Categoria": cat,
                "Mediana_Dias_1a2": round(statistics.median(deltas_1a2), 2) if deltas_1a2 else None,
                "Muestra_1a2": len(deltas_1a2),
                "Excluidos_1a2": excl_1a2,
                
                "Mediana_Dias_2a3": round(statistics.median(deltas_2a3), 2) if deltas_2a3 else None,
                "Muestra_2a3": len(deltas_2a3),
                "Excluidos_2a3": excl_2a3,
                
                "Mediana_Dias_3a4": round(statistics.median(deltas_3a4), 2) if deltas_3a4 else None,
                "Muestra_3a4": len(deltas_3a4),
                "Excluidos_3a4": excl_3a4,
                
                "Mediana_Dias_5ta_o_Mas": round(statistics.median(deltas_5plus), 2) if deltas_5plus else None,
                "Muestra_5ta_o_Mas": len(deltas_5plus),
                "Excluidos_5ta_o_Mas": excl_5plus,
                
                "Total_Filas_CSV_Auditoria": total_filas_cat 
            })
            
        return sorted(report, key=lambda x: (x["Muestra_1a2"] or 0), reverse=True)