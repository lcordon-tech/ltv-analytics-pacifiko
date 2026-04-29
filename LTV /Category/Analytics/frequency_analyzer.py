from typing import List, Dict, Any

class CategoryFrequencyAnalyzer:
    """
    Analiza la frecuencia de compra por categoría.
    Mantiene hitos fijos (2, 3, 4) y acumula todas las compras excedentes 
    en una sola columna de volumen '5ta o más'.
    """
    @staticmethod
    def analyze(grouped_data: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        report = []
        
        for cat, customers in grouped_data.items():
            if not customers: continue
            
            order_counts = []
            total_filas_cat = 0
            
            # Variables para la columna especial
            abs_5ta_o_mas = 0
            
            for c in customers:
                # 1. Obtención de datos (Lógica de auditoría validada)
                orders_del_cliente = getattr(c, 'orders', c.get_orders_sorted())
                
                # 2. AUDITORÍA: Conteo total de filas
                total_filas_cat += len(orders_del_cliente)
                
                # 3. CONVERSIÓN: Conteo de items/tickets
                n_compras = len(orders_del_cliente)
                order_counts.append(n_compras)
                
                # 4. LÓGICA DE EXCEDENTES (5ta compra en adelante)
                # Si tiene 5 compras, suma 1. Si tiene 6, suma 2, etc.
                if n_compras >= 5:
                    excedente = n_compras - 4
                    abs_5ta_o_mas += excedente

            n_base = len(customers) 
            # Hitos fijos (Clientes únicos que alcanzaron el nivel)
            n_c2 = sum(1 for count in order_counts if count >= 2)
            n_c3 = sum(1 for count in order_counts if count >= 3)
            n_c4 = sum(1 for count in order_counts if count >= 4)
            
            # Porcentajes de retención base
            ret_c2 = (n_c2 / n_base * 100) if n_base > 0 else 0
            ret_c3 = (n_c3 / n_base * 100) if n_base > 0 else 0
            ret_c4 = (n_c4 / n_base * 100) if n_base > 0 else 0
            
            # Saltos de conversión (Pasar de un nivel al siguiente)
            salt_2a3 = (n_c3 / n_c2 * 100) if n_c2 > 0 else 0
            salt_3a4 = (n_c4 / n_c3 * 100) if n_c3 > 0 else 0
            
            report.append({
                "Categoria": cat,
                "Total_Clientes": n_base,
                "Pct_2da_Compra": round(ret_c2, 2),
                "Pct_3ra_Compra": round(ret_c3, 2),
                "Pct_4ta_Compra": round(ret_c4, 2),
                "Abs_2da_Compra": n_c2,
                "Abs_3ra_Compra": n_c3,
                "Abs_4ta_Compra": n_c4,
                "Abs_5ta_o_Mas": abs_5ta_o_mas, # <--- LA NUEVA COLUMNA
                "Salto_2a3_%": round(salt_2a3, 2),
                "Salto_3a4_%": round(salt_3a4, 2),
                "Total_Filas_CSV_Auditoria": total_filas_cat
            })
            
        return sorted(report, key=lambda x: x["Total_Clientes"], reverse=True)