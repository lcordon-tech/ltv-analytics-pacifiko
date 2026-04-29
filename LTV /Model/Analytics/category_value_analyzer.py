from typing import List, Dict, Any
from collections import defaultdict
from Model.Domain.customer import Customer   # ← ajustado

class CategoryValueAnalyzer:
    """
    Analiza la correlación entre las dimensiones de entrada (Category/Subcategory) 
    y el valor del cliente (LTV).
    """

    def __init__(self, customers: List[Customer], mode: int = 2):
        self.customers = customers
        self.mode = mode

    def ltv_by_entry_category(self) -> Dict[str, Dict[str, Any]]:
        """
        Agrupa clientes por la dimensión de entrada (según mode).
        Retorna: LTV promedio, Count de clientes y Revenue promedio por dimensión inicial.
        """
        analysis = defaultdict(lambda: {"total_ltv": 0.0, "count": 0, "total_rev": 0.0})

        for customer in self.customers:
            # CAMBIO: Usar get_entry_dimension en lugar de first_category directo
            entry_dim = customer.get_entry_dimension(self.mode)
            
            analysis[entry_dim]["total_ltv"] += customer.final_ltv()
            analysis[entry_dim]["total_rev"] += customer.total_revenue()
            analysis[entry_dim]["count"] += 1

        # Formatear resultados finales
        report = {}
        for dim, data in analysis.items():
            report[dim] = {
                "avg_ltv": round(data["total_ltv"] / data["count"], 2),
                "avg_rev": round(data["total_rev"] / data["count"], 2),
                "sample_size": data["count"]
            }
        
        return dict(sorted(report.items(), key=lambda x: x[1]['avg_ltv'], reverse=True))

    def ltv_by_category_count(self) -> Dict[int, Dict[str, Any]]:
        """
        Agrupa clientes por el número de CATEGORÍAS distintas compradas.
        NO SE CAMBIA: La diversificación se mide a nivel categoría superior siempre.
        """
        analysis = defaultdict(lambda: {"total_ltv": 0.0, "count": 0})

        for customer in self.customers:
            num_cats = len(customer.get_categories())
            analysis[num_cats]["total_ltv"] += customer.final_ltv()
            analysis[num_cats]["count"] += 1

        report = {}
        for count, data in analysis.items():
            report[count] = {
                "avg_ltv": round(data["total_ltv"] / data["count"], 2),
                "sample_size": data["count"]
            }
            
        return dict(sorted(report.items()))

    def print_category_strategic_report(self):
        """Imprime el análisis con etiquetas dinámicas según el modo."""
        entry_data = self.ltv_by_entry_category()
        count_data = self.ltv_by_category_count()
        
        # Etiqueta dinámica para los títulos
        dim_label = "CATEGORÍA" if self.mode == 1 else "SUBCATEGORÍA"

        print("\n" + f" ANÁLISIS ESTRATÉGICO DE {dim_label} (LTV) ".center(70, "="))
        
        print(f"\n1. LTV POR {dim_label} DE ENTRADA (ENTRY POINT)")
        print(f"{dim_label:<20} | {'CLIENTES':<10} | {'AVG LTV':<12} | {'AVG REV'}")
        print("-" * 70)
        for dim, metrics in entry_data.items():
            print(f"{dim:<20} | {metrics['sample_size']:<10} | ${metrics['avg_ltv']:>10.2f} | ${metrics['avg_rev']:>8.2f}")

        print(f"\n2. IMPACTO DE LA MULTI-CATEGORÍA (CROSS-SELL)")
        print(f"{'Nº CATEGORÍAS':<20} | {'CLIENTES':<10} | {'AVG LTV':<12}")
        print("-" * 70)
        for num, metrics in count_data.items():
            label = f"{num} Categoría(s)"
            print(f"{label:<20} | {metrics['sample_size']:<10} | ${metrics['avg_ltv']:>10.2f}")
        
        print("=" * 70)
