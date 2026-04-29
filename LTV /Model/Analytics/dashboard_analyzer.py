from typing import List, Dict, Any
from collections import Counter
from Model.Domain.customer import Customer
from Model.Utils.cohort_utils import CohortUtils


class DashboardAnalyzer:

    def __init__(self, customers: List[Customer], unit_econ_results: Dict, 
                 cohort_data: Dict = None, mode=2, granularity: str = "quarterly"):
        self.customers = customers
        self.ue_results = unit_econ_results
        self.cohort_data = cohort_data
        self.mode = mode
        self.granularity = granularity

    def _print_separator(self, char="=", length=110):
        print(char * length)

    def _get_dimension_label(self) -> str:
        labels = {1: "CATEGORÍA", 2: "SUBCATEGORÍA", 3: "BRAND", 4: "PRODUCTO"}
        return labels.get(self.mode, "DIMENSIÓN")

    def validate_consistency(self):
        """
        Validación cruzada de métricas clave.
        Detecta inconsistencias entre diferentes fuentes de datos.
        """
        if not self.customers or not self.cohort_data:
            return
        
        total_cp = sum(c.total_cp() for c in self.customers)
        total_cohort_ltv = sum(data.get("total_ltv", 0) for data in self.cohort_data.values())
        total_acq_spend = sum(res.get("acq_spend", 0) for res in self.ue_results.values())
        
        print("\n" + " VALIDACIÓN DE CONSISTENCIA ".center(70, "-"))
        print(f"Total CP (Customers):    ${total_cp:,.2f}")
        print(f"Total LTV (Cohorts):     ${total_cohort_ltv:,.2f}")
        
        diff = abs(total_cp - total_cohort_ltv)
        print(f"Diferencia:              ${diff:,.2f}")
        
        if diff > 1.0:
            pct_diff = (diff / max(total_cp, total_cohort_ltv)) * 100
            print(f"⚠️ ADVERTENCIA: Inconsistencia del {pct_diff:.2f}% entre fuentes")
        else:
            print("✅ Datos consistentes entre Customers y Cohorts")
        
        if total_acq_spend > 0:
            roi = (total_cp - total_acq_spend) / total_acq_spend * 100
            print(f"ROI de Adquisición:      {roi:.1f}%")
            status_roi = "🟢 EXCELENTE" if roi > 200 else "🟡 NORMAL" if roi > 100 else "🔴 CRÍTICO"
            print(f"Estado ROI:              {status_roi}")
        
        print("-" * 70)

    def print_global_summary(self):
        """Muestra los KPIs macro ajustados al CAC de Adquisición."""
        if not self.customers:
            print("⚠️ No hay clientes para analizar en el Dashboard.")
            return

        total_rev = sum(c.total_revenue() for c in self.customers)
        total_ltv = sum(c.total_cp() for c in self.customers)
        total_orders = sum(c.total_orders() for c in self.customers)
        n_customers = len(self.customers)

        total_acq_spend = sum(res.get("acq_spend", 0) for res in self.ue_results.values())
        total_ret_spend = sum(res.get("retention_spend_total", 0) for res in self.ue_results.values())
        total_marketing_global = total_acq_spend + total_ret_spend

        avg_cac = total_acq_spend / n_customers if n_customers > 0 else 0
        avg_ticket = total_rev / total_orders if total_orders > 0 else 0
        
        avg_ltv_per_customer = total_ltv / n_customers if n_customers > 0 else 0
        avg_ltv_per_order = total_ltv / total_orders if total_orders > 0 else 0
        
        avg_cac_per_order = total_acq_spend / total_orders if total_orders > 0 else 0

        ltv_cac_customer = avg_ltv_per_customer / avg_cac if avg_cac > 0 else 0
        ltv_cac_order = avg_ltv_per_order / avg_cac_per_order if avg_cac_per_order > 0 else 0
        final_ratio = total_ltv / total_acq_spend if total_acq_spend > 0 else 0

        self._print_separator()
        print(" RESUMEN EJECUTIVO GLOBAL (UNIT ECONOMICS & LTV) ".center(110))
        self._print_separator()

        print(f"Total Clientes:      {n_customers:<10} | Total Órdenes:       {total_orders}")
        print(f"Revenue Bruto (GMV): ${total_rev:,.2f} | Ticket Promedio:     ${avg_ticket:,.2f}")
        print(f"LTV Neto Total:      ${total_ltv:,.2f} | CAC Promedio Real:    ${avg_cac:,.2f}")

        print("-" * 110)
        print(" UNIT ECONOMICS DE ADQUISICIÓN (PROMEDIOS) ".center(110))
        print(f"LTV Promedio / Cliente:   ${avg_ltv_per_customer:,.2f} | CAC por Cliente:     ${avg_cac:,.2f}")
        print(f"LTV Promedio / Orden:     ${avg_ltv_per_order:,.2f} | CAC por Orden:       ${avg_cac_per_order:,.2f}")
        
        print("-" * 110)
        print(" RATIOS DE EFICIENCIA (LTV/CAC) ".center(110))
        print(f"Ratio por Cliente:        {ltv_cac_customer:.2f}x")
        print(f"Ratio por Orden:          {ltv_cac_order:.2f}x")
        print(f"RATIO GLOBAL (LTV/CAC):   {final_ratio:.2f}x")
        
        status = "🟢 SALUDABLE" if final_ratio >= 3 else "🟡 REVISAR" if final_ratio >= 1 else "🔴 CRÍTICO"
        print(f"ESTADO ADQUISICIÓN:       {status}")

        print("-" * 110)
        print(" DESGLOSE DE INVERSIÓN EN MARKETING (LINEAL) ".center(110))
        
        pct_acq = (total_acq_spend / total_marketing_global * 100) if total_marketing_global > 0 else 0
        pct_ret = (total_ret_spend / total_marketing_global * 100) if total_marketing_global > 0 else 0
        
        print(f"Inversión en Adquisición (Nuevos):   ${total_acq_spend:,.2f} ({pct_acq:.1f}%)")
        print(f"Inversión en Retención (Recurrentes): ${total_ret_spend:,.2f} ({pct_ret:.1f}%)")
        print(f"GASTO MARKETING TOTAL:               ${total_marketing_global:,.2f}")
        
        ret_efficiency = total_ltv / total_ret_spend if total_ret_spend > 0 else 0
        print(f"Eficiencia de Retención (LTV/Ret):   {ret_efficiency:.2f}x")

        self._print_separator()
        
        # Ejecutar validación de consistencia
        if self.cohort_data:
            self.validate_consistency()

    def print_business_unit_performance(self):
        """Analiza qué unidad de negocio atrae clientes con mayor LTV."""
        if not self.customers:
            return
        
        bu_data = {}
        for c in self.customers:
            unique_orders = c.get_unique_orders()
            if not unique_orders:
                continue
            first_order = unique_orders[0]
            bu = getattr(first_order, 'business_unit', 'N/A')
            
            if bu not in bu_data:
                bu_data[bu] = []
            bu_data[bu].append(c.total_cp())

        print("\n" + " RENDIMIENTO POR BUSINESS UNIT (ADQUISICIÓN) ".center(110, " "))
        print(f"{'BUSINESS UNIT':<25} | {'CLIENTES':<10} | {'LTV PROMEDIO':<15} | {'CONTRIBUCIÓN LTV TOTAL'}")
        print("-" * 85)

        sorted_bus = sorted(bu_data.keys(), key=lambda x: sum(bu_data[x])/len(bu_data[x]), reverse=True)

        for bu in sorted_bus:
            ltvs = bu_data[bu]
            avg_ltv = sum(ltvs) / len(ltvs)
            total_bu_ltv = sum(ltvs)
            print(f"{bu:<25} | {len(ltvs):<10} | ${avg_ltv:>13,.2f} | ${total_bu_ltv:>15,.2f}")
        self._print_separator("-")

    def print_customer_segments(self) -> Dict[str, List[Customer]]:
        """Divide la base en 6 percentiles de valor real."""
        if not self.customers:
            return {}

        sorted_customers = sorted(self.customers, key=lambda c: c.total_cp())
        n = len(sorted_customers)
        
        cut_points = [
            ("TOP 5% (P95-100)", 0.95, 1.00),
            ("HIGH (P90-P95)",   0.90, 0.95),
            ("UPPER (P75-P90)",  0.75, 0.90),
            ("MID (P50-P75)",    0.50, 0.75),
            ("LOW-MID (P25-50)", 0.25, 0.50),
            ("BOTTOM (P0-P25)",  0.00, 0.25)
        ]

        segments = {}
        for name, start, end in cut_points:
            idx_start = int(n * start)
            idx_end = int(n * end)
            if end == 1.00:
                segments[name] = sorted_customers[idx_start:]
            else:
                segments[name] = sorted_customers[idx_start:idx_end]

        print("\n" + " SEGMENTACIÓN DETALLADA POR PERCENTILES DE VALOR (LTV) ".center(110, " "))
        header = f"{'SEGMENTO':<18} | {'CLIENTES':<10} | {'REVENUE TOTAL':<15} | {'LTV TOTAL':<15} | {'% DEL LTV'}"
        print(header)
        print("-" * len(header))

        total_ltv_global = sum(c.total_cp() for c in self.customers)

        for name, _, _ in cut_points:
            custs = segments[name]
            if not custs:
                continue
            s_rev = sum(c.total_revenue() for c in custs)
            s_ltv = sum(c.total_cp() for c in custs)
            p_ltv = (s_ltv / total_ltv_global * 100) if total_ltv_global != 0 else 0
            
            print(f"{name:<18} | {len(custs):<10} | ${s_rev:>13,.2f} | ${s_ltv:>13,.2f} | {p_ltv:>7.1f}%")
        
        self._print_separator("-")
        return segments

    def print_segment_deep_dive(self, segments: Dict[str, List[Customer]]):
        """Analiza la recurrencia y categorías clave por los 6 segmentos de valor."""
        print("\n" + " DEEP DIVE: COMPORTAMIENTO POR SEGMENTO ".center(110, " "))
        header = f"{'SEGMENTO':<18} | {'ORD/CL':<6} | {'REV AVG':<10} | {'LTV AVG':<12} | {'TOP CATEGORIES (Penetración)'}"
        print(header)
        print("-" * 110)

        for name in segments.keys():
            group = segments[name]
            if not group:
                continue

            n = len(group)
            avg_orders = sum(c.total_orders() for c in group) / n
            avg_rev = sum(c.total_revenue() for c in group) / n
            avg_ltv = sum(c.total_cp() for c in group) / n
            
            all_categories = []
            for c in group:
                all_categories.extend(list(c.get_categories()))
            
            cat_counts = Counter(all_categories)
            top_3 = cat_counts.most_common(3)
            cat_str = ", ".join([f"{k} ({int(v/n*100)}%)" for k, v in top_3])

            print(f"{name:<18} | {avg_orders:<6.1f} | ${avg_rev:>9.2f} | ${avg_ltv:>11.2f} | {cat_str}")
        self._print_separator("=")

    def print_dimension_performance(self):
        """Analiza el rendimiento (LTV promedio) según la dimensión configurada."""
        dim_data = {}
        for c in self.customers:
            dim = c.get_entry_dimension(self.mode)
            if dim not in dim_data:
                dim_data[dim] = []
            dim_data[dim].append(c.total_cp())
        
        if not dim_data:
            print(f"⚠️ No hay datos para la dimensión {self._get_dimension_label()}")
            return

        dim_label = self._get_dimension_label()
        
        print("\n" + f" RENDIMIENTO POR {dim_label} DE ADQUISICIÓN ".center(110, " "))
        print(f"{dim_label:<30} | {'CLIENTES':<10} | {'LTV PROMEDIO':<15} | {'VALOR RELATIVO'}")
        print("-" * 85)

        sorted_dims = sorted(dim_data.keys(), key=lambda x: sum(dim_data[x])/len(dim_data[x]), reverse=True)
        global_avg_ltv = sum(c.total_cp() for c in self.customers) / len(self.customers) if self.customers else 0

        for dim in sorted_dims:
            ltvs = dim_data[dim]
            avg = sum(ltvs) / len(ltvs)
            index = (avg / global_avg_ltv) if global_avg_ltv > 0 else 0
            
            performance_icon = "🚀" if index > 1.2 else "⚖️" if index > 0.8 else "⚠️"
            display_dim = dim[:28] + "..." if len(dim) > 28 else dim
            print(f"{display_dim:<30} | {len(ltvs):<10} | ${avg:>13.2f} | {index:>6.2f}x {performance_icon}")
        
        self._print_separator("-")

    def print_category_performance(self):
        """Legacy: mantiene compatibilidad con código existente."""
        self.print_dimension_performance()