from typing import Dict, Any, List
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np
from Model.Domain.customer import Customer
from Model.Utils.cohort_utils import CohortUtils, UniqueOrderMixin


class UnitEconomicsAnalyzer(UniqueOrderMixin):
    """
    Analiza Unit Economics por cohorte: CAC, LTV, ROI, Decay Rate.
    
    MEJORAS v3:
    - Soporte para valores None en LTV (celdas vacías)
    - Manejo seguro de None en operaciones aritméticas
    """
    
    def __init__(self, cohort_data: Dict, ad_spend: Dict, customers: List[Customer],
                 granularity: str = "quarterly"):
        super().__init__()
        self.cohort_data = cohort_data
        self.ad_spend = ad_spend
        self.customers = customers
        self.granularity = granularity
        self.results = {}
        self._retention_cache = None

    def calculate_period_retention_costs(self) -> Dict[str, Dict[str, Any]]:
        if self._retention_cache is not None:
            return self._retention_cache
        
        period_stats = defaultdict(lambda: {"spend": 0.0, "active_recurrent": set()})
        
        for customer in self.customers:
            cohort_id = customer.get_cohort_id(self.granularity)
            orders_by_period = defaultdict(list)
            
            for order in customer.get_orders():
                period_id = CohortUtils.get_cohort_id(order.order_date, self.granularity)
                orders_by_period[period_id].append(order)
            
            for period_id, orders in orders_by_period.items():
                total_spend = sum(getattr(o, 'retention_cost', 0.0) for o in orders)
                period_stats[period_id]["spend"] += total_spend
                
                if period_id != cohort_id:
                    period_stats[period_id]["active_recurrent"].add(customer.customer_id)
        
        self._retention_cache = {
            p_id: {
                "spend": round(stats["spend"], 2),
                "existing_count": len(stats["active_recurrent"])
            }
            for p_id, stats in period_stats.items()
        }
        return self._retention_cache

    def run_analysis(self) -> Dict[str, Any]:
        retention_report = self.calculate_period_retention_costs()
        
        for cohort_id, data in self.cohort_data.items():
            cac = self.ad_spend.get(cohort_id)
            
            if cac is None:
                continue
            
            size = data["size"]
            ltv_months = data["ltv"]
            
            period_data = retention_report.get(cohort_id, {"spend": 0.0, "existing_count": 0})
            ret_spend = period_data["spend"]
            existing_customers = period_data["existing_count"]
            
            acq_spend = round(size * cac, 2)
            
            # LTV bruto y neto (manejando None)
            ltv_bruto_months = ltv_months.copy()
            ltv_neto_months = {}
            for month, ltv_bruto in ltv_bruto_months.items():
                if ltv_bruto is None:
                    ltv_neto_months[month] = None
                    continue
                ltv_neto = max(0, ltv_bruto - cac)
                ltv_neto_months[month] = round(ltv_neto, 2)
            
            # Eficiencia LTV/CAC (manejando None)
            ltv_cac_ratio = {}
            payback_month = None
            for m, ltv_val in ltv_bruto_months.items():
                if ltv_val is None:
                    ltv_cac_ratio[m] = None
                    continue
                ratio = round(ltv_val / cac, 2) if cac > 0 else 0
                ltv_cac_ratio[m] = ratio
                if payback_month is None and ltv_val >= cac:
                    payback_month = m
        
            self.results[cohort_id] = {
                "size": size,
                "existing_count": existing_customers,
                "cac": cac,
                "spend": acq_spend,
                "acq_spend": acq_spend,
                "retention_spend_total": ret_spend,
                "total_marketing_spend": acq_spend + ret_spend,
                "ltv_nominal": ltv_bruto_months,
                "ltv_neto": ltv_neto_months,
                "ltv_cac": ltv_cac_ratio,
                "payback_month": f"Q{payback_month}" if payback_month is not None else "En progreso"
            }
            
        return self.results
    
    def get_cohort_roi(self) -> Dict[str, float]:
        roi = {}
        for cohort_id, data in self.results.items():
            # Filtrar valores None para el cálculo
            ltv_values = [v for v in data["ltv_nominal"].values() if v is not None]
            total_ltv = sum(ltv_values) if ltv_values else 0
            total_cac = data["size"] * data["cac"]
            if total_cac > 0:
                roi[cohort_id] = round(((total_ltv - total_cac) / total_cac) * 100, 2)
            else:
                roi[cohort_id] = 0
        return roi
    
    def get_cohort_decay_rate(self) -> Dict[str, float]:
        decay = {}
        for cohort_id, data in self.results.items():
            ltv_values = [v for v in data["ltv_nominal"].values() if v is not None]
            if len(ltv_values) >= 2:
                first_val = ltv_values[0]
                last_val = ltv_values[-1]
                if first_val > 0:
                    decay_rate = ((last_val - first_val) / first_val) * 100
                    decay[cohort_id] = round(decay_rate, 2)
                else:
                    decay[cohort_id] = 0
            else:
                decay[cohort_id] = 0
        return decay

    def get_strategic_status(self, current_ratio: float) -> str:
        if current_ratio is None:
            return "⚪ SIN DATOS"
        if current_ratio < 2.0:
            return "🔴 DÉBIL"
        if 3.0 <= current_ratio <= 4.0:
            return "🟢 SALUDABLE"
        if 4.0 < current_ratio <= 6.0:
            return "🚀 ACELERAR"
        if current_ratio > 6.0:
            return "⚠️ SUBINVERSIÓN"
        return "🟡 EN MADURACIÓN"

    def print_unit_economics(self):
        if not self.results:
            self.run_analysis()
        
        analysis = self.results
        limit_date = datetime(2026, 3, 30)
        
        if self.granularity == "monthly":
            months_to_show = list(range(0, 36, 3))
            period_label = "M"
        elif self.granularity == "quarterly":
            months_to_show = list(range(0, 76, 3))
            period_label = "Q"
        else:
            months_to_show = list(range(0, 76, 3))
            period_label = "P"
        
        period_headers = [f"{period_label}{i//3}" for i in months_to_show[:10]]
        header = f"{'COHORTE':<12} | {'SIZE':<6} | {'CAC':<8} | {'RET_SPEND':<12} | " + \
                 " | ".join(period_headers) + " | PAYBACK | STATUS | ROI% | DECAY%"
        
        print("\n" + "=" * len(header))
        print("UNIT ECONOMICS: ADQUISICIÓN vs RETENCIÓN".center(len(header)))
        print("=" * len(header))
        print(header)
        print("-" * len(header))
        
        roi_by_cohort = self.get_cohort_roi()
        decay_by_cohort = self.get_cohort_decay_rate()
        
        for cid in sorted(analysis.keys()):
            res = analysis[cid]
            
            try:
                parsed = CohortUtils.parse_cohort_id(cid)
                if parsed:
                    year, period, _ = parsed
                    if self.granularity == "quarterly":
                        month = (period - 1) * 3 + 1
                        cohort_start = datetime(year, month, 1)
                    elif self.granularity == "monthly":
                        cohort_start = datetime(year, period, 1)
                    else:
                        cohort_start = datetime(year, 1, 1)
                else:
                    cohort_start = datetime(2020, 1, 1)
            except:
                cohort_start = datetime(2020, 1, 1)
            
            row = f"{cid:<12} | {res['size']:<6} | ${res['cac']:>6.2f} | ${res['retention_spend_total']:>10.2f} | "
            
            ratios = []
            running_ratio = 0.0
            for i in months_to_show[:10]:
                if self.granularity == "monthly":
                    projected_date = cohort_start + timedelta(days=i * 30)
                else:
                    projected_date = cohort_start + timedelta(days=i * 30)
                
                if projected_date > limit_date:
                    ratios.append(f"{' -':>6}")
                else:
                    val = res["ltv_cac"].get(i, None)
                    if val is not None:
                        running_ratio = val
                    ratios.append(f"{running_ratio:>6.1f}" if running_ratio > 0 else f"{' -':>6}")
            
            status = self.get_strategic_status(running_ratio)
            roi_val = roi_by_cohort.get(cid, 0)
            decay_val = decay_by_cohort.get(cid, 0)
            decay_icon = "📉" if decay_val < 0 else "📈" if decay_val > 0 else "➡️"
            
            print(row + " | ".join(ratios) + f" | {res['payback_month']:<7} | {status} | {roi_val:>5.1f}% | {decay_icon} {decay_val:>5.1f}%")
        
        print("\n" + " RESUMEN DE CAC ".center(len(header), "-"))
        print(f"Total cohortes en datos: {len(self.cohort_data)}")
        print(f"Cohortes con CAC definido: {len(self.ad_spend)}")
        print(f"Cohortes sin CAC (excluidas): {len(self.cohort_data) - len(self.ad_spend)}")
        
        if len(self.cohort_data) - len(self.ad_spend) > 0:
            missing = set(self.cohort_data.keys()) - set(self.ad_spend.keys())
            print(f"   Ejemplo: {list(missing)[:5]}")