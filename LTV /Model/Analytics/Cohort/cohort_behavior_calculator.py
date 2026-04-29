from collections import defaultdict
from typing import List, Dict
import numpy as np
from Model.Domain.customer import Customer
from Model.Utils.cohort_utils import UniqueOrderMixin, CohortUtils


class CohortBehaviorCalculator(UniqueOrderMixin):
    """
    Clase especializada en métricas de comportamiento y recurrencia 
    segmentadas por cohortes de adquisición.
    
    MEJORAS v2:
    - Usa UniqueOrderMixin (elimina duplicación)
    - Usa CohortUtils para cohort_id (centralizado)
    - Cache de cohort groups
    """
    
    def __init__(self, customers: List[Customer], granularity: str = "quarterly"):
        super().__init__()
        self.customers = customers
        self.granularity = granularity
        self._cohort_groups = None  # Cache
    
    def _group_customers_by_cohort(self) -> Dict[str, List[Customer]]:
        """Agrupa los objetos Customer por su cohorte de entrada (con cache)."""
        if self._cohort_groups is not None:
            return self._cohort_groups
        
        groups = defaultdict(list)
        for customer in self.customers:
            cohort_id = customer.get_cohort_id(self.granularity)
            groups[cohort_id].append(customer)
        
        self._cohort_groups = groups
        return groups
    
    def get_purchase_frequency_stats(self) -> List[Dict]:
        """Calcula el % de clientes que alcanzan hitos de compra (2da, 3ra, 4ta)."""
        results = []
        cohort_groups = self._group_customers_by_cohort()
        sorted_cohorts = sorted(cohort_groups.keys())
        
        for cid in sorted_cohorts:
            customers = cohort_groups[cid]
            total_n = len(customers)
            if total_n == 0:
                continue
            
            # Usar get_unique_orders del mixin
            c_counts = [len(self.get_unique_orders(c)) for c in customers]
            
            count_2 = sum(1 for n in c_counts if n >= 2)
            count_3 = sum(1 for n in c_counts if n >= 3)
            count_4 = sum(1 for n in c_counts if n >= 4)
            
            results.append({
                "Cohorte": cid,
                "Total_Clientes": total_n,
                "Pct_2da_Compra": round((count_2 / total_n) * 100, 2),
                "Pct_3ra_Compra": round((count_3 / total_n) * 100, 2),
                "Pct_4ta_Compra": round((count_4 / total_n) * 100, 2),
                "Abs_2da_Compra": count_2,
                "Abs_3ra_Compra": count_3,
                "Abs_4ta_Compra": count_4
            })
        return results
    
    def get_time_to_reorder_stats(self) -> List[Dict]:
        """Calcula la MEDIANA de días entre saltos de compra."""
        cohort_deltas = defaultdict(lambda: {'1->2': [], '2->3': [], '3->4': []})
        
        for customer in self.customers:
            orders = self.get_unique_orders(customer)
            if len(orders) < 2:
                continue
            
            cohort_id = customer.get_cohort_id(self.granularity)
            
            # Delta 1 -> 2
            d12 = (orders[1].order_date - orders[0].order_date).days
            cohort_deltas[cohort_id]['1->2'].append(max(0, d12))
            
            # Delta 2 -> 3
            if len(orders) >= 3:
                d23 = (orders[2].order_date - orders[1].order_date).days
                cohort_deltas[cohort_id]['2->3'].append(max(0, d23))
            
            # Delta 3 -> 4
            if len(orders) >= 4:
                d34 = (orders[3].order_date - orders[2].order_date).days
                cohort_deltas[cohort_id]['3->4'].append(max(0, d34))
        
        report_rows = []
        for cohort_id in sorted(cohort_deltas.keys()):
            data = cohort_deltas[cohort_id]
            report_rows.append({
                "Cohorte": cohort_id,
                "Mediana_Dias_1a2": int(np.median(data['1->2'])) if data['1->2'] else None,
                "Muestra_2da_Compra": len(data['1->2']),
                "Mediana_Dias_2a3": int(np.median(data['2->3'])) if data['2->3'] else None,
                "Muestra_3ra_Compra": len(data['2->3']),
                "Mediana_Dias_3a4": int(np.median(data['3->4'])) if data['3->4'] else None,
                "Muestra_4ta_Compra": len(data['3->4'])
            })
        return report_rows
    
    def get_conversion_windows_stats(self) -> List[Dict]:
        """Calcula conversión acumulada a 2da compra con auditoría absoluta."""
        results = []
        cohort_groups = self._group_customers_by_cohort()
        sorted_cohorts = sorted(cohort_groups.keys())
        
        for cid in sorted_cohorts:
            customers = cohort_groups[cid]
            total_n = len(customers)
            if total_n == 0:
                continue
            
            win_30 = 0
            win_60 = 0
            win_90 = 0
            win_180 = 0
            win_360 = 0
            
            for customer in customers:
                orders = self.get_unique_orders(customer)
                if len(orders) >= 2:
                    days_to_second = (orders[1].order_date - orders[0].order_date).days
                    
                    if days_to_second <= 30:
                        win_30 += 1
                    if days_to_second <= 60:
                        win_60 += 1
                    if days_to_second <= 90:
                        win_90 += 1
                    if days_to_second <= 180:
                        win_180 += 1
                    if days_to_second <= 360:
                        win_360 += 1
            
            results.append({
                "Cohorte": cid,
                "Total_Clientes": total_n,
                "Clientes_30d": win_30,
                "Clientes_60d": win_60,
                "Clientes_90d": win_90,
                "Clientes_180d": win_180,
                "Clientes_360d": win_360,
                "Pct_Conv_30d": round((win_30 / total_n) * 100, 2),
                "Pct_Conv_60d": round((win_60 / total_n) * 100, 2),
                "Pct_Conv_90d": round((win_90 / total_n) * 100, 2),
                "Pct_Conv_180d": round((win_180 / total_n) * 100, 2),
                "Pct_Conv_360d": round((win_360 / total_n) * 100, 2)
            })
        return results
    
    def get_loyalty_index(self) -> List[Dict]:
        """Promedio de órdenes únicas por cliente."""
        results = []
        cohort_groups = self._group_customers_by_cohort()
        
        for cid in sorted(cohort_groups.keys()):
            customers = cohort_groups[cid]
            total_unique_orders = sum(len(self.get_unique_orders(c)) for c in customers)
            avg = total_unique_orders / len(customers) if customers else 0
            results.append({
                "Cohorte": cid,
                "Unique_Orders_per_Customer": round(avg, 2)
            })
        return results