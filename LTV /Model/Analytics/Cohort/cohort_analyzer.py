from typing import List, Dict, Any, Optional
from datetime import datetime
from collections import defaultdict
from Model.Domain.customer import Customer
from Model.Utils.cohort_utils import CohortUtils, UniqueOrderMixin


class CohortAnalyzer(UniqueOrderMixin):
    """
    Construye matriz de LTV por cohorte con soporte para múltiples granularidades.
    
    Granularidades soportadas:
    - quarterly (default) - comportamiento original
    - monthly
    - weekly
    - semiannual
    - yearly
    
    MEJORAS v4:
    - SIN Forward Fill: los períodos sin compra quedan como None (celda vacía en Excel)
    - Límites dinámicos basados en fechas reales
    - Uso de CohortUtils para IDs y valores de período
    """
    
    def __init__(self, customers: List[Customer], granularity: str = "quarterly"):
        """
        Args:
            customers: Lista de objetos Customer
            granularity: 'quarterly', 'monthly', 'weekly', 'semiannual', 'yearly'
        """
        super().__init__()
        self.customers = customers
        self.granularity = granularity
        self._cohort_table = None
        self._n_periods = None
    
    def _get_date_range(self) -> tuple:
        """Obtiene la fecha mínima y máxima de todos los customers."""
        min_date = None
        max_date = None
        
        for customer in self.customers:
            orders = self.get_unique_orders(customer)
            if orders:
                first_date = orders[0].order_date
                last_date = orders[-1].order_date
                
                if min_date is None or first_date < min_date:
                    min_date = first_date
                if max_date is None or last_date > max_date:
                    max_date = last_date
        
        return min_date, max_date
    
    def _calculate_n_periods(self) -> int:
        """
        Calcula el número de períodos necesario basado en las fechas reales.
        """
        if self._n_periods is not None:
            return self._n_periods
        
        min_date, max_date = self._get_date_range()
        
        if min_date is None or max_date is None:
            # Fallback: valores por defecto según granularidad
            defaults = {
                "quarterly": 25,
                "monthly": 76,
                "weekly": 313,
                "semiannual": 13,
                "yearly": 7
            }
            self._n_periods = defaults.get(self.granularity, 25)
            return self._n_periods
        
        min_val = CohortUtils.get_period_value(min_date, self.granularity)
        max_val = CohortUtils.get_period_value(max_date, self.granularity)
        
        # Agregar margen de 2 períodos (para ver tendencia futura)
        self._n_periods = max_val - min_val + 3
        return self._n_periods
    
    def get_cohort_id(self, customer: Customer) -> str:
        """
        Identifica la cohorte usando la primera transacción ÚNICA.
        Usa CohortUtils para generar el ID según granularidad.
        """
        unique_orders = self.get_unique_orders(customer)
        if not unique_orders:
            return "Unknown"
        
        first_date = unique_orders[0].order_date
        return CohortUtils.get_cohort_id(first_date, self.granularity)
    
    def _calculate_period_diff(self, start: datetime, end: datetime) -> int:
        """
        Calcula la distancia en períodos según la granularidad.
        """
        start_val = CohortUtils.get_period_value(start, self.granularity)
        end_val = CohortUtils.get_period_value(end, self.granularity)
        return end_val - start_val
    
    def build_cohort_table(self) -> Dict[str, Dict[str, Any]]:
        """
        Construye la matriz de LTV.
        
        IMPORTANTE: SIN Forward Fill.
        - Los períodos donde el cliente no compró quedan como None
        - Esto genera celdas vacías en Excel (escalera real)
        
        Returns:
            Dict con estructura:
            {
                "2024-Q1": {
                    "size": 100,
                    "ltv": {0: 50.0, 1: None, 2: 80.0, ...},
                    "total_ltv": 5000.0,
                    "total_gmv": 10000.0
                },
                ...
            }
        """
        if self._cohort_table is not None:
            return self._cohort_table
        
        n_periods = self._calculate_n_periods()
        
        # Agrupar customers por cohorte
        cohort_map = defaultdict(list)
        for customer in self.customers:
            cohort_id = self.get_cohort_id(customer)
            if cohort_id != "Unknown":
                cohort_map[cohort_id].append(customer)
        
        final_table = {}
        
        for cohort_id, customer_list in cohort_map.items():
            # Inicializar estructura de datos por período
            period_data = {p: [] for p in range(n_periods)}
            cohort_total_gmv = 0.0
            cohort_total_ltv = 0.0
            cohort_size = len(customer_list)
            
            for customer in customer_list:
                cohort_total_gmv += customer.total_revenue()
                cohort_total_ltv += customer.total_cp()
                
                timeline = customer.ltv_timeline()
                if not timeline:
                    # Cliente sin órdenes: None en todos los períodos
                    for p in range(n_periods):
                        period_data[p].append(None)
                    continue
                
                # Mapear qué LTV acumulado tenía el cliente en cada período
                customer_history = {}
                first_date = timeline[0]['date']
                
                for entry in timeline:
                    period_rel = self._calculate_period_diff(first_date, entry['date'])
                    if 0 <= period_rel < n_periods:
                        customer_history[period_rel] = entry['ltv']
                
                # --- SIN FORWARD FILL ---
                # Solo se asigna valor en períodos donde hubo transacción
                # El resto queda como None (celda vacía en Excel)
                for p in range(n_periods):
                    if p in customer_history:
                        period_data[p].append(customer_history[p])
                    else:
                        period_data[p].append(None)
            
            # Calcular promedios (ignorando None)
            ltv_metrics = {}
            for p in range(n_periods):
                # Filtrar valores None para el promedio
                valid_values = [v for v in period_data[p] if v is not None]
                if valid_values:
                    avg_ltv = sum(valid_values) / cohort_size
                    ltv_metrics[p] = round(avg_ltv, 2)
                else:
                    ltv_metrics[p] = None  # Celda vacía en Excel
            
            final_table[cohort_id] = {
                "size": cohort_size,
                "ltv": ltv_metrics,
                "total_gmv": round(cohort_total_gmv, 2),
                "total_ltv": round(cohort_total_ltv, 2),
                "granularity": self.granularity,
                "n_periods": n_periods
            }
        
        self._cohort_table = final_table
        return final_table
    
    def get_cohort_table_tabular(self) -> List[Dict]:
        """
        Retorna la tabla de cohortes en formato tabular para exportación.
        Los valores None se convertirán en celdas vacías en Excel.
        """
        matrix = self.build_cohort_table()
        n_periods = self._calculate_n_periods()
        
        # Determinar prefijo para las columnas
        prefix_map = {
            "quarterly": "Q",
            "monthly": "M",
            "weekly": "W",
            "semiannual": "H",
            "yearly": "Y"
        }
        prefix = prefix_map.get(self.granularity, "P")
        
        table = []
        for cohort_id, data in sorted(matrix.items()):
            row = {"Cohorte": cohort_id, "Size": data["size"]}
            
            for p in range(n_periods):
                col_name = f"{prefix}{p}"
                val = data["ltv"].get(p, None)
                # Si es None, se queda None (Excel mostrará celda vacía)
                row[col_name] = round(val, 2) if val is not None else None
            
            row["LTV_Total"] = data["total_ltv"]
            row["GMV_Total"] = data["total_gmv"]
            table.append(row)
        
        return table
    
    def print_frequency_report(self):
        """Imprime reporte de frecuencia por cohorte."""
        cohort_table = self.build_cohort_table()
        
        print("\n" + "=" * 70)
        print(f" MATRIZ DE COHORTES ({self.granularity.upper()}) ".center(70))
        print("=" * 70)
        
        if not cohort_table:
            print("⚠️ No hay datos de cohortes para mostrar")
            return
        
        print(f"\n📊 Resumen de cohortes:")
        print(f"   Granularidad: {self.granularity}")
        print(f"   Total cohortes: {len(cohort_table)}")
        print(f"   Períodos por cohorte: {self._calculate_n_periods()}")
        print(f"   NOTA: Celdas vacías = sin actividad en ese período")
        
        print(f"\n📋 Cohortes detectadas:")
        for i, (cohort_id, data) in enumerate(sorted(cohort_table.items())):
            if i >= 10:
                print(f"   ... y {len(cohort_table) - 10} más")
                break
            print(f"   {cohort_id}: {data['size']} clientes, LTV total: ${data['total_ltv']:,.2f}")
        
        print("=" * 70)
    
    def get_cohort_summary(self) -> Dict[str, Any]:
        """
        Retorna un resumen estadístico de las cohortes.
        """
        cohort_table = self.build_cohort_table()
        
        if not cohort_table:
            return {}
        
        sizes = [data["size"] for data in cohort_table.values()]
        total_ltvs = [data["total_ltv"] for data in cohort_table.values()]
        
        return {
            "granularity": self.granularity,
            "total_cohorts": len(cohort_table),
            "avg_cohort_size": round(sum(sizes) / len(sizes), 2),
            "min_cohort_size": min(sizes),
            "max_cohort_size": max(sizes),
            "total_ltv_all_cohorts": sum(total_ltvs),
            "avg_ltv_per_cohort": round(sum(total_ltvs) / len(total_ltvs), 2),
            "n_periods": self._calculate_n_periods(),
            "cohorts": list(cohort_table.keys())
        }