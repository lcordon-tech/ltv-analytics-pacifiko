# Archivo: Model/Analytics/Cohort/cohort_retention_matrix.py

from typing import List, Dict, Any
from collections import defaultdict
from Model.Domain.customer import Customer
from Model.Utils.cohort_utils import UniqueOrderMixin, CohortUtils

class CohortRetentionMatrix(UniqueOrderMixin):
    """
    Construye una matriz de retención basada en actividad transaccional.
    
    Soporta todas las granularidades:
    - quarterly (25 períodos)
    - monthly (76 períodos)
    - weekly (313 períodos)
    - semiannual (13 períodos)
    - yearly (7 períodos)
    
    MEJORAS v3:
    - Límites dinámicos según granularidad
    - Soporte para weekly, semiannual, yearly
    - Uso de CohortUtils para IDs y períodos
    - Sin forward fill (solo marca actividad real)
    """

    def __init__(self, customers: List[Customer], mode: str = "quarterly"):
        """
        Args:
            customers: Lista de objetos Customer
            mode: Granularidad ('quarterly', 'monthly', 'weekly', 'semiannual', 'yearly')
        """
        super().__init__()
        self.customers = customers
        self.mode = mode.lower()
        
        # Límites dinámicos según granularidad
        self.limits = {
            "quarterly": 25,      # 6 años × 4 trimestres + margen
            "monthly": 76,        # 6 años × 12 meses + margen
            "weekly": 313,        # 6 años × 52 semanas + margen
            "semiannual": 13,     # 6 años × 2 semestres + margen
            "yearly": 7           # 6 años + margen
        }
        
        self.limit = self.limits.get(self.mode, 25)
        
        # Cache para la matriz
        self._matrix_cache = None

    def _calculate_dynamic_limit(self) -> int:
        """
        Calcula el límite dinámicamente basado en fechas reales.
        Útil si los datos no cubren 6 años completos.
        """
        if not self.customers:
            return self.limits.get(self.mode, 25)
        
        min_date = None
        max_date = None
        
        for customer in self.customers:
            unique_orders = self.get_unique_orders(customer)
            if unique_orders:
                first_date = unique_orders[0].order_date
                last_date = unique_orders[-1].order_date
                
                if min_date is None or first_date < min_date:
                    min_date = first_date
                if max_date is None or last_date > max_date:
                    max_date = last_date
        
        if min_date is None or max_date is None:
            return self.limits.get(self.mode, 25)
        
        min_val = CohortUtils.get_period_value(min_date, self.mode)
        max_val = CohortUtils.get_period_value(max_date, self.mode)
        
        # Agregar margen de 2 períodos
        return max_val - min_val + 3

    def build_retention_matrix(self) -> Dict[str, Dict]:
        """
        Calcula la retención activa.
        
        Returns:
            Dict con estructura:
            {
                "2024-Q1": {
                    "size": 100,
                    "retention": {0: 100, 1: 45, 2: 30, ...}
                },
                ...
            }
        """
        if self._matrix_cache is not None:
            return self._matrix_cache
        
        matrix_data = defaultdict(lambda: defaultdict(set))
        cohort_sizes = defaultdict(int)
        
        for customer in self.customers:
            unique_orders = self.get_unique_orders(customer)
            if not unique_orders:
                continue
            
            # 1. Definir Cohorte (Nacimiento)
            first_date = unique_orders[0].order_date
            cohort_id = CohortUtils.get_cohort_id(first_date, self.mode)
            birth_period_val = CohortUtils.get_period_value(first_date, self.mode)
            
            # Sumamos el cliente al tamaño inicial de su cohorte
            cohort_sizes[cohort_id] += 1
            
            # 2. Procesar cada orden única para ver re-actividad
            for order in unique_orders:
                curr_date = order.order_date
                current_period_val = CohortUtils.get_period_value(curr_date, self.mode)
                idx = current_period_val - birth_period_val
                
                if 0 <= idx < self.limit:
                    matrix_data[cohort_id][idx].add(customer.customer_id)
        
        # 3. Formatear resultado final
        final_matrix = {}
        for cohort_id in sorted(matrix_data.keys()):
            size = cohort_sizes[cohort_id]
            retention_counts = {}
            
            for i in range(self.limit):
                count = len(matrix_data[cohort_id][i])
                # El período 0 (nacimiento) siempre es el tamaño total
                if i == 0:
                    retention_counts[i] = size
                else:
                    retention_counts[i] = count if count > 0 else 0
            
            final_matrix[cohort_id] = {
                "size": size,
                "retention": retention_counts
            }
        
        self._matrix_cache = final_matrix
        return final_matrix

    def get_tabular_format(self, as_percentage: bool = False) -> List[Dict]:
        """
        Retorna la matriz formateada para Pandas/Excel.
        
        Args:
            as_percentage: Si True, convierte a porcentaje
        
        Returns:
            Lista de diccionarios lista para DataFrame
        """
        matrix = self.build_retention_matrix()
        table = []
        
        # Prefijo según granularidad
        prefix_map = {
            "quarterly": "Q",
            "monthly": "M",
            "weekly": "W",
            "semiannual": "H",
            "yearly": "Y"
        }
        prefix = prefix_map.get(self.mode, "P")
        
        for cohort_id, data in matrix.items():
            size = data["size"]
            row = {"Cohorte": cohort_id, "Size": size}
            
            # Recorrer hasta el límite configurado
            for i in range(self.limit):
                count = data["retention"].get(i, 0)
                col_name = f"{prefix}{i}"
                
                if count == 0:
                    row[col_name] = None  # Celda vacía en Excel
                else:
                    if as_percentage:
                        row[col_name] = round((count / size) * 100, 2) if size > 0 else 0
                    else:
                        row[col_name] = count
            
            table.append(row)
        
        return table
    
    def get_retention_summary(self) -> Dict[str, Any]:
        """
        Retorna un resumen estadístico de la matriz de retención.
        """
        matrix = self.build_retention_matrix()
        
        if not matrix:
            return {}
        
        total_customers = sum(data["size"] for data in matrix.values())
        
        # Calcular retención promedio en período 1, 2, 3...
        period_retention = {}
        for i in range(1, min(5, self.limit)):
            period_counts = []
            for data in matrix.values():
                count = data["retention"].get(i, 0)
                size = data["size"]
                if size > 0:
                    period_counts.append(count / size)
            if period_counts:
                period_retention[f"period_{i}"] = round(sum(period_counts) / len(period_counts) * 100, 2)
        
        return {
            "granularity": self.mode,
            "total_cohorts": len(matrix),
            "total_customers": total_customers,
            "max_periods": self.limit,
            "avg_retention": period_retention
        }
    
    def print_retention_summary(self):
        """Imprime un resumen legible de la matriz de retención."""
        matrix = self.build_retention_matrix()
        
        print("\n" + "=" * 70)
        print(f" MATRIZ DE RETENCIÓN ({self.mode.upper()}) ".center(70))
        print("=" * 70)
        
        if not matrix:
            print("⚠️ No hay datos de retención para mostrar")
            return
        
        print(f"\n📊 Resumen de retención:")
        print(f"   Granularidad: {self.mode}")
        print(f"   Total cohortes: {len(matrix)}")
        print(f"   Períodos por cohorte: {self.limit}")
        
        # Mostrar retención promedio en primeros períodos
        print(f"\n📈 Retención promedio (clientes activos):")
        for i in range(1, min(5, self.limit)):
            period_counts = []
            for data in matrix.values():
                count = data["retention"].get(i, 0)
                size = data["size"]
                if size > 0:
                    period_counts.append((count / size) * 100)
            if period_counts:
                avg = sum(period_counts) / len(period_counts)
                print(f"   Período {i}: {avg:.1f}%")
        
        # Mostrar primeras cohortes
        print(f"\n📋 Cohortes:")
        for i, (cohort_id, data) in enumerate(sorted(matrix.items())):
            if i >= 10:
                print(f"   ... y {len(matrix) - 10} más")
                break
            ret_p1 = data["retention"].get(1, 0)
            ret_pct = (ret_p1 / data["size"] * 100) if data["size"] > 0 else 0
            print(f"   {cohort_id}: {data['size']} clientes, retención período 1: {ret_pct:.1f}%")
        
        print("=" * 70)