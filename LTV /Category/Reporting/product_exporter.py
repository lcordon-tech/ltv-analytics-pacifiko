"""
Exporter específico para la dimensión Producto (Product Name).
VERSIÓN MEJORADA: Incluye columna PID.
"""

from Category.Reporting.base_exporter import BaseExporter
from Category.Utils.dimension_config import DimensionMode
import pandas as pd


class ProductExporter(BaseExporter):
    """
    Exporter para análisis por Producto.
    Hereda toda la lógica de BaseExporter.
    Incluye columna PID proveniente de prod_pid.
    """
    
    def _get_dimension_mode(self) -> int:
        return DimensionMode.PRODUCT
    
    def build_summary_dataframe(self, mode: str = "historical") -> pd.DataFrame:
        """
        Construye resumen ejecutivo con scoring.
        VERSIÓN MEJORADA: Incluye columna PID.
        """
        # Llamar al método base primero
        df = super().build_summary_dataframe(mode)
        
        if df.empty:
            return df
        
        # Solo agregar PID si estamos en modo PRODUCT y la columna Producto existe
        config = self._get_config()
        main_key = config['main_key']  # 'Producto'
        
        if main_key not in df.columns:
            print(f"  ⚠️ No se encontró columna {main_key} para agregar PID")
            return df
        
        # Construir mapa de Producto -> PID
        pid_map = {}
        for customer in self.customers:
            orders = customer.get_orders_sorted()
            for order in orders:
                product_name = getattr(order, 'name', None)
                pid = getattr(order, 'prod_pid', None)
                
                if product_name and pid:
                    product_name_clean = str(product_name).strip()
                    pid_clean = str(pid).strip()
                    
                    if product_name_clean and product_name_clean.lower() not in ['', 'nan', 'none', 'n/a', 'null']:
                        if pid_clean and pid_clean.lower() not in ['', 'nan', 'none', 'n/a', 'null']:
                            # Si ya existe, mantener el primero (o podríamos tomar el más común)
                            if product_name_clean not in pid_map:
                                pid_map[product_name_clean] = pid_clean
        
        # Aplicar PID al DataFrame
        df['PID'] = df[main_key].map(pid_map)
        
        # Mover PID después del nombre del producto
        cols = df.columns.tolist()
        if 'PID' in cols and main_key in cols:
            idx = cols.index(main_key)
            cols.remove('PID')
            cols.insert(idx + 1, 'PID')
            df = df[cols]
        
        # Verificar resultado
        pid_count = df['PID'].notna().sum()
        print(f"  ✅ PID agregado: {pid_count}/{len(df)} productos tienen PID")
        if pid_count == 0:
            print(f"  ⚠️ No se encontraron PID. Verifica que prod_pid existe en los datos.")
            # Debug: mostrar un par de productos y sus PID
            sample_pids = []
            for customer in self.customers[:10]:
                for order in customer.get_orders_sorted()[:5]:
                    product = getattr(order, 'name', None)
                    pid = getattr(order, 'prod_pid', None)
                    if product and pid:
                        sample_pids.append(f"{product[:30]} -> {pid}")
                        if len(sample_pids) >= 5:
                            break
                if len(sample_pids) >= 5:
                    break
            if sample_pids:
                print(f"  📋 Ejemplos encontrados:")
                for sp in sample_pids:
                    print(f"     {sp}")
        
        return df