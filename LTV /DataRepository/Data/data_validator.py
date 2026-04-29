import pandas as pd
import numpy as np

class DataValidator:
    """
    Responsabilidad: Validar, renombrar y limpiar los DataFrames.
    Incluye lógica de rescate para registros sin PID y manejo de comisiones desde DB.
    """

    def __init__(self):
        # Mapeo de nombres de la base de datos a nombres internos del pipeline
        self.ORDERS_MAPPING = {
            "fecha_colocada": "order_date",
            "product_pid": "prod_pid",
            "cost_item": "item_cost"
        }

        # Mapeo para el archivo de SOIS
        self.SOIS_MAPPING = {
            "PID": "prod_pid",
            "SOI_USD": "sois",
            "Fecha_inicio": "fecha_inicio",
            "Fecha_fin": "fecha_fin"
        }

        # Definición de columnas obligatorias por cada fuente
        self.REQUIRED_COLUMNS = {
            "orders": [
                "order_id", "customer_id", "order_date", 
                "quantity", "price", "item_cost", "prod_pid",
                "commission_percent"  # <--- SE QUEDA AQUÍ (Viene de la Database)
            ],
            "sois": ["prod_pid", "sois", "fecha_inicio", "fecha_fin"],
            "assumptions": [
                "cohort", "shipping_cost", "shipping_revenue", "credit_card_payment", 
                "cash_on_delivery_comision", "fc_variable_headcount", 
                "cs_variable_headcount", "fraud", "infrastructure"
                # <--- ELIMINADA DE AQUÍ: No está en tu Excel de Supuestos
            ]
        }

    def _prepare_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara el DataFrame de órdenes: renombra y asegura que existan 
        las columnas necesarias antes de validar el esquema.
        """
        df = df.rename(columns=self.ORDERS_MAPPING)
        
        # Seguro para PID: Si por error la columna no existe, la inicializa
        if 'prod_pid' not in df.columns:
            df['prod_pid'] = np.nan
            
        # Seguro para Comisión: Si la DB no la envió, inicializa con 0.0
        if 'commission_percent' not in df.columns:
            df['commission_percent'] = 0.0
            print("ℹ️ Nota: 'commission_percent' no detectada en DB. Se inicializa en 0.0 para cálculos.")
        
        # Filtramos solo las columnas requeridas que estén presentes
        cols_to_keep = self.REQUIRED_COLUMNS["orders"]
        available_cols = [col for col in cols_to_keep if col in df.columns]
        
        return df[available_cols].copy()

    def _prepare_sois(self, df: pd.DataFrame) -> pd.DataFrame:
        """Procesa y valida la fuente de SOIS."""
        df = df.copy()
        base_mapping = {"PID": "prod_pid", "Fecha_inicio": "fecha_inicio", "Fecha_fin": "fecha_fin"}
        df = df.rename(columns=base_mapping)

        if "SOI_USD" in df.columns:
            df["sois"] = df["SOI_USD"]
        elif "SOI_QTZ" in df.columns:
            TIPO_CAMBIO = 7.66
            df["sois"] = df["SOI_QTZ"] / TIPO_CAMBIO
            print(f"💱 Conversión SOIS: QTZ -> USD (TC: {TIPO_CAMBIO})")
        else:
            raise ValueError("🚨 ERROR: No se encontró 'SOI_QTZ' ni 'SOI_USD' en el archivo SOIS.")

        cols_to_keep = self.REQUIRED_COLUMNS["sois"]
        available_cols = [col for col in cols_to_keep if col in df.columns]
        return df[available_cols]

    def _handle_nulls_and_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Lógica de Rescate: Si no hay PID, asigna 'PID-DESCONOCIDO' para no perder ventas.
        Solo elimina si falta Precio o Cantidad (datos imposibles de calcular).
        """
        # 1. Rescate de PIDs nulos o vacíos
        mask_pid_missing = df['prod_pid'].isna() | (df['prod_pid'].astype(str).str.strip() == "")
        if mask_pid_missing.any():
            missing_count = mask_pid_missing.sum()
            df.loc[mask_pid_missing, 'prod_pid'] = "PID-DESCONOCIDO"
            print(f"⚠️  RESCATE: {missing_count} filas sin PID detectadas. Se asignó 'PID-DESCONOCIDO'.")

        # 2. Limpieza de críticos matemáticos
        initial_count = len(df)
        critical_math = ["quantity", "price"]
        df_clean = df.dropna(subset=[c for c in critical_math if c in df.columns])
        
        removed = initial_count - len(df_clean)
        if removed > 0:
            print(f"🗑️  LIMPIEZA: Se eliminaron {removed} filas con Cantidad o Precio nulos.")
        
        return df_clean

    def _validate_schema(self, df: pd.DataFrame, dataset_name: str):
        """Verifica que todas las columnas requeridas existan en el DataFrame."""
        required = self.REQUIRED_COLUMNS[dataset_name]
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas obligatorias en {dataset_name}: {missing}")

    def _validate_coverage(self, df_orders: pd.DataFrame, df_sois: pd.DataFrame):
        """Alerta sobre productos que no tienen SOI definido."""
        orders_pids = set(df_orders["prod_pid"].unique())
        sois_pids = set(df_sois["prod_pid"].unique())
        diff = orders_pids - sois_pids
        if diff:
            diff.discard("PID-DESCONOCIDO") # No alertar por el genérico
            if diff:
                print(f"⚠️  COBERTURA: {len(diff)} PIDs reales no están en SOIS. Se aplicará SOI = 0.")

    def run(self, df_orders: pd.DataFrame, df_sois: pd.DataFrame, df_assumptions: pd.DataFrame):
        """Ejecuta el proceso completo de validación y limpieza."""
        print("\n" + "="*60)
        print(" INICIANDO VALIDACIÓN Y LIMPIEZA (CON RESCATE) ".center(60))
        print("="*60)

        # 1. Preparar y Renombrar
        df_orders = self._prepare_orders(df_orders)
        df_sois = self._prepare_sois(df_sois)

        # 2. Validar Esquemas (Revisa que el Excel y DB coincidan con REQUIRED_COLUMNS)
        self._validate_schema(df_orders, "orders")
        self._validate_schema(df_sois, "sois")
        self._validate_schema(df_assumptions, "assumptions")

        # 3. Limpieza y Rescate
        df_orders = self._handle_nulls_and_clean(df_orders)
        
        # 4. Verificación de Cobertura
        self._validate_coverage(df_orders, df_sois)

        print(f"\n✅ VALIDACIÓN FINALIZADA. Registros listos: {len(df_orders)}")
        print("="*60)

        return df_orders, df_sois, df_assumptions