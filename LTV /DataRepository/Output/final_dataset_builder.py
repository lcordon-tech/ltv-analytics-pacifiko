import pandas as pd
import numpy as np


class FinalDatasetBuilder:
    """
    Responsabilidad: Formatear el DataFrame final para cumplir con el esquema 
    estricto del sistema downstream.
    
    AHORA: Garantiza existencia de prod_pid y business_unit.
    """

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            print("⚠️ Advertencia: No hay datos para estructurar.")
            return pd.DataFrame()

        # 1. RENOMBRADO DE COLUMNAS (CORREGIDO)
        rename_map = {
            'b_unit': 'business_unit',
            'shipping_cost_usd': 'shipping_cost_$',
            'shipping_revenue_usd': 'shipping_revenue_$',
            'fc_variable_usd': 'fc_variable_$',
            'cs_variable_usd': 'cs_variable_$',
            'product_pid': 'prod_pid'
        }
        
        # Aplicar solo las que existen
        existing_renames = {k: v for k, v in rename_map.items() if k in df.columns}
        if existing_renames:
            print(f"   🔄 Renombradas: {list(existing_renames.values())}")
        df = df.rename(columns=existing_renames)

        # 🔧 GARANTIZAR prod_pid
        if 'prod_pid' not in df.columns:
            df['prod_pid'] = 'UNKNOWN'
            print("⚠️ 'prod_pid' no existía. Inicializado como 'UNKNOWN'")
        else:
            df['prod_pid'] = df['prod_pid'].fillna('UNKNOWN').astype(str)
        
        # 🔧 GARANTIZAR business_unit
        if 'business_unit' not in df.columns:
            if 'b_unit' in df.columns:
                df['business_unit'] = df['b_unit']
            else:
                df['business_unit'] = 'UNKNOWN'
                print("⚠️ 'business_unit' no existía. Inicializado como 'UNKNOWN'")
        
        df['business_unit'] = df['business_unit'].fillna('UNKNOWN')

        # 2. CREACIÓN DE COLUMNAS FALTANTES
        if 'category' not in df.columns:
            df['category'] = 'UNKNOWN'
        if 'subcategory' not in df.columns:
            df['subcategory'] = 'UNKNOWN'
        if 'brand' not in df.columns:
            df['brand'] = 'UNKNOWN'
        if 'name' not in df.columns:
            df['name'] = 'UNKNOWN'

        # 3. COLUMNAS NUMÉRICAS ESPERADAS
        expected_numeric_cols = [
            'quantity', 'price', 'item_cost', 'revenue', 'base_cost', 
            'shipping_cost_$', 'credit_card_cost', 'cod_cost', 'fraud_cost', 
            'infra_cost', 'shipping_revenue_$', 'sois', 'contribution_profit',
            'commission_percent', 'fc_variable_$', 'cs_variable_$',
            'retention_cost_$'
        ]
        
        for col in expected_numeric_cols:
            if col not in df.columns:
                df[col] = 0.0

        # 4. ELIMINACIÓN DE COLUMNAS NO REQUERIDAS
        columns_to_drop = ['cohort_index', 'raw_soi', 'temp_raw_total']
        for col in columns_to_drop:
            if col in df.columns:
                df = df.drop(columns=[col])

        # 5. DEFINICIÓN DEL ORDEN EXACTO
        columns_order = [
            'business_unit',
            'category',
            'subcategory',
            'brand',
            'name',
            'order_id',
            'customer_id',
            'order_date',
            'prod_pid',
            'quantity',
            'price',
            'item_cost',
            'commission_percent',
            'revenue',
            'base_cost',
            'shipping_cost_$',
            'credit_card_cost',
            'cod_cost',
            'fraud_cost',
            'infra_cost',
            'fc_variable_$',
            'cs_variable_$',
            'retention_cost_$',
            'shipping_revenue_$',
            'sois',
            'contribution_profit',
            'cohort'
        ]

        # 6. BLINDAJE Y FORMATEO
        for col in columns_order:
            if col not in df.columns:
                df[col] = 0.0 if col not in ['order_date', 'cohort', 'business_unit', 'brand', 'name'] else pd.NaT

        df_final = df[columns_order].copy()

        # Tipos de datos
        for col in ['order_id', 'customer_id', 'prod_pid']:
            df_final[col] = df_final[col].astype(str).str.replace('.0', '', regex=False)
        
        df_final['brand'] = df_final['brand'].astype(str)
        df_final['name'] = df_final['name'].astype(str)

        for col in expected_numeric_cols:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0.0)

        df_final['order_date'] = pd.to_datetime(df_final['order_date'], errors='coerce')

        # 7. ORDENAMIENTO FINAL
        df_final = df_final.sort_values(by=['order_date', 'order_id'], ascending=[True, True])
        df_final = df_final.reset_index(drop=True)

        # 🔧 VALIDACIÓN FINAL
        print("="*60)
        print("✅ Estructura final completada")
        print(f"   prod_pid presente: {'prod_pid' in df_final.columns}")
        print(f"   business_unit presente: {'business_unit' in df_final.columns}")
        print(f"   prod_pid nulos: {df_final['prod_pid'].isnull().sum()}")
        print(f"   business_unit nulos: {df_final['business_unit'].isnull().sum()}")
        
        # Validar que las columnas de costos tengan valores
        for col in ['shipping_cost_$', 'fc_variable_$', 'cs_variable_$']:
            if col in df_final.columns:
                non_zero = (df_final[col] != 0).sum()
                print(f"   {col}: {non_zero} / {len(df_final)} filas con valores != 0")
        
        print(f"📊 CP Total: ${df_final['contribution_profit'].sum():,.2f}")
        print("="*60)

        return df_final