import pandas as pd
import numpy as np

class DataMerger:
    """
    Responsabilidad: Unir órdenes con Catálogo (Business Unit) y SOIS
    respetando la multiplicidad de registros, validando vigencia y
    protegiendo contra PIDs inválidos.
    """

    def merge_catalog(self, df_orders: pd.DataFrame, df_catalog: pd.DataFrame) -> pd.DataFrame:
        """
        Une las órdenes con el catálogo.
        Garantiza que un PID solo tenga una b_unit y una category para evitar duplicar ventas.
        """
        print("\n" + "-"*60)
        print(" ASIGNANDO BUSINESS UNIT Y CATEGORY (UNIFICADO) ".center(60))
        print("-" * 60)

        # 1. Limpieza de llaves y estandarización
        df_orders['prod_pid'] = df_orders['prod_pid'].astype(str).str.strip()
        
        if 'product_pid' in df_catalog.columns:
            df_catalog = df_catalog.rename(columns={'product_pid': 'prod_pid'})
        
        df_catalog['prod_pid'] = df_catalog['prod_pid'].astype(str).str.strip()

        # 2. ELIMINAR DUPLICADOS EN CATÁLOGO (Incluyendo 'category' y 'subcategory')
        if 'category' not in df_catalog.columns:
            df_catalog['category'] = np.nan
        if 'subcategory' not in df_catalog.columns:
            df_catalog['subcategory'] = np.nan
        
        # NUEVO: Validar existencia de brand y name
        if 'brand' not in df_catalog.columns:
            df_catalog['brand'] = np.nan
        if 'name' not in df_catalog.columns:
            df_catalog['name'] = np.nan

        # NUEVO: Incluir brand y name en columnas a mantener
        cols_catalog = ['prod_pid', 'b_unit', 'category', 'subcategory', 'brand', 'name']
        df_catalog_unique = df_catalog[cols_catalog].drop_duplicates(subset=['prod_pid'], keep='first')

        # 3. Merge Seguro
        rows_before = len(df_orders)
        df_final = pd.merge(df_orders, df_catalog_unique, on='prod_pid', how='left')

        # 4. Validación de Integridad y Manejo de Nulos
        rows_after = len(df_final)
        if rows_after != rows_before:
            print(f"🚨 ERROR DE INTEGRIDAD: Se generaron {rows_after - rows_before} filas extra.")

        # Relleno de nulos para b_unit y las nuevas columnas
        df_final['b_unit'] = df_final['b_unit'].fillna('OTROS')
        df_final['category'] = df_final['category'].fillna('UNKNOWN')
        df_final['subcategory'] = df_final['subcategory'].fillna('UNKNOWN')
        
        # NUEVO: Rellenar nulos en brand y name
        df_final['brand'] = df_final['brand'].fillna('UNKNOWN')
        df_final['name'] = df_final['name'].fillna('UNKNOWN')

        # NUEVO: Validación de nulos
        if df_final['brand'].isnull().any():
            print(f"⚠️ WARNING: Se detectaron nulos en 'brand' después de fillna")
        if df_final['name'].isnull().any():
            print(f"⚠️ WARNING: Se detectaron nulos en 'name' después de fillna")

        print(f"✅ Catálogo procesado. PIDs únicos mapeados: {len(df_catalog_unique)}")
        print(f"📦 Categorías rescatadas: {df_final['category'].nunique()} tipos detectados.")
        print(f"📦 Subcategorías rescatadas: {df_final['subcategory'].nunique()} tipos detectados.")
        print(f"🏷️  Marcas únicas: {df_final['brand'].nunique()} tipos detectados.")
        print(f"📝 Productos únicos: {df_final['name'].nunique()} tipos detectados.")

        return df_final

    def merge_sois(self, df_orders: pd.DataFrame, df_sois: pd.DataFrame) -> pd.DataFrame:
        """
        Pega el valor de SOIS validando vigencia de fecha y PID válido.
        Evita matches incorrectos de nulos/vacíos con nulos/vacíos.
        """
        print("\n" + "="*60)
        print(" INICIANDO UNIÓN DE DATOS (SOIS POR VIGENCIA) ".center(60))
        print("="*60)

        # 1. Preparación y Limpieza de PID
        df_orders['prod_pid'] = df_orders['prod_pid'].astype(str).str.strip()
        df_sois['prod_pid'] = df_sois['prod_pid'].astype(str).str.strip()

        # Normalización de fechas
        fecha_col = 'order_date'
        df_orders[fecha_col] = pd.to_datetime(df_orders[fecha_col])
        # En data_merger.py, alrededor de la línea 69:

        # Usamos errors='coerce' para que si encuentra un 'time' u otra cosa rara, no explote
        df_sois['fecha_inicio'] = pd.to_datetime(df_sois['fecha_inicio'], errors='coerce')
        df_sois['fecha_fin'] = pd.to_datetime(df_sois['fecha_fin'], errors='coerce')

        # Opcional: Limpiar filas que se quedaron sin fecha después de la conversión
        df_sois = df_sois.dropna(subset=['fecha_inicio', 'fecha_fin'])

        # 2. IDENTIFICACIÓN DE PIDs INVÁLIDOS
        invalid_pids = ["", "nan", "NONE", "NULL", "NAN"]
        mask_valid_orders = ~df_orders['prod_pid'].str.upper().isin(invalid_pids)
        mask_valid_sois = ~df_sois['prod_pid'].str.upper().isin(invalid_pids)

        # Separación de datasets
        df_orders_valid = df_orders[mask_valid_orders].copy()
        df_orders_invalid = df_orders[~mask_valid_orders].copy()
        df_sois_valid = df_sois[mask_valid_sois].copy()

        # 3. MERGE SÓLO CON PIDs VÁLIDOS (Candidatos por fecha)
        df_merged = pd.merge(
            df_orders_valid,
            df_sois_valid[['prod_pid', 'sois', 'fecha_inicio', 'fecha_fin']],
            on='prod_pid',
            how='left'
        )

        # 4. FILTRO DE VIGENCIA
        mask_valid_date = (
            (df_merged[fecha_col] >= df_merged['fecha_inicio']) &
            (df_merged[fecha_col] <= df_merged['fecha_fin'])
        )
        df_matches = df_merged[mask_valid_date].copy()

        # 5. LIMPIEZA DE DUPLICADOS (Priorizar el registro más reciente)
        df_matches = df_matches.sort_values(
            by=['order_id', 'prod_pid', 'fecha_inicio'],
            ascending=[True, True, False]
        ).drop_duplicates(subset=['order_id', 'prod_pid'], keep='first')

        # 6. RE-ENSAMBLAJE (Dataset Válido)
        df_final_valid = pd.merge(
            df_orders_valid,
            df_matches[['order_id', 'prod_pid', 'sois']],
            on=['order_id', 'prod_pid'],
            how='left'
        )
        df_final_valid['sois'] = df_final_valid['sois'].fillna(0.0)

        # 7. MANEJO DE REGISTROS INVÁLIDOS (SOIS = 0)
        df_orders_invalid['sois'] = 0.0

        # 8. CONCATENACIÓN FINAL
        df_final = pd.concat([df_final_valid, df_orders_invalid], ignore_index=True)

        # Logging final
        print(f"⚠️  PIDs inválidos excluidos del merge: {len(df_orders_invalid)}")
        print(f"📊 Merge SOIS finalizado. Registros totales: {len(df_final)}")
        print("-" * 60)

        return df_final