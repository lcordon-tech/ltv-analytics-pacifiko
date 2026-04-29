import pandas as pd
import os
import logging
import numpy as np
from typing import List, Dict, Optional

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


class RealDataRepository:
    """
    Clase responsable de leer datos reales desde un archivo Excel específico (Modelo $)
    y formatearlos para que sean consumibles por el LTVController.
    
    VERSIÓN MULTI-PAÍS: Soporta filtrado por país si la columna 'country' existe.
    VERSIÓN CON PID: Soporta lectura de prod_pid.
    VERSIÓN CON FILTRO POR DIMENSIONES: Soporta category, subcategory, brand, name.
    """

    def get_orders_from_excel(
        self, 
        path_or_dir: str, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None, 
        sample_size: Optional[int] = None,
        country_config = None,
        dimension_filter: Optional[Dict[str, List[str]]] = None  # ← NUEVO PARÁMETRO
    ) -> List[Dict]:
        
        # 1. IDENTIFICACIÓN DEL ARCHIVO
        PREFIJO = "Resultado_Unit_Economics_"
        
        if os.path.isdir(path_or_dir):
            archivos = [f for f in os.listdir(path_or_dir) 
                       if f.startswith(PREFIJO) and f.endswith(".csv")]
            
            if not archivos:
                raise FileNotFoundError(f"🚨 Error: No se encontraron archivos que empiecen con '{PREFIJO}' en: {path_or_dir}")
            
            archivos.sort(key=lambda x: os.path.getmtime(os.path.join(path_or_dir, x)), reverse=True)
            target_path = os.path.join(path_or_dir, archivos[0])
            logging.info(f"🔎 Archivo más reciente detectado automáticamente: {archivos[0]}")
        else:
            target_path = path_or_dir
        
        # 2. CARGA DE DATOS
        if target_path.endswith(".csv"):
            df = pd.read_csv(target_path)
        else:
            df = pd.read_excel(target_path, engine='openpyxl')
        logging.info(f"✅ Se leyeron {len(df)} filas del modelo financiero.")
        
        # 3. FILTRADO POR PAÍS
        if country_config is not None:
            country_code = country_config.code
            if 'country' in df.columns:
                before_filter = len(df)
                df = df[df['country'].astype(str).str.upper() == country_code]
                logging.info(f"🌎 Filtrado por país {country_code}: {before_filter} → {len(df)} filas")
            else:
                logging.info(f"🌎 País seleccionado: {country_config.name} ({country_code}) - Sin columna 'country' en datos")
        else:
            logging.info("🌎 Sin configuración de país - usando todos los datos")
        
        # 3b. FILTRADO POR DIMENSIONES (NUEVO)
        if dimension_filter:
            before_filter = len(df)
            logging.info(f"🔍 Aplicando filtro por dimensiones: {list(dimension_filter.keys())}")
            
            # Aplicar filtros en orden jerárquico (opcional pero recomendado)
            if 'category' in dimension_filter and dimension_filter['category']:
                categories = [c for c in dimension_filter['category'] if c]
                if categories:
                    df = df[df['category'].astype(str).str.strip().isin(categories)]
                    logging.info(f"   📂 Filtrado por categoría: {before_filter} → {len(df)} filas")
                    before_filter = len(df)
            
            if 'subcategory' in dimension_filter and dimension_filter['subcategory']:
                subcategories = [s for s in dimension_filter['subcategory'] if s]
                if subcategories:
                    df = df[df['subcategory'].astype(str).str.strip().isin(subcategories)]
                    logging.info(f"   📁 Filtrado por subcategoría: {before_filter} → {len(df)} filas")
                    before_filter = len(df)
            
            if 'brand' in dimension_filter and dimension_filter['brand']:
                brands = [b for b in dimension_filter['brand'] if b]
                if brands:
                    df = df[df['brand'].astype(str).str.strip().isin(brands)]
                    logging.info(f"   🏷️ Filtrado por marca: {before_filter} → {len(df)} filas")
                    before_filter = len(df)
            
            if 'product' in dimension_filter and dimension_filter['product']:
                products = [p for p in dimension_filter['product'] if p]
                if products:
                    df = df[df['name'].astype(str).str.strip().isin(products)]
                    logging.info(f"   🎯 Filtrado por producto: {before_filter} → {len(df)} filas")
                    
            if dimension_filter:
                logging.info(f"🔍 FILTRO APLICADO: {dimension_filter}")
                logging.info(f"   • Categorías únicas después de filtrar: {df['category'].unique()[:10]}")
                logging.info(f"   • Subcategorías únicas después de filtrar: {df['subcategory'].unique()[:10]}")
                logging.info(f"   • Marcas únicas después de filtrar: {df['brand'].unique()[:10]}")
                logging.info(f"   • Productos únicos después de filtrar: {df['name'].unique()[:10]}")
        
        # 4. VALIDACIÓN DE COLUMNAS REQUERIDAS
        columnas_requeridas = [
            'order_id', 'customer_id', 'order_date', 'revenue', 
            'base_cost', 'sois', 'shipping_cost_$', 'shipping_revenue_$', 
            'credit_card_cost', 'cod_cost', 'fc_variable_$', 
            'cs_variable_$', 'fraud_cost', 'infra_cost',
            'retention_cost_$'
        ]
        
        faltantes = [col for col in columnas_requeridas if col not in df.columns]
        if faltantes:
            raise ValueError(f"🚨 Columnas faltantes en Excel: {faltantes}.")

        # 5. LIMPIEZA Y MAPEO NUMÉRICO
        campos_num = [
            'revenue', 'base_cost', 'sois', 'shipping_cost_$', 'shipping_revenue_$',
            'credit_card_cost', 'cod_cost', 'fc_variable_$', 
            'cs_variable_$', 'fraud_cost', 'infra_cost',
            'retention_cost_$'
        ]
        
        for col in campos_num:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        # BLINDAJE: COSTOS NEGATIVOS
        mask_negativo = df['base_cost'] < 0
        if mask_negativo.any():
            count_neg = mask_negativo.sum()
            df.loc[mask_negativo, 'base_cost'] = df.loc[mask_negativo, 'base_cost'].abs()
            logging.warning(f"🛠️ Se corrigieron {count_neg} registros con costo negativo (Set to Absolute).")

        # 6. MAPEO AL MODELO DE 'ORDER'
        df_mapped = df.rename(columns={
            'base_cost': 'cost',
            'shipping_cost_$': 'shipping_cost',
            'shipping_revenue_$': 'shipping_revenue',
            'fc_variable_$': 'fc_variable',
            'cs_variable_$': 'cs_variable',
            'infra_cost': 'infrastructure_cost',
            'retention_cost_$': 'retention_cost'
        })

        # 7. FILTROS Y FORMATEO DE FECHAS
        df_mapped['order_date'] = df_mapped['order_date'].replace(0, pd.NA)
        df_mapped['order_date'] = pd.to_datetime(df_mapped['order_date'], errors='coerce')
        
        invalid_count = df_mapped['order_date'].isna().sum()
        if invalid_count > 0:
            logging.warning(f"⚠️ Se descartaron {invalid_count} registros con fechas ilegibles.")
        
        df_mapped = df_mapped.dropna(subset=['order_date'])

        if start_date:
            df_mapped = df_mapped[df_mapped['order_date'] >= pd.to_datetime(start_date)]
        if end_date:
            df_mapped = df_mapped[df_mapped['order_date'] <= pd.to_datetime(end_date)]
        
        # Limpieza de columnas categóricas
        for col in ['business_unit', 'category', 'subcategory', 'brand', 'name']:
            if col in df_mapped.columns:
                df_mapped[col] = df_mapped[col].astype(str).str.strip().replace(['nan', 'None', '', 'UNKNOWN'], 'N/A')
            elif col in ['brand', 'name']:
                df_mapped[col] = 'N/A'
        
        # Limpieza de prod_pid
        if 'prod_pid' in df_mapped.columns:
            df_mapped['prod_pid'] = df_mapped['prod_pid'].astype(str).str.strip().replace(['nan', 'None', '', 'UNKNOWN'], 'N/A')
        else:
            df_mapped['prod_pid'] = 'N/A'
            logging.info("ℹ️ Columna 'prod_pid' no encontrada. Usando 'N/A' por defecto.")

        if sample_size and sample_size < len(df_mapped):
            df_mapped = df_mapped.sample(n=sample_size, random_state=42)

        # 8. SELECCIÓN FINAL DE COLUMNAS
        columnas_finales = [
            'order_id', 'customer_id', 'order_date', 'revenue', 'cost', 'sois',
            'shipping_cost', 'shipping_revenue', 'credit_card_cost', 'cod_cost',
            'fc_variable', 'cs_variable', 'fraud_cost', 'infrastructure_cost',
            'retention_cost',
            'category', 'subcategory', 'business_unit',
            'brand', 'name', 'prod_pid'
        ]
        
        columnas_actuales = df_mapped.columns.tolist()
        final_cols = [c for c in columnas_finales if c in columnas_actuales]
        
        df_final = df_mapped[final_cols].copy()
        df_final['order_date'] = df_final['order_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Mostrar muestra de PID para diagnóstico
        pid_sample = df_final['prod_pid'].dropna().unique()[:5]
        if len(pid_sample) > 0 and pid_sample[0] != 'N/A':
            logging.info(f"📦 PID encontrados en datos: {list(pid_sample)}")
        else:
            logging.info("ℹ️ No se encontraron PID en los datos")

        logging.info(f"✅ Transformación exitosa: {len(df_final)} registros listos (incluye Costo de Retención, Brand, Product Name y PID).")
        return df_final.to_dict(orient='records')
