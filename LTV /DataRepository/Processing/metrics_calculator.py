import pandas as pd
import numpy as np
import time
from typing import Dict, Optional

from Run.Services.time_granularity_adapter import TimeGranularityAdapter
from DataRepository.Processing.fallback_utils import get_closest_cohort_value, log_fallback_stats


class MetricsCalculator:
    """
    Responsabilidad: Calcular métricas de Unit Economics.
    
    AHORA: 
    - SIN IVA (los valores vienen netos desde las queries)
    - SIN FALLBACKS HARDCODEADOS para COGS
    - price y item_cost convertidos a USD
    """
    
    def __init__(self, granularidad: str = 'quarterly', 
                 country_context=None, 
                 fx_engine=None):
        self.granularidad = granularidad
        self.adapter = TimeGranularityAdapter(granularidad, country_context=country_context)
        self.country_context = country_context
        self.fx_engine = fx_engine
        self.default_fx_rate = country_context.default_fx_rate if country_context else 7.66
        self.cogs_fallback_stats = {}

    def _get_fx_rate(self, cohort: str) -> float:
        """Obtiene tipo de cambio para una cohorte específica."""
        if self.fx_engine is not None:
            try:
                rate = self.fx_engine.get_rate(cohort, self.granularidad)
                if rate > 0:
                    return rate
            except Exception as e:
                print(f"⚠️ Error obteniendo tasa para {cohort}: {e}")
        
        if self.country_context is not None:
            return self.country_context.default_fx_rate
        
        return 7.66

    def _load_cogs_from_assumptions(self, assumptions_dict: dict) -> dict:
        """Carga COGS desde CUALQUIER pestaña que tenga la columna."""
        cogs_data = {}
        
        if not assumptions_dict:
            return cogs_data
        
        for sheet_name, df_sheet in assumptions_dict.items():
            if 'cohort' in df_sheet.columns and 'cogs' in df_sheet.columns:
                df_sheet['cohort'] = df_sheet['cohort'].astype(str).str.strip().str.upper()
                
                for _, row in df_sheet.iterrows():
                    cohort = row['cohort']
                    try:
                        value = float(row['cogs'])
                        if cohort not in cogs_data:
                            cogs_data[cohort] = value
                    except (ValueError, TypeError):
                        print(f"⚠️ COGS inválido para {cohort}: {row['cogs']}")
        
        return cogs_data

    def run(self, df: pd.DataFrame, assumptions_dict: dict = None) -> pd.DataFrame:
        print("\n" + "="*60)
        country_name = self.country_context.name if self.country_context else "Desconocido"
        print(f" INICIANDO CÁLCULO DE MÉTRICAS ({self.granularidad.upper()}) - {country_name} ".center(60))
        print("="*60)
        
        start_total = time.time()

        # --- SUB-FASE 1: BLINDAJE Y FX ---
        t0 = time.time()
        
        # Cargar COGS desde Excel
        cogs_from_excel = self._load_cogs_from_assumptions(assumptions_dict) if assumptions_dict else {}
        
        if cogs_from_excel:
            print(f"✅ Cargados {len(cogs_from_excel)} valores de COGS desde Excel")
        else:
            print("⚠️ No se encontraron valores de COGS en Excel")
        
        # Transformar COGS según granularidad
        _, transformed_cogs = self.adapter.transform({}, cogs_from_excel)
        
        # Aplicar fallback dinámico para cohortes faltantes
        self.cogs_fallback_stats = {}
        df_cohorts = df['cohort'].unique()
        
        final_cogs_map = {}
        for cohort in df_cohorts:
            if cohort in transformed_cogs:
                final_cogs_map[cohort] = transformed_cogs[cohort]
            else:
                closest_value = get_closest_cohort_value(cohort, transformed_cogs)
                final_cogs_map[cohort] = closest_value
                if closest_value != 0:
                    self.cogs_fallback_stats[cohort] = "dynamic_fallback"
        
        log_fallback_stats(self.cogs_fallback_stats, len(df_cohorts))
        
        # Columnas de supuestos
        assumptions_cols = [
            'shipping_cost', 'shipping_revenue', 'credit_card_payment', 
            'cash_on_delivery_comision', 'fraud', 'infrastructure',
            'fc_variable_headcount', 'cs_variable_headcount', 'commission_percent'
        ]

        for col in assumptions_cols:
            if col not in df.columns:
                df[col] = 0.0
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        # SOIS
        if 'sois' not in df.columns:
            df['sois'] = 0.0
        else:
            df['sois'] = pd.to_numeric(df['sois'], errors='coerce').fillna(0.0)

        # Retention
        if 'retention_cost_$' not in df.columns:
            df['retention_cost_$'] = 0.0
        else:
            df['retention_cost_$'] = pd.to_numeric(df['retention_cost_$'], errors='coerce').fillna(0.0)

        # --- APLICAR TIPO DE CAMBIO Y CONVERTIR TODO A USD ---
        df['_fx_rate'] = df['cohort'].apply(self._get_fx_rate)
        
        mask_zero_rate = (df['_fx_rate'] <= 0) | (df['_fx_rate'].isna())
        if mask_zero_rate.any():
            print(f"⚠️ {mask_zero_rate.sum()} filas con tasa inválida. Usando default.")
            df.loc[mask_zero_rate, '_fx_rate'] = self.default_fx_rate
        
        print(f"💱 Tipos de cambio aplicados: min={df['_fx_rate'].min():.4f}, max={df['_fx_rate'].max():.4f}")
        
        # 1. NETEAR IVA (SOLO PARA GT)
        if self.country_context and self.country_context.code == "GT":
            df['price'] = df['price'] / 1.12
            df['item_cost'] = df['item_cost'] / 1.12
            print(f"   🇬🇹 Aplicando neteo de IVA (12%) a price e item_cost")

        # 2. APLICAR TIPO DE CAMBIO (CRC → USD o GTQ → USD)
        df['_fx_rate'] = df['cohort'].apply(self._get_fx_rate)

        # 3. CONVERTIR A USD
        df['price_usd'] = df['price'] / df['_fx_rate']
        df['item_cost_usd'] = df['item_cost'] / df['_fx_rate']

        # 4. CALCULAR REVENUE
        df['revenue'] = df['quantity'] * df['price_usd']

        # 5. REEMPLAZAR COLUMNAS ORIGINALES
        df['price'] = df['price_usd']
        df['item_cost'] = df['item_cost_usd']
        df = df.drop(columns=['price_usd', 'item_cost_usd'])
        
        # Diagnóstico de revenue
        print(f"\n📊 REVENUE EN USD:")
        print(f"   Promedio: ${df['revenue'].mean():.2f}")
        print(f"   Total: ${df['revenue'].sum():,.2f}")
        print(f"   Ejemplo (primeros 3): {df['revenue'].head(3).tolist()}")
        
        print(f"⏱️  Sub-fase 1 (Blindaje + FX): {time.time() - t0:.2f}s")

        # Normalización de commission_percent
        mask_to_fix = (df['commission_percent'] > 1) & (df['commission_percent'] <= 100)
        if mask_to_fix.any():
            df.loc[mask_to_fix, 'commission_percent'] /= 100

        mask_error = (df['commission_percent'] > 1) | (df['commission_percent'] < 0)
        if mask_error.any():
            df.loc[mask_error, 'commission_percent'] = 0.9

        # Ajuste item_cost faltante (usando price ya en USD)
        mask_missing_cost = (
            df['b_unit'].isin(['DS', '1P', 'OTROS']) & 
            ((df['item_cost'].isna()) | (df['item_cost'] <= 0))
        )
        if mask_missing_cost.any():
            df.loc[mask_missing_cost, 'item_cost'] = df.loc[mask_missing_cost, 'price'] * 0.9
            print(f"⚠️ AJUSTE ITEM_COST: {mask_missing_cost.sum()} filas corregidas")

        # --- SUB-FASE 2: BASE COST ---
        t1 = time.time()
        df['base_cost'] = 0.0
        
        # TM - Usa COGS transformados sobre revenue USD
        mask_tm = df['b_unit'] == 'TM'
        df.loc[mask_tm, 'base_cost'] = df['revenue'] * df['cohort'].map(final_cogs_map).fillna(0).abs()

        # 3P - Comisión sobre revenue USD
        mask_3p = df['b_unit'] == '3P'
        mask_3p_zero = mask_3p & (df['commission_percent'] == 0)
        df.loc[mask_3p_zero, 'commission_percent'] = 0.1
        df.loc[mask_3p, 'base_cost'] = df['revenue'] * (1 - df['commission_percent'])

        # Estándar (1P, FBP, DS) - usando item_cost ya en USD
        mask_std_cost = df['b_unit'].isin(['FBP', '1P', 'DS'])
        df.loc[mask_std_cost, 'base_cost'] = df['quantity'] * df['item_cost']

        # Otros
        mask_others = ~df['b_unit'].isin(['TM', '3P', 'FBP', '1P', 'DS'])
        df.loc[mask_others, 'base_cost'] = df['revenue'] * 0.90
        
        print(f"📊 BASE COST EN USD:")
        print(f"   Promedio: ${df['base_cost'].mean():.2f}")
        print(f"   Total: ${df['base_cost'].sum():,.2f}")
        print(f"⏱️  Sub-fase 2 (Base Cost): {time.time() - t1:.2f}s")

        # --- SUB-FASE 3: DISTRIBUCIÓN FIJA ---
        t2 = time.time()
        
        orders_per_cohort = df.groupby('cohort')['order_id'].transform('nunique')
        items_per_order = df.groupby('order_id')['order_id'].transform('count')

        ops_cols = {
            'shipping_cost': 'shipping_cost_usd',
            'shipping_revenue': 'shipping_revenue_usd',
            'fc_variable_headcount': 'fc_variable_usd',
            'cs_variable_headcount': 'cs_variable_usd'
        }
        
        for raw_col, final_col in ops_cols.items():
            df['temp_raw_total'] = df[raw_col] * df['quantity']
            total_cohort_cost = df.groupby('cohort')['temp_raw_total'].transform('sum')
            cost_per_order = total_cohort_cost / orders_per_cohort
            df[final_col] = cost_per_order / items_per_order
            
        df = df.drop(columns=['temp_raw_total'])
        
        # Asegurar que los costos sean negativos (restan del CP)
        for col in ['shipping_cost_usd', 'fc_variable_usd', 'cs_variable_usd']:
            if col in df.columns:
                df[col] = -df[col].abs()
        
        print(f"⏱️  Sub-fase 3 (Distribución): {time.time() - t2:.2f}s")

        # --- SUB-FASE 4: CONTRIBUTION PROFIT ---
        t3 = time.time()

        df['credit_card_cost'] = df['revenue'] * df['credit_card_payment']
        df['cod_cost'] = df['revenue'] * df['cash_on_delivery_comision']
        df['fraud_cost'] = df['revenue'] * df['fraud']
        df['infra_cost'] = df['revenue'] * df['infrastructure']

        df['contribution_profit'] = (
            df['revenue'] + df['shipping_revenue_usd'] + df['sois']
            - df['base_cost'] + df['shipping_cost_usd']
            + df['credit_card_cost'] + df['cod_cost'] + df['fraud_cost'] + df['infra_cost']
            + df['fc_variable_usd'] + df['cs_variable_usd']
            - df['retention_cost_$']
        )

        print(f"📊 CONTRIBUTION PROFIT EN USD:")
        print(f"   Promedio: ${df['contribution_profit'].mean():.2f}")
        print(f"   Total: ${df['contribution_profit'].sum():,.2f}")
        print(f"⏱️  Sub-fase 4 (CP): {time.time() - t3:.2f}s")

        # Limpiar columnas auxiliares
        if '_fx_rate' in df.columns:
            df = df.drop(columns=['_fx_rate'])

        total_time = time.time() - start_total
        print("-" * 60)
        print(f"✅ METRICS CALCULATOR FINALIZADO EN {total_time:.2f}s")
        print("-" * 60)
        
        return df