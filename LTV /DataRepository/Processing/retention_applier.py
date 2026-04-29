import pandas as pd
import numpy as np
from Run.Services.time_granularity_adapter import TimeGranularityAdapter
from DataRepository.Processing.fallback_utils import get_closest_cohort_value, log_fallback_stats


class RetentionApplier:
    
    def __init__(self, granularidad: str = 'quarterly', country_context=None):
        self.granularidad = granularidad
        # 🔧 Pasar country_context al adapter
        self.adapter = TimeGranularityAdapter(granularidad, country_context=country_context)
        self.fallback_stats = {}
    
    def _load_retention_from_assumptions(self, assumptions_dict: dict) -> dict:
        """Carga retention desde CUALQUIER pestaña que tenga la columna."""
        retention_data = {}
        
        if not assumptions_dict:
            return retention_data
        
        for sheet_name, df_sheet in assumptions_dict.items():
            if 'cohort' in df_sheet.columns and 'retention' in df_sheet.columns:
                df_sheet['cohort'] = df_sheet['cohort'].astype(str).str.strip().str.upper()
                
                for _, row in df_sheet.iterrows():
                    cohort = row['cohort']
                    try:
                        value = float(row['retention'])
                        if cohort not in retention_data:
                            retention_data[cohort] = value
                    except (ValueError, TypeError):
                        pass
        
        return retention_data
    
    def apply(self, df: pd.DataFrame, assumptions_dict: dict = None) -> pd.DataFrame:
        """Aplica retención usando fallback dinámico."""
        print("\n" + "="*60)
        print(f" APLICANDO RETENCIÓN ({self.granularidad.upper()}) ".center(60))
        print(" SIN FALLBACKS HARDCODEADOS ".center(60))
        print("="*60)

        if df.empty:
            return df
        
        # 1. Cargar retention desde Excel (única fuente)
        retention_from_excel = {}
        if assumptions_dict:
            retention_from_excel = self._load_retention_from_assumptions(assumptions_dict)
        
        if retention_from_excel:
            print(f"✅ Cargados {len(retention_from_excel)} valores de retention desde Excel")
        else:
            print("⚠️ No se encontraron valores de retention en Excel")
        
        # 2. Transformar según granularidad (sin fallbacks)
        transformed_retention, _ = self.adapter.transform(retention_from_excel, {})
        
        # 3. Aplicar fallback dinámico para cohortes faltantes
        self.fallback_stats = {}
        df_cohorts = df['cohort'].unique()
        
        final_retention_map = {}
        for cohort in df_cohorts:
            if cohort in transformed_retention:
                final_retention_map[cohort] = transformed_retention[cohort]
            else:
                # Fallback dinámico
                closest_value = get_closest_cohort_value(cohort, transformed_retention)
                final_retention_map[cohort] = closest_value
                if closest_value != 0:
                    self.fallback_stats[cohort] = "dynamic_fallback"
        
        # 4. Logging de fallback
        missing_cohorts = [c for c in df_cohorts if c not in transformed_retention]
        if missing_cohorts:
            print(f"\n⚠️ {len(missing_cohorts)} cohortes sin retention definida en Excel")
            print(f"   → Usando fallback dinámico (búsqueda por cercanía)")
        
        log_fallback_stats(self.fallback_stats, len(df_cohorts))
        
        # --- LÓGICA ORIGINAL DE RETENCIÓN ---
        df = self._add_cohort_numeric_order(df)
        
        df['birth_cohort_order'] = df.groupby('customer_id')['cohort_order'].transform('min')
        df['is_recurrence'] = df['cohort_order'] > df['birth_cohort_order']

        df['retention_budget'] = df['cohort'].map(final_retention_map).fillna(0.0)
        df.loc[~df['is_recurrence'], 'retention_budget'] = 0.0

        orders_in_cohort = df[df['is_recurrence']].groupby(['customer_id', 'cohort'])['order_id'].transform('nunique')
        items_in_order = df.groupby('order_id')['order_id'].transform('count')

        df['retention_cost_$'] = 0.0
        mask = df['is_recurrence']
        
        df.loc[mask, 'retention_cost_$'] = (
            df.loc[mask, 'retention_budget'] / orders_in_cohort
        ) / items_in_order

        if df['retention_cost_$'].isna().any():
            df['retention_cost_$'] = df['retention_cost_$'].fillna(0.0)

        total_spent = df['retention_cost_$'].sum()
        print(f"\n✅ Análisis de retención finalizado.")
        print(f"📈 Total invertido: ${total_spent:,.2f}")
        
        cols_to_drop = ['cohort_order', 'birth_cohort_order', 'is_recurrence', 'retention_budget']
        return df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    def _add_cohort_numeric_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """Agrega un orden numérico a las cohortes para comparación."""
        unique_cohorts = sorted(df['cohort'].unique())
        cohort_to_order = {cohort: i for i, cohort in enumerate(unique_cohorts)}
        df['cohort_order'] = df['cohort'].map(cohort_to_order)
        return df