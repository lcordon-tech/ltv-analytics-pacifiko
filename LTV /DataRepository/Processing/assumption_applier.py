import pandas as pd
import numpy as np


class AssumptionApplier:
    """
    Responsabilidad: Sincronizar órdenes con supuestos financieros (1P, 3P, FBP, TM, DS).
    """
    
    CRITICAL_COLS = [
        'shipping_cost', 'shipping_revenue', 'credit_card_payment',
        'cash_on_delivery_comision', 'fc_variable_headcount',
        'cs_variable_headcount', 'fraud', 'infrastructure'
    ]

    def apply(self, df: pd.DataFrame, assumptions_dict: dict) -> pd.DataFrame:
        print("\n" + "="*60)
        print(" APLICANDO SUPUESTOS Y NORMALIZACIÓN HISTÓRICA ".center(60))
        print("="*60)

        # Preparación
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['cohort'] = df['cohort'].astype(str).str.strip().str.upper()

        # Ajustes pre-2021
        mask_pre_2021 = df['order_date'] < '2020-01-01'
        if mask_pre_2021.any():
            mask_1p_ds = mask_pre_2021 & df['b_unit'].isin(['1P', 'DS'])
            df.loc[mask_1p_ds, 'cohort'] = 'Q1'
            mask_3p_fbp = mask_pre_2021 & df['b_unit'].isin(['3P', 'FBP'])
            df.loc[mask_3p_fbp, 'cohort'] = 'Q9'
            mask_tm = mask_pre_2021 & (df['b_unit'] == 'TM')
            df.loc[mask_tm, 'cohort'] = 'Q8'
            print(f"⚠️ AJUSTE PRE-MODELO: {mask_pre_2021.sum()} registros reasignados.")

        # Ajustes estratégicos
        def extract_cohort_num(c):
            try:
                return int(str(c).replace('Q', ''))
            except:
                return 0
        
        cohort_num = df['cohort'].apply(extract_cohort_num)

        mask_early_3p = (df['b_unit'] == '3P') & (cohort_num < 9)
        if mask_early_3p.any():
            valid_3p = (df['b_unit'] == '3P') & (cohort_num >= 9)
            avg_comm = df.loc[valid_3p, 'commission_percent'].mean() if valid_3p.any() else 0.9
            df.loc[mask_early_3p, 'commission_percent'] = avg_comm if not pd.isna(avg_comm) else 0.9
            df.loc[mask_early_3p, 'cohort'] = 'Q9'
            print(f"⚠️ AJUSTE 3P: {mask_early_3p.sum()} filas normalizadas.")

        mask_early_fbp = (df['b_unit'] == 'FBP') & (cohort_num < 9)
        if mask_early_fbp.any():
            df.loc[mask_early_fbp, 'cohort'] = 'Q9'
            print(f"⚠️ AJUSTE FBP: {mask_early_fbp.sum()} filas normalizadas.")

        mask_early_tm = (df['b_unit'] == 'TM') & (cohort_num < 8)
        if mask_early_tm.any():
            df.loc[mask_early_tm, 'cohort'] = 'Q8'
            print(f"⚠️ AJUSTE TM: {mask_early_tm.sum()} filas normalizadas.")

        # Procesar cada BU
        required_units = ["1P", "3P", "FBP", "TM", "DS"]
        df_final_list = []

        for unit in required_units:
            df_unit = df[df['b_unit'] == unit].copy()
            if df_unit.empty:
                continue

            if unit not in assumptions_dict:
                print(f"⚠️ BU '{unit}': sin supuestos. Valores en 0.")
                df_final_list.append(df_unit)
                continue

            df_assump = assumptions_dict[unit].copy()
            
            # Verificar columna cohort
            if 'cohort' not in df_assump.columns:
                print(f"❌ BU '{unit}': columna 'cohort' no encontrada. Valores en 0.")
                df_final_list.append(df_unit)
                continue

            df_assump['cohort'] = df_assump['cohort'].astype(str).str.strip().str.upper()
            
            # Seleccionar columnas (incluyendo cohort)
            cols_to_keep = ['cohort'] + [c for c in self.CRITICAL_COLS if c in df_assump.columns]
            df_assump = df_assump[cols_to_keep].sort_values('cohort').ffill()
            
            # Merge
            df_merged = pd.merge(df_unit, df_assump, on='cohort', how='left')

            # Después del merge
            print(f"   📊 BU '{unit}': valores de shipping_cost después del merge:")
            print(f"      min={df_merged['shipping_cost'].min():.4f}, max={df_merged['shipping_cost'].max():.4f}, sum={df_merged['shipping_cost'].sum():.4f}")
            
            # Rellenar nulos
            for col in self.CRITICAL_COLS:
                if col in df_merged.columns:
                    df_merged[col] = df_merged[col].fillna(0.0)
                else:
                    df_merged[col] = 0.0
            
            df_final_list.append(df_merged)

        # Consolidar
        df_final = pd.concat(df_final_list, ignore_index=True) if df_final_list else df

        # OTROS: heredan de 1P
        df_others = df[~df['b_unit'].isin(required_units)].copy()
        if not df_others.empty:
            print(f"⚠️ RESIDUOS: {len(df_others)} filas de 'OTROS' heredan supuestos de 1P.")
            
            df_assump_base = assumptions_dict.get('1P', pd.DataFrame()).copy()
            if not df_assump_base.empty and 'cohort' in df_assump_base.columns:
                df_assump_base['cohort'] = df_assump_base['cohort'].astype(str).str.strip().str.upper()
                # 🔧 FIX: incluir 'cohort' en la selección
                cols_to_use = ['cohort'] + [c for c in self.CRITICAL_COLS if c in df_assump_base.columns]
                df_others = pd.merge(df_others, df_assump_base[cols_to_use], on='cohort', how='left')
                
                for col in self.CRITICAL_COLS:
                    if col in df_others.columns:
                        df_others[col] = df_others[col].fillna(0.0)
                    else:
                        df_others[col] = 0.0
            else:
                for col in self.CRITICAL_COLS:
                    df_others[col] = 0.0
            
            df_final = pd.concat([df_final, df_others], ignore_index=True)

        # Validación final
        print("\n🔍 COBERTURA DE SUPUESTOS:")
        for col in self.CRITICAL_COLS:
            if col in df_final.columns:
                pct = (df_final[col] != 0).mean() * 100
                print(f"   • {col}: {pct:.1f}%")

        print("-" * 60)
        print("✅ AssumptionApplier finalizado.")
        return df_final