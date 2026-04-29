import pandas as pd
import numpy as np
from typing import List, Dict, Any

# ==============================================================================
# CLASE: METRICS QUALITY ANALYZER (PENALIZACIÓN AGRESIVA)
# VERSIÓN: 5.1 - CON FINAL_SCORE Y COLUMNAS CRÍTICAS
# ==============================================================================

class MetricsQualityAnalyzer:
    """
    Sistema de Scoring con Penalización Agresiva por Muestra Pequeña.
    
    REGLAS:
        1. Mínimo N clientes para ser considerado confiable (configurable)
        2. Menos de N → penalización cuadrática: (n/N)²
        3. Score final = Performance_raw × Penalty × Confidence
    """

    # Umbral mínimo de clientes para considerar una categoría confiable
    # AJUSTA ESTE VALOR SEGÚN TU NECESIDAD:
    # - 30: muy permisivo (categorías con 30 clientes tienen score completo)
    # - 100: moderado
    # - 500: agresivo
    # - 1000: extremo (solo categorías gigantes tienen score alto)
    MIN_CONFIDENCE_CLIENTS = 100  # ← CAMBIA ESTO SEGÚN NECESITES
    
    @classmethod
    def evaluate_all(cls, all_metrics_list: List[Dict]) -> List[Dict]:
        if not all_metrics_list:
            return []

        df = pd.DataFrame(all_metrics_list)

        # ==============================================================
        # 1. PREPARACIÓN: Buckets de AOV para comparación justa
        # ==============================================================
        try:
            df['Quality_Bucket'] = pd.qcut(
                df['AOV_Ref'], 
                q=[0, 0.33, 0.66, 1.0], 
                labels=['LOW_TICKET', 'MID_TICKET', 'HIGH_TICKET']
            )
        except ValueError:
            df['Quality_Bucket'] = 'GENERAL'

        # ==============================================================
        # 2. MÉTRICAS BASE: Configuración
        # ==============================================================
        config = {
            "Pct_2da_Compra": "desc",
            "Pct_3ra_Compra": "desc",
            "Pct_4ta_Compra": "desc",
            "Mediana_Dias_1a2": "asc",
            "Mediana_Dias_2a3": "asc",
            "Mediana_Dias_3a4": "asc",
            "Pct_Conv_30d": "desc",
            "Pct_Conv_60d": "desc",
            "Pct_Conv_90d": "desc",
            "Pct_Conv_180d": "desc",
            "Pct_Conv_360d": "desc"
        }

        # ==============================================================
        # 3. LIMPIEZA DE DATOS
        # ==============================================================
        df = cls._clean_invalid_metrics(df, config)

        # ==============================================================
        # 4. SCORES POR MÉTRICA: Percentiles crudos
        # ==============================================================
        for metric, direction in config.items():
            if metric not in df.columns:
                continue
            
            raw_score_key = f"{metric}_Score_Raw"
            final_score_key = f"{metric}_Score"
            label_key = f"{metric}_Quality"

            # Calcular percentil crudo
            df[raw_score_key] = df.groupby('Quality_Bucket')[metric].rank(pct=True)
            
            if direction == "asc":
                df[raw_score_key] = 1 - df[raw_score_key]
            
            df[raw_score_key] = df[raw_score_key].round(2)
            
            # APLICAR PENALIZACIÓN POR MUESTRA PEQUEÑA
            df[final_score_key] = cls._apply_sample_penalty(df, df[raw_score_key])
            df[label_key] = df[final_score_key].apply(cls._get_performance_label)

        # ==============================================================
        # 5. CONFIDENCE SCORE (basado en percentil de clientes)
        # ==============================================================
        min_c = df['Total_Clientes'].min()
        max_c = df['Total_Clientes'].max()
        
        if max_c > min_c:
            df['Confidence_Score'] = (df['Total_Clientes'] - min_c) / (max_c - min_c)
        else:
            df['Confidence_Score'] = 1.0
        
        df['Confidence_Score'] = df['Confidence_Score'].clip(0, 1).round(4)

        # ==============================================================
        # 6. PERFORMANCE SCORE (promedio de métricas YA penalizadas)
        # ==============================================================
        metric_scores = [f"{metric}_Score" for metric in config.keys() if f"{metric}_Score" in df.columns]
        
        df['Performance_Score'] = df[metric_scores].mean(axis=1).fillna(0).round(4)

        # ==============================================================
        # 7. GLOBAL SCORE: Performance × Confidence
        # ==============================================================
        df['Global_Score'] = (df['Performance_Score'] * df['Confidence_Score']).round(4)
        
        # 🔥 NUEVO: Final_Score (alias para compatibilidad con exporters)
        df['Final_Score'] = df['Global_Score'].copy()
        
        df['Global_Quality'] = df['Global_Score'].apply(cls._get_global_label)

        # ==============================================================
        # 8. LTV SCORE (si existe LTV_Promedio_Cliente_$)
        # ==============================================================
        if 'LTV_Promedio_Cliente_$' in df.columns:
            ltv_values = df['LTV_Promedio_Cliente_$'].fillna(0).clip(lower=0)
            if ltv_values.max() > 0 and ltv_values.nunique() > 1:
                df['LTV_Score'] = ltv_values.rank(pct=True).round(4)
            elif ltv_values.max() > 0:
                df['LTV_Score'] = 1.0
            else:
                df['LTV_Score'] = 0.0
        else:
            df['LTV_Score'] = 0.0

        # ==============================================================
        # 9. MÉTRICAS DE DIAGNÓSTICO
        # ==============================================================
        df['Sample_Penalty'] = cls._calculate_sample_penalty_factor(df)
        df['Sample_Quality'] = df['Total_Clientes'].apply(cls._get_sample_quality)

        # ==============================================================
        # 10. GARANTIZAR QUE TODAS LAS COLUMNAS CRÍTICAS EXISTEN
        # ==============================================================
        # Esto evita errores en exporters
        for col in ['Final_Score', 'Confidence_Score', 'Performance_Score', 'LTV_Score']:
            if col not in df.columns:
                df[col] = 0.0

        return df.to_dict(orient='records')

    # =========================================================================
    # NÚCLEO DE LA PENALIZACIÓN
    # =========================================================================

    @classmethod
    def _apply_sample_penalty(cls, df: pd.DataFrame, scores: pd.Series) -> pd.Series:
        """
        Aplica penalización cuadrática a los scores basada en tamaño de muestra.
        
        Fórmula: score_penalizado = score_raw × (clientes / MIN_CONFIDENCE_CLIENTS)²
        
        Si clientes >= MIN_CONFIDENCE_CLIENTS → sin penalización (factor = 1)
        Si clientes = 0 → score = 0
        """
        n_clientes = df['Total_Clientes']
        
        # Calcular factor de penalización
        penalty_factor = np.where(
            n_clientes < cls.MIN_CONFIDENCE_CLIENTS,
            (n_clientes / cls.MIN_CONFIDENCE_CLIENTS) ** 2,
            1.0
        )
        
        # Aplicar penalización
        penalized = scores * penalty_factor
        
        return penalized.round(4)

    @classmethod
    def _calculate_sample_penalty_factor(cls, df: pd.DataFrame) -> pd.Series:
        """Calcula el factor de penalización aplicado (para diagnóstico)."""
        n_clientes = df['Total_Clientes']
        
        penalty_factor = np.where(
            n_clientes < cls.MIN_CONFIDENCE_CLIENTS,
            (n_clientes / cls.MIN_CONFIDENCE_CLIENTS) ** 2,
            1.0
        )
        
        return penalty_factor.round(4)

    # =========================================================================
    # MÉTODOS AUXILIARES
    # =========================================================================

    @staticmethod
    def _clean_invalid_metrics(df: pd.DataFrame, config: Dict) -> pd.DataFrame:
        """Limpia métricas sin suficiente muestra."""
        df_clean = df.copy()
        
        for metric in config.keys():
            if metric not in df_clean.columns:
                continue
            
            if "Mediana_Dias" in metric:
                suffix = metric.split('_')[-1]
                sample_col = f"Muestra_{suffix}"
                
                if sample_col in df_clean.columns:
                    invalid_mask = (df_clean[sample_col] < 3) | (df_clean[sample_col].isna())
                    df_clean.loc[invalid_mask, metric] = np.nan
            
            elif "Pct_" in metric:
                if "Total_Clientes" in df_clean.columns:
                    invalid_mask = df_clean['Total_Clientes'] < 3
                    df_clean.loc[invalid_mask, metric] = np.nan
        
        return df_clean

    @staticmethod
    def _get_performance_label(score: float) -> str:
        if pd.isna(score):
            return "N/A"
        if score >= 0.8:
            return "Excelente"
        if score >= 0.6:
            return "Bueno"
        if score >= 0.4:
            return "Regular"
        if score >= 0.2:
            return "Bajo"
        return "Crítico"

    @staticmethod
    def _get_global_label(score: float) -> str:
        if pd.isna(score):
            return "Sin Datos"
        if score >= 0.8:
            return "Excelente"
        if score >= 0.6:
            return "Bueno"
        if score >= 0.4:
            return "Regular"
        if score >= 0.2:
            return "Bajo"
        return "Crítico"

    @classmethod
    def _get_sample_quality(cls, total_clientes: int) -> str:
        """Diagnóstico de calidad de muestra (NO afecta scores)."""
        if total_clientes >= 5000:
            return "MUY_ALTA"
        elif total_clientes >= 1000:
            return "ALTA"
        elif total_clientes >= 500:
            return "MEDIA"
        elif total_clientes >= cls.MIN_CONFIDENCE_CLIENTS:
            return "MÍNIMA_CONFIANZA"
        elif total_clientes >= 50:
            return "MUY_BAJA"
        elif total_clientes >= 10:
            return "CRÍTICA"
        else:
            return "SIN_DATOS"

    # =========================================================================
    # MÉTODOS LEGACY
    # =========================================================================

    @staticmethod
    def calculate_aov(customers: List[Any]) -> float:
        total_rev = sum(c.total_revenue() for c in customers)
        total_orders = sum(c.total_orders() for c in customers)
        return round(total_rev / total_orders, 2) if total_orders > 0 else 0

    @staticmethod
    def get_label(score: float) -> str:
        return MetricsQualityAnalyzer._get_performance_label(score)

    @classmethod
    def export_summary_log(cls, all_evals: List[Dict], filename: str = "summary_health.txt"):
        with open(filename, "w", encoding="utf-8") as f:
            f.write("📊 REPORTE DE SCORING - PENALIZACIÓN AGRESIVA\n")
            f.write("="*60 + "\n")
            f.write(f"UMBRAL MÍNIMO DE CONFIANZA: {cls.MIN_CONFIDENCE_CLIENTS} clientes\n")
            f.write("PENALIZACIÓN: (clientes/umbral)² para muestras pequeñas\n")
            f.write("="*60 + "\n\n")
            
            for ev in all_evals:
                if ev.get("Tag") != "General":
                    continue
                
                categoria = ev.get('Categoria', ev.get('Subcategoria', 'N/A'))
                n = ev.get('Total_Clientes', 0)
                penalty = ev.get('Sample_Penalty', 1.0)
                
                f.write(f"\n📌 {categoria}\n")
                f.write(f"   Clientes: {n:,}\n")
                f.write(f"   Penalización aplicada: {penalty:.3f}\n")
                f.write(f"   Performance (penalizada): {ev.get('Performance_Score', 0):.3f}\n")
                f.write(f"   Confidence: {ev.get('Confidence_Score', 0):.3f}\n")
                f.write(f"   → FINAL SCORE: {ev.get('Final_Score', ev.get('Global_Score', 0)):.3f}\n")
                f.write("-"*40 + "\n")