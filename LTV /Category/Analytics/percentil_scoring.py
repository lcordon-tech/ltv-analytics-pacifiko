import pandas as pd
import numpy as np
from typing import List, Dict, Any
from .metrics_analyzer import MetricsQualityAnalyzer

class PercentileScoringSystem:
    """
    Sistema de scoring (wrapper de MetricsQualityAnalyzer).
    
    VERSIÓN CORREGIDA:
    - Propaga TODAS las columnas de MetricsQualityAnalyzer
    - NO sobrescribe columnas críticas
    - Añade scores por dimensión para compatibilidad
    """

    def calculate_scores(
        data: List[Dict[str, Any]], 
        metrics_to_score: Dict[str, str] = None  # ← opcional, no usado
    ) -> pd.DataFrame:
        """
        Calcula scores usando MetricsQualityAnalyzer.
        
        Args:
            data: Lista de diccionarios con métricas
            metrics_to_score: (legacy, no usado)
        
        Returns:
            DataFrame con TODAS las columnas de scoring
        """
        # ==============================================================
        # 1. OBTENER SCORES DEL MOTOR PRINCIPAL
        # ==============================================================
        result = MetricsQualityAnalyzer.evaluate_all(data)
        df = pd.DataFrame(result)
        
        if df.empty:
            return df
        
        # ==============================================================
        # 2. GARANTIZAR COLUMNAS CRÍTICAS (NO SOBRESCRIBIR)
        # ==============================================================
        # MetricsQualityAnalyzer YA genera:
        # - Final_Score
        # - Confidence_Score  
        # - Performance_Score
        # - LTV_Score
        # - Global_Score
        # - Global_Quality
        
        # Solo agregar si NO existen (fallback)
        if 'Final_Score' not in df.columns and 'Global_Score' in df.columns:
            df['Final_Score'] = df['Global_Score']
        
        if 'Base_Score' not in df.columns and 'Performance_Score' in df.columns:
            df['Base_Score'] = df['Performance_Score']
        
        if 'LTV_Score' not in df.columns:
            # Intentar calcular LTV_Score si existe LTV_Promedio_Cliente_$
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
        # 3. SCORES POR DIMENSIÓN (para dashboards legacy)
        # ==============================================================
        # Frequency Performance Score
        freq_cols = ['Pct_2da_Compra_Score', 'Pct_3ra_Compra_Score', 'Pct_4ta_Compra_Score']
        existing_freq = [c for c in freq_cols if c in df.columns]
        if existing_freq:
            df['Frequency_Performance_Score'] = df[existing_freq].mean(axis=1).fillna(0)
        else:
            df['Frequency_Performance_Score'] = 0.0
        
        # Time Performance Score
        time_cols = ['Mediana_Dias_1a2_Score', 'Mediana_Dias_2a3_Score', 'Mediana_Dias_3a4_Score']
        existing_time = [c for c in time_cols if c in df.columns]
        if existing_time:
            df['Time_Performance_Score'] = df[existing_time].mean(axis=1).fillna(0)
        else:
            df['Time_Performance_Score'] = 0.0
        
        # Conversion Performance Score
        conv_cols = ['Pct_Conv_30d_Score', 'Pct_Conv_60d_Score', 'Pct_Conv_90d_Score', 
                     'Pct_Conv_180d_Score', 'Pct_Conv_360d_Score']
        existing_conv = [c for c in conv_cols if c in df.columns]
        if existing_conv:
            df['Conversion_Performance_Score'] = df[existing_conv].mean(axis=1).fillna(0)
        else:
            df['Conversion_Performance_Score'] = 0.0
        
        # ==============================================================
        # 4. GLOBAL PERFORMANCE SCORE (si no existe)
        # ==============================================================
        if 'Performance_Score' not in df.columns:
            all_scores = existing_freq + existing_time + existing_conv
            if all_scores:
                df['Performance_Score'] = df[all_scores].mean(axis=1).fillna(0)
            else:
                df['Performance_Score'] = 0.0
        
        # ==============================================================
        # 5. REDONDEO Y LIMPIEZA
        # ==============================================================
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].round(4)
        
        # ==============================================================
        # 6. DEBUG: VERIFICAR COLUMNAS CRÍTICAS
        # ==============================================================
        critical_cols = ['Final_Score', 'Confidence_Score', 'Performance_Score', 'LTV_Score']
        missing = [c for c in critical_cols if c not in df.columns]
        if missing:
            print(f"⚠️ PercentileScoringSystem: Faltan columnas {missing}")
        
        return df

    @staticmethod
    def _get_label(score: float) -> str:
        """Legacy: mantenido para compatibilidad."""
        if pd.isna(score) or score is None:
            return "Sin Datos"
        if score >= 0.75:
            return "Excelente"
        if score >= 0.25:
            return "Regular"
        return "Malo"