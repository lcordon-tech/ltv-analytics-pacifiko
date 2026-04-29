import pandas as pd
import numpy as np
from scipy import stats
from typing import Dict, Any, List, Optional


class CategoryDashboardCalculator:
    """
    Dashboard Calculator MULTI-DIMENSIÓN.
    Soporta: Categoria, Subcategoria, Brand, Producto, Subcategoria_Marca.
    
    VERSIÓN CORREGIDA: Detecta automáticamente la columna de dimensión.
    """

    def __init__(self, category_results: Dict[str, Any]):
        self.results = category_results
        self.metrics_map = {
            "frequency": ["Pct_2da_Compra", "Pct_3ra_Compra", "Pct_4ta_Compra"],
            "time": ["Mediana_Dias_1a2", "Mediana_Dias_2a3", "Mediana_Dias_3a4"],
            "conversion": ["Pct_Conv_30d", "Pct_Conv_90d", "Pct_Conv_360d"]
        }
        
        # Posibles nombres de columnas de dimensión (orden de preferencia)
        self.dimension_columns = ['Subcategoria_Marca', 'Subcategoria', 'Categoria', 'Brand', 'Producto']

    def _get_dimension_column(self, df: pd.DataFrame) -> Optional[str]:
        """
        Detecta qué columna contiene la dimensión (Categoria, Subcategoria, Brand, Producto, etc.)
        Returns: nombre de la columna o None
        """
        for col in self.dimension_columns:
            if col in df.columns:
                return col
        
        # Fallback: buscar cualquier columna que contenga palabras clave
        keywords = ['categoria', 'subcategoria', 'brand', 'producto', 'marca']
        for col in df.columns:
            col_lower = col.lower()
            for kw in keywords:
                if kw in col_lower:
                    return col
        
        return None

    def _get_dimension_value(self, record: Dict, dim_col: str) -> str:
        """
        Obtiene el valor de la dimensión de un registro.
        """
        # Intentar obtener de la columna detectada
        if dim_col in record:
            val = record.get(dim_col)
            if val and str(val).strip() not in ["", "N/A", "nan", "None"]:
                return str(val)
        
        # Fallback: buscar en columnas alternativas
        for col in self.dimension_columns:
            if col in record:
                val = record.get(col)
                if val and str(val).strip() not in ["", "N/A", "nan", "None"]:
                    return str(val)
        
        return "N/A"

    def run(self) -> Dict[str, Any]:
        dashboard_data = {}
        
        # DEBUG: Ver qué hay en self.results
        for section in ['frequency', 'time', 'conversion']:
            if section in self.results:
                historical = self.results[section].get("historical", [])
                print(f"🔍 {section}: {len(historical)} registros en historical")
                if historical:
                    print(f"   Columnas en primer registro: {list(historical[0].keys())[:10]}")
            else:
                print(f"⚠️ {section}: No está en self.results")
        
        # Detectar la columna de dimensión desde la primera sección con datos
        detected_dim_col = None
        for section in ['frequency', 'time', 'conversion']:
            if section in self.results:
                historical = self.results[section].get("historical", [])
                if historical:
                    df_temp = pd.DataFrame(historical[:5])  # Solo primeras filas para detectar
                    detected_dim_col = self._get_dimension_column(df_temp)
                    if detected_dim_col:
                        print(f"🔍 Dimensión detectada: '{detected_dim_col}'")
                        break
        
        for section, metrics in self.metrics_map.items():
            if section not in self.results or "historical" not in self.results[section]:
                continue

            raw_data = self.results[section]["historical"]
            df = pd.DataFrame(raw_data)
            
            if df.empty: 
                continue
            
            # Detectar columna de dimensión (usar la ya detectada o buscar de nuevo)
            dim_col = detected_dim_col or self._get_dimension_column(df)
            if dim_col is None:
                print(f"⚠️ {section}: No se encontró columna de dimensión. Columnas: {list(df.columns)}")
                # Crear columna por defecto
                df['Dimension'] = 'GENERAL'
                dim_col = 'Dimension'
            
            print(f"   📊 {section} - Usando columna de dimensión: '{dim_col}'")
            
            section_analysis = {}
            for metric in metrics:
                score_col = f"{metric}_Score"
                
                if metric not in df.columns or score_col not in df.columns:
                    continue
                
                required_cols = [dim_col, "AOV_Ref", metric, score_col, "Total_Clientes"]
                available_for_points = [c for c in required_cols if c in df.columns]
                
                try:
                    section_analysis[metric] = {
                        "rankings": self._get_rankings(df, metric, dim_col),
                        "stats": self._calculate_regression(df, "AOV_Ref", metric),
                        "data_points": df[available_for_points].to_dict(orient="records"),
                    }
                except Exception as e:
                    print(f"⚠️ Error en métrica {metric}: {e}")
                    continue
            
            if section_analysis:
                dashboard_data[section] = section_analysis
            
        # GLOBAL: Usar la dimensión detectada
        dashboard_data["global"] = self._get_global_rankings(detected_dim_col)
            
        return dashboard_data

    def _get_rankings(self, df: pd.DataFrame, metric: str, dim_col: str = None) -> Dict[str, Any]:
        """
        Genera rankings de forma SEGURA.
        
        Args:
            df: DataFrame con datos
            metric: Nombre de la métrica
            dim_col: Nombre de la columna de dimensión
        """
        score_col = f"{metric}_Score"
        
        # Auto-detectar columna de dimensión si no se proporcionó
        if dim_col is None:
            dim_col = self._get_dimension_column(df)
            if dim_col is None:
                dim_col = "Dimension"
                if dim_col not in df.columns:
                    df[dim_col] = "GENERAL"
        
        is_time = "Mediana_Dias" in metric
        sample_col = f"Muestra_{metric.split('_')[-1]}" if is_time else "Total_Clientes"
        val_ascending = True if is_time else False

        # Inicializar con DataFrames vacíos
        top_score = pd.DataFrame()
        bottom_score = pd.DataFrame()
        top_val = pd.DataFrame()
        bottom_val = pd.DataFrame()

        # Rankings por Score
        if score_col in df.columns:
            top_score = df.nlargest(min(5, len(df)), score_col).copy()
            bottom_score = df.nsmallest(min(5, len(df)), score_col).copy()

        # Rankings por Valor
        clean_df = df.dropna(subset=[metric]) if metric in df.columns else pd.DataFrame()
        if not clean_df.empty:
            top_val = clean_df.sort_values(by=metric, ascending=val_ascending).head(5).copy()
            bottom_val = clean_df.sort_values(by=metric, ascending=not val_ascending).head(5).copy()
        
        # Columnas a incluir en los rankings
        target_cols = [dim_col, metric, score_col, "AOV_Ref", "Total_Clientes", sample_col]
        available_cols = [c for c in target_cols if c in df.columns]
        
        # Asegurar que dim_col esté en available_cols
        if dim_col not in available_cols and dim_col in df.columns:
            available_cols.insert(0, dim_col)
        
        result = {
            "by_score": {
                "top": top_score[available_cols] if not top_score.empty and all(c in top_score.columns for c in available_cols if c in top_score.columns) else pd.DataFrame(),
                "bottom": bottom_score[available_cols] if not bottom_score.empty and all(c in bottom_score.columns for c in available_cols if c in bottom_score.columns) else pd.DataFrame()
            },
            "by_value": {
                "top": top_val[available_cols] if not top_val.empty and all(c in top_val.columns for c in available_cols if c in top_val.columns) else pd.DataFrame(),
                "bottom": bottom_val[available_cols] if not bottom_val.empty and all(c in bottom_val.columns for c in available_cols if c in bottom_val.columns) else pd.DataFrame()
            }
        }
        
        return result

    def _get_global_rankings(self, dim_col: str = None) -> Dict[str, Any]:
        """Genera rankings globales de forma SEGURA usando la dimensión detectada."""
        all_scores = []
        
        # Si no se pasó dim_col, intentar detectar
        if dim_col is None:
            # Buscar en los datos
            for section in ["frequency", "time", "conversion"]:
                if section not in self.results:
                    continue
                historical = self.results[section].get("historical", [])
                if historical:
                    df_temp = pd.DataFrame(historical[:5])
                    dim_col = self._get_dimension_column(df_temp)
                    if dim_col:
                        break
        
        # Si aún no hay dim_col, usar valores por defecto
        if dim_col is None:
            dim_col = "Dimension"
        
        for section in ["frequency", "time", "conversion"]:
            if section not in self.results:
                continue
            
            historical = self.results[section].get("historical", [])
            for record in historical:
                # Obtener valor de la dimensión
                dim_value = self._get_dimension_value(record, dim_col)
                
                # Buscar cualquier score disponible
                score = record.get("Final_Score", record.get("Global_Score", record.get("Performance_Score", 0)))
                
                all_scores.append({
                    dim_col: dim_value,
                    "Global_Score": score,
                    "Total_Clientes": record.get("Total_Clientes", 0)
                })
        
        if not all_scores:
            return {"rankings": {"by_score": {"top": pd.DataFrame(), "bottom": pd.DataFrame()}}}
        
        df = pd.DataFrame(all_scores)
        df = df.drop_duplicates(subset=[dim_col])
        
        top_final = df.nlargest(min(10, len(df)), "Global_Score").copy()
        bottom_final = df.nsmallest(min(10, len(df)), "Global_Score").copy()
        
        return {
            "rankings": {
                "by_score": {
                    "top": top_final,
                    "bottom": bottom_final
                }
            }
        }

    def _calculate_regression(self, df: pd.DataFrame, x_col: str, y_col: str) -> Dict[str, float]:
        if x_col not in df.columns or y_col not in df.columns:
            return {"r_squared": 0, "slope": 0, "intercept": 0}

        clean_df = df[[x_col, y_col]].dropna()
        if len(clean_df) < 3:
            return {"r_squared": 0, "slope": 0, "intercept": 0}
            
        try:
            slope, intercept, r_value, p_value, std_err = stats.linregress(clean_df[x_col], clean_df[y_col])
            return {
                "r_squared": round(float(r_value**2), 4),
                "slope": round(float(slope), 6),
                "intercept": round(float(intercept), 4)
            }
        except:
            return {"r_squared": 0, "slope": 0, "intercept": 0}