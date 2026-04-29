"""
Exportador de Dashboard ROBUSTO.
VERSIÓN MEJORADA: Soporta dimensiones dinámicas (Category, Subcategory, Brand, Product).
"""

import pandas as pd
import os


class CategoryDashboardExporter:
    """
    Exportador de dashboard que soporta dimensiones dinámicas.
    """
    
    # Definición de métricas a rankear
    METRICS = {
        "Frecuencia": "Frequency_Score",
        "Velocidad": "Time_Score",
        "Conversión": "Conversion_Score",
        "LTV": "LTV_Score",
        "Final": "Final_Score"
    }
    
    def __init__(self, dashboard_data: dict, summary_df: pd.DataFrame = None,
                 dimension_name: str = "Categoria"):
        """
        Args:
            dashboard_data: Datos del dashboard (data_points)
            summary_df: DataFrame con scores agregados (opcional)
            dimension_name: Nombre de la columna de dimensión ("Categoria", "Subcategoria", "Brand", "Producto")
        """
        self.data = dashboard_data
        self.summary_df = summary_df
        self.dimension_name = dimension_name
    
    def _get_aggregated_data(self) -> pd.DataFrame:
        """Extrae y combina datos del dashboard_data y summary_df."""
        dashboard_records = []
        
        for section in ['frequency', 'time', 'conversion']:
            if section not in self.data:
                continue
            
            section_data = self.data[section]
            if isinstance(section_data, dict):
                for metric, analysis in section_data.items():
                    if isinstance(analysis, dict) and 'data_points' in analysis:
                        data_points = analysis['data_points']
                        if data_points:
                            dashboard_records.extend(data_points)
        
        if dashboard_records:
            df_dashboard = pd.DataFrame(dashboard_records)
            print(f"📊 Datos de dashboard: {len(df_dashboard)} filas")
        else:
            df_dashboard = pd.DataFrame()
        
        if self.summary_df is None or self.summary_df.empty:
            if not df_dashboard.empty:
                return df_dashboard
            return pd.DataFrame()
        
        print(f"📊 Datos de summary: {len(self.summary_df)} filas")
        
        if df_dashboard.empty:
            return self.summary_df
        
        # Buscar columna común
        common_col = None
        for col in [self.dimension_name, 'Categoria', 'Subcategoria', 'Brand', 'Producto']:
            if col in df_dashboard.columns and col in self.summary_df.columns:
                common_col = col
                break
        
        if common_col:
            score_cols = [
                'Final_Score', 'LTV_Score', 'Performance_Score',
                'Confidence_Score', 'Global_Score', 'Frequency_Score',
                'Time_Score', 'Conversion_Score'
            ]
            existing_score_cols = [c for c in score_cols if c in self.summary_df.columns]
            
            if existing_score_cols:
                df_result = df_dashboard.merge(
                    self.summary_df[[common_col] + existing_score_cols],
                    on=common_col,
                    how='left'
                )
                print(f"   ✅ Merge completado: {len(df_result)} filas")
                return df_result
        
        return df_dashboard
    
    def _get_label(self, row: pd.Series) -> str:
        """Obtiene la etiqueta de la dimensión de una fila."""
        if self.dimension_name in row.index:
            val = row[self.dimension_name]
            if pd.notna(val) and str(val).strip() != "":
                return str(val)
        
        # Fallback: buscar en columnas alternativas
        for col in ['Subcategoria', 'Categoria', 'Brand', 'Producto']:
            if col in row.index:
                val = row[col]
                if pd.notna(val) and str(val).strip() != "":
                    return str(val)
        return "N/A"
    
    def _validate_required_columns(self, df: pd.DataFrame) -> bool:
        """Valida que todas las columnas necesarias existan."""
        missing_cols = []
        for metric_name, score_col in self.METRICS.items():
            if score_col not in df.columns:
                missing_cols.append(score_col)
        
        if missing_cols:
            print(f"⚠️ Faltan columnas requeridas: {missing_cols}")
            return False
        
        return True
    
    def get_top_bottom(self, df: pd.DataFrame, score_col: str,
                       n: int = 5) -> tuple:
        """Obtiene los top y bottom n registros por una columna de score."""
        valid_df = df[df[score_col].notna()].copy()
        
        if valid_df.empty:
            return pd.DataFrame(), pd.DataFrame()
        
        max_score = valid_df[score_col].max()
        min_score = valid_df[score_col].min()
        
        if max_score == min_score:
            return pd.DataFrame(), pd.DataFrame()
        
        top_df = valid_df.nlargest(n, score_col)[[self.dimension_name, score_col, 'Total_Clientes']].copy()
        bottom_df = valid_df.nsmallest(n, score_col)[[self.dimension_name, score_col, 'Total_Clientes']].copy()
        
        if not top_df.empty:
            top_df[score_col] = top_df[score_col].round(4)
        if not bottom_df.empty:
            bottom_df[score_col] = bottom_df[score_col].round(4)
        
        return top_df, bottom_df
    
    def _format_ranking_table(self, df: pd.DataFrame, title: str,
                               score_col: str, is_top: bool = True, n: int = 5) -> str:
        """Formatea una tabla de ranking como texto."""
        if df.empty:
            emoji = "🏆" if is_top else "💀"
            rank_type = "TOP" if is_top else "BOTTOM"
            return f"\n{emoji} {rank_type} {n} - {title.upper()}\n⚠️ No hay datos suficientes\n"
        
        emoji = "🏆" if is_top else "💀"
        rank_type = "TOP" if is_top else "BOTTOM"
        
        result = f"\n{emoji} {rank_type} {n} - {title.upper()}\n"
        result += "-" * 65 + "\n"
        result += f"{self.dimension_name:<30} | {'Score':<10} | {'Clientes':<10}\n"
        result += "-" * 65 + "\n"
        
        for _, row in df.iterrows():
            label = str(row[self.dimension_name])[:28]
            score = row[score_col]
            clientes = int(row['Total_Clientes']) if pd.notna(row['Total_Clientes']) else 0
            result += f"{label:<30} | {score:<10.4f} | {clientes:<10,}\n"
        
        result += "-" * 65 + "\n"
        return result
    
    def export_as_txt(self, filepath: str):
        """Exporta dashboard como TXT con rankings Top/Bottom 5."""
        df = self._get_aggregated_data()
        
        if df.empty:
            print("⚠️ No hay datos para exportar dashboard")
            return
        
        # Eliminar duplicados
        df = df.drop_duplicates(subset=[self.dimension_name])
        print(f"📊 Rankings: {len(df)} elementos únicos")
        
        if not self._validate_required_columns(df):
            print("❌ Faltan columnas necesarias")
            return
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("="*95 + "\n")
            f.write(f" 📊 DASHBOARD ESTRATÉGICO - {self.dimension_name.upper()}\n".center(95))
            f.write("="*95 + "\n")
            f.write(f"\n📌 Total de elementos analizados: {len(df)}\n")
            f.write("-"*95 + "\n\n")
            
            # TOP 5
            f.write("\n" + "⭐"*30 + "\n")
            f.write(" 🏆 RANKINGS TOP 5 - MEJORES POR MÉTRICA\n".center(95))
            f.write("⭐"*30 + "\n")
            
            for metric_name, score_col in self.METRICS.items():
                top_df, _ = self.get_top_bottom(df, score_col, n=5)
                f.write(self._format_ranking_table(top_df, metric_name, score_col, is_top=True, n=5))
            
            # BOTTOM 5
            f.write("\n" + "💀"*30 + "\n")
            f.write(" 🔻 RANKINGS BOTTOM 5 - PEORES POR MÉTRICA\n".center(95))
            f.write("💀"*30 + "\n")
            
            for metric_name, score_col in self.METRICS.items():
                _, bottom_df = self.get_top_bottom(df, score_col, n=5)
                f.write(self._format_ranking_table(bottom_df, metric_name, score_col, is_top=False, n=5))
            
            # Estadísticas
            f.write("\n" + "="*95 + "\n")
            f.write(" 📈 RESUMEN EJECUTIVO\n".center(95))
            f.write("="*95 + "\n")
            f.write("\n📊 Estadísticas generales de scores:\n")
            f.write("-"*50 + "\n")
            
            for metric_name, score_col in self.METRICS.items():
                if score_col in df.columns:
                    valid_scores = df[score_col][df[score_col].notna()]
                    if not valid_scores.empty:
                        f.write(f"\n{metric_name} ({score_col}):\n")
                        f.write(f"  • Promedio: {valid_scores.mean():.4f}\n")
                        f.write(f"  • Mediana:  {valid_scores.median():.4f}\n")
                        f.write(f"  • Máximo:   {valid_scores.max():.4f}\n")
                        f.write(f"  • Mínimo:   {valid_scores.min():.4f}\n")
            
            f.write("\n" + "="*95 + "\n")
            f.write(" ✅ Dashboard generado automáticamente\n".center(95))
            f.write("="*95 + "\n")
        
        print(f"📈 Dashboard TXT generado: {os.path.basename(filepath)}")