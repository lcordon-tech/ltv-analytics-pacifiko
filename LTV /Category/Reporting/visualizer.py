import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import os
from datetime import datetime
from typing import Dict, Any, List, Optional


class CategoryVisualizer:
    """
    Clase especializada en transformar los insights del Dashboard 
    en representaciones visuales estratégicas.
    
    VERSIÓN MEJORADA v5.0 - SOPORTE MULTI-DIMENSIÓN:
    - Soporta Category, Subcategory, Brand, Product, Subcategoria_Marca
    - Frequency, Time, Conversion vs AOV y Clientes
    - LTV vs comportamiento
    - LTV vs Final Score
    """
    
    # Lista de posibles nombres de columnas de dimensión (orden de preferencia)
    DIMENSION_COLUMNS = ['Categoria', 'Subcategoria', 'Brand', 'Producto', 'Subcategoria_Marca']
    
    def __init__(self, dashboard_data: dict, output_dir: str, timestamp: str, summary_df: pd.DataFrame = None):
        """
        Args:
            dashboard_data: Datos del dashboard (data_points)
            output_dir: Directorio de salida para las gráficas
            timestamp: Timestamp para nombrar carpetas
            summary_df: DataFrame con scores agregados (opcional)
        """
        self.data = dashboard_data
        self.output_dir = output_dir
        self.timestamp = timestamp
        self.summary_df = summary_df
        
        # Colores para diferentes métricas
        self.colors = {
            'Pct_2da_Compra': '#1f77b4',
            'Pct_3ra_Compra': '#4a90e2',
            'Pct_4ta_Compra': '#90caf9',
            'Mediana_Dias_1a2': '#2ecc71',
            'Mediana_Dias_2a3': '#27ae60',
            'Mediana_Dias_3a4': '#a3e4d7',
            'Pct_Conv_30d': '#e74c3c',
            'Pct_Conv_60d': '#e67e22',
            'Pct_Conv_90d': '#f39c12',
            'Pct_Conv_180d': '#f1c40f',
            'Pct_Conv_360d': '#f9e79f'
        }

    def run(self, analysis_type: str = "category"):
        """
        Orquesta la generación de todas las gráficas estratégicas.
        
        Args:
            analysis_type: "category", "subcategory", "brand", "product", "subcategory_brand"
                           para nombrar carpeta y archivos
        """
        # Mapeo de analysis_type a nombres de carpeta
        folder_mapping = {
            "category": "Plots_Categorias",
            "subcategory": "Plots_Subcategorias",
            "brand": "Plots_Brands",
            "product": "Plots_Productos",
            "subcategory_brand": "Plots_Subcategoria_Marca"
        }
        
        # Determinar sufijo y nombre de carpeta según tipo de análisis
        folder_base = folder_mapping.get(analysis_type, "Plots_Dimension")
        folder_name = f"{folder_base}_{self.timestamp}"
        
        # Determinar sufijo para archivos
        suffix_mapping = {
            "category": "_category",
            "subcategory": "_subcategory",
            "brand": "_brand",
            "product": "_product",
            "subcategory_brand": "_subcategory_brand"
        }
        file_suffix = suffix_mapping.get(analysis_type, "_dimension")
        
        plot_folder = os.path.join(self.output_dir, folder_name)
        
        if not self.data and (self.summary_df is None or self.summary_df.empty):
            print("⚠️ No hay datos para generar visualizaciones")
            return

        if not os.path.exists(plot_folder):
            os.makedirs(plot_folder)

        print(f"\n🎨 Generando visualizaciones estratégicas en: {os.path.basename(plot_folder)}")
        print(f"   Tipo de análisis: {analysis_type.upper()}")
        
        sns.set_theme(style="whitegrid")
        plt.rcParams['figure.figsize'] = (12, 8)
        plt.rcParams['font.size'] = 10
        
        df = self._get_aggregated_data()
        
        if df.empty:
            print("⚠️ No hay datos para generar visualizaciones")
            return
        
        # Detectar la columna de dimensión activa
        self.dimension_col = self._detect_dimension_column(df)
        print(f"   Dimensión detectada: {self.dimension_col}")
        
        self._validate_required_columns(df)
        
        # Generar todas las gráficas requeridas
        self._create_behavior_plots(df, plot_folder, "frequency", file_suffix)
        self._create_behavior_plots(df, plot_folder, "time", file_suffix)
        self._create_behavior_plots(df, plot_folder, "conversion", file_suffix)
        self._create_ltv_vs_behavior_plots(df, plot_folder, file_suffix)
        self._create_ltv_vs_finalscore_plot(df, plot_folder, file_suffix)
        
        print(f"✅ Visualizaciones completadas en: {os.path.basename(plot_folder)}")
    
    def _detect_dimension_column(self, df: pd.DataFrame) -> str:
        """
        Detecta qué columna de dimensión está presente en el DataFrame.
        Retorna la primera columna encontrada en DIMENSION_COLUMNS.
        """
        for col in self.DIMENSION_COLUMNS:
            if col in df.columns:
                return col
        # Fallback: buscar cualquier columna que contenga palabras clave
        for col in df.columns:
            col_lower = col.lower()
            if any(dim.lower() in col_lower for dim in self.DIMENSION_COLUMNS):
                return col
        return "Dimension"
    
    def _get_aggregated_data(self) -> pd.DataFrame:
        """
        Extrae y combina datos del dashboard_data y summary_df.
        
        Soporta:
        - Modo normal (una columna de dimensión)
        - Modo dual (Subcategoria + Marca como dos columnas separadas)
        - Subcategoria_Marca (columna compuesta con formato "Subcategoria (Marca)")
        """
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
        
        # ==========================================================================
        # DETECTAR MODO DUAL (Subcategoria + Marca como columnas separadas)
        # ==========================================================================
        has_subcat = 'Subcategoria' in df_dashboard.columns and 'Subcategoria' in self.summary_df.columns
        has_brand = 'Marca' in df_dashboard.columns and 'Marca' in self.summary_df.columns
        
        if has_subcat and has_brand:
            print(f"   🔧 Modo dual detectado: Subcategoria + Marca")
            
            # Normalizar valores
            df_dashboard['Subcategoria'] = df_dashboard['Subcategoria'].astype(str).str.strip()
            df_dashboard['Marca'] = df_dashboard['Marca'].astype(str).str.strip()
            self.summary_df['Subcategoria'] = self.summary_df['Subcategoria'].astype(str).str.strip()
            self.summary_df['Marca'] = self.summary_df['Marca'].astype(str).str.strip()
            
            # Columnas de score que vienen del summary
            score_cols = [
                'Final_Score', 'LTV_Score', 'Performance_Score', 
                'Confidence_Score', 'Global_Score', 'Frequency_Score',
                'Time_Score', 'Conversion_Score', 
                'LTV_Promedio_Cliente_$',
                'LTV_Neto_Promedio_Cliente_$',
                'CAC_Promedio_$',
                'LTV_Acumulado_Total_$',
                'GMV_Total_$',
                'Total_Clientes'
            ]
            existing_score_cols = [c for c in score_cols if c in self.summary_df.columns]
            
            if existing_score_cols:
                df_result = df_dashboard.merge(
                    self.summary_df[['Subcategoria', 'Marca'] + existing_score_cols], 
                    on=['Subcategoria', 'Marca'], 
                    how='left'
                )
                print(f"   ✅ Merge completado (dual): {len(df_result)} filas")
                print(f"   Columnas añadidas: {existing_score_cols}")
                
                # Verificar columnas críticas
                for col in ['Final_Score', 'LTV_Promedio_Cliente_$']:
                    if col in df_result.columns:
                        non_null = df_result[col].notna().sum()
                        print(f"   📊 {col}: {non_null}/{len(df_result)} filas con datos")
                    else:
                        print(f"   ⚠️ {col}: no está en el resultado")
                
                return df_result
        
        # ==========================================================================
        # MODO NORMAL (una columna de dimensión)
        # ==========================================================================
        # Detectar columna común - INCLUYE Subcategoria_Marca
        common_col = None
        for col in self.DIMENSION_COLUMNS:
            if col in df_dashboard.columns and col in self.summary_df.columns:
                common_col = col
                break
        
        if common_col:
            print(f"   🔧 Columna común detectada: '{common_col}'")
            
            # Normalización según tipo de columna
            if common_col == 'Subcategoria_Marca':
                # Formato: "Subcategoria (Marca)" - unificar espacios alrededor de paréntesis
                df_dashboard[common_col] = (
                    df_dashboard[common_col]
                    .astype(str)
                    .str.strip()
                    .str.replace(r'\s+', ' ', regex=True)
                    .str.replace(r'\s*\(\s*', '(', regex=True)
                    .str.replace(r'\s*\)\s*', ')', regex=True)
                )
                self.summary_df[common_col] = (
                    self.summary_df[common_col]
                    .astype(str)
                    .str.strip()
                    .str.replace(r'\s+', ' ', regex=True)
                    .str.replace(r'\s*\(\s*', '(', regex=True)
                    .str.replace(r'\s*\)\s*', ')', regex=True)
                )
                print(f"   🔧 Normalización aplicada para '{common_col}'")
            else:
                # Normalización simple
                df_dashboard[common_col] = df_dashboard[common_col].astype(str).str.strip()
                self.summary_df[common_col] = self.summary_df[common_col].astype(str).str.strip()
            
            # DEBUG: Verificar coincidencias
            dashboard_vals = set(df_dashboard[common_col])
            summary_vals = set(self.summary_df[common_col])
            matching = dashboard_vals & summary_vals
            
            print(f"   🔍 Valores únicos en dashboard: {len(dashboard_vals)}")
            print(f"   🔍 Valores únicos en summary: {len(summary_vals)}")
            print(f"   🔍 Valores que coinciden: {len(matching)}")
            
            if len(matching) < len(dashboard_vals):
                missing_count = len(dashboard_vals) - len(matching)
                print(f"   ⚠️ {missing_count} valores NO coinciden")
                missing = list(dashboard_vals - summary_vals)[:3]
                for val in missing:
                    display_val = val[:50] + "..." if len(val) > 50 else val
                    print(f"      - '{display_val}'")
            
            # Columnas de score
            score_cols = [
                'Final_Score', 'LTV_Score', 'Performance_Score', 
                'Confidence_Score', 'Global_Score', 'Frequency_Score',
                'Time_Score', 'Conversion_Score', 
                'LTV_Promedio_Cliente_$',
                'LTV_Neto_Promedio_Cliente_$',
                'CAC_Promedio_$',
                'LTV_Acumulado_Total_$',
                'GMV_Total_$',
                'Total_Clientes'
            ]
            existing_score_cols = [c for c in score_cols if c in self.summary_df.columns]
            
            if existing_score_cols:
                df_result = df_dashboard.merge(
                    self.summary_df[[common_col] + existing_score_cols], 
                    on=common_col, 
                    how='left'
                )
                print(f"   ✅ Merge completado: {len(df_result)} filas")
                print(f"   Columnas añadidas: {existing_score_cols}")
                
                # Verificar columnas críticas
                for col in ['Final_Score', 'LTV_Promedio_Cliente_$']:
                    if col in df_result.columns:
                        non_null = df_result[col].notna().sum()
                        print(f"   📊 {col}: {non_null}/{len(df_result)} filas con datos")
                        if non_null == 0:
                            print(f"   ⚠️ ¡ADVERTENCIA! {col} tiene 0 valores no nulos")
                    else:
                        print(f"   ⚠️ {col}: no está en el resultado")
                
                return df_result
        
        print(f"   ⚠️ No se encontró columna común para merge")
        print(f"   Columnas en dashboard: {list(df_dashboard.columns)[:10]}...")
        print(f"   Columnas en summary: {list(self.summary_df.columns)[:10]}...")
        
        return df_dashboard
    
    def _validate_required_columns(self, df: pd.DataFrame):
        """Valida que existan las columnas necesarias."""
        # Buscar Total_Clientes
        has_clientes = any('Total_Clientes' in col for col in df.columns)
        
        required_cols = ['Final_Score', 'LTV_Promedio_Cliente_$']
        missing = [col for col in required_cols if col not in df.columns]
        
        if not has_clientes:
            missing.append('Total_Clientes')
        
        if missing:
            print(f"⚠️ Faltan columnas: {missing}")
        else:
            print(f"✅ Columnas validadas")
    
    def _create_behavior_plots(self, df: pd.DataFrame, plot_folder: str, 
                                behavior_type: str, file_suffix: str = ""):
        """Genera gráficas de comportamiento vs AOV y vs Clientes."""
        if behavior_type == "frequency":
            metrics = ['Pct_2da_Compra', 'Pct_3ra_Compra', 'Pct_4ta_Compra']
            xlabel = 'Porcentaje de Repetición (%)'
            title_base = 'Relación entre Repetición de Compra y Ticket Promedio'
            filename_base = "frequency"
        elif behavior_type == "time":
            metrics = ['Mediana_Dias_1a2', 'Mediana_Dias_2a3', 'Mediana_Dias_3a4']
            xlabel = 'Mediana de Días entre Compras'
            title_base = 'Relación entre Tiempo entre Compras y Ticket Promedio'
            filename_base = "time"
        elif behavior_type == "conversion":
            metrics = ['Pct_Conv_30d', 'Pct_Conv_60d', 'Pct_Conv_90d', 
                       'Pct_Conv_180d', 'Pct_Conv_360d']
            xlabel = 'Tasa de Conversión Acumulada (%)'
            title_base = 'Relación entre Tasas de Conversión y Ticket Promedio'
            filename_base = "conversion"
        else:
            return
        
        available_metrics = [m for m in metrics if m in df.columns]
        if not available_metrics:
            print(f"  ⚠️ No hay métricas de {behavior_type} disponibles")
            return
        
        # Behavior vs AOV
        if 'AOV_Ref' in df.columns or 'AOV' in df.columns:
            aov_col = 'AOV_Ref' if 'AOV_Ref' in df.columns else 'AOV'
            
            self._plot_metric_vs_behavior(
                df=df, x_cols=available_metrics, y_col=aov_col,
                color_map=self.colors, xlabel=xlabel,
                ylabel='Ticket Promedio (AOV $)',
                title=f'{title_base}\n({behavior_type.capitalize()} vs AOV)',
                filename=f"{filename_base}_vs_aov{file_suffix}.png", 
                plot_folder=plot_folder
            )
        
        # Behavior vs Total_Clientes
        clientes_col = self._find_clientes_column(df)
        if clientes_col:
            self._plot_metric_vs_behavior(
                df=df, x_cols=available_metrics, y_col=clientes_col,
                color_map=self.colors, xlabel=xlabel,
                ylabel='Número de Clientes',
                title=f'{title_base}\n({behavior_type.capitalize()} vs Clientes)',
                filename=f"{filename_base}_vs_clientes{file_suffix}.png", 
                plot_folder=plot_folder
            )
    
    def _find_clientes_column(self, df: pd.DataFrame) -> Optional[str]:
        """Encuentra la columna de Total_Clientes."""
        for col in ['Total_Clientes_y', 'Total_Clientes_x', 'Total_Clientes']:
            if col in df.columns:
                return col
        return None
    
    def _plot_metric_vs_behavior(self, df: pd.DataFrame, x_cols: List[str], 
                                y_col: str, color_map: Dict, xlabel: str, 
                                ylabel: str, title: str, filename: str, 
                                plot_folder: str):
        """Función genérica para graficar métricas vs comportamiento."""
        
        plt.figure(figsize=(12, 8))
        
        plotted = False
        for metric in x_cols:
            plot_df = df[[metric, y_col]].dropna()
            plot_df = plot_df[plot_df[metric] > 0]
            
            if not plot_df.empty:
                color = color_map.get(metric, '#333333')
                plt.scatter(plot_df[metric], plot_df[y_col], 
                        label=metric, alpha=0.6, s=60, color=color)
                plotted = True
        
        if plotted:
            plt.xlabel(xlabel, fontsize=12)
            plt.ylabel(ylabel, fontsize=12)
            plt.title(title, fontsize=14)
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.savefig(os.path.join(plot_folder, filename), dpi=120, bbox_inches='tight')
            plt.close()
            print(f"  📊 Gráfica creada: {filename}")
        else:
            plt.close()
    
    def _create_ltv_vs_behavior_plots(self, df: pd.DataFrame, plot_folder: str, file_suffix: str = ""):
        """Genera gráficas de LTV vs comportamiento."""
        ltv_col = self._find_ltv_column(df)
        
        if ltv_col is None:
            return
        
        behaviors = {
            'frequency': {
                'metrics': ['Pct_2da_Compra', 'Pct_3ra_Compra', 'Pct_4ta_Compra'],
                'xlabel': 'Porcentaje de Repetición (%)',
                'title': 'Relación entre LTV y Repetición de Compra',
                'filename': f'LTV_vs_Frequency{file_suffix}.png'
            },
            'time': {
                'metrics': ['Mediana_Dias_1a2', 'Mediana_Dias_2a3', 'Mediana_Dias_3a4'],
                'xlabel': 'Mediana de Días entre Compras',
                'title': 'Relación entre LTV y Tiempo entre Compras',
                'filename': f'LTV_vs_Time{file_suffix}.png'
            },
            'conversion': {
                'metrics': ['Pct_Conv_30d', 'Pct_Conv_60d', 'Pct_Conv_90d', 
                           'Pct_Conv_180d', 'Pct_Conv_360d'],
                'xlabel': 'Tasa de Conversión Acumulada (%)',
                'title': 'Relación entre LTV y Tasas de Conversión',
                'filename': f'LTV_vs_Conversion{file_suffix}.png'
            }
        }
        
        for behavior, config in behaviors.items():
            available_metrics = [m for m in config['metrics'] if m in df.columns]
            if not available_metrics:
                continue
            
            self._plot_metric_vs_behavior(
                df=df, x_cols=available_metrics, y_col=ltv_col,
                color_map=self.colors, xlabel=config['xlabel'],
                ylabel='LTV Promedio por Cliente ($)',
                title=config['title'], filename=config['filename'], 
                plot_folder=plot_folder
            )
    
    def _find_ltv_column(self, df: pd.DataFrame) -> Optional[str]:
        """Encuentra la columna de LTV."""
        ltv_candidates = ['LTV_Promedio_Cliente_$', 'LTV_Neto_Real_$', 'LTV_Promedio_$']
        for col in ltv_candidates:
            if col in df.columns:
                return col
        return None
    
    def _create_ltv_vs_finalscore_plot(self, df: pd.DataFrame, plot_folder: str, file_suffix: str = ""):
        """Genera gráfica de LTV vs Final Score."""
        score_col = None
        for col in ['Final_Score', 'Global_Score', 'Performance_Score']:
            if col in df.columns:
                score_col = col
                break
        
        if score_col is None:
            return
        
        ltv_col = self._find_ltv_column(df)
        
        if ltv_col is None:
            return
        
        plot_df = df[[ltv_col, score_col]].dropna()
        plot_df = plot_df[plot_df[ltv_col] > 0]
        
        if plot_df.empty:
            return
        
        plt.figure(figsize=(12, 8))
        
        if 'Quality_Bucket' in df.columns:
            plot_df_with_bucket = plot_df.copy()
            quality_bucket_series = df['Quality_Bucket']
            plot_df_with_bucket['Quality_Bucket'] = quality_bucket_series
            
            buckets = plot_df_with_bucket['Quality_Bucket'].unique()
            colors_buckets = {
                'LOW_TICKET': '#3498db', 
                'MID_TICKET': '#2ecc71', 
                'HIGH_TICKET': '#e74c3c', 
                'GENERAL': '#95a5a6'
            }
            
            for bucket in buckets:
                bucket_data = plot_df_with_bucket[plot_df_with_bucket['Quality_Bucket'] == bucket]
                if not bucket_data.empty and pd.notna(bucket):
                    color = colors_buckets.get(bucket, '#7f8c8d')
                    plt.scatter(bucket_data[score_col], bucket_data[ltv_col], 
                               label=bucket, alpha=0.6, s=80, color=color, 
                               edgecolors='white', linewidth=1.5)
        else:
            plt.scatter(plot_df[score_col], plot_df[ltv_col], s=100, alpha=0.6, c='#27ae60')
        
        # Línea de tendencia
        if len(plot_df) > 1:
            z = np.polyfit(plot_df[score_col], plot_df[ltv_col], 1)
            p = np.poly1d(z)
            x_sorted = plot_df[score_col].sort_values()
            plt.plot(x_sorted, p(x_sorted), "r--", alpha=0.8, linewidth=2)
        
        correlation = plot_df[score_col].corr(plot_df[ltv_col]) if len(plot_df) > 1 else 0
        
        plt.xlabel(f'{score_col} (0-1)', fontsize=12)
        plt.ylabel('LTV Promedio por Cliente ($)', fontsize=12)
        plt.title(f'Final Score vs LTV Promedio (Correlación: {correlation:.3f})', fontsize=14)
        
        if 'Quality_Bucket' in df.columns:
            plt.legend(loc='best')
        
        plt.grid(True, alpha=0.3)
        
        filename = f"LTV_vs_FinalScore{file_suffix}.png"
        plt.savefig(os.path.join(plot_folder, filename), dpi=120, bbox_inches='tight')
        plt.close()
        print(f"  📊 Gráfica creada: {filename}")