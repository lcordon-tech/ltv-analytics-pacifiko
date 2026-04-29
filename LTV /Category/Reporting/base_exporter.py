# Category/Reporting/base_exporter.py
"""
Clase base abstracta para todos los exporters de dimensión.
Elimina la duplicación de código entre Category, Subcategory, Brand, Product.
VERSIÓN MEJORADA: Soporta jerarquía de padres (parent_dimension).
"""

import os
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
from openpyxl.utils import get_column_letter

from Category.Analytics.metrics_analyzer import MetricsQualityAnalyzer
from Category.Utils.dimension_config import get_dimension_config, DimensionMode
from Category.Utils.utils_ue import build_unit_economics_dataframe


class BaseExporter(ABC):
    """
    Exporter base para cualquier dimensión (category, subcategory, brand, product).
    
    Uso:
        class BrandExporter(BaseExporter):
            def _get_dimension_mode(self) -> int:
                return DimensionMode.BRAND
    """
    
    # Configuración de transformación del Confidence Score
    confidence_transform = "cuberoot"
    
    def __init__(self, results_dict: dict, customers: list, 
                 ue_results: dict = None, grouping_mode: str = "entry_based"):
        """
        Args:
            results_dict: Diccionario con resultados del orquestador
            customers: Lista de objetos Customer
            ue_results: Resultados de Unit Economics
            grouping_mode: "behavioral" o "entry_based"
        """
        self.results = results_dict
        self.customers = customers
        self.ue_results = ue_results
        self.grouping_mode = grouping_mode
        self._dimension_config = None
        
        self.output_dir = os.environ.get("LTV_OUTPUT_DIR", "Final_Reports")
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        # Aplicar scoring automático
        self._apply_scoring_to_results()
    
    @abstractmethod
    def _get_dimension_mode(self) -> int:
        """Retorna el modo de dimensión (1,2,3,4,5,6)."""
        pass
    
    def _get_config(self) -> dict:
        """Obtiene la configuración de la dimensión."""
        if self._dimension_config is None:
            mode = self._get_dimension_mode()
            self._dimension_config = get_dimension_config(mode)
        return self._dimension_config
    
    def _apply_scoring_to_results(self):
        """Aplica scoring automático si los datos no tienen scores."""
        if MetricsQualityAnalyzer is None:
            print("⚠️ MetricsQualityAnalyzer no disponible")
            return
        
        config = self._get_config()
        dim_name = config['output_key']
        print(f"🔍 Verificando scores para dimensión: {dim_name}")
        
        for section in ['frequency', 'time', 'conversion']:
            if section not in self.results:
                continue
            
            historical = self.results[section].get('historical', [])
            if historical:
                if 'Final_Score' in historical[0]:
                    print(f"  ✅ {section}.historical YA tiene scores")
                else:
                    print(f"  🔧 {section}.historical SIN scores - aplicando...")
                    try:
                        scored = MetricsQualityAnalyzer.evaluate_all(historical)
                        self.results[section]['historical'] = scored
                        print(f"  ✅ scoring aplicado a {len(scored)} registros")
                    except Exception as e:
                        print(f"  ❌ Error: {e}")
            
            cohorts = self.results[section].get('cohorts', {})
            for cohort_id, cohort_data in cohorts.items():
                if cohort_data:
                    if 'Final_Score' in cohort_data[0]:
                        print(f"  ✅ {section}.cohorts[{cohort_id}] YA tiene scores")
                    else:
                        print(f"  🔧 Aplicando scoring a cohorte {cohort_id}...")
                        try:
                            scored = MetricsQualityAnalyzer.evaluate_all(cohort_data)
                            self.results[section]['cohorts'][cohort_id] = scored
                            print(f"  ✅ scoring aplicado")
                        except Exception as e:
                            print(f"  ❌ Error: {e}")
    
    def _enrich_with_parent(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega columna padre a la izquierda según configuración de dimensión.
        El padre se extrae de la misma fuente que el hijo (primera orden del primer cliente).
        """
        config = self._get_config()
        parent_dim = config.get('parent_dimension', None)
        parent_col = config.get('parent_col_name', None)
        
        if not parent_dim or not parent_col:
            return df  # Sin padre para esta dimensión
        
        # Si la columna padre ya existe, solo reordenar
        if parent_col in df.columns:
            cols = [parent_col] + [c for c in df.columns if c != parent_col]
            return df[cols]
        
        # Construir mapa de hijo -> padre
        parent_map = {}
        main_key = config['main_key']
        
        print(f"  🔧 Construyendo jerarquía: {parent_col} ← {parent_dim}")
        
        for customer in self.customers:
            orders = customer.get_orders_sorted()
            if not orders:
                continue
            
            # Tomar la primera orden como referencia
            first_order = orders[0]
            
            # Valor del hijo (dimensión actual)
            if main_key == 'Subcategoria_Marca':
                # Para modo 5, el hijo es compuesto
                subcat = getattr(first_order, 'subcategory', None)
                brand = getattr(first_order, 'brand', None)
                if subcat and brand:
                    child_value = f"{subcat} ({brand})"
                else:
                    child_value = subcat or brand
            elif main_key == 'Subcategoria':
                child_value = getattr(first_order, 'subcategory', None)
            elif main_key == 'Categoria':
                child_value = getattr(first_order, 'category', None)
            elif main_key == 'Brand':
                child_value = getattr(first_order, 'brand', None)
            elif main_key == 'Producto':
                child_value = getattr(first_order, 'name', None)
            else:
                child_value = getattr(first_order, parent_dim, None)
            
            # Valor del padre
            parent_value = getattr(first_order, parent_dim, None)
            
            if child_value and parent_value:
                child_clean = str(child_value).strip()
                parent_clean = str(parent_value).strip()
                
                if child_clean and parent_clean:
                    if child_clean.lower() not in ['', 'nan', 'none', 'n/a', 'null']:
                        if parent_clean.lower() not in ['', 'nan', 'none', 'n/a', 'null']:
                            parent_map[child_clean] = parent_clean
        
        # Aplicar padre al DataFrame
        if parent_map and main_key in df.columns:
            df[parent_col] = df[main_key].map(parent_map)
            
            # Reordenar: padre a la izquierda
            cols = [parent_col] + [c for c in df.columns if c != parent_col]
            df = df[cols]
            
            print(f"  ✅ Jerarquía agregada: {parent_col} → {main_key}")
            print(f"     {len(parent_map)} mapeos creados")
        else:
            print(f"  ⚠️ No se pudo agregar jerarquía: {len(parent_map)} mapeos, main_key={main_key}")
        
        return df
    
    def _safe_sheet_name(self, name: str) -> str:
        """Asegura que el nombre de la hoja no exceda 31 caracteres."""
        if len(name) > 31:
            return name[:28] + "..."
        return name

    def export_to_excel(self, filename: str = None, 
                        df_summary_hist: pd.DataFrame = None,
                        df_summary_cohort: pd.DataFrame = None) -> Optional[str]:
        """
        Exporta a Excel con todas las hojas necesarias.
        ORDEN DE HOJAS:
        1. Resumen_Historico
        2. Resumen_Cohortes
        3. Resto de hojas
        """
        config = self._get_config()
        main_key = config['main_key']
        sheet_suffix = config['sheet_suffix']
        excel_filename = config['excel_filename']
        
        if filename is None:
            filename = excel_filename
        
        path = self._get_path(filename, ".xlsx")
        print(f"🚀 Exportando: {os.path.basename(path)}")
        
        sheets_config = {
            "frequency": (f"Frecuencia_{sheet_suffix}", f"Frecuencia_Cohortes_{sheet_suffix}"),
            "time": (f"Velocidad_{sheet_suffix}", f"Velocidad_Cohortes_{sheet_suffix}"),
            "conversion": (f"Conversion_{sheet_suffix}", f"Conversion_Cohortes_{sheet_suffix}")
        }
        
        raw_metadata = self.results.get("metadata", {})
        metadata = raw_metadata if isinstance(raw_metadata, dict) else {}
        total_rows_audit_sum = 0
        audit_confirmed = False
        
        try:
            with pd.ExcelWriter(path, engine='openpyxl') as writer:
                # 1. PRIMERO: Hojas de Resumen
                self._add_summary_sheets(writer, df_summary_hist, df_summary_cohort, main_key)
                
                # 2. SEGUNDO: Unit Economics
                if self.customers and self.ue_results:
                    self._add_unit_economics_sheets(writer, main_key, config)
                
                # 3. TERCERO: Resto de análisis
                for key, (hist_sheet, cohort_sheet) in sheets_config.items():
                    section = self.results.get(key, {})
                    
                    # Histórico
                    data_h = section.get("historical", [])
                    df_h = self._flatten_data(data_h, main_key)
                    
                    if not df_h.empty:
                        df_h = self._ensure_critical_columns(df_h, hist_sheet)
                        
                        if "Total_Filas_CSV_Auditoria" in df_h.columns and not audit_confirmed:
                            total_rows_audit_sum = df_h["Total_Filas_CSV_Auditoria"].sum()
                            audit_confirmed = True
                        
                        if key == "conversion":
                            cols_inc = [c for c in df_h.columns if "_inc" in c]
                            cols_base = [main_key, "Total_Clientes"]
                            
                            # Agregar columna padre si existe para hoja de conversión
                            parent_col = config.get('parent_col_name', None)
                            if parent_col and parent_col in df_h.columns:
                                cols_base.insert(0, parent_col)
                            
                            df_accum = df_h.drop(columns=cols_inc).round(2)
                            df_accum = self._sort_dataframe(df_accum, hist_sheet, main_key)
                            df_accum.to_excel(writer, sheet_name=self._safe_sheet_name(hist_sheet), index=False)
                            self._auto_adjust_columns(writer.sheets[self._safe_sheet_name(hist_sheet)], df_accum)
                            
                            if cols_inc:
                                inc_sheet_name = f"Conv_Inc_{sheet_suffix}"[:31]
                                df_inc = df_h[cols_base + cols_inc].round(2)
                                df_inc = self._sort_dataframe(df_inc, inc_sheet_name, main_key)
                                df_inc.to_excel(writer, sheet_name=self._safe_sheet_name(inc_sheet_name), index=False)
                                self._auto_adjust_columns(writer.sheets[self._safe_sheet_name(inc_sheet_name)], df_inc)
                        else:
                            df_h = self._sort_dataframe(df_h, hist_sheet, main_key).round(2)
                            df_h.to_excel(writer, sheet_name=self._safe_sheet_name(hist_sheet), index=False)
                            self._auto_adjust_columns(writer.sheets[self._safe_sheet_name(hist_sheet)], df_h)
                    
                    # Cohorts
                    cohorts_dict = section.get("cohorts", {})
                    all_cohort_dfs = []
                    
                    for cohort_id, cohort_data in cohorts_dict.items():
                        if not cohort_data:
                            continue
                        df_c = self._flatten_data(cohort_data, main_key, cohort_id)
                        if not df_c.empty:
                            df_c = self._ensure_critical_columns(df_c, cohort_sheet)
                            all_cohort_dfs.append(df_c)
                    
                    if all_cohort_dfs:
                        df_c = pd.concat(all_cohort_dfs, ignore_index=True)
                        
                        if key == "conversion":
                            cols_inc_c = [c for c in df_c.columns if "_inc" in c]
                            cols_base_c = ["Cohorte", main_key, "Total_Clientes"]
                            
                            parent_col = config.get('parent_col_name', None)
                            if parent_col and parent_col in df_c.columns:
                                cols_base_c.insert(0, parent_col)
                            
                            df_c_accum = df_c.drop(columns=cols_inc_c).round(2)
                            df_c_accum.to_excel(writer, sheet_name=self._safe_sheet_name(cohort_sheet), index=False)
                            self._auto_adjust_columns(writer.sheets[self._safe_sheet_name(cohort_sheet)], df_c_accum)
                            
                            if cols_inc_c:
                                inc_cohort_sheet = f"Conv_Inc_Coh_{sheet_suffix}"[:31]
                                df_c_inc = df_c[cols_base_c + cols_inc_c].round(2)
                                df_c_inc.to_excel(writer, sheet_name=self._safe_sheet_name(inc_cohort_sheet), index=False)
                                self._auto_adjust_columns(writer.sheets[self._safe_sheet_name(inc_cohort_sheet)], df_c_inc)
                        else:
                            df_c = self._sort_dataframe(df_c, cohort_sheet, main_key).round(2)
                            df_c.to_excel(writer, sheet_name=self._safe_sheet_name(cohort_sheet), index=False)
                            self._auto_adjust_columns(writer.sheets[self._safe_sheet_name(cohort_sheet)], df_c)
            
            self._print_final_audit(metadata, total_rows_audit_sum, path)
            return path
            
        except Exception as e:
            print(f"🚨 Error crítico: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _get_path(self, base_name: str, ext: str) -> str:
        """Genera ruta de archivo con timestamp."""
        nombre_archivo = f"{base_name}_{self.timestamp}{ext}"
        return os.path.join(self.output_dir, nombre_archivo)
    
    def _get_ordered_columns(self, available_cols: list, main_key: str) -> list:
        """Prioriza las columnas importantes."""
        priority_start = [
            main_key, "Cohorte",
            "Total_Clientes",
            "Confidence_Score",
            "Performance_Score",
            "Global_Score",
            "Final_Score",
            "LTV_Score",
            "Frequency_Score",
            "Time_Score",
            "Conversion_Score",
            "LTV_Promedio_Cliente_$",
            "GMV_Total_$",
            "LTV_Neto_Real_$",
            "Global_Quality",
            "AOV_Ref",
            "Quality_Bucket",
            "Sample_Quality",
            "Sample_Penalty",
            "Total_Filas_CSV_Auditoria"
        ]
        
        ordered_list = []
        for col in priority_start:
            if col in available_cols and col not in ordered_list:
                ordered_list.append(col)
        
        remaining = [c for c in available_cols if c not in ordered_list]
        ordered_list.extend(remaining)
        
        return ordered_list
    
    def _flatten_data(self, data_list: list, main_key: str, cohort_id: str = None) -> pd.DataFrame:
        """Aplana los datos y aplica jerarquía de padre."""
        if not data_list:
            return pd.DataFrame()
        
        all_records = []
        for record in data_list:
            new_record = record.copy()
            
            # Si hay paréntesis en el label, extraer la parte principal
            if main_key in new_record:
                full_label = str(new_record[main_key])
                if " (" in full_label:
                    new_record[main_key] = full_label.split(" (")[0]
            
            if cohort_id:
                new_record["Cohorte"] = cohort_id
            
            all_records.append(new_record)
        
        df = pd.DataFrame(all_records)
        
        # Aplicar jerarquía de padre
        df = self._enrich_with_parent(df)
        
        ordered_cols = self._get_ordered_columns(df.columns.tolist(), main_key)
        return df[ordered_cols] if ordered_cols else df
    
    def _ensure_critical_columns(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """Asegura columnas críticas."""
        df_copy = df.copy()
        
        new_score_cols = [
            'Frequency_Score', 'Time_Score', 'Conversion_Score',
            'Performance_Score', 'Final_Score', 'Global_Score', 'LTV_Score'
        ]
        
        for col in new_score_cols:
            if col not in df_copy.columns:
                if col == 'Final_Score' and 'Global_Score' in df_copy.columns:
                    df_copy['Final_Score'] = df_copy['Global_Score']
                elif col == 'Global_Score' and 'Final_Score' in df_copy.columns:
                    df_copy['Global_Score'] = df_copy['Final_Score']
                else:
                    df_copy[col] = 0.0
        
        if "Confidence_Score" not in df_copy.columns:
            df_copy['Confidence_Score'] = 0.0
        
        return df_copy
    
    def _sort_dataframe(self, df: pd.DataFrame, sheet_name: str, main_key: str) -> pd.DataFrame:
        """Ordena dataframe."""
        if df.empty:
            return df
        
        if "Cohortes" in sheet_name:
            sort_cols = [c for c in [main_key, "Cohorte"] if c in df.columns]
            return df.sort_values(by=sort_cols) if sort_cols else df
        
        if "Final_Score" in df.columns:
            return df.sort_values(by="Final_Score", ascending=False)
        
        if "Total_Clientes" in df.columns:
            return df.sort_values(by="Total_Clientes", ascending=False)
        
        if main_key in df.columns:
            return df.sort_values(by=main_key)
        
        return df
    
    def _auto_adjust_columns(self, worksheet, df: pd.DataFrame):
        """Ajusta ancho de columnas."""
        for idx, col in enumerate(df.columns):
            header_len = len(str(col))
            if not df.empty:
                content_len = df[col].apply(lambda x: len(str(x)) if pd.notnull(x) else 0).max()
            else:
                content_len = 0
            max_len = max(header_len, content_len)
            worksheet.column_dimensions[get_column_letter(idx + 1)].width = min(max_len + 3, 45)
    
    def _add_unit_economics_sheets(self, writer, main_key: str, config: dict):
        """Añade hojas de Unit Economics."""
        try:
            is_subcat = config['mode_id'] == 2
            mode_val = config['mode_id']
            dim_label = config['ue_dim_label']
            
            df_ue_hist = build_unit_economics_dataframe(
                customers=self.customers, mode=mode_val,
                ue_results=self.ue_results, grouping_mode=self.grouping_mode,
                by_cohort=False
            )
            
            df_ue_coh = build_unit_economics_dataframe(
                customers=self.customers, mode=mode_val,
                ue_results=self.ue_results, grouping_mode=self.grouping_mode,
                by_cohort=True
            )
            
            if not df_ue_hist.empty:
                s_name = f"UnitEconomics_{dim_label}"
                df_ue_hist.to_excel(writer, sheet_name=s_name, index=False)
                self._auto_adjust_columns(writer.sheets[s_name], df_ue_hist)
            
            if not df_ue_coh.empty:
                s_name_c = f"UnitEconomics_Cohortes_{dim_label}"
                df_ue_coh.to_excel(writer, sheet_name=s_name_c, index=False)
                self._auto_adjust_columns(writer.sheets[s_name_c], df_ue_coh)
            
            print(f"✅ Unit Economics añadidas para {dim_label}")
        except Exception as e:
            print(f"⚠️ Error en Unit Economics: {e}")
    
    def _add_summary_sheets(self, writer, df_summary_hist, df_summary_cohort, main_key: str):
        """Añade hojas de resumen."""
        try:
            if df_summary_hist is not None and not df_summary_hist.empty:
                df_summary_hist = self._ensure_critical_columns(df_summary_hist, "Resumen_Historico")
                df_summary_hist.to_excel(writer, sheet_name="Resumen_Historico", index=False)
                self._auto_adjust_columns(writer.sheets["Resumen_Historico"], df_summary_hist)
                print(f"✅ Resumen_Historico añadido")
            
            if df_summary_cohort is not None and not df_summary_cohort.empty:
                df_summary_cohort = self._ensure_critical_columns(df_summary_cohort, "Resumen_Cohortes")
                df_summary_cohort.to_excel(writer, sheet_name="Resumen_Cohortes", index=False)
                self._auto_adjust_columns(writer.sheets["Resumen_Cohortes"], df_summary_cohort)
                print(f"✅ Resumen_Cohortes añadido")
        except Exception as e:
            print(f"⚠️ Error en hojas resumen: {e}")
    
    def _print_final_audit(self, metadata, audit_sum, path):
        """Imprime auditoría final."""
        f_brutas = metadata.get('filas_leidas_brutas', 0)
        f_sin_dim = metadata.get('filas_sin_categoria', 0)
        f_duplicados = metadata.get('filas_duplicadas_bloqueadas', 0)
        f_ok = metadata.get('filas_procesadas_ok', 0)
        
        print(f"\n✅ Reporte generado: {os.path.basename(path)}")
        print("-" * 65)
        print(f"📊 AUDITORÍA TÉCNICA:")
        print(f"  1. Filas brutas leídas:           {int(f_brutas)}")
        print(f"  2. Filas sin dimensión:           -{int(f_sin_dim)}")
        print(f"  3. Duplicados bloqueados:         -{int(f_duplicados)}")
        print("-" * 65)
        print(f"  (=) TOTAL PROCESADAS OK:          {int(f_ok)}")
        print("-" * 65)
        
        if audit_sum != f_ok and audit_sum != 0:
            print(f"⚠️ Discrepancia detectada ({f_ok} vs {audit_sum}).")
        else:
            print(f"✨ Integridad confirmada.")
        print("-" * 65)
    
    # ======================================================================
    # MÉTODOS DE SCORING (comunes a todas las dimensiones)
    # ======================================================================
    
    def _transform_confidence(self, confidence_scores: pd.Series) -> pd.Series:
        """Aplica transformación al Confidence Score."""
        if self.confidence_transform == "sqrt":
            return np.sqrt(confidence_scores)
        elif self.confidence_transform == "cuberoot":
            return np.power(confidence_scores, 1/3)
        elif self.confidence_transform == "none":
            return confidence_scores
        else:
            return np.power(confidence_scores, 1/3)
    
    def _calculate_weighted_scores(self, df: pd.DataFrame, mode: str = "historical") -> pd.DataFrame:
        """Calcula scores normalizados y ponderados."""
        df_result = df.copy()
        
        ltv_score_exists = 'LTV_Score' in df_result.columns
        
        # 1. FREQUENCY SCORE
        freq_cols = ['Pct_2da_Compra_Score', 'Pct_3ra_Compra_Score', 'Pct_4ta_Compra_Score']
        freq_weights = [0.5, 0.3, 0.2]
        
        if all(col in df.columns for col in freq_cols):
            df_result['Frequency_Raw'] = (df[freq_cols[0]] * freq_weights[0] +
                                          df[freq_cols[1]] * freq_weights[1] +
                                          df[freq_cols[2]] * freq_weights[2])
            max_freq = df_result['Frequency_Raw'].max()
            df_result['Frequency_Score'] = (df_result['Frequency_Raw'] / max_freq).round(4) if max_freq > 0 else 0.0
            df_result.drop('Frequency_Raw', axis=1, inplace=True)
        else:
            df_result['Frequency_Score'] = 0.0
        
        # 2. TIME SCORE
        time_cols = ['Mediana_Dias_1a2_Score', 'Mediana_Dias_2a3_Score', 'Mediana_Dias_3a4_Score']
        time_weights = [0.5, 0.3, 0.2]
        
        if all(col in df.columns for col in time_cols):
            df_result['Time_Raw'] = (df[time_cols[0]] * time_weights[0] +
                                     df[time_cols[1]] * time_weights[1] +
                                     df[time_cols[2]] * time_weights[2])
            max_time = df_result['Time_Raw'].max()
            df_result['Time_Score'] = (df_result['Time_Raw'] / max_time).round(4) if max_time > 0 else 0.0
            df_result.drop('Time_Raw', axis=1, inplace=True)
        else:
            df_result['Time_Score'] = 0.0
        
        # 3. CONVERSION SCORE
        conv_cols = ['Pct_Conv_30d_Score', 'Pct_Conv_60d_Score', 'Pct_Conv_90d_Score',
                     'Pct_Conv_180d_Score', 'Pct_Conv_360d_Score']
        conv_weights = [0.3, 0.25, 0.2, 0.15, 0.1]
        
        existing_conv = [(col, w) for col, w in zip(conv_cols, conv_weights) if col in df.columns]
        if existing_conv:
            total_weight = sum(w for _, w in existing_conv)
            df_result['Conversion_Raw'] = sum(df[col] * (w / total_weight) for col, w in existing_conv)
            max_conv = df_result['Conversion_Raw'].max()
            df_result['Conversion_Score'] = (df_result['Conversion_Raw'] / max_conv).round(4) if max_conv > 0 else 0.0
            df_result.drop('Conversion_Raw', axis=1, inplace=True)
        else:
            df_result['Conversion_Score'] = 0.0
        
        # 4. LTV SCORE
        if not ltv_score_exists:
            df_result['LTV_Score'] = 0.0
        
        # 5. LTV_ADJUSTED (raíz cuadrada)
        df_result['LTV_Adjusted'] = np.sqrt(df_result['LTV_Score'])
        
        # 6. PERFORMANCE SCORE
        perf_weights = {'Frequency_Score': 0.2, 'Time_Score': 0.3,
                        'Conversion_Score': 0.1, 'LTV_Adjusted': 0.4}
        
        available_scores = [(col, w) for col, w in perf_weights.items() if col in df_result.columns]
        if available_scores:
            total_weight = sum(w for _, w in available_scores)
            df_result['Performance_Raw'] = sum(df_result[col] * (w / total_weight) for col, w in available_scores)
            max_perf = df_result['Performance_Raw'].max()
            df_result['Performance_Score'] = (df_result['Performance_Raw'] / max_perf).round(4) if max_perf > 0 else 0.0
            df_result.drop('Performance_Raw', axis=1, inplace=True)
        else:
            df_result['Performance_Score'] = 0.0
        
        # 7. CONFIDENCE SCORE
        if "Confidence_Score" not in df_result.columns and "Total_Clientes" in df_result.columns:
            min_c = df_result['Total_Clientes'].min()
            max_c = df_result['Total_Clientes'].max()
            if max_c > min_c:
                df_result['Confidence_Score'] = ((df_result['Total_Clientes'] - min_c) / (max_c - min_c)).clip(0, 1).round(4)
            else:
                df_result['Confidence_Score'] = 1.0
        elif "Confidence_Score" not in df_result.columns:
            df_result['Confidence_Score'] = 0.0
        
        df_result['Confidence_Transformed'] = self._transform_confidence(df_result['Confidence_Score'])
        
        # 8. FINAL SCORE
        if 'Performance_Score' in df_result.columns:
            df_result['Final_Score_Raw'] = df_result['Performance_Score'] * df_result['Confidence_Transformed']
            max_final = df_result['Final_Score_Raw'].max()
            df_result['Final_Score'] = (df_result['Final_Score_Raw'] / max_final).round(4) if max_final > 0 else 0.0
            df_result.drop('Final_Score_Raw', axis=1, inplace=True)
            df_result['Global_Score'] = df_result['Final_Score']
        
        df_result.drop(['LTV_Adjusted', 'Confidence_Transformed'], axis=1, inplace=True, errors='ignore')
        
        return df_result
    
    def build_summary_dataframe(self, mode: str = "historical") -> pd.DataFrame:
        """Construye resumen ejecutivo con scoring.
        
        VERSIÓN MEJORADA v5.0:
        - LTV_Promedio_Cliente_$ ahora es BRUTO (sin CAC)
        - LTV_Neto_Promedio_Cliente_$ es NETO (con CAC restado)
        - CAC_Promedio_$ es columna independiente
        - LTV_Score basado en LTV bruto
        - Soporte para modo dual (Subcategoria + Marca)
        - Soporte para jerarquía de padres (agrega columna padre)
        
        Args:
            mode: "historical" o "cohorts"
        
        Returns:
            DataFrame con todas las métricas y scores
        """
        config = self._get_config()
        main_key = config['main_key']
        is_dual = config.get('has_brand_column', False)
        parent_col = config.get('parent_col_name', None)
        
        print(f"\n🔧 Construyendo resumen para {config['output_key']} - mode: {mode}")
        
        # ==========================================================================
        # 1. EXTRACCIÓN DE DATOS SEGÚN MODO
        # ==========================================================================
        if mode == "historical":
            freq_data = self.results.get("frequency", {}).get("historical", [])
            time_data = self.results.get("time", {}).get("historical", [])
            conv_data = self.results.get("conversion", {}).get("historical", [])
        else:
            freq_cohorts = self.results.get("frequency", {}).get("cohorts", {})
            time_cohorts = self.results.get("time", {}).get("cohorts", {})
            conv_cohorts = self.results.get("conversion", {}).get("cohorts", {})
            
            freq_data = []
            for cohort_id, cohort_list in freq_cohorts.items():
                if isinstance(cohort_list, list):
                    for record in cohort_list:
                        record_copy = record.copy()
                        record_copy['Cohorte'] = cohort_id
                        freq_data.append(record_copy)
            
            time_data = []
            for cohort_id, cohort_list in time_cohorts.items():
                if isinstance(cohort_list, list):
                    for record in cohort_list:
                        record_copy = record.copy()
                        record_copy['Cohorte'] = cohort_id
                        time_data.append(record_copy)
            
            conv_data = []
            for cohort_id, cohort_list in conv_cohorts.items():
                if isinstance(cohort_list, list):
                    for record in cohort_list:
                        record_copy = record.copy()
                        record_copy['Cohorte'] = cohort_id
                        conv_data.append(record_copy)
        
        # ==========================================================================
        # 2. VALIDACIÓN DE DATOS
        # ==========================================================================
        if not freq_data:
            print(f"  ⚠️ No hay datos de frecuencia para {mode}")
            return pd.DataFrame()
        
        df = pd.DataFrame(freq_data) if freq_data else pd.DataFrame()
        df_time = pd.DataFrame(time_data) if time_data else pd.DataFrame()
        df_conv = pd.DataFrame(conv_data) if conv_data else pd.DataFrame()
        
        if df.empty:
            return pd.DataFrame()
        
        # Claves para merge
        if is_dual:
            keys = ['Subcategoria', 'Marca']
            if mode == "cohorts":
                keys.append("Cohorte")
        else:
            keys = [main_key]
            if mode == "cohorts":
                keys.append("Cohorte")
        
        # ==========================================================================
        # 3. MERGES CON TIME Y CONVERSION
        # ==========================================================================
        if not df_time.empty:
            new_cols = list(df_time.columns.difference(df.columns)) + keys
            df = df.merge(df_time[new_cols], on=keys, how="left")
        
        if not df_conv.empty:
            new_cols_conv = list(df_conv.columns.difference(df.columns)) + keys
            df = df.merge(df_conv[new_cols_conv], on=keys, how="left")
        
        # ==========================================================================
        # 4. INTEGRACIÓN DE UNIT ECONOMICS
        # ==========================================================================
        try:
            mode_val = config['mode_id']
            df_ue = build_unit_economics_dataframe(
                customers=self.customers, 
                mode=mode_val,
                ue_results=self.ue_results, 
                grouping_mode=self.grouping_mode,
                by_cohort=(mode == "cohorts")
            )
            
            if not df_ue.empty:
                # Determinar claves de merge según modo dual
                if is_dual:
                    merge_keys = ['Subcategoria', 'Marca']
                    if mode == "cohorts":
                        merge_keys.append("Cohorte")
                    merge_keys = [k for k in merge_keys if k in df_ue.columns and k in df.columns]
                else:
                    if config['ue_dim_label'] in df_ue.columns:
                        df_ue = df_ue.rename(columns={config['ue_dim_label']: main_key})
                    merge_keys = [k for k in keys if k in df_ue.columns]
                
                if merge_keys:
                    # Columnas de Unit Economics que queremos traer
                    ue_cols_to_add = [
                        'LTV_Acumulado_Total_$',
                        'LTV_Promedio_Cliente_$',
                        'LTV_Neto_Promedio_Cliente_$',
                        'CAC_Promedio_$',
                        'GMV_Total_$',
                        'AOV_$',
                        'Ordenes_Promedio',
                        'LTV/CAC_Ratio',
                        'Payback_Proxy_Meses'
                    ]
                    ue_cols_existing = [c for c in ue_cols_to_add if c in df_ue.columns]
                    cols_to_add = ue_cols_existing + merge_keys
                    df = df.merge(df_ue[cols_to_add], on=merge_keys, how="left")
                    print(f"  ✅ Unit Economics integrado")
                    print(f"     Columnas añadidas: {ue_cols_existing}")
                    print(f"     Merge keys: {merge_keys}")
        except Exception as e:
            print(f"  ⚠️ Error integrando Unit Economics: {e}")
        
        # ==========================================================================
        # 5. CÁLCULO DE LTV_SCORE (basado en LTV BRUTO - LTV_Acumulado_Total_$)
        # ==========================================================================
        if 'GMV_Total_$' in df.columns and 'LTV_Acumulado_Total_$' in df.columns:
            df['LTV_Raw'] = df.apply(
                lambda r: r['LTV_Acumulado_Total_$'] / r['GMV_Total_$']
                if r.get('GMV_Total_$', 0) > 0 and r.get('LTV_Acumulado_Total_$', 0) > 0 else 0, 
                axis=1
            )
            max_ltv_raw = df['LTV_Raw'].max()
            df['LTV_Score'] = (df['LTV_Raw'] / max_ltv_raw).round(4) if max_ltv_raw > 0 else 0.0
            df.drop('LTV_Raw', axis=1, inplace=True)
            print(f"  ✅ LTV_Score calculado (basado en LTV_Acumulado_Total_$ / GMV_Total_$)")
        else:
            df['LTV_Score'] = 0.0
            print(f"  ⚠️ No se pudo calcular LTV_Score: faltan GMV o LTV_Acumulado_Total_$")
        
        # ==========================================================================
        # 6. CÁLCULO DE SCORES PONDERADOS
        # ==========================================================================
        df = self._calculate_weighted_scores(df, mode)
        
        # ==========================================================================
        # 7. AGREGAR JERARQUÍA DE PADRE
        # ==========================================================================
        # Construir mapa de hijo -> padre desde los customers
        parent_map = {}
        parent_dim = config.get('parent_dimension', None)
        
        if parent_dim and parent_col:
            print(f"  🔧 Construyendo mapa de padre para {parent_col} ← {parent_dim}")
            for customer in self.customers:
                orders = customer.get_orders_sorted()
                if not orders:
                    continue
                
                first_order = orders[0]
                
                # Valor del hijo (dimensión actual)
                if main_key == 'Subcategoria_Marca':
                    subcat = getattr(first_order, 'subcategory', None)
                    brand = getattr(first_order, 'brand', None)
                    if subcat and brand:
                        child_value = f"{subcat} ({brand})"
                    else:
                        child_value = subcat or brand
                elif main_key == 'Subcategoria':
                    child_value = getattr(first_order, 'subcategory', None)
                elif main_key == 'Categoria':
                    child_value = getattr(first_order, 'category', None)
                elif main_key == 'Brand':
                    child_value = getattr(first_order, 'brand', None)
                elif main_key == 'Producto':
                    child_value = getattr(first_order, 'name', None)
                else:
                    child_value = getattr(first_order, parent_dim, None)
                
                # Valor del padre
                parent_value = getattr(first_order, parent_dim, None)
                
                if child_value and parent_value:
                    child_clean = str(child_value).strip()
                    parent_clean = str(parent_value).strip()
                    
                    if child_clean and parent_clean:
                        if child_clean.lower() not in ['', 'nan', 'none', 'n/a', 'null']:
                            if parent_clean.lower() not in ['', 'nan', 'none', 'n/a', 'null']:
                                parent_map[child_clean] = parent_clean
            
            if parent_map and main_key in df.columns:
                df[parent_col] = df[main_key].map(parent_map)
                print(f"  ✅ Padre agregado: {parent_col}, {len(parent_map)} mapeos")
        
        # ==========================================================================
        # 8. SELECCIÓN Y ORDEN DE COLUMNAS FINALES
        # ==========================================================================
        if is_dual:
            base_cols = ['Subcategoria', 'Marca']
        else:
            base_cols = [main_key]
        
        # Insertar padre al inicio si existe
        if parent_col and parent_col in df.columns:
            base_cols = [parent_col] + base_cols
        
        if mode == "cohorts":
            base_cols.append("Cohorte")
        
        all_cols = base_cols + [
            # Scores principales
            "LTV_Score", "Frequency_Score", "Time_Score", "Conversion_Score",
            "Performance_Score", "Confidence_Score", "Final_Score", "Global_Score",
            
            # Métricas de volumen
            "Total_Clientes", "Total_Ordenes",
            
            # Métricas económicas
            "GMV_Total_$",
            "LTV_Acumulado_Total_$",
            "LTV_Promedio_Cliente_$",
            "LTV_Neto_Promedio_Cliente_$",
            "CAC_Promedio_$",
            "AOV_$",
            "Ordenes_Promedio",
            "LTV/CAC_Ratio",
            "Payback_Proxy_Meses",
            
            # Métricas de frecuencia
            "Pct_2da_Compra", "Pct_3ra_Compra", "Pct_4ta_Compra",
            "Abs_2da_Compra", "Abs_3ra_Compra", "Abs_4ta_Compra",
            "Abs_5ta_o_Mas",
            
            # Métricas de tiempo
            "Mediana_Dias_1a2", "Mediana_Dias_2a3", "Mediana_Dias_3a4",
            "Muestra_1a2", "Muestra_2a3", "Muestra_3a4",
            "Mediana_Dias_5ta_o_Mas", "Muestra_5ta_o_Mas",
            
            # Métricas de conversión
            "Pct_Conv_30d", "Pct_Conv_60d", "Pct_Conv_90d", 
            "Pct_Conv_180d", "Pct_Conv_360d",
            "Clientes_30d", "Clientes_60d", "Clientes_90d",
            "Clientes_180d", "Clientes_360d",
            "Clientes_30d_inc", "Clientes_60d_inc", "Clientes_90d_inc",
            "Clientes_180d_inc", "Clientes_360d_inc",
            "Pct_Conv_30d_inc", "Pct_Conv_60d_inc", "Pct_Conv_90d_inc",
            "Pct_Conv_180d_inc", "Pct_Conv_360d_inc",
            
            # Métricas de calidad
            "Quality_Bucket", "Sample_Quality", "Sample_Penalty",
            "Total_Filas_CSV_Auditoria"
        ]
        
        # Filtrar solo columnas que existen
        final_selection = [c for c in all_cols if c in df.columns]
        df_result = df[final_selection].fillna(0).round(4)
        
        # ==========================================================================
        # 9. ORDENAMIENTO FINAL
        # ==========================================================================
        if "Final_Score" in df_result.columns:
            df_result = df_result.sort_values("Final_Score", ascending=False)
        elif "Total_Clientes" in df_result.columns:
            df_result = df_result.sort_values("Total_Clientes", ascending=False)
        elif is_dual and "Subcategoria" in df_result.columns:
            df_result = df_result.sort_values(["Subcategoria", "Marca"])
        elif main_key in df_result.columns:
            df_result = df_result.sort_values(main_key)
        
        # ==========================================================================
        # 10. LOG DE VERIFICACIÓN
        # ==========================================================================
        print(f"\n  📊 Resumen {mode} generado:")
        print(f"     - Filas: {len(df_result)}")
        print(f"     - Columnas: {len(df_result.columns)}")
        
        # Verificar columnas críticas
        critical_cols = ['LTV_Promedio_Cliente_$', 'LTV_Neto_Promedio_Cliente_$', 'CAC_Promedio_$']
        for col in critical_cols:
            if col in df_result.columns:
                sample_val = df_result[col].iloc[0] if not df_result.empty else 0
                print(f"     - {col}: presente (ej: {sample_val})")
            else:
                print(f"     ⚠️ {col}: NO ENCONTRADA")
        
        return df_result