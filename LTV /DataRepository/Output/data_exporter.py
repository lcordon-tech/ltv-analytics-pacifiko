# Archivo: DataRepository/Output/data_exporter.py

import pandas as pd
import numpy as np
import os
from datetime import datetime


class DataExporter:
    """
    Responsabilidad: Gestionar la salida física del dataset y generar 
    un informe de auditoría en texto (.txt) que espeje la terminal.
    """

    def __init__(self):
        self.report_logs = [] 

    def add_to_report(self, message: str):
        self.report_logs.append(message)

    def export(self, df: pd.DataFrame, base_path: str, stats: dict, file_format: str = "excel", add_timestamp: bool = True) -> str:
        if df is None or df.empty:
            raise ValueError("🚨 ERROR: El DataFrame está vacío. No hay datos para exportar.")

        final_path = base_path
        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H%M")
        
        if add_timestamp:
            name, ext = os.path.splitext(base_path)
            final_path = f"{name}_{timestamp_str}{ext}"

        if len(df) > 1_000_000:
            print("⚠️ Dataset grande detectado → exportando como CSV automáticamente")
            res_path = self.export_to_csv(df, final_path.replace('.xlsx', '.csv'))
        elif file_format.lower() == "excel":
            res_path = self.export_to_excel(df, final_path)
        elif file_format.lower() == "csv":
            res_path = self.export_to_csv(df, final_path)
        else:
            raise ValueError(f"🚨 ERROR: Formato '{file_format}' no soportado.")

        self.generate_txt_report(stats, res_path)
        
        return res_path

    def export_to_excel(self, df: pd.DataFrame, path: str) -> str:
        try:
            print(f"\n💾 Iniciando exportación a Excel...")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_excel(path, index=False, engine='openpyxl')
            self._log_success(path, len(df), "Excel")
            return path
        except Exception as e:
            raise Exception(f"🚨 ERROR CRÍTICO Excel: {str(e)}")

    def export_to_csv(self, df: pd.DataFrame, path: str) -> str:
        try:
            print(f"\n💾 Iniciando exportación a CSV...")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_csv(path, index=False, encoding='utf-8-sig', chunksize=100000)
            self._log_success(path, len(df), "CSV")
            return path
        except Exception as e:
            raise Exception(f"🚨 ERROR CRÍTICO CSV: {str(e)}")

    def generate_txt_report(self, stats: dict, data_file_path: str):
        """Crea el archivo .txt robusto con metadata de rango y granularidad incluida."""
        try:
            report_path = os.path.splitext(data_file_path)[0] + "_INFORME.txt"
            os.makedirs(os.path.dirname(report_path), exist_ok=True)
            
            def s(key, default=0, fmt=None):
                val = stats.get(key)
                if val is None or (isinstance(val, pd.Timestamp) and pd.isna(val)):
                    return default
                if isinstance(val, list):
                    clean_list = [x for x in val if not (isinstance(x, float) and np.isnan(x))]
                    return ", ".join(map(str, clean_list)) if clean_list else str(default)
                if fmt == ".2f":
                    try:
                        return f"{float(val):.2f}"
                    except (ValueError, TypeError):
                        return "0.00"
                if fmt == ",":
                    try:
                        return f"{int(float(val)):,}"
                    except (ValueError, TypeError):
                        return "0"
                return str(val)

            report = []
            report.append("="*60)
            report.append(f" PIPELINE UNIT ECONOMICS: PACIFIKO v5.6 ".center(60))
            report.append("="*60)
            report.append(f"✅ Descarga exitosa: {s('raw_rows', fmt=',')} filas obtenidas.")
            report.append(f"✅ SOIS cargado ({s('sois_count', fmt=',')}) | CATALOGO ({s('catalogo_count', fmt=',')})")
            report.append(f"⏱️  Fase 1 (Ingesta): {s('time_f1', fmt='.2f')}s")
            
            # --- SECCIÓN DE GRANULARIDAD Y RANGO ---
            report.append("\n" + "-"*60)
            report.append(" CONFIGURACIÓN DE ANÁLISIS ".center(60))
            report.append("-" * 60)
            report.append(f"📊 Granularidad de cohortes: {s('granularity_mode', 'quarterly')}")
            report.append(f"📅 Rango definido por usuario: {s('cohort_start_date', 'FULL_DATASET')} → {s('cohort_end_date', 'FULL_DATASET')}")
            report.append(f"📅 Rango real post-filtro: {s('min_date_post_filter', 'N/A')} → {s('max_date_post_filter', 'N/A')}")
            report.append(f"📊 Cohortes generadas: {s('cohort_count_dynamic', s('cohort_count', 'N/A'))}")
            
            report.append("\n" + "-"*60)
            report.append(" VALIDACION Y LOGICA DE NEGOCIO ".center(60))
            report.append("-" * 60)
            report.append(f"⚠️  Rescate PID: {s('rescue_count', fmt=',')} | Sin SOIS: {s('no_sois_count', fmt=',')}")
            report.append(f"📅 Rango original dataset: {s('min_date', 'N/A')} al {s('max_date', 'N/A')}")
            report.append(f"📊 Cohortes ({s('cohort_count')}): {s('cohort_list', 'None')}")
            
            report.append("\n" + "-"*60)
            report.append(" RESULTADOS FINALES ".center(60))
            report.append("-" * 60)
            report.append(f"📦 Qty Promedio: {s('avg_qty', fmt='.2f')} | CP Promedio: ${s('avg_cp', fmt='.2f')}")
            report.append(f"💰 CP TOTAL: ${s('total_cp', fmt=',')}")
            report.append(f"📊 Columnas: {s('col_count')} | Tiempo Total: {s('total_time', fmt='.2f')}s")
            
            if stats.get('filter_warning'):
                report.append(f"\n⚠️ ADVERTENCIA DE FILTRO: {stats.get('filter_warning')}")
            
            report.append("\n" + "="*60)
            report.append(" 🚀 FIN DEL PROCESO ".center(60))
            report.append("="*60)

            with open(report_path, "w", encoding="utf-8") as f:
                f.write("\n".join(report))
            print(f"✅ Informe de auditoría guardado: {os.path.basename(report_path)}")

        except PermissionError:
            print(f"⚠️ WARNING: Permiso denegado para escribir informe en {report_path}")
        except OSError as e:
            print(f"⚠️ WARNING: Error de sistema al generar informe TXT: {e}")
        except Exception as e:
            print(f"⚠️ WARNING: No se pudo generar el informe TXT, pero el Excel es válido. Error: {e}")

    def _log_success(self, path: str, rows: int, fmt: str):
        print("-" * 60)
        print(f"✅ EXPORTACIÓN {fmt} EXITOSA")
        print(f"   📊 Filas exportadas: {rows:,}")
        print("-" * 60)