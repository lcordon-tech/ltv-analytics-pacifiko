# Archivo: Model/Output/data_exporter.py
# Versión MULTI-PAÍS con prefijo por país

import pandas as pd
import os
from datetime import datetime
from collections import defaultdict
from Model.Utils.cohort_utils import CohortUtils


class DataExporter:
    """
    Exporta los resultados del análisis LTV a Excel y TXT.
    
    VERSIÓN MULTI-PAÍS:
    - Prefija nombres de archivo con código de país
    - Recibe country_config para metadata
    """
    
    def __init__(self, customers, ue_results, cohort_data, behavior_report=None, 
                 retention_data=None, retention_abs_data=None, retention_pct_data=None, 
                 mode=2, granularity="quarterly", country_config=None):
        """
        Args:
            country_config: Configuración del país (para prefijo y metadata)
        """
        self.customers = customers
        self.ue_results = ue_results
        self.cohort_data = cohort_data 
        self.behavior_report = behavior_report
        self.retention_abs_data = retention_abs_data
        self.retention_pct_data = retention_pct_data
        self.mode = mode
        self.granularity = granularity
        self.country_config = country_config
        
        # Directorio de salida desde variable de entorno
        self.output_dir = os.environ.get("LTV_OUTPUT_DIR", ".")
        
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def _get_country_prefix(self) -> str:
        """Retorna prefijo para archivos según el país."""
        if self.country_config:
            return f"{self.country_config.code}_"
        return ""

    def _get_path(self, base_name, ext):
        """Genera ruta con timestamp y prefijo de país."""
        fecha_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        prefix = self._get_country_prefix()
        nombre_archivo = f"{prefix}{base_name}_{fecha_str}{ext}"
        return os.path.join(self.output_dir, nombre_archivo)
    
    # ... (resto de métodos igual, solo cambia _get_path)
    
    def _get_dynamic_period_count(self) -> int:
        """
        Calcula el número de períodos necesario basado en las fechas reales.
        """
        if not self.customers:
            return 25 if self.granularity == "quarterly" else 76
        
        min_date = None
        max_date = None
        
        for customer in self.customers:
            orders = customer.get_orders_sorted()
            if orders:
                first_date = orders[0].order_date
                last_date = orders[-1].order_date
                if min_date is None or first_date < min_date:
                    min_date = first_date
                if max_date is None or last_date > max_date:
                    max_date = last_date
        
        if min_date is None or max_date is None:
            return 25 if self.granularity == "quarterly" else 76
        
        min_val = CohortUtils.get_period_value(min_date, self.granularity)
        max_val = CohortUtils.get_period_value(max_date, self.granularity)
        
        # Agregar margen de 2 períodos
        return max_val - min_val + 3
    
    def _get_period_label(self, idx: int) -> str:
        """Retorna la etiqueta para un período según la granularidad."""
        if self.granularity == "quarterly":
            return f"Q_{idx}"
        elif self.granularity == "monthly":
            return f"M_{idx}"
        elif self.granularity == "weekly":
            return f"W_{idx}"
        elif self.granularity == "semiannual":
            return f"H_{idx}"
        elif self.granularity == "yearly":
            return f"Y_{idx}"
        else:
            return f"P_{idx}"

    def export_to_excel(self):
        """Exporta todos los reportes a un archivo Excel."""
        path = self._get_path("Analisis_LTV_Pacifiko", ".xlsx")
        if not path:
            return

        # --- PRE-PROCESAMIENTO: Mapeo de CAC por Cohorte ---
        cac_por_cohorte = {cid: data['cac'] for cid, data in self.ue_results.items()}

        # --- HOJA 1: AUDITORÍA DETALLADA DE CLIENTES ---
        c_list = []
        for c in self.customers:
            orders = c.get_orders()
            sorted_orders = c.get_orders_sorted()
            if not sorted_orders:
                continue
            
            # 1. Datos de Identidad, Tiempo y Cohorte (USANDO CohortUtils)
            first_order = sorted_orders[0]
            fecha_inc = first_order.order_date
            cohort_id = CohortUtils.get_cohort_id(fecha_inc, self.granularity)
            
            # 2. Atributos de Adquisición
            bu_entrada = getattr(first_order, 'business_unit', 'N/A')
            
            # 3. Obtener TODAS las dimensiones de entrada
            categoria_entrada = getattr(first_order, 'category', 'N/A')
            subcategoria_entrada = getattr(first_order, 'subcategory', 'N/A')
            marca_entrada = getattr(first_order, 'brand', 'N/A')
            producto_entrada = getattr(first_order, 'name', 'N/A')
            
            # 4. Cálculos Financieros
            total_rev = c.total_revenue()
            total_cost = sum(o.cost for o in orders)
            total_sois = sum(o.sois for o in orders)
            total_ship_rev = sum(o.shipping_revenue for o in orders)
            total_ship_cost = sum(o.shipping_cost for o in orders)
            total_pay_costs = sum(o.credit_card_cost + o.cod_cost for o in orders)
            total_ops_costs = sum(o.fc_variable + o.cs_variable for o in orders)
            total_fixed_costs = sum(o.fraud_cost + o.infrastructure_cost for o in orders)
            total_retention_cost = sum(o.retention_cost for o in orders)
            
            margen_real_cp = c.total_cp()
            
            # CAC específico
            cac_especifico = cac_por_cohorte.get(cohort_id, 0)
            ltv_neto_real = margen_real_cp - cac_especifico

            c_list.append({
                "ID_Cliente": c.customer_id,
                "Fecha_Incorporacion": fecha_inc.strftime('%Y-%m-%d'),
                "Cohorte_Entrada": cohort_id,
                "BU_Entrada": bu_entrada,
                "Marca_Entrada": marca_entrada,
                "Producto_Entrada": producto_entrada,
                "Cat_Entrada": categoria_entrada,
                "Dimension_Entrada": subcategoria_entrada,
                "Cant_Compras": c.total_orders(),
                "GMV_Total_$": round(total_rev, 2),
                "(-) Costo_Producto_$": round(total_cost, 2),
                "(+) SOIS_$": round(total_sois, 2),
                "(+) Ingreso_Envio_$": round(total_ship_rev, 2),
                "(-) Gasto_Envio_$": round(total_ship_cost, 2),
                "(-) Comisiones_Pago_$": round(total_pay_costs, 2),
                "(-) Ops_Variables_$": round(total_ops_costs, 2),
                "(-) Fraude_Infra_$": round(total_fixed_costs, 2),
                "(-) Retencion_$": round(total_retention_cost, 2),
                "MARGEN_OPERATIVO_NETO_$": round(margen_real_cp, 2),
                "CAC_COHORTE_$": round(cac_especifico, 2),
                "LTV_NETO_REAL_$": round(ltv_neto_real, 2),
                "Diversificacion_Cats": len(c.get_categories())
            })
        
        df_clientes = pd.DataFrame(c_list)
        if not df_clientes.empty:
            df_clientes = df_clientes.sort_values("LTV_NETO_REAL_$", ascending=False)

        # --- HOJA 2: MATRIZ DE COHORTES (DINÁMICA) ---
        matrix_rows = []
        n_periods = self._get_dynamic_period_count()
        
        # Agrupar clientes por cohorte
        clientes_por_cohorte = defaultdict(list)
        for c in self.customers:
            sorted_orders = c.get_orders_sorted()
            if sorted_orders:
                f_date = sorted_orders[0].order_date
                cohort_id = CohortUtils.get_cohort_id(f_date, self.granularity)
                clientes_por_cohorte[cohort_id].append(c)

        # Procesar cada cohorte
        for cohort_id, data in self.cohort_data.items():
            if cohort_id not in clientes_por_cohorte:
                continue

            row = {"Cohorte": cohort_id, "Size": data["size"]}
            
            # Calcular máximo período permitido
            cohort_start = None
            try:
                parsed = CohortUtils.parse_cohort_id(cohort_id)
                if parsed:
                    year, period, _ = parsed
                    if self.granularity == "quarterly":
                        month = (period - 1) * 3 + 1
                        cohort_start = datetime(year, month, 1)
                    elif self.granularity == "monthly":
                        cohort_start = datetime(year, period, 1)
                    elif self.granularity == "yearly":
                        cohort_start = datetime(year, 1, 1)
            except:
                cohort_start = None
            
            # Usar el LTV del cohort_data o calcular límite dinámico
            for i in range(n_periods):
                col = self._get_period_label(i)
                val = data["ltv"].get(i, None)
                row[col] = round(val, 2) if val is not None else None
            
            total_real_cp = sum(c.total_cp() for c in clientes_por_cohorte[cohort_id])
            row["LTV_Total_Acumulado"] = round(total_real_cp, 2)
            matrix_rows.append(row)

        df_cohortes = pd.DataFrame(matrix_rows)
        if not df_cohortes.empty:
            df_cohortes = df_cohortes.sort_values("Cohorte")
            period_cols = [self._get_period_label(i) for i in range(n_periods)]
            columnas_finales = ["Cohorte", "Size"] + period_cols + ["LTV_Total_Acumulado"]
            # Solo incluir columnas que existen
            columnas_finales = [c for c in columnas_finales if c in df_cohortes.columns]
            df_cohortes = df_cohortes[columnas_finales]

        # --- HOJA 3: UNIT ECONOMICS ---
        ue_list = []
        sorted_ue = sorted(self.ue_results.items())

        for cid, data in sorted_ue:
            ratios_vals = [v for v in data["ltv_cac"].values() if v is not None]
            final_ratio = ratios_vals[-1] if ratios_vals else 0
            
            inversion_adquisicion = round(data.get("acq_spend", 0), 2)
            inversion_retencion = round(data.get("retention_spend_total", 0), 2)
            mkt_total = inversion_adquisicion + inversion_retencion
            
            nuevos = data.get("size", 0)
            existentes = data.get("existing_count", 0)
            total_clientes_activos = nuevos + existentes
            costo_mantenimiento_por_cliente = (inversion_retencion / existentes) if existentes > 0 else 0
            
            ue_list.append({
                "Cohorte": cid,
                "Nuevos_Adquiridos": nuevos,
                "Existentes_Recurrentes": existentes,
                "Total_Clientes_Activos": total_clientes_activos,
                "CAC_ADQUISICION_$": round(data["cac"], 2),
                "Inversion_Adquisicion_$": inversion_adquisicion,
                "Inversion_Retencion_Total_$": inversion_retencion,
                "Costo_Mantenimiento_Unitario_$": round(costo_mantenimiento_por_cliente, 2),
                "MARKETING_SPEND_TOTAL_$": round(mkt_total, 2),
                "Trimestre_Payback": data["payback_month"],
                "Ratio_LTV_CAC_Actual": round(final_ratio, 2),
                "%_Inversion_en_Retencion": f"{round((inversion_retencion / mkt_total) * 100, 1)}%" if mkt_total > 0 else "0%",
                "Status": "SALUDABLE" if final_ratio >= 3 else "REVISAR"
            })
        
        df_ue = pd.DataFrame(ue_list)
        if not df_ue.empty:
            df_ue = df_ue.sort_values("Cohorte")

        # --- HOJA 4: RESUMEN DE MÉTRICAS POR COHORTE ---
        resumen_list = []
        for cid, clientes in clientes_por_cohorte.items():
            t_items = 0
            t_orders = 0
            t_rev = 0
            t_cost = 0
            t_sois = 0
            t_s_rev = 0
            t_s_cost = 0
            t_pay = 0
            t_ops = 0
            t_fix = 0
            t_cp = 0
            t_retention = 0

            for c in clientes:
                orders = c.get_orders()
                t_items += sum(o.quantity for o in orders)
                t_orders += len(c.get_unique_orders())
                t_rev += c.total_revenue()
                t_cost += sum(o.cost for o in orders)
                t_sois += sum(o.sois for o in orders)
                t_s_rev += sum(o.shipping_revenue for o in orders)
                t_s_cost += sum(o.shipping_cost for o in orders)
                t_pay += sum(o.credit_card_cost + o.cod_cost for o in orders)
                t_ops += sum(o.fc_variable + o.cs_variable for o in orders)
                t_fix += sum(o.fraud_cost + o.infrastructure_cost for o in orders)
                t_retention += sum(o.retention_cost for o in orders)
                t_cp += c.total_cp()

            n = t_orders if t_orders > 0 else 1

            resumen_list.append({
                "Cohorte": cid,
                "Cant_Clientes": len(clientes),
                "Total_Ordenes": t_orders,
                "Units_Vendidas": t_items,
                "Units_por_Orden": round(t_items / n, 2),
                "GMV_Total_$": round(t_rev, 2),
                "GMV_por_Orden_$": round(t_rev / n, 2),
                "(-) Costo_Prod_Total_$": round(t_cost, 2),
                "(-) Costo_Prod_por_Orden_$": round(t_cost / n, 2),
                "(+) SOIS_Total_$": round(t_sois, 2),
                "(+) SOIS_por_Orden_$": round(t_sois / n, 2),
                "(+) Ingr_Envio_Total_$": round(t_s_rev, 2),
                "(+) Ingr_Envio_por_Orden_$": round(t_s_rev / n, 2),
                "(-) Gasto_Envio_Total_$": round(t_s_cost, 2),
                "(-) Gasto_Envio_por_Orden_$": round(t_s_cost / n, 2),
                "(-) Comis_Pago_Total_$": round(t_pay, 2),
                "(-) Comis_Pago_por_Orden_$": round(t_pay / n, 2),
                "(-) Ops_Var_Total_$": round(t_ops, 2),
                "(-) Ops_Var_por_Orden_$": round(t_ops / n, 2),
                "(-) Fraude_Infra_Total_$": round(t_fix, 2),
                "(-) Fraude_Infra_por_Orden_$": round(t_fix / n, 2),
                "(-) Retencion_Total_$": round(t_retention, 2),
                "(-) Retencion_por_Orden_$": round(t_retention / n, 2),
                "MARGEN_NETO_CP_TOTAL_$": round(t_cp, 2),
                "MARGEN_NETO_por_Orden_$": round(t_cp / n, 2),
                "%_Margen_CP": round((t_cp / t_rev) * 100, 2) if t_rev > 0 else 0
            })

        df_resumen = pd.DataFrame(resumen_list)
        if not df_resumen.empty:
            df_resumen = df_resumen.sort_values("Cohorte")

        # --- HOJAS DE COMPORTAMIENTO ---
        df_freq = pd.DataFrame()
        df_time = pd.DataFrame()
        df_conv = pd.DataFrame()

        if self.behavior_report:
            if 'frequency' in self.behavior_report:
                df_freq = pd.DataFrame(self.behavior_report['frequency'])
                if not df_freq.empty:
                    df_freq = df_freq.sort_values("Cohorte")
            if 'time' in self.behavior_report:
                df_time = pd.DataFrame(self.behavior_report['time'])
                if not df_time.empty:
                    df_time = df_time.sort_values("Cohorte")
            if 'conversion' in self.behavior_report:
                df_conv = pd.DataFrame(self.behavior_report['conversion'])
                if not df_conv.empty:
                    df_conv = df_conv.sort_values("Cohorte")

        # --- MATRICES DE RETENCIÓN ---
        df_ret_abs = pd.DataFrame(self.retention_abs_data) if self.retention_abs_data else pd.DataFrame()
        if not df_ret_abs.empty:
            df_ret_abs = df_ret_abs.sort_values("Cohorte")
            # Detectar prefijo dinámicamente
            prefix = "M" if any(col.startswith("M") for col in df_ret_abs.columns) else "Q"
            period_cols = sorted([c for c in df_ret_abs.columns if c.startswith(prefix)], 
                                key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)
            cols_abs = ["Cohorte", "Size"] + period_cols
            df_ret_abs = df_ret_abs[[c for c in cols_abs if c in df_ret_abs.columns]]

        df_ret_pct = pd.DataFrame(self.retention_pct_data) if self.retention_pct_data else pd.DataFrame()
        if not df_ret_pct.empty:
            df_ret_pct = df_ret_pct.sort_values("Cohorte")
            prefix = "M" if any(col.startswith("M") for col in df_ret_pct.columns) else "Q"
            period_cols = sorted([c for c in df_ret_pct.columns if c.startswith(prefix)], 
                                key=lambda x: int(x[1:]) if x[1:].isdigit() else 0)
            cols_pct = ["Cohorte", "Size"] + period_cols
            df_ret_pct = df_ret_pct[[c for c in cols_pct if c in df_ret_pct.columns]]

        # --- ESCRITURA FINAL AL EXCEL ---
        try:
            with pd.ExcelWriter(path, engine='openpyxl') as writer:
                if not df_clientes.empty:
                    df_clientes.to_excel(writer, sheet_name="1. Auditoria Clientes", index=False)
                if not df_cohortes.empty:
                    df_cohortes.to_excel(writer, sheet_name="2. Matriz LTV", index=False)
                if not df_ue.empty:
                    df_ue.to_excel(writer, sheet_name="3. Unit Economics", index=False)
                if not df_resumen.empty:
                    df_resumen.to_excel(writer, sheet_name="4. Resumen Por Cohorte", index=False)
                
                if not df_freq.empty:
                    df_freq.to_excel(writer, sheet_name="5. Frecuencia de Compra", index=False)
                if not df_time.empty:
                    df_time.to_excel(writer, sheet_name="6. Velocidad de Recompra", index=False)
                if not df_conv.empty:
                    df_conv.to_excel(writer, sheet_name="7. Ventanas de Conversion", index=False)
                if not df_ret_abs.empty:
                    df_ret_abs.to_excel(writer, sheet_name="8. Retencion Activa (Abs)", index=False)
                if not df_ret_pct.empty:
                    df_ret_pct.to_excel(writer, sheet_name="9. Retencion Activa (%)", index=False)
            
            print(f"✅ Excel exportado exitosamente: {path}")
            
        except PermissionError:
            print(f"⚠️ ERROR: El archivo Excel está abierto. Ciérralo para guardar.")
        except Exception as e:
            print(f"⚠️ Error exportando Excel: {e}")

    def export_summary_text(self, content):
        """Exporta el resumen ejecutivo a un archivo TXT."""
        path = self._get_path("Resumen_Ejecutivo_LTV", ".txt")
        if not path:
            return
        
        try:
            with open(path, "w", encoding="utf-8") as f:
                # Agregar metadata de granularidad al inicio
                f.write(f"=== REPORTE LTV - {self.granularity.upper()} ===\n")
                f.write(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 60 + "\n\n")
                f.write(content)
            print(f"✅ Resumen TXT guardado: {path}")
        except Exception as e:
            print(f"⚠️ Error guardando resumen TXT: {e}")