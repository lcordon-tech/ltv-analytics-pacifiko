#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
mainDR.py - Data Repository Pipeline para LTV
Versión MULTI-PAÍS con tipo de cambio dinámico y queries específicas por país
"""

import sys
import os
import pandas as pd
import numpy as np
import time
import traceback
from datetime import datetime
from pathlib import Path

# Configuración de path
current_file = os.path.abspath(__file__)
output_folder = os.path.dirname(current_file)
data_repo_folder = os.path.dirname(output_folder)
project_root = os.path.dirname(data_repo_folder)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
if data_repo_folder not in sys.path:
    sys.path.insert(0, data_repo_folder)

print(f"📂 Project root agregado al path: {project_root}")
print(f"📂 DataRepository folder: {data_repo_folder}")
print("-"*60)

from DataRepository.Data.query_engine import QueryEngine
from DataRepository.Data.data_loader import DataLoader
from DataRepository.Data.data_validator import DataValidator
from DataRepository.Data.data_merger import DataMerger
from DataRepository.Processing.cohort_builder import CohortBuilder
from DataRepository.Processing.retention_applier import RetentionApplier
from DataRepository.Processing.assumption_applier import AssumptionApplier
from DataRepository.Processing.metrics_calculator import MetricsCalculator
from DataRepository.Output.final_dataset_builder import FinalDatasetBuilder
from DataRepository.Output.data_exporter import DataExporter

# Importaciones multi-país
from Run.Country.country_context import CountryContextFactory
from Run.FX.fx_engine import FXEngine


def get_cohort_year_from_context(cohort: str, country_context) -> int:
    """
    Extrae el año real de una cohorte según el contexto del país.
    Soporta múltiples formatos: Q1, YYYY-MM, YYYY-Wxx, YYYY-H1, YYYY
    """
    try:
        cohort_str = str(cohort).upper().strip()
        
        # Formato Q1, Q2, Q3... (quarterly)
        if cohort_str.startswith('Q'):
            num = int(cohort_str[1:])
            return country_context.cohort_start_year + (num - 1) // 4
        
        # Formato YYYY-MM (monthly)
        elif '-' in cohort_str and len(cohort_str) == 7 and not 'W' in cohort_str and not 'H' in cohort_str:
            return int(cohort_str.split('-')[0])
        
        # Formato YYYY-Wxx (weekly)
        elif '-W' in cohort_str:
            return int(cohort_str.split('-')[0])
        
        # Formato YYYY-H1 / YYYY-H2 (semiannual)
        elif '-H' in cohort_str:
            return int(cohort_str.split('-')[0])
        
        # Formato YYYY (yearly)
        elif cohort_str.isdigit() and len(cohort_str) == 4:
            return int(cohort_str)
        
        else:
            # Fallback: intentar extraer cualquier número
            import re
            numbers = re.findall(r'\d+', cohort_str)
            if numbers:
                return int(numbers[0])
            return country_context.cohort_start_year
            
    except Exception as e:
        return country_context.cohort_start_year


def run_pipeline():
    # --- 1. LEER CONFIGURACIÓN MULTI-PAÍS DESDE VARIABLES DE ENTORNO ---
    country_code = os.environ.get("LTV_COUNTRY", "GT").upper().strip()
    print(f"\n🌎 País seleccionado para DR: {country_code}")
    
    # Crear contexto del país
    country_context = CountryContextFactory.create(country_code)
    print(f"   Moneda: {country_context.currency}")
    print(f"   FX Default: {country_context.default_fx_rate}")
    print(f"   Cohortes desde: {country_context.cohort_start_year}")
    
    # --- 2. CONFIGURACIÓN DE RUTAS ---
    PATH_DIR = os.environ.get("LTV_PATH_CONTROL")
    BASE_DIR = PATH_DIR if PATH_DIR else str(project_root / f"Data_LTV_{country_code}")
    
    OUTPUT_DIR = os.environ.get("LTV_OUTPUT_DIR")
    if not OUTPUT_DIR:
        OUTPUT_DIR = os.path.join(BASE_DIR, "Data_LTV")
    
    INPUT_DIR = os.environ.get("LTV_INPUT_DIR", os.path.join(BASE_DIR, "inputs"))
    
    # Nombres de archivos (unificados por tipo, con hojas por país)
    SOIS_FILE = os.environ.get("LTV_SOIS_FILE", "SOIS.xlsx")
    SUPUESTOS_FILE = os.environ.get("LTV_SUPUESTOS_FILE", "SUPUESTOS.xlsx")
    CATALOGO_FILE = os.environ.get("LTV_CATALOGO_FILE", "catalogLTV.xlsx")
    CAC_FILE = os.environ.get("LTV_CAC_FILE", "CAC.xlsx")
    FX_FILE = os.environ.get("LTV_FX_FILE", "TIPO_DE_CAMBIO.xlsx")
    
    PATH_SOIS = os.path.join(INPUT_DIR, SOIS_FILE)
    PATH_SUPUESTOS = os.path.join(INPUT_DIR, SUPUESTOS_FILE)
    PATH_CATALOGO = os.path.join(INPUT_DIR, CATALOGO_FILE)
    PATH_CAC = os.path.join(INPUT_DIR, CAC_FILE)
    PATH_FX = os.path.join(INPUT_DIR, FX_FILE)
    
    PATH_OUTPUT_BASE = os.path.join(OUTPUT_DIR, f"Resultado_Unit_Economics_{country_code}.csv")
    
    # Validar archivos requeridos
    required_files = [PATH_SOIS, PATH_SUPUESTOS, PATH_CATALOGO]
    for path in required_files:
        if not os.path.exists(path):
            raise FileNotFoundError(f"🚨 ERROR: No se encuentra el archivo {path}")
    
    # Verificar archivo FX (warning si no existe)
    fx_engine = None
    if os.path.exists(PATH_FX):
        print(f"✅ Archivo FX encontrado: {os.path.basename(PATH_FX)}")
        fx_engine = FXEngine(country_context, Path(PATH_FX))
    else:
        print(f"⚠️ Archivo FX no encontrado: {PATH_FX}")
        print(f"   Usando tasa por defecto: {country_context.default_fx_rate}")
    
    # Verificar CAC (warning, no error)
    if not os.path.exists(PATH_CAC):
        print(f"⚠️ Archivo CAC no encontrado: {PATH_CAC}")
        print("   El LTV neto (con CAC) no estará disponible en el modelo")
    else:
        print(f"✅ Archivo CAC encontrado: {os.path.basename(PATH_CAC)}")
    
    # --- 3. CREDENCIALES DE BD ---
    USER = os.environ.get("DB_USER")
    PASSWORD = os.environ.get("DB_PASSWORD")
    HOST = os.environ.get("DB_HOST")
    DB = os.environ.get("DB_NAME")

    missing_vars = [k for k, v in {
        "DB_USER": USER,
        "DB_PASSWORD": PASSWORD,
        "DB_HOST": HOST,
        "DB_NAME": DB
    }.items() if not v]

    if missing_vars:
        raise ValueError(f"🚨 ERROR: Faltan variables de entorno para DB: {missing_vars}")
    
    print(f"🔌 Conectando a BD: {HOST}/{DB} (país: {country_code})")

    # --- 4. LEER RANGO DE FECHAS ---
    start_date_str = os.environ.get("LTV_START_DATE")
    end_date_str = os.environ.get("LTV_END_DATE")
    
    start_date = None
    end_date = None
    
    # 🔧 PROCESAR START_DATE (robusto contra dict, string, datetime)
    if start_date_str:
        try:
            if isinstance(start_date_str, dict):
                # Extraer valor de diccionario (viene de menu_executor)
                val = start_date_str.get('start_date', start_date_str.get('date', '2020-01-01'))
                start_date = pd.to_datetime(val)
            else:
                start_date = pd.to_datetime(start_date_str)
            
            print(f"📅 Filtro inicio: {start_date.date()}")
        except Exception as e:
            print(f"⚠️ Fecha inicio inválida: {start_date_str} - {e}")
            start_date = None
    
    # 🔧 PROCESAR END_DATE (robusto contra dict, string, datetime)
    if end_date_str:
        try:
            if isinstance(end_date_str, dict):
                # Extraer valor de diccionario (viene de menu_executor)
                val = end_date_str.get('end_date', end_date_str.get('date', '2026-04-15'))
                end_date = pd.to_datetime(val)
            else:
                end_date = pd.to_datetime(end_date_str)
            
            print(f"📅 Filtro fin: {end_date.date()}")
        except Exception as e:
            print(f"⚠️ Fecha fin inválida: {end_date_str} - {e}")
            end_date = None
    
    if not start_date and not end_date:
        print("📅 Sin filtro de rango definido. Usando dataset COMPLETO.")

    # --- 5. LEER GRANULARIDAD ---
    granularidad = os.environ.get("LTV_GRANULARITY", "quarterly")
    print(f"📊 Granularidad de cohortes: {granularidad}")

    pipeline_start = time.time()
    stats = {} 

    print("="*60)
    print(f" PIPELINE UNIT ECONOMICS v6.0 - {country_context.name} ".center(60)) 
    print("="*60)
    print(f"📂 INPUT_DIR: {INPUT_DIR}")
    print(f"📂 OUTPUT_DIR: {OUTPUT_DIR}")
    print(f"📄 SOIS: {SOIS_FILE} (hoja: {country_context.get_excel_sheet('sois')})")
    print(f"📄 SUPUESTOS: {SUPUESTOS_FILE} (hoja: {country_context.get_excel_sheet('supuestos')})")
    print(f"📄 CATALOGO: {CATALOGO_FILE} (hoja: {country_context.get_excel_sheet('catalog')})")
    print(f"📄 FX: {FX_FILE if os.path.exists(PATH_FX) else 'NO ENCONTRADO'}")
    print("-"*60)

    try:
        # --- Inicializar componentes ---
        # ⭐ QUERY ENGINE CON CÓDIGO DE PAÍS
        q_engine = QueryEngine(
            user=USER, 
            password=PASSWORD, 
            host=HOST, 
            db=DB,
            country_code=country_code
        )
        
        loader = DataLoader(query_engine=q_engine, base_dir=OUTPUT_DIR)
        validator = DataValidator()
        merger = DataMerger()
        
        # ⭐ CohortBuilder con contexto
        cohort_handler = CohortBuilder(granularidad=granularidad, country_context=country_context)
        
        # ⭐ RetentionApplier con contexto (pasa a TimeGranularityAdapter)
        retention_handler = RetentionApplier(granularidad=granularidad, country_context=country_context)
        
        applier = AssumptionApplier()

        # ⭐ MetricsCalculator con contexto y fx_engine
        calculator = MetricsCalculator(
            granularidad=granularidad,
            country_context=country_context,
            fx_engine=fx_engine
        )
        
        final_builder = FinalDatasetBuilder()
        exporter = DataExporter()

        # --- FASE 1: INGESTA CON RANGO ---
        start_f1 = time.time()
        
        datasets = loader.load_all_sources(
            PATH_SOIS, PATH_SUPUESTOS, PATH_CATALOGO,
            start_date=start_date, 
            end_date=end_date,
            country_context=country_context
        )
        
        df_orders_raw = datasets.get("orders", pd.DataFrame())
        df_sois_raw = datasets.get("sois", pd.DataFrame())
        df_catalog_raw = datasets.get("catalog", pd.DataFrame())
        assumptions_dict = datasets.get("assumptions", {})
        
        stats.update({
            'time_f1': time.time() - start_f1,
            'raw_rows': len(df_orders_raw),
            'sois_count': len(df_sois_raw),
            'catalogo_count': len(df_catalog_raw)
        })
        print(f"⏱️  Fase 1 (Ingesta) completada en: {stats['time_f1']:.2f}s")

        # --- FASE 2: VALIDACIÓN ---
        start_f2 = time.time()
        df_assumptions_1p = assumptions_dict.get('1P', pd.DataFrame())
        df_orders_val, df_sois_val, val_stats = validator.run(
            df_orders=df_orders_raw,
            df_sois=df_sois_raw,
            df_assumptions=df_assumptions_1p
        )
        stats.update({
            'time_f2': time.time() - start_f2,
            'rescue_count': val_stats.get('rescued_pids', 0),
            'no_sois_count': val_stats.get('missing_sois', 0)
        })
        print(f"⏱️  Fase 2 (Validación) completada en: {stats['time_f2']:.2f}s")

        # --- FASE 3: MERGES ---
        start_f3 = time.time()
        df_with_cat = merger.merge_catalog(df_orders_val, df_catalog_raw)
        df_orders_merged = merger.merge_sois(df_with_cat, df_sois_val)
        stats['time_f3'] = time.time() - start_f3
        print(f"⏱️  Fase 3 (Merges) completada en: {stats['time_f3']:.2f}s")

        # --- FASE 4: COHORTES ---
        start_f4 = time.time()
        df_orders_cohorts = cohort_handler.build_cohort(df_orders_merged)
        
        # 🔧 FILTRO CORREGIDO: Usar función que respeta el año base del país
        if country_context.cohort_start_year > 2020:
            df_orders_cohorts['_cohort_year'] = df_orders_cohorts['cohort'].apply(
                lambda c: get_cohort_year_from_context(c, country_context)
            )
            mask_year = df_orders_cohorts['_cohort_year'] >= country_context.cohort_start_year
            df_orders_cohorts = df_orders_cohorts[mask_year].copy()
            df_orders_cohorts = df_orders_cohorts.drop(columns=['_cohort_year'])
            print(f"📅 Filtrado por año inicio {country_context.cohort_start_year}: {len(df_orders_cohorts)} filas restantes")
        
        unique_cohorts = sorted(
            df_orders_cohorts['cohort'].unique().tolist(), 
            key=lambda x: int(x[1:]) if len(x) > 1 and x[1:].lstrip('-').isdigit() else 0
        )
        min_date = df_orders_cohorts['order_date'].min()
        max_date = df_orders_cohorts['order_date'].max()
        stats.update({
            'time_f4': time.time() - start_f4,
            'min_date': min_date if pd.notna(min_date) else "No disponible",
            'max_date': max_date if pd.notna(max_date) else "No disponible",
            'cohort_list': unique_cohorts,
            'cohort_count': len(unique_cohorts)
        })
        print(f"⏱️  Fase 4 (Cohortes) completada en: {stats['time_f4']:.2f}s")

        # --- FASE RETENCIÓN ---
        start_ret = time.time()
        df_orders_retention = retention_handler.apply(df_orders_cohorts, assumptions_dict=assumptions_dict)
        stats['time_retention'] = time.time() - start_ret
        print(f"⏱️  Fase Retención completada en: {stats['time_retention']:.2f}s")

        # --- FASE 5: SUPUESTOS ---
        start_f5 = time.time()
        df_with_assump = applier.apply(df_orders_retention, assumptions_dict)
        stats['time_f5'] = time.time() - start_f5
        print(f"⏱️  Fase 5 (Supuestos) completada en: {stats['time_f5']:.2f}s")

        # --- FASE 6: MÉTRICAS (con FX dinámico) ---
        start_f6 = time.time()
        df_with_metrics = calculator.run(df_with_assump, assumptions_dict=assumptions_dict)
        stats['time_f6'] = time.time() - start_f6
        stats['avg_qty'] = df_with_metrics['quantity'].mean()
        stats['avg_cp'] = df_with_metrics['contribution_profit'].mean()
        print(f"⏱️  Fase 6 (Métricas) completada en: {stats['time_f6']:.2f}s")

        # --- FASE 7: ESTRUCTURACIÓN ---
        start_f7 = time.time()
        df_final = final_builder.build(df_with_metrics)

        if len(df_final) > 0:
            cat_cov = (df_final['category'] != 'UNKNOWN').mean() * 100
            category_coverage_str = f"{cat_cov:.1f}%"
            sub_cov = (df_final['subcategory'] != 'UNKNOWN').mean() * 100
            subcategory_coverage_str = f"{sub_cov:.1f}%"
            
            brand_cov = (df_final['brand'] != 'UNKNOWN').mean() * 100
            brand_coverage_str = f"{brand_cov:.1f}%"
            name_cov = (df_final['name'] != 'UNKNOWN').mean() * 100
            name_coverage_str = f"{name_cov:.1f}%"
            
            brand_nulls = df_final['brand'].isnull().sum()
            name_nulls = df_final['name'].isnull().sum()
            
            if brand_nulls > 0 or name_nulls > 0:
                print(f"⚠️ WARNING: brand nulos={brand_nulls}, name nulos={name_nulls}")
            else:
                print(f"✅ Validación brand/name: 0 nulos")
        else:
            category_coverage_str = "N/A"
            subcategory_coverage_str = "N/A"
            brand_coverage_str = "N/A"
            name_coverage_str = "N/A"

        # --- AUTO-VALIDACIÓN DEL RANGO ---
        print("\n" + "🔍 AUTO-VALIDACIÓN DE RANGO".center(60, "-"))
        
        if start_date:
            actual_min = df_final['order_date'].min()
            if actual_min >= start_date:
                print(f"✅ Filtro inicio OK: {actual_min.date()} >= {start_date.date()}")
            else:
                print(f"❌ ERROR: min_date ({actual_min.date()}) < start_date ({start_date.date()})")
                stats['filter_warning'] = "start_date_filter_failed"
        
        if end_date:
            actual_max = df_final['order_date'].max()
            if actual_max <= end_date:
                print(f"✅ Filtro fin OK: {actual_max.date()} <= {end_date.date()}")
            else:
                print(f"❌ ERROR: max_date ({actual_max.date()}) > end_date ({end_date.date()})")
                stats['filter_warning'] = "end_date_filter_failed"
        
        cohort_dates = df_final.groupby('cohort')['order_date'].min()
        print(f"📊 Cohortes generadas: {len(cohort_dates)}")
        
        stats.update({
            'time_f7': time.time() - start_f7,
            'col_count': len(df_final.columns),
            'final_rows': len(df_final),
            'category_coverage': category_coverage_str,
            'subcategory_coverage': subcategory_coverage_str,
            'brand_coverage': brand_coverage_str,
            'name_coverage': name_coverage_str,
            'cohort_start_date': start_date.strftime("%Y-%m-%d") if start_date else "FULL_DATASET",
            'cohort_end_date': end_date.strftime("%Y-%m-%d") if end_date else "FULL_DATASET",
            'cohort_count_dynamic': len(cohort_dates),
            'min_date_post_filter': df_final['order_date'].min().strftime("%Y-%m-%d"),
            'max_date_post_filter': df_final['order_date'].max().strftime("%Y-%m-%d"),
            'granularity_mode': granularidad,
            'country': country_context.code
        })
        print("-" * 60)

        print(f"⏱️  Fase 7 (Estructuración) completada en: {stats['time_f7']:.2f}s")
        print(f"📊 Cobertura: Cat ({category_coverage_str}) | Subcat ({subcategory_coverage_str})")
        print(f"🏷️  Cobertura: Brand ({brand_coverage_str}) | Name ({name_coverage_str})")

        # --- FASE 8: EXPORTACIÓN ---
        start_f8 = time.time()
        stats['total_time'] = time.time() - pipeline_start
        stats['total_cp'] = df_final['contribution_profit'].sum() if len(df_final) > 0 else 0

        safe_stats = {}
        for k, v in stats.items():
            if v is None:
                safe_stats[k] = 0 if isinstance(stats.get(k, 0), (int, float)) else "N/A"
            elif isinstance(v, float) and (pd.isna(v) or np.isnan(v)):
                safe_stats[k] = 0
            else:
                safe_stats[k] = v

        ruta_final = exporter.export(
            df=df_final, 
            base_path=PATH_OUTPUT_BASE, 
            stats=safe_stats,
            file_format="csv",
            add_timestamp=True
        )

        print(f"📊 Filas finales: {len(df_final):,}")
        
        stats['time_f8'] = time.time() - start_f8
        print(f"⏱️  Fase 8 (Exportación) completada en: {stats['time_f8']:.2f}s")
        
        print("\n" + "="*60)
        print(f" 🚀 PIPELINE FINALIZADO EXITOSAMENTE - {country_context.name} ".center(60))
        print(f"   ⏳ Tiempo Total: {stats['total_time']:.2f} segundos")
        print(f"   📊 Registros procesados: {stats['final_rows']:,}")
        print(f"   💸 CP Total: ${stats['total_cp']:,.2f} USD")
        print(f"   📍 Archivo: {os.path.basename(ruta_final)}")
        print(f"   📂 Ubicación: {OUTPUT_DIR}")
        print("="*60)
        
        return df_final

    except Exception as e:
        print(f"\n🚨 ERROR CRÍTICO: {e}")
        traceback.print_exc()
        return None


if __name__ == "__main__":
    resultado = run_pipeline()