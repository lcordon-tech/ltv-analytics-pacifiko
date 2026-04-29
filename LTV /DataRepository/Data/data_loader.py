import pandas as pd
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

# Importaciones multi-país
try:
    from Run.Country.country_context import CountryContext
except ImportError:
    # Para ejecución independiente del DataRepository
    class CountryContext:
        def __init__(self, code="GT", name="Guatemala"):
            self.code = code
            self.name = name
        def get_excel_sheet(self, file_type: str) -> str:
            return self.code


class DataLoader:
    """
    Responsabilidad: Orquestar la carga de todas las fuentes.
    Recibe el motor de query y la ruta base desde el main.
    
    AHORA: Soporta multi-país con hojas específicas por país.
    - SOIS: hoja = código país (GT, CR)
    - CATALOGO: hoja = código país (GT, CR)
    - SUPUESTOS: hojas = {BU}{código país} (ej: 1PGT, 1PCR, 3PGT, 3PCR, etc.)
    """
    
    def __init__(self, query_engine, base_dir: str):
        self.query_engine = query_engine
        self.base_dir = base_dir

    def _auto_save_backup(self, df: pd.DataFrame, country_code: str = "GT"):
        """Guarda una copia de seguridad local de los datos crudos en formato CSV."""
        if df is None or df.empty:
            print("⚠️ Backup omitido: DataFrame vacío.")
            return

        try:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
            file_name = f"Backup_Ordenes_DB_{country_code}_{timestamp}.csv"
            file_path = os.path.join(self.base_dir, file_name)
            os.makedirs(self.base_dir, exist_ok=True)
            df.to_csv(file_path, index=False, encoding='utf-8-sig', chunksize=100000)
            print(f"✅ AUTO-BACKUP: Guardado en {file_path}")
        except Exception as e:
            print(f"⚠️ Error al generar auto-backup: {e}")

    def load_all_sources(self, sois_path: str, assumptions_path: str, catalog_path: str, 
                         start_date=None, end_date=None, save_excel=True,
                         country_context: Optional[CountryContext] = None) -> dict:
        """
        Coordina la DB y los 3 Excels (SOIS, Supuestos, Catálogo).
        
        Args:
            sois_path: Ruta al archivo SOIS.xlsx
            assumptions_path: Ruta al archivo SUPUESTOS.xlsx
            catalog_path: Ruta al archivo catalogLTV.xlsx
            start_date: datetime - fecha inicio para filtrar órdenes
            end_date: datetime - fecha fin para filtrar órdenes
            save_excel: bool - guardar copia de seguridad
            country_context: CountryContext - contexto del país (GT, CR, etc.)
        """
        # Determinar país para logging
        if country_context is None:
            country_context = CountryContext("GT", "Guatemala")
        
        country_code = country_context.code
        print(f"\n🌎 DataLoader: Cargando datos para {country_context.name} ({country_code})")
        
        # Validar archivos Excel
        paths = {
            "SOIS": sois_path,
            "SUPUESTOS": assumptions_path,
            "CATALOGO": catalog_path
        }
        
        for name, path in paths.items():
            if not os.path.exists(path):
                print(f"🛑 Error: No se encontró el archivo {name} en: {path}")
                sys.exit(1)

        # Obtener órdenes con filtro de rango
        print("🔌 Conectando a la base de datos y extrayendo órdenes...")
        if start_date or end_date:
            print(f"📅 Aplicando filtro temporal: {start_date} → {end_date}")
        
        # ⭐ EL QUERY ENGINE YA TIENE EL country_code INTERNO
        df_orders = self.query_engine.fetch_orders(start_date=start_date, end_date=end_date)
        
        if df_orders.empty:
            print("🛑 Error: No se puede continuar sin datos de órdenes.")
            sys.exit(1)

        self._auto_save_backup(df_orders, country_code)

        # Cargar archivos Excel con las hojas correctas según país
        print(f"📂 Cargando archivos maestros locales para {country_context.name}...")
        
        try:
            # --- SOIS: cargar hoja específica del país ---
            sois_sheet = country_context.get_excel_sheet("sois")
            print(f"   📄 SOIS: leyendo hoja '{sois_sheet}'")
            df_sois = pd.read_excel(sois_path, sheet_name=sois_sheet)
            
            # --- SUPUESTOS: cargar hojas {BU}{código país} ---
            # Ejemplo: 1PGT, 3PGT, FBPGT, TMGT, DSGT para Guatemala
            #          1PCR, 3PCR, FBPCR, TMCR, DSCR para Costa Rica
            expected_bus = ['1P', '3P', 'FBP', 'TM', 'DS']
            expected_sheets = [f"{bu}{country_code}" for bu in expected_bus]
            
            print(f"   📄 SUPUESTOS: leyendo hojas específicas para {country_code}")
            df_assumptions = {}
            
            for sheet_name in expected_sheets:
                try:
                    df_sheet = pd.read_excel(assumptions_path, sheet_name=sheet_name)
                    # El nombre de la hoja sin el sufijo del país es la BU
                    bu_name = sheet_name[:-2] if sheet_name.endswith(country_code) else sheet_name
                    df_assumptions[bu_name] = df_sheet
                    print(f"      - Hoja '{sheet_name}' → BU: {bu_name} ({len(df_sheet)} filas)")
                except ValueError as e:
                    if f"Worksheet named '{sheet_name}' not found" in str(e):
                        print(f"      ⚠️ Hoja '{sheet_name}' no encontrada (se esperaba para {country_code})")
                    else:
                        print(f"      ⚠️ Error leyendo hoja '{sheet_name}': {e}")
                except Exception as e:
                    print(f"      ⚠️ Error leyendo hoja '{sheet_name}': {e}")
            
            # Verificar que se cargaron todas las BUs esperadas
            loaded_bus = list(df_assumptions.keys())
            missing_bus = [bu for bu in expected_bus if bu not in loaded_bus]
            if missing_bus:
                print(f"      ⚠️ Faltan BUs para {country_code}: {missing_bus}")
                print(f"      Se usarán valores por defecto (0) para esas BUs")
            
            # --- CATÁLOGO: cargar hoja específica del país ---
            catalog_sheet = country_context.get_excel_sheet("catalog")
            print(f"   📄 CATALOGO: leyendo hoja '{catalog_sheet}'")
            df_catalog = pd.read_excel(catalog_path, sheet_name=catalog_sheet)
            
            print(f"✅ SOIS cargado ({len(df_sois)} filas) - hoja: {sois_sheet}")
            print(f"✅ SUPUESTOS cargado ({len(df_assumptions)} BUs: {', '.join(df_assumptions.keys())})")
            print(f"✅ CATALOGO cargado ({len(df_catalog)} filas) - hoja: {catalog_sheet}")
            print("-" * 60)
            print("✨ Ingesta Completada: DB + 3 Excels listos.")
            print(f"🌎 País: {country_context.name}")
            print("-" * 60)
            
            return {
                "orders": df_orders,
                "sois": df_sois,
                "assumptions": df_assumptions,
                "catalog": df_catalog
            }
            
        except ValueError as e:
            if "Sheet" in str(e) and "not found" in str(e):
                print(f"❌ Error: Hoja no encontrada en {sois_path}")
                print(f"   Verifica que el archivo tenga una hoja llamada '{country_context.code}'")
            else:
                print(f"❌ Error al leer archivos Excel: {e}")
            return {}
            
        except Exception as e:
            print(f"❌ Error al leer archivos Excel: {e}")
            import traceback
            traceback.print_exc()
            return {}