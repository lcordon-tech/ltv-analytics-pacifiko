"""
Gestor de supuestos por cohorte para múltiples países.
Soporta archivos Excel con hojas por Business Unit y por país.
VERSIÓN CORREGIDA: Soporte correcto para hojas con sufijo (1PGT, 3PCR, etc.)
"""

import pandas as pd
import os
from typing import List, Dict, Optional, Tuple
from openpyxl import load_workbook


class CohortSupuestosManager:
    """
    Gestiona la lectura, validación y edición de supuestos por cohorte.
    
    AHORA: Soporta multi-país mediante country_code.
    - Busca hojas con formato {BU}{country_code} (ej: 1PGT, 1PCR)
    - O mantiene compatibilidad con estructura legacy (hojas 1P, 3P, etc.)
    """
    
    # Business Units estándar
    EXPECTED_SHEETS = ['1P', '3P', 'FBP', 'TM', 'DS']
    
    # Columnas esperadas en cada hoja
    EXPECTED_COLUMNS = [
        'cohort', 'shipping_cost', 'shipping_revenue', 'credit_card_payment',
        'cash_on_delivery_comision', 'fc_variable_headcount', 'cs_variable_headcount',
        'fraud', 'infrastructure', 'cogs', 'retention', 'cac'
    ]
    
    def __init__(self, supuestos_path: str, country_code: str = "CR"):
        """
        Args:
            supuestos_path: Ruta al archivo SUPUESTOS.xlsx
            country_code: Código del país (GT, CR, etc.) para seleccionar hojas correctas
        """
        self.supuestos_path = supuestos_path
        self.country_code = country_code.upper()
        self._excel_file = None
        self._sheet_names = None
        self._existing_cohorts = {}  # {sheet_name: set(cohorts)}
        
        # Detectar modo de operación
        self._mode = self._detect_mode()
        print(f"   🔧 CohortSupuestosManager: país={self.country_code}, modo={self._mode}")
        self._load_existing_cohorts()
    
    def _detect_mode(self) -> str:
        """Detecta si las hojas tienen sufijo de país o no."""
        try:
            xl = pd.ExcelFile(self.supuestos_path)
            sheet_names = xl.sheet_names
            print(f"   📋 Hojas encontradas: {sheet_names}")
            
            # Buscar hojas que terminan con el código del país
            for sheet in sheet_names:
                if sheet.endswith(self.country_code):
                    print(f"   ✅ Modo multi-país detectado (sufijo '{self.country_code}')")
                    return 'country_suffix'
            
            # Buscar hojas exactas sin sufijo
            for sheet in sheet_names:
                if sheet in self.EXPECTED_SHEETS:
                    print(f"   📋 Modo legacy detectado (hojas sin sufijo)")
                    return 'legacy'
            
            # Fallback: modo legacy
            print(f"   ⚠️ Usando modo legacy por defecto")
            return 'legacy'
            
        except Exception as e:
            print(f"⚠️ Error detectando modo: {e}. Usando modo legacy.")
            return 'legacy'
    
    def _get_sheet_name_for_bu(self, bu: str) -> str:
        """
        Retorna el nombre de la hoja para una Business Unit según el modo.
        
        Args:
            bu: Business Unit (1P, 3P, FBP, TM, DS)
        
        Returns:
            Nombre de la hoja (ej: '1PGT' o '1P')
        """
        if self._mode == 'country_suffix':
            return f"{bu}{self.country_code}"
        return bu
    
    def _load_existing_cohorts(self):
        """Carga todas las cohortes existentes del archivo Excel."""
        try:
            xl = pd.ExcelFile(self.supuestos_path)
            self._sheet_names = xl.sheet_names
            
            for bu in self.EXPECTED_SHEETS:
                sheet_name = self._get_sheet_name_for_bu(bu)
                
                if sheet_name not in self._sheet_names:
                    continue
                
                df = pd.read_excel(self.supuestos_path, sheet_name=sheet_name)
                df['cohort'] = df['cohort'].astype(str).str.strip().str.upper()
                
                cohorts = set(df['cohort'].unique())
                self._existing_cohorts[bu] = cohorts
                
        except Exception as e:
            print(f"⚠️ Error cargando cohortes existentes: {e}")
    
    def validate_supuestos_file(self) -> List[str]:
        """
        Valida la estructura del archivo SUPUESTOS.xlsx.
        
        Returns:
            Lista de advertencias encontradas
        """
        warnings = []
        
        if not os.path.exists(self.supuestos_path):
            return [f"❌ Archivo no encontrado: {self.supuestos_path}"]
        
        try:
            xl = pd.ExcelFile(self.supuestos_path)
            sheet_names = xl.sheet_names
            
            for bu in self.EXPECTED_SHEETS:
                sheet_name = self._get_sheet_name_for_bu(bu)
                
                if sheet_name not in sheet_names:
                    warnings.append(f"⚠️ Hoja '{sheet_name}' no encontrada (BU: {bu})")
                    continue
                
                df = pd.read_excel(self.supuestos_path, sheet_name=sheet_name)
                df.columns = [str(col).strip().lower() for col in df.columns]
                
                # Validar columnas requeridas
                missing_cols = [col for col in self.EXPECTED_COLUMNS if col not in df.columns]
                if missing_cols:
                    warnings.append(f"⚠️ Hoja '{sheet_name}': faltan columnas {missing_cols}")
                
                # Validar que no haya cohortes vacías
                if 'cohort' in df.columns:
                    empty_cohorts = df['cohort'].isna().sum()
                    if empty_cohorts > 0:
                        warnings.append(f"⚠️ Hoja '{sheet_name}': {empty_cohorts} cohortes vacías")
            
            if not warnings:
                print(f"✅ Archivo SUPUESTOS.xlsx válido para {self.country_code}")
            
            return warnings
            
        except Exception as e:
            return [f"❌ Error validando SUPUESTOS.xlsx: {e}"]
    
    def get_existing_cohorts(self, bu: str = None) -> Dict[str, set]:
        """
        Retorna las cohortes existentes.
        
        Args:
            bu: Business Unit específica (opcional)
        
        Returns:
            Dict con cohortes por BU, o set de cohortes si se especifica BU
        """
        if bu:
            return self._existing_cohorts.get(bu, set())
        return self._existing_cohorts
    
    def interactive_setup(self, cohorts_in_data: List[str]) -> bool:
        """
        Configura interactivamente las cohortes nuevas.
        
        Args:
            cohorts_in_data: Lista de cohortes detectadas en los datos
        
        Returns:
            True si el usuario continúa, False si cancela
        """
        if not cohorts_in_data:
            return True
        
        # Obtener todas las cohortes existentes
        all_existing = set()
        for cohorts in self._existing_cohorts.values():
            all_existing.update(cohorts)
        
        # Detectar cohortes nuevas
        new_cohorts = set(cohorts_in_data) - all_existing
        
        if not new_cohorts:
            print(f"✅ Todas las cohortes ya están configuradas en SUPUESTOS.xlsx")
            return True
        
        print(f"\n📊 Se detectaron {len(new_cohorts)} cohortes nuevas no configuradas:")
        for cohort in sorted(new_cohorts)[:10]:
            print(f"   • {cohort}")
        if len(new_cohorts) > 10:
            print(f"   ... y {len(new_cohorts) - 10} más")
        
        print("\n⚠️ Estas cohortes no tienen supuestos definidos (shipping, cogs, retention, cac)")
        print("   El pipeline puede continuar usando valores por defecto (0)")
        
        respuesta = input("\n👉 ¿Deseas configurarlas ahora? (s/n): ").strip().lower()
        
        if respuesta in ['s', 'si', 'sí', 'yes', 'y']:
            self._add_new_cohorts_interactive(list(new_cohorts))
            return True
        else:
            print("   Continuando con valores por defecto para cohortes nuevas...")
            return True
    
    def _add_new_cohorts_interactive(self, new_cohorts: List[str]):
        """
        Agrega nuevas cohortes al archivo Excel de forma interactiva.
        """
        print("\n" + "=" * 50)
        print("   AGREGAR NUEVAS COHORTES".center(50))
        print("=" * 50)
        
        print(f"\n📌 Nuevas cohortes a agregar: {new_cohorts}")
        
        # Preguntar por pestañas
        print("\n📋 ¿En qué pestañas quieres agregarlas?")
        print("   1. Todas (1P, 3P, FBP, TM, DS)")
        print("   2. Seleccionar manualmente")
        print("   3. Cancelar")
        
        option = input("\n👉 Opción (1/2/3): ").strip()
        
        if option == '3':
            return
        
        sheets_to_update = []
        if option == '1':
            sheets_to_update = self.EXPECTED_SHEETS
        else:
            print("\nSelecciona pestañas (separadas por número):")
            for i, sheet in enumerate(self.EXPECTED_SHEETS, 1):
                print(f"   {i}. {sheet}")
            sheet_input = input("\n👉 Números (ej: 1,3,5): ").strip()
            try:
                indices = [int(x.strip()) - 1 for x in sheet_input.split(',')]
                sheets_to_update = [self.EXPECTED_SHEETS[i] for i in indices if 0 <= i < len(self.EXPECTED_SHEETS)]
            except:
                print("❌ Selección inválida")
                return
        
        if not sheets_to_update:
            print("❌ No se seleccionaron pestañas")
            return
        
        # Preguntar por valores por defecto
        use_defaults = input("\n¿Usar valores por defecto para retention y cogs? (s/n): ").strip().lower()
        auto_defaults = use_defaults in ['s', 'si', 'sí', 'yes', 'y']
        
        # Agregar a cada pestaña
        for bu in sheets_to_update:
            sheet_name = self._get_sheet_name_for_bu(bu)
            print(f"\n📝 Procesando pestaña: {sheet_name}")
            
            new_rows = []
            for cohort in new_cohorts:
                row = self._prompt_for_values(cohort, bu, use_defaults=auto_defaults)
                new_rows.append(row)
            
            if new_rows:
                self._append_to_excel(sheet_name, new_rows)
        
        print("\n✅ Cohortes agregadas exitosamente")
        self._load_existing_cohorts()
    
    def _prompt_for_values(self, cohort: str, bu: str, use_defaults: bool = False) -> Dict:
        """
        Solicita valores para una nueva cohorte.
        
        Args:
            cohort: Identificador de cohorte
            bu: Business Unit
            use_defaults: Si True, usa valores por defecto sin preguntar
        """
        if use_defaults:
            return {
                'cohort': cohort,
                'shipping_cost': 0,
                'shipping_revenue': 0,
                'credit_card_payment': 0,
                'cash_on_delivery_comision': 0,
                'fc_variable_headcount': 0,
                'cs_variable_headcount': 0,
                'fraud': 0,
                'infrastructure': 0,
                'cogs': 0.7 if bu == 'TM' else 0,
                'retention': 0,
                'cac': 0
            }
        
        print(f"\n📝 Configurando cohorte {cohort} para BU {bu}:")
        
        cogs_default = 0.7 if bu == 'TM' else 0
        retention_default = 0
        cac_default = 0
        
        cogs = input(f"   COGS (default {cogs_default}): ").strip()
        retention = input(f"   Retention (default {retention_default}): ").strip()
        cac = input(f"   CAC (default {cac_default}): ").strip()
        
        return {
            'cohort': cohort,
            'shipping_cost': 0,
            'shipping_revenue': 0,
            'credit_card_payment': 0,
            'cash_on_delivery_comision': 0,
            'fc_variable_headcount': 0,
            'cs_variable_headcount': 0,
            'fraud': 0,
            'infrastructure': 0,
            'cogs': float(cogs) if cogs else cogs_default,
            'retention': float(retention) if retention else retention_default,
            'cac': float(cac) if cac else cac_default
        }
    
    def _append_to_excel(self, sheet_name: str, new_rows: List[Dict]):
        """
        Agrega nuevas filas a una hoja existente del Excel.
        """
        try:
            # Leer hoja existente
            df = pd.read_excel(self.supuestos_path, sheet_name=sheet_name)
            df['cohort'] = df['cohort'].astype(str).str.strip().str.upper()
            
            # Convertir nuevas filas a DataFrame
            df_new = pd.DataFrame(new_rows)
            df_new['cohort'] = df_new['cohort'].astype(str).str.strip().str.upper()
            
            # Filtrar cohortes que ya existen
            existing_cohorts = set(df['cohort'])
            truly_new = df_new[~df_new['cohort'].isin(existing_cohorts)]
            
            if truly_new.empty:
                print(f"   ⚠️ Todas las cohortes ya existían en {sheet_name}")
                return
            
            # Concatenar
            df_combined = pd.concat([df, truly_new], ignore_index=True)
            
            # Guardar
            with pd.ExcelWriter(self.supuestos_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                # Preservar otras hojas
                excel_file = pd.ExcelFile(self.supuestos_path)
                for sheet in excel_file.sheet_names:
                    if sheet != sheet_name:
                        df_sheet = pd.read_excel(self.supuestos_path, sheet_name=sheet)
                        df_sheet.to_excel(writer, sheet_name=sheet, index=False)
                df_combined.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"   ✅ Agregadas {len(truly_new)} cohortes a {sheet_name}")
            
        except Exception as e:
            print(f"   ❌ Error agregando cohortes a {sheet_name}: {e}")
    
    def get_cohort_supuestos(self, cohort_id: str, bu: str) -> Optional[Dict]:
        """
        Obtiene los supuestos para una cohorte específica.
        
        Args:
            cohort_id: Identificador de cohorte
            bu: Business Unit
        
        Returns:
            Diccionario con valores o None si no existe
        """
        sheet_name = self._get_sheet_name_for_bu(bu)
        
        try:
            df = pd.read_excel(self.supuestos_path, sheet_name=sheet_name)
            df['cohort'] = df['cohort'].astype(str).str.strip().str.upper()
            
            mask = df['cohort'] == cohort_id.upper()
            if mask.any():
                row = df[mask].iloc[0]
                return {col: row[col] for col in self.EXPECTED_COLUMNS if col in row}
            
            return None
            
        except Exception as e:
            print(f"⚠️ Error obteniendo supuestos para {cohort_id}/{bu}: {e}")
            return None
    
    def print_summary(self):
        """Imprime resumen de cohortes existentes."""
        print(f"\n📊 SUPUESTOS EXISTENTES - {self.country_code}")
        print("-" * 40)
        
        for bu, cohorts in self._existing_cohorts.items():
            if cohorts:
                sheet_name = self._get_sheet_name_for_bu(bu)
                sorted_cohorts = sorted(list(cohorts), 
                                       key=lambda x: int(x[1:]) if x[1:].lstrip('-').isdigit() else -999)
                print(f"\n📋 {sheet_name} ({bu}): {len(cohorts)} cohortes")
                display = sorted_cohorts[:10]
                if len(sorted_cohorts) > 10:
                    display.append("...")
                print(f"   {', '.join(display)}")