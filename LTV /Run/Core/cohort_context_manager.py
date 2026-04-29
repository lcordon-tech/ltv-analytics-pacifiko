"""
Sistema centralizado de gestión de cohortes multi-país.
Unifica todos los supuestos incluyendo CAC.
"""

import re
from typing import Dict, Any, List, Optional
from pathlib import Path
import pandas as pd
from dataclasses import dataclass, field

from Run.Country.country_context import CountryContext


@dataclass
class CohortData:
    """Estructura de datos para una cohorte."""
    cohort_id: str
    shipping_cost: float = 0.0
    shipping_revenue: float = 0.0
    credit_card_payment: float = 0.0
    cash_on_delivery_comision: float = 0.0
    fc_variable_headcount: float = 0.0
    cs_variable_headcount: float = 0.0
    fraud: float = 0.0
    infrastructure: float = 0.0
    cogs: float = 0.0
    retention: float = 0.0
    cac: float = 0.0
    
    def to_dict(self) -> Dict[str, float]:
        return {k: v for k, v in self.__dict__.items() if k != 'cohort_id' and not k.startswith('_')}
    
    def update(self, field: str, value: float):
        if hasattr(self, field):
            setattr(self, field, value)
        else:
            raise ValueError(f"Campo desconocido: {field}")


class CohortContextManager:
    """
    Gestor central de cohortes multi-país.
    Unifica shipping, variable, fraud, cogs, retention, CAC.
    """
    
    EDITABLE_FIELDS = [
        'shipping_cost', 'shipping_revenue', 'credit_card_payment',
        'cash_on_delivery_comision', 'fc_variable_headcount',
        'cs_variable_headcount', 'fraud', 'infrastructure',
        'cogs', 'retention', 'cac'
    ]
    
    BUSINESS_UNITS = ['1P', '3P', 'FBP', 'TM', 'DS']
    
    def __init__(self, supuestos_path: Path, country_context: CountryContext):
        self.supuestos_path = supuestos_path
        self.country_context = country_context
        self._cohorts: Dict[str, Dict[str, CohortData]] = {}
        self._load_all()
    
    def _parse_cohort_key(self, cohort: str) -> tuple:
        cohort_clean = cohort.upper().strip()
        
        if cohort_clean.startswith('Q'):
            num_str = cohort_clean[1:]
            if num_str.startswith('-'):
                return (int(num_str), 0)
            else:
                return (int(num_str), 0)
        elif '-' in cohort_clean:
            parts = cohort_clean.split('-')
            if len(parts) == 2:
                if parts[1].startswith('Q'):
                    return (int(parts[0]), int(parts[1][1:]))
                else:
                    return (int(parts[0]), int(parts[1]))
        return (0, 0)
    
    def _load_all(self):
        """Carga todos los supuestos desde Excel (hoja por país)."""
        if not self.supuestos_path.exists():
            print(f"⚠️ Archivo no encontrado: {self.supuestos_path}")
            return
        
        sheet_name = self.country_context.get_excel_sheet("supuestos")
        
        try:
            excel_file = pd.ExcelFile(self.supuestos_path)
            
            if sheet_name not in excel_file.sheet_names:
                print(f"⚠️ Hoja '{sheet_name}' no encontrada en {self.supuestos_path}")
                return
            
            for bu in self.BUSINESS_UNITS:
                self._cohorts[bu] = {}
            
            df = pd.read_excel(self.supuestos_path, sheet_name=sheet_name)
            df['cohort'] = df['cohort'].astype(str).str.strip().str.upper()
            
            # Filtrar por año de inicio del país
            start_year = self.country_context.cohort_start_year
            
            for _, row in df.iterrows():
                cohort_id = row['cohort']
                
                # Validar que la cohorte esté dentro del rango del país
                if not self._is_valid_cohort_for_country(cohort_id, start_year):
                    continue
                
                cohort_data = CohortData(
                    cohort_id=cohort_id,
                    shipping_cost=float(row.get('shipping_cost', 0)),
                    shipping_revenue=float(row.get('shipping_revenue', 0)),
                    credit_card_payment=float(row.get('credit_card_payment', 0)),
                    cash_on_delivery_comision=float(row.get('cash_on_delivery_comision', 0)),
                    fc_variable_headcount=float(row.get('fc_variable_headcount', 0)),
                    cs_variable_headcount=float(row.get('cs_variable_headcount', 0)),
                    fraud=float(row.get('fraud', 0)),
                    infrastructure=float(row.get('infrastructure', 0)),
                    cogs=float(row.get('cogs', 0)),
                    retention=float(row.get('retention', 0)),
                    cac=float(row.get('cac', 0))
                )
                
                # Determinar BU desde el Excel (si tiene columna bu, si no, 1P por defecto)
                bu = row.get('bu', '1P')
                if bu in self._cohorts:
                    self._cohorts[bu][cohort_id] = cohort_data
            
            print(f"✅ CohortContextManager cargado para {self.country_context.code}: {sum(len(c) for c in self._cohorts.values())} cohortes")
            
        except Exception as e:
            print(f"❌ Error cargando cohortes para {self.country_context.code}: {e}")
    
    def _is_valid_cohort_for_country(self, cohort_id: str, start_year: int) -> bool:
        """Valida si una cohorte pertenece al rango del país."""
        try:
            if cohort_id.startswith('Q'):
                num_str = cohort_id[1:]
                if num_str.startswith('-'):
                    year = start_year + (int(num_str) // 4)
                else:
                    year = start_year + ((int(num_str) - 1) // 4)
                return year >= start_year
            elif '-' in cohort_id:
                parts = cohort_id.split('-')
                year = int(parts[0])
                return year >= start_year
            return True
        except:
            return True
    
    def get_cohort(self, bu: str, cohort_id: str) -> Optional[CohortData]:
        return self._cohorts.get(bu, {}).get(cohort_id)
    
    def get_all_cohorts(self, bu: str) -> List[str]:
        cohorts = list(self._cohorts.get(bu, {}).keys())
        return self._sort_cohorts(cohorts)
    
    def _sort_cohorts(self, cohorts: List[str]) -> List[str]:
        def sort_key(c):
            if c.startswith('Q'):
                num_str = c[1:]
                if num_str.startswith('-'):
                    return (int(num_str), 0)
                else:
                    return (int(num_str), 0)
            return (0, 0)
        return sorted(cohorts, key=sort_key)
    
    def update_cohort(self, bu: str, cohort_id: str, field: str, value: float) -> bool:
        if bu not in self._cohorts:
            print(f"❌ BU '{bu}' no encontrado")
            return False
        
        if cohort_id not in self._cohorts[bu]:
            print(f"❌ Cohorte '{cohort_id}' no encontrada en {bu}")
            return False
        
        if field not in self.EDITABLE_FIELDS:
            print(f"❌ Campo '{field}' no es editable")
            return False
        
        self._cohorts[bu][cohort_id].update(field, value)
        return self._persist_to_excel(bu, cohort_id, field, value)
    
    def apply_to_bu(self, bu_list: List[str], cohort_id: str, field: str, value: float) -> Dict[str, bool]:
        results = {}
        for bu in bu_list:
            if bu in self.BUSINESS_UNITS:
                results[bu] = self.update_cohort(bu, cohort_id, field, value)
            else:
                results[bu] = False
        return results
    
    def _persist_to_excel(self, bu: str, cohort_id: str, field: str, value: float) -> bool:
        try:
            sheet_name = self.country_context.get_excel_sheet("supuestos")
            df = pd.read_excel(self.supuestos_path, sheet_name=sheet_name)
            df['cohort'] = df['cohort'].astype(str).str.strip().str.upper()
            
            mask = (df['cohort'] == cohort_id)
            if mask.any():
                df.loc[mask, field] = value
                
                with pd.ExcelWriter(self.supuestos_path, engine='openpyxl', 
                                   mode='a', if_sheet_exists='replace') as writer:
                    excel_file = pd.ExcelFile(self.supuestos_path)
                    for sheet in excel_file.sheet_names:
                        if sheet != sheet_name:
                            df_sheet = pd.read_excel(self.supuestos_path, sheet_name=sheet)
                            df_sheet.to_excel(writer, sheet_name=sheet, index=False)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                return True
        except Exception as e:
            print(f"❌ Error persistiendo: {e}")
            return False
    
    def get_cac_map(self, granularity: str = "quarterly") -> Dict[str, float]:
        cac_map = {}
        if '1P' in self._cohorts:
            for cohort_id, cohort_data in self._cohorts['1P'].items():
                cac_map[cohort_id] = cohort_data.cac
        return cac_map
    
    def get_retention_map(self, granularity: str = "quarterly") -> Dict[str, float]:
        retention_map = {}
        if '1P' in self._cohorts:
            for cohort_id, cohort_data in self._cohorts['1P'].items():
                retention_map[cohort_id] = cohort_data.retention
        return retention_map
    
    def get_cogs_map(self, granularity: str = "quarterly") -> Dict[str, float]:
        cogs_map = {}
        if 'TM' in self._cohorts:
            for cohort_id, cohort_data in self._cohorts['TM'].items():
                cogs_map[cohort_id] = cohort_data.cogs
        return cogs_map
    
    def get_all_cohort_ids(self) -> List[str]:
        all_cohorts = set()
        for bu in self.BUSINESS_UNITS:
            all_cohorts.update(self._cohorts.get(bu, {}).keys())
        return self._sort_cohorts(list(all_cohorts))
    
    def print_summary(self):
        all_cohorts = self.get_all_cohort_ids()
        print(f"\n📊 CohortContextManager Summary ({self.country_context.code})")
        print(f"   Total cohortes únicas: {len(all_cohorts)}")
        print(f"   Cohortes: {all_cohorts[:15]}{'...' if len(all_cohorts) > 15 else ''}")
        print(f"   Business Units: {self.BUSINESS_UNITS}")
        print(f"   Campos editables: {self.EDITABLE_FIELDS}")