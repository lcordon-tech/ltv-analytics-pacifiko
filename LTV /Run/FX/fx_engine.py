"""
Sistema de tipo de cambio (FX) multi-país.
Con validación de hojas, manejo de errores y FALLBACK DINÁMICO por cercanía.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Optional, List
from Run.Country.country_context import CountryContext


class FXEngine:
    """Gestiona tipos de cambio por país y cohorte con FALLBACK DINÁMICO."""
    
    def __init__(self, country_context: CountryContext, fx_path: Path):
        """
        Args:
            country_context: Contexto del país
            fx_path: Ruta al archivo TIPO_DE_CAMBIO.xlsx
        """
        self.context = country_context
        self.fx_path = fx_path
        self._rates: Dict[str, float] = {}
        self._available_sheets: List[str] = []
        self._fallback_stats: Dict[str, str] = {}  # {cohort: cohort_used}
        self._load_rates()
    
    def _extract_cohort_number(self, cohort: str) -> Optional[int]:
        """
        Extrae número numérico de una cohorte para comparación.
        
        Soporta:
        - Q1, Q2, Q10 → 1, 2, 10
        - 2024-03 → 202403
        - 2024-W12 → 202412
        - 2024-H1 → 20241
        - 2024 → 2024
        """
        import re
        
        cohort = str(cohort).upper().strip()
        
        # Formato Q* (quarterly)
        if cohort.startswith('Q'):
            try:
                return int(cohort[1:])
            except ValueError:
                pass
        
        # Formato YYYY-MM (monthly)
        match = re.match(r'(\d{4})-(\d{2})', cohort)
        if match:
            return int(match.group(1) + match.group(2))
        
        # Formato YYYY-Wxx (weekly)
        match = re.match(r'(\d{4})-W(\d{2})', cohort)
        if match:
            return int(match.group(1) + match.group(2))
        
        # Formato YYYY-H1 / YYYY-H2 (semiannual)
        match = re.match(r'(\d{4})-H([12])', cohort)
        if match:
            return int(match.group(1) + match.group(2))
        
        # Formato YYYY (yearly)
        if cohort.isdigit() and len(cohort) == 4:
            return int(cohort) * 100
        
        return None
    
    def _get_closest_rate(self, cohort: str) -> Optional[float]:
        """
        Busca la tasa más cercana por proximidad de cohorte.
        
        Returns:
            Tasa encontrada o None si no hay ninguna tasa disponible
        """
        if not self._rates:
            return None
        
        # Si existe exactamente, usarlo
        if cohort in self._rates:
            return self._rates[cohort]
        
        cohort_num = self._extract_cohort_number(cohort)
        if cohort_num is None:
            # Si no se puede extraer número, buscar por orden alfabético
            available_sorted = sorted(self._rates.keys())
            if not available_sorted:
                return None
            
            # Buscar el inmediatamente anterior o siguiente
            for i, key in enumerate(available_sorted):
                if key > cohort:
                    if i == 0:
                        return self._rates[key]
                    prev_dist = abs(len(key) - len(available_sorted[i-1]))
                    curr_dist = abs(len(key) - len(cohort))
                    if curr_dist < prev_dist:
                        return self._rates[key]
                    return self._rates[available_sorted[i-1]]
            return self._rates[available_sorted[-1]]
        
        # Extraer números de todas las cohortes disponibles
        available_nums = []
        for k, v in self._rates.items():
            num = self._extract_cohort_number(k)
            if num is not None:
                available_nums.append((num, k, v))
        
        if not available_nums:
            return None
        
        # Ordenar y buscar el más cercano
        available_nums.sort(key=lambda x: x[0])
        
        # Búsqueda del más cercano
        closest_num = min(available_nums, key=lambda x: abs(x[0] - cohort_num))
        
        # Registrar estadística de fallback
        if closest_num[1] != cohort:
            self._fallback_stats[cohort] = closest_num[1]
        
        return closest_num[2]
    
    def _load_rates(self):
        """Carga tipos de cambio desde Excel con validación robusta."""
        
        print(f"\n📂 [FXEngine] Cargando tipos de cambio para {self.context.code}")
        print(f"   Archivo: {self.fx_path}")
        
        if not self.fx_path.exists():
            print(f"   ❌ Archivo FX no encontrado: {self.fx_path}")
            print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
            return
        
        print(f"   ✅ Archivo encontrado")
        
        try:
            sheet_name = self.context.get_excel_sheet("fx")
            print(f"   🔍 Buscando hoja: '{sheet_name}'")
            
            try:
                excel_file = pd.ExcelFile(self.fx_path)
                self._available_sheets = excel_file.sheet_names
                print(f"   📄 Hojas disponibles: {self._available_sheets}")
            except Exception as e:
                print(f"   ❌ No se pudo leer el archivo FX: {e}")
                print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
                return
            
            if sheet_name not in self._available_sheets:
                print(f"   ❌ Hoja '{sheet_name}' NO encontrada")
                print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
                return
            
            print(f"   ✅ Hoja '{sheet_name}' encontrada, cargando datos...")
            
            df = pd.read_excel(self.fx_path, sheet_name=sheet_name)
            print(f"   📊 Filas leídas: {len(df)}")
            
            if df.empty:
                print(f"   ⚠️ Hoja '{sheet_name}' está vacía")
                print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
                return
            
            print(f"   📋 Columnas encontradas: {list(df.columns)}")
            
            # Identificar columna de tasa
            rate_col = None
            possible_rate_cols = ['rate', 'rate_usd', 'rate_crc_usd', 'rate_gtq_usd', 
                                  'fx_rate', 'tipo_cambio', 'exchange_rate']
            
            for col in df.columns:
                col_lower = str(col).lower().strip()
                if col_lower in possible_rate_cols or 'rate' in col_lower:
                    rate_col = col
                    print(f"   🔍 Columna de tasa detectada: '{rate_col}'")
                    break
            
            if rate_col is None:
                for col in df.columns:
                    if col != 'cohort' and pd.api.types.is_numeric_dtype(df[col]):
                        rate_col = col
                        print(f"   🔍 Usando primera columna numérica como tasa: '{rate_col}'")
                        break
            
            if rate_col is None:
                print(f"   ❌ No se encontró columna de tasa en hoja '{sheet_name}'")
                print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
                return
            
            # Limpiar y cargar datos
            df['cohort'] = df['cohort'].astype(str).str.strip().str.upper()
            
            rate_count = 0
            for _, row in df.iterrows():
                cohort = row['cohort']
                try:
                    rate = float(row[rate_col])
                    if rate > 0:
                        self._rates[cohort] = rate
                        rate_count += 1
                except (ValueError, TypeError):
                    continue
            
            if self._rates:
                print(f"   ✅ FXEngine: {rate_count} tasas cargadas para {self.context.code}")
                print(f"   📌 Hoja: '{sheet_name}' | Columna: '{rate_col}'")
                
                sample = list(self._rates.items())[:5]
                for cohort, rate in sample:
                    print(f"      {cohort}: {rate:.4f}")
                if len(self._rates) > 5:
                    print(f"      ... y {len(self._rates) - 5} más")
            else:
                print(f"   ⚠️ No se cargaron tasas válidas")
                print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
            
        except Exception as e:
            print(f"   ❌ Error cargando FX: {e}")
            print(f"   💡 Usando tasa por defecto: {self.context.default_fx_rate}")
            import traceback
            traceback.print_exc()
    
    def get_rate(self, cohort: str, granularity: str = "quarterly") -> float:
        """
        Retorna tipo de cambio para una cohorte con FALLBACK DINÁMICO.
        
        Prioridad:
        1. Búsqueda exacta en tabla
        2. Mapeo para granularidades no quarterly (YYYY-MM → YYYY-Q)
        3. Fallback dinámico por cercanía de cohorte
        4. Tasa por defecto del país
        
        Args:
            cohort: Identificador de cohorte (ej: 'Q1', '2024-01', '2024-W12')
            granularity: Granularidad de cohorte
        
        Returns:
            Tasa de cambio (moneda local → USD)
        """
        cohort_clean = cohort.upper().strip()
        
        # 1. Búsqueda exacta
        if cohort_clean in self._rates:
            return self._rates[cohort_clean]
        
        # 2. Mapeo para granularidad mensual/semanal/semestral
        if granularity != "quarterly":
            # Formato YYYY-MM
            if '-' in cohort_clean and len(cohort_clean) == 7 and 'W' not in cohort_clean and 'H' not in cohort_clean:
                year, month = cohort_clean.split('-')
                quarter = (int(month) - 1) // 3 + 1
                quarterly_cohort = f"Q{quarter}"
                # Buscar Q1, Q2, etc. (sin año)
                if quarterly_cohort in self._rates:
                    return self._rates[quarterly_cohort]
                # Buscar YYYY-Q1
                yearly_quarter = f"{year}-{quarterly_cohort}"
                if yearly_quarter in self._rates:
                    return self._rates[yearly_quarter]
            
            # Formato YYYY-Wxx (weekly)
            elif '-W' in cohort_clean:
                year = cohort_clean.split('-')[0]
                for existing_cohort in self._rates:
                    if existing_cohort.startswith(year) and existing_cohort.endswith('Q1'):
                        return self._rates[existing_cohort]
            
            # Formato YYYY-H1 (semiannual)
            elif '-H' in cohort_clean:
                year = cohort_clean.split('-')[0]
                for existing_cohort in self._rates:
                    if existing_cohort.startswith(year):
                        return self._rates[existing_cohort]
        
        # 3. Fallback dinámico por cercanía
        closest_rate = self._get_closest_rate(cohort_clean)
        if closest_rate is not None:
            return closest_rate
        
        # 4. Tasa por defecto del país
        return self.context.default_fx_rate
    
    def get_rates_map(self) -> Dict[str, float]:
        """Retorna el mapa completo de tasas."""
        return self._rates.copy()
    
    def convert_to_usd(self, amount: float, cohort: str, granularity: str = "quarterly") -> float:
        """Convierte monto de moneda local a USD."""
        rate = self.get_rate(cohort, granularity)
        if rate <= 0:
            return amount
        return amount / rate
    
    def convert_from_usd(self, amount: float, cohort: str, granularity: str = "quarterly") -> float:
        """Convierte monto de USD a moneda local."""
        rate = self.get_rate(cohort, granularity)
        if rate <= 0:
            return amount
        return amount * rate
    
    def get_available_sheets(self) -> List[str]:
        """Retorna las hojas disponibles en el archivo FX."""
        return self._available_sheets.copy()
    
    def validate_coverage(self, cohorts: List[str]) -> Dict[str, any]:
        """
        Valida qué cohortes tienen tasa definida vs cuáles usan fallback.
        
        Args:
            cohorts: Lista de cohortes a validar
        
        Returns:
            Dict con estadísticas de cobertura
        """
        exact = []
        fallback = []
        missing = []
        
        for cohort in cohorts:
            cohort_clean = cohort.upper().strip()
            
            if cohort_clean in self._rates:
                exact.append(cohort_clean)
            elif self._get_closest_rate(cohort_clean) is not None:
                fallback.append(cohort_clean)
            else:
                missing.append(cohort_clean)
        
        total = len(cohorts)
        return {
            'exact': exact,
            'fallback': fallback,
            'missing': missing,
            'exact_pct': round(len(exact) / total * 100, 2) if total else 0,
            'fallback_pct': round(len(fallback) / total * 100, 2) if total else 0,
            'missing_pct': round(len(missing) / total * 100, 2) if total else 0
        }
    
    def print_summary(self):
        """Imprime resumen completo del FXEngine con estadísticas de fallback."""
        print("\n" + "=" * 50)
        print(f" FX ENGINE SUMMARY - {self.context.code} ".center(50))
        print("=" * 50)
        print(f"📁 Archivo: {self.fx_path.name if self.fx_path else 'No definido'}")
        print(f"🌎 País: {self.context.name} ({self.context.code})")
        print(f"💱 Tasa default: {self.context.default_fx_rate}")
        print(f"📊 Tasas cargadas: {len(self._rates)}")
        
        if self._rates:
            print(f"\n📋 Ejemplo de tasas:")
            sample = list(self._rates.items())[:5]
            for cohort, rate in sample:
                print(f"   {cohort}: {rate:.4f}")
            if len(self._rates) > 5:
                print(f"   ... y {len(self._rates) - 5} más")
        
        if self._fallback_stats:
            print(f"\n🔄 FALLBACK DINÁMICO UTILIZADO:")
            for original, used in list(self._fallback_stats.items())[:10]:
                print(f"   {original} → {used}")
            if len(self._fallback_stats) > 10:
                print(f"   ... y {len(self._fallback_stats) - 10} más")
        
        if self._available_sheets:
            print(f"\n📄 Hojas disponibles en {self.fx_path.name}: {self._available_sheets}")
        
        print("=" * 50)
    
    def get_fallback_stats(self) -> Dict[str, str]:
        """Retorna estadísticas de fallback utilizadas."""
        return self._fallback_stats.copy()