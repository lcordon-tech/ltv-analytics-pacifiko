"""
Adaptador de granularidades temporales para cohortes.

Convierte supuestos trimestrales (base) a otras granularidades:
- monthly (mensual)
- weekly (semanal)
- semiannual (semestral)
- yearly (anual)

Mantiene compatibilidad con el modelo quarterly existente.

AHORA: Soporta año base DINÁMICO según país (cohort_start_year).
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta


class TimeGranularityAdapter:
    """
    Adapta supuestos de cohortes trimestrales a otras granularidades.
    
    Granularidades soportadas:
    - 'quarterly' (default) - sin transformación
    - 'monthly' - divide por 3
    - 'weekly' - divide por 12 (aprox 4.33 semanas por mes)
    - 'semiannual' - suma de 2 quarters
    - 'yearly' - suma de 4 quarters
    
    AHORA: Año base dinámico según país.
    - GT: base_year = 2020
    - CR: base_year = 2022
    """
    
    GRANULARITIES = ['quarterly', 'monthly', 'weekly', 'semiannual', 'yearly']
    
    # Mapeo de granularidad a función de transformación
    TRANSFORM_FUNCTIONS = {
        'quarterly': '_transform_quarterly',
        'monthly': '_transform_monthly',
        'weekly': '_transform_weekly',
        'semiannual': '_transform_semiannual',
        'yearly': '_transform_yearly'
    }
    
    def __init__(self, granularity: str = 'quarterly', country_context=None):
        """
        Inicializa el adaptador.
        
        Args:
            granularity: Tipo de granularidad ('quarterly', 'monthly', etc.)
            country_context: Contexto del país (para año base dinámico)
        """
        if granularity not in self.GRANULARITIES:
            print(f"⚠️ Granularidad '{granularity}' no soportada. Usando 'quarterly'")
            self.granularity = 'quarterly'
        else:
            self.granularity = granularity
        
        self.country_context = country_context
        # 🔧 Año base DINÁMICO según país
        if country_context:
            self.base_year = country_context.cohort_start_year
            print(f"📅 TimeGranularityAdapter: año base = {self.base_year} (desde CountryContext)")
        else:
            self.base_year = 2020
            print(f"📅 TimeGranularityAdapter: año base = {self.base_year} (default)")
        
        self._transformed_retention = {}
        self._transformed_cogs = {}
    
    def _parse_quarter(self, cohort: str) -> Tuple[int, int]:
        """
        Parsea una cohorte trimestral (Q1, Q2, Q3, Q4, Q5...).
        
        AHORA: Retorna (year, quarter) con año REAL según base_year del país.
        
        Ejemplo base_year=2020:
        - Q1 → (2020, 1)
        - Q5 → (2021, 1)  porque Q1=2020, Q2=2020, Q3=2020, Q4=2020, Q5=2021
        
        Ejemplo base_year=2022:
        - Q1 → (2022, 1)
        - Q5 → (2023, 1)
        
        Returns:
            (year, quarter_number)
        """
        cohort_clean = str(cohort).upper().replace('Q', '')
        
        # Manejar cohortes negativas (Q-3, Q-2, Q-1, Q0)
        if cohort_clean.startswith('-'):
            num = int(cohort_clean)
            # Para cohortes negativas: Q-1 es el trimestre anterior al base_year
            # Ej: base_year=2022, Q-1 = 2021-Q4
            year_offset = (num - 1) // 4
            quarter_num = ((num - 1) % 4) + 1
            if quarter_num < 1:
                quarter_num = 4
                year_offset -= 1
            year = self.base_year + year_offset
            return (year, max(1, min(4, quarter_num)))
        
        num = int(cohort_clean)
        # Q1 = base_year-Q1 (año 0, trimestre 1)
        # Q5 = base_year+1-Q1 (año 1, trimestre 1)
        year_offset = (num - 1) // 4
        quarter_num = ((num - 1) % 4) + 1
        year = self.base_year + year_offset
        return (year, quarter_num)
    
    def _quarter_to_month_cohorts(self, cohort: str, retention_value: float, cogs_value: float) -> Dict[str, Dict]:
        """
        Convierte una cohorte trimestral en 3 cohortes mensuales.
        """
        year, quarter = self._parse_quarter(cohort)
        
        # Meses del trimestre
        quarter_months = {
            1: [1, 2, 3],
            2: [4, 5, 6],
            3: [7, 8, 9],
            4: [10, 11, 12]
        }
        
        months = quarter_months.get(quarter, [1, 2, 3])
        retention_per_month = retention_value / 3.0
        cogs_per_month = cogs_value  # COGS se mantiene igual por mes
        
        result = {}
        for month in months:
            month_cohort = f"{year}-{month:02d}"
            result[month_cohort] = {
                'retention': retention_per_month,
                'cogs': cogs_per_month
            }
        return result
    
    def _quarter_to_week_cohorts(self, cohort: str, retention_value: float, cogs_value: float) -> Dict[str, Dict]:
        """
        Convierte una cohorte trimestral en cohortes semanales (aprox 12-13 semanas).
        """
        year, quarter = self._parse_quarter(cohort)
        
        # Semanas por trimestre (aprox)
        weeks_per_quarter = 13
        retention_per_week = retention_value / weeks_per_quarter
        cogs_per_week = cogs_value
        
        result = {}
        
        # Calcular semana inicial del trimestre
        quarter_start_weeks = {
            1: 1,   # Semana 1
            2: 14,  # Semana 14
            3: 27,  # Semana 27
            4: 40   # Semana 40
        }
        start_week = quarter_start_weeks.get(quarter, 1)
        
        for i in range(weeks_per_quarter):
            week_num = start_week + i
            week_cohort = f"{year}-W{week_num:02d}"
            result[week_cohort] = {
                'retention': retention_per_week,
                'cogs': cogs_per_week
            }
        return result
    
    def _quarter_to_semiannual_cohorts(self, cohort: str, retention_value: float, cogs_value: float, 
                                        next_cohort_retention: Optional[float] = None) -> Dict[str, Dict]:
        """
        Convierte cohortes trimestrales a semestrales.
        Necesita dos trimestres para formar un semestre.
        """
        year, quarter = self._parse_quarter(cohort)
        
        # Semestre: Q1+Q2 = H1, Q3+Q4 = H2
        if quarter in [1, 2]:
            half_cohort = f"{year}-H1"
            # Sumar retention de Q1 y Q2
            if next_cohort_retention is not None:
                total_retention = retention_value + next_cohort_retention
            else:
                total_retention = retention_value * 2  # Estimación
        else:
            half_cohort = f"{year}-H2"
            total_retention = retention_value * 2
        
        return {half_cohort: {'retention': total_retention, 'cogs': cogs_value}}
    
    def _quarter_to_yearly_cohorts(self, cohorts_retention: Dict[str, float], 
                                    cohorts_cogs: Dict[str, float]) -> Dict[str, Dict]:
        """
        Convierte 4 cohortes trimestrales a 1 cohorte anual.
        """
        yearly_map = {}
        
        # Agrupar por año REAL
        for cohort, retention in cohorts_retention.items():
            year, _ = self._parse_quarter(cohort)
            year_cohort = str(year)
            
            if year_cohort not in yearly_map:
                yearly_map[year_cohort] = {'retention': 0.0, 'cogs': 0.0}
            
            yearly_map[year_cohort]['retention'] += retention
            yearly_map[year_cohort]['cogs'] = cohorts_cogs.get(cohort, 0.0)
        
        return yearly_map
    
    def _transform_quarterly(self, retention_map: Dict[str, float], 
                              cogs_map: Dict[str, float]) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Sin transformación (mantiene quarterly)."""
        return retention_map, cogs_map
    
    def _transform_monthly(self, retention_map: Dict[str, float], 
                            cogs_map: Dict[str, float]) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Transforma quarterly a monthly."""
        monthly_retention = {}
        monthly_cogs = {}
        
        print(f"   📆 Transformando {len(retention_map)} cohortes trimestrales a mensuales...")
        
        for cohort, retention in retention_map.items():
            cogs = cogs_map.get(cohort, 0.0)
            monthly_data = self._quarter_to_month_cohorts(cohort, retention, cogs)
            
            for month_cohort, values in monthly_data.items():
                monthly_retention[month_cohort] = values['retention']
                monthly_cogs[month_cohort] = values['cogs']
        
        print(f"   ✅ Generadas {len(monthly_retention)} cohortes mensuales")
        return monthly_retention, monthly_cogs
    
    def _transform_weekly(self, retention_map: Dict[str, float], 
                           cogs_map: Dict[str, float]) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Transforma quarterly a weekly."""
        weekly_retention = {}
        weekly_cogs = {}
        
        print(f"   📆 Transformando {len(retention_map)} cohortes trimestrales a semanales...")
        
        for cohort, retention in retention_map.items():
            cogs = cogs_map.get(cohort, 0.0)
            weekly_data = self._quarter_to_week_cohorts(cohort, retention, cogs)
            
            for week_cohort, values in weekly_data.items():
                weekly_retention[week_cohort] = values['retention']
                weekly_cogs[week_cohort] = values['cogs']
        
        print(f"   ✅ Generadas {len(weekly_retention)} cohortes semanales")
        return weekly_retention, weekly_cogs
    
    def _transform_semiannual(self, retention_map: Dict[str, float], 
                               cogs_map: Dict[str, float]) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Transforma quarterly a semiannual."""
        semiannual_retention = {}
        semiannual_cogs = {}
        
        print(f"   📆 Transformando {len(retention_map)} cohortes trimestrales a semestrales...")
        
        # Ordenar cohortes
        def extract_num(c):
            try:
                return int(c.replace('Q', '').replace('Q', ''))
            except:
                return 0
        
        sorted_cohorts = sorted([c for c in retention_map.keys() if c.startswith('Q') and len(c) > 1],
                                key=extract_num)
        
        for i, cohort in enumerate(sorted_cohorts):
            if i % 2 == 0 and i + 1 < len(sorted_cohorts):
                next_cohort = sorted_cohorts[i + 1]
                retention_sum = retention_map[cohort] + retention_map[next_cohort]
                cogs_val = cogs_map.get(cohort, 0.0)
                
                # Determinar semestre usando año REAL
                year, quarter = self._parse_quarter(cohort)
                
                if quarter in [1, 2]:
                    half_cohort = f"{year}-H1"
                else:
                    half_cohort = f"{year}-H2"
                
                semiannual_retention[half_cohort] = retention_sum
                semiannual_cogs[half_cohort] = cogs_val
        
        print(f"   ✅ Generadas {len(semiannual_retention)} cohortes semestrales")
        return semiannual_retention, semiannual_cogs
    
    def _transform_yearly(self, retention_map: Dict[str, float], 
                           cogs_map: Dict[str, float]) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Transforma quarterly a yearly."""
        yearly_data = self._quarter_to_yearly_cohorts(retention_map, cogs_map)
        
        yearly_retention = {k: v['retention'] for k, v in yearly_data.items()}
        yearly_cogs = {k: v['cogs'] for k, v in yearly_data.items()}
        
        print(f"   ✅ Generadas {len(yearly_retention)} cohortes anuales")
        return yearly_retention, yearly_cogs
    
    def transform(self, retention_map: Dict[str, float], 
                  cogs_map: Dict[str, float]) -> Tuple[Dict[str, float], Dict[str, float]]:
        """
        Transforma los mapas de retención y COGS según la granularidad configurada.
        
        Args:
            retention_map: Dict {cohort: retention_value}
            cogs_map: Dict {cohort: cogs_value}
        
        Returns:
            Tuple (transformed_retention, transformed_cogs)
        """
        print(f"\n🔄 [TimeGranularityAdapter] Transformando a {self.granularity}")
        print(f"   País base_year: {self.base_year}")
        print(f"   Retention map size: {len(retention_map)}")
        print(f"   COGS map size: {len(cogs_map)}")
        
        if self.granularity == 'quarterly':
            print("   (sin transformación - manteniendo quarterly)")
            return retention_map.copy(), cogs_map.copy()
        
        transform_func_name = self.TRANSFORM_FUNCTIONS.get(self.granularity)
        if not transform_func_name:
            print(f"⚠️ Transformación no encontrada para '{self.granularity}'. Usando quarterly.")
            return retention_map.copy(), cogs_map.copy()
        
        transform_func = getattr(self, transform_func_name)
        transformed_ret, transformed_cogs = transform_func(retention_map, cogs_map)
        
        print(f"   Cohortes originales: {len(retention_map)} → Transformadas: {len(transformed_ret)}")
        
        self._transformed_retention = transformed_ret
        self._transformed_cogs = transformed_cogs
        
        return transformed_ret, transformed_cogs
    
    def get_cohort_format_hint(self) -> str:
        """Retorna un hint sobre el formato esperado de cohortes para la granularidad actual."""
        hints = {
            'quarterly': 'Q1, Q2, Q3...',
            'monthly': 'YYYY-MM (ej: 2024-01)',
            'weekly': 'YYYY-Wxx (ej: 2024-W01)',
            'semiannual': 'YYYY-H1, YYYY-H2',
            'yearly': 'YYYY'
        }
        return hints.get(self.granularity, 'Q1, Q2...')
    
    def get_base_year(self) -> int:
        """Retorna el año base actual."""
        return self.base_year


# Función auxiliar para formatear cohortes según granularidad
def format_cohort_for_granularity(date: datetime, granularity: str) -> str:
    """
    Formatea una fecha a string de cohorte según la granularidad.
    
    Args:
        date: Fecha datetime
        granularity: 'quarterly', 'monthly', 'weekly', 'semiannual', 'yearly'
    
    Returns:
        String formateado de cohorte
    """
    if granularity == 'quarterly':
        quarter = (date.month - 1) // 3 + 1
        return f"Q{quarter}"
    
    elif granularity == 'monthly':
        return date.strftime("%Y-%m")
    
    elif granularity == 'weekly':
        return date.strftime("%Y-W%W")
    
    elif granularity == 'semiannual':
        half = 1 if date.month <= 6 else 2
        return f"{date.year}-H{half}"
    
    elif granularity == 'yearly':
        return str(date.year)
    
    else:
        return f"Q{(date.month - 1) // 3 + 1}"