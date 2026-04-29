# Model/Data/cac_adapter.py
"""
Adaptador para transformar CAC trimestral a otras granularidades.
"""

from typing import Dict
from Category.Cohort.cohort_config import TimeGranularity


class CACAdapter:
    """
    Adapta valores de CAC desde granularidad trimestral (base) a otras granularidades.
    
    Reglas de transformación:
    - quarterly → misma granularidad (sin cambio)
    - monthly → CADA mes recibe el CAC COMPLETO del trimestre
    - weekly → CADA semana recibe el CAC COMPLETO del trimestre
    - semiannual → promedio de los 2 trimestres
    - yearly → suma de los 4 trimestres
    - daily → CADA día recibe el CAC COMPLETO del trimestre
    """
    
    @staticmethod
    def transform(cac_map: Dict[str, float], target_granularity: TimeGranularity) -> Dict[str, float]:
        """
        Transforma un mapa de CAC desde trimestral a otra granularidad.
        
        Args:
            cac_map: Diccionario {cohort_id: cac_value} (formato trimestral: YYYY-QX)
            target_granularity: Granularidad destino
        
        Returns:
            Diccionario transformado para la granularidad destino
        """
        if not cac_map:
            return {}
        
        if target_granularity == TimeGranularity.QUARTERLY:
            return cac_map.copy()
        
        transformed = {}
        
        # Agrupar trimestres por año para semianual y anual
        quarterly_by_year = {}
        for cohort_id, cac in cac_map.items():
            if '-Q' not in cohort_id:
                # Si no es formato trimestral, mantener como está
                transformed[cohort_id] = cac
                continue
            
            parts = cohort_id.split('-Q')
            if len(parts) != 2:
                transformed[cohort_id] = cac
                continue
                
            try:
                year = int(parts[0])
                quarter = int(parts[1])
            except ValueError:
                transformed[cohort_id] = cac
                continue
            
            if year not in quarterly_by_year:
                quarterly_by_year[year] = {}
            quarterly_by_year[year][quarter] = cac
        
        if target_granularity == TimeGranularity.SEMIANNUAL:
            # H1 = promedio de Q1 + Q2, H2 = promedio de Q3 + Q4
            for year, quarters in quarterly_by_year.items():
                q1 = quarters.get(1, 0)
                q2 = quarters.get(2, 0)
                q3 = quarters.get(3, 0)
                q4 = quarters.get(4, 0)
                
                # Semestre 1 (Q1+Q2)
                if q1 > 0 or q2 > 0:
                    cac_h1 = round((q1 + q2) / 2, 2) if (q1 > 0 or q2 > 0) else 0
                    transformed[f"{year}-H1"] = cac_h1
                # Semestre 2 (Q3+Q4)
                if q3 > 0 or q4 > 0:
                    cac_h2 = round((q3 + q4) / 2, 2) if (q3 > 0 or q4 > 0) else 0
                    transformed[f"{year}-H2"] = cac_h2
        
        elif target_granularity == TimeGranularity.YEARLY:
            # Suma de 4 quarters
            for year, quarters in quarterly_by_year.items():
                total = sum(quarters.values())
                if total > 0:
                    transformed[str(year)] = round(total, 2)
        
        elif target_granularity == TimeGranularity.MONTHLY:
            # CADA mes recibe el CAC COMPLETO del trimestre (no dividido)
            month_map = {
                1: [1, 2, 3],
                2: [4, 5, 6],
                3: [7, 8, 9],
                4: [10, 11, 12]
            }
            
            for cohort_id, cac in cac_map.items():
                if '-Q' not in cohort_id:
                    transformed[cohort_id] = cac
                    continue
                
                parts = cohort_id.split('-Q')
                if len(parts) != 2:
                    transformed[cohort_id] = cac
                    continue
                
                try:
                    year = int(parts[0])
                    quarter = int(parts[1])
                except ValueError:
                    transformed[cohort_id] = cac
                    continue
                
                months = month_map.get(quarter, [1, 2, 3])
                
                # CADA mes recibe el CAC COMPLETO
                for month in months:
                    transformed[f"{year}-{month:02d}"] = cac
        
        elif target_granularity == TimeGranularity.WEEKLY:
            # CADA semana recibe el CAC COMPLETO del trimestre (no dividido)
            # Aprox 13 semanas por trimestre
            week_start_map = {
                1: 1,    # Q1 empieza en semana 1
                2: 14,   # Q2 empieza en semana 14
                3: 27,   # Q3 empieza en semana 27
                4: 40    # Q4 empieza en semana 40
            }
            
            for cohort_id, cac in cac_map.items():
                if '-Q' not in cohort_id:
                    transformed[cohort_id] = cac
                    continue
                
                parts = cohort_id.split('-Q')
                if len(parts) != 2:
                    transformed[cohort_id] = cac
                    continue
                
                try:
                    year = int(parts[0])
                    quarter = int(parts[1])
                except ValueError:
                    transformed[cohort_id] = cac
                    continue
                
                start_week = week_start_map.get(quarter, 1)
                
                # CADA semana recibe el CAC COMPLETO
                for w in range(13):
                    week_num = start_week + w
                    week_str = f"{week_num:02d}"
                    transformed[f"{year}-W{week_str}"] = cac
        
        elif target_granularity == TimeGranularity.DAILY:
            # CADA día recibe el CAC COMPLETO del trimestre (no dividido)
            # Días por trimestre (aprox)
            days_in_quarter = {
                1: 90,   # Q1: 90 días (Ene-Mar)
                2: 91,   # Q2: 91 días (Abr-Jun)
                3: 92,   # Q3: 92 días (Jul-Sep)
                4: 92    # Q4: 92 días (Oct-Dic)
            }
            
            # Fecha inicio por trimestre
            month_start = {1: 1, 2: 4, 3: 7, 4: 10}
            
            for cohort_id, cac in cac_map.items():
                if '-Q' not in cohort_id:
                    transformed[cohort_id] = cac
                    continue
                
                parts = cohort_id.split('-Q')
                if len(parts) != 2:
                    transformed[cohort_id] = cac
                    continue
                
                try:
                    year = int(parts[0])
                    quarter = int(parts[1])
                except ValueError:
                    transformed[cohort_id] = cac
                    continue
                
                days = days_in_quarter.get(quarter, 90)
                start_month = month_start.get(quarter, 1)
                
                from datetime import datetime, timedelta
                start_date = datetime(year, start_month, 1)
                
                # CADA día recibe el CAC COMPLETO
                for d in range(days):
                    current_date = start_date + timedelta(days=d)
                    if current_date.year == year:  # Solo dentro del mismo año
                        transformed[current_date.strftime("%Y-%m-%d")] = cac
        
        return transformed
    
    @staticmethod
    def get_quarterly_from_custom(cac_map: Dict[str, float], granularity: TimeGranularity) -> Dict[str, float]:
        """
        Convierte desde una granularidad custom a trimestral (inverso).
        Útil si el usuario define CAC en monthly/weekly y queremos trimestral.
        
        Args:
            cac_map: Diccionario de CAC en granularidad custom
            granularity: Granularidad de origen
        
        Returns:
            Diccionario en formato trimestral YYYY-QX
        """
        if granularity == TimeGranularity.QUARTERLY:
            return cac_map.copy()
        
        quarterly = {}
        
        if granularity == TimeGranularity.MONTHLY:
            # Agrupar meses por trimestre (usar el primer mes del trimestre como referencia)
            from collections import defaultdict
            monthly_by_quarter = defaultdict(list)
            
            for cohort_id, cac in cac_map.items():
                if '-' in cohort_id and len(cohort_id) == 7:  # YYYY-MM
                    year_str, month_str = cohort_id.split('-')
                    year = int(year_str)
                    month = int(month_str)
                    quarter = (month - 1) // 3 + 1
                    key = f"{year}-Q{quarter}"
                    monthly_by_quarter[key].append(cac)
            
            for key, values in monthly_by_quarter.items():
                # Usar el primer valor del trimestre (o el promedio)
                quarterly[key] = values[0] if values else 0
        
        elif granularity == TimeGranularity.WEEKLY:
            # Agrupar semanas por trimestre
            from collections import defaultdict
            weekly_by_quarter = defaultdict(list)
            
            for cohort_id, cac in cac_map.items():
                if '-W' in cohort_id:
                    year_str, week_str = cohort_id.split('-W')
                    year = int(year_str)
                    week = int(week_str)
                    
                    # Determinar trimestre por semana
                    if 1 <= week <= 13:
                        quarter = 1
                    elif 14 <= week <= 26:
                        quarter = 2
                    elif 27 <= week <= 39:
                        quarter = 3
                    else:
                        quarter = 4
                    
                    key = f"{year}-Q{quarter}"
                    weekly_by_quarter[key].append(cac)
            
            for key, values in weekly_by_quarter.items():
                quarterly[key] = values[0] if values else 0
        
        elif granularity == TimeGranularity.SEMIANNUAL:
            # Dividir semestre en dos trimestres con el mismo valor
            for cohort_id, cac in cac_map.items():
                if '-H' in cohort_id:
                    year_str, half_str = cohort_id.split('-H')
                    year = int(year_str)
                    half = int(half_str)
                    
                    if half == 1:
                        quarterly[f"{year}-Q1"] = cac
                        quarterly[f"{year}-Q2"] = cac
                    else:
                        quarterly[f"{year}-Q3"] = cac
                        quarterly[f"{year}-Q4"] = cac
        
        elif granularity == TimeGranularity.YEARLY:
            # Distribuir año en 4 trimestres iguales
            for cohort_id, cac in cac_map.items():
                if cohort_id.isdigit():
                    year = int(cohort_id)
                    quarterly_cac = round(cac / 4, 2)
                    for q in range(1, 5):
                        quarterly[f"{year}-Q{q}"] = quarterly_cac
        
        return quarterly