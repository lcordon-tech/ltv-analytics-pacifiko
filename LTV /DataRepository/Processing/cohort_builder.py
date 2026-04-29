import pandas as pd
import numpy as np


class CohortBuilder:
    """
    Responsabilidad: Asignar a cada orden una cohorte según granularidad seleccionada.
    
    AHORA: Cohortes RELATIVAS al inicio del negocio por país.
    - cohort_start_year proviene de CountryContext
    - Cohortes < 1 se asignan a Q1 (no negativas)
    """
    
    def __init__(self, granularidad: str = 'quarterly', country_context=None):
        """
        Args:
            granularidad: Tipo de cohorte ('quarterly', 'monthly', 'weekly', 'semiannual', 'yearly')
            country_context: Contexto del país (contiene cohort_start_year)
        """
        self.granularidad = granularidad
        self.country_context = country_context
        self.start_year = country_context.cohort_start_year if country_context else 2020
    
    def build_cohort(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcula la cohorte basada en la granularidad seleccionada."""
        print("\n" + "="*60)
        print(f" INICIANDO CONSTRUCCIÓN DE COHORTES ({self.granularidad.upper()}) ".center(60))
        print(f" AÑO BASE: {self.start_year} ".center(60))
        print("="*60)

        if df.empty:
            print("⚠️ El DataFrame está vacío. No se pueden generar cohortes.")
            return df

        if not pd.api.types.is_datetime64_any_dtype(df['order_date']):
            df['order_date'] = pd.to_datetime(df['order_date'])

        if self.granularidad == 'quarterly':
            df = self._build_quarterly_cohorts(df)
        elif self.granularidad == 'monthly':
            df = self._build_monthly_cohorts(df)
        elif self.granularidad == 'weekly':
            df = self._build_weekly_cohorts(df)
        elif self.granularidad == 'semiannual':
            df = self._build_semiannual_cohorts(df)
        elif self.granularidad == 'yearly':
            df = self._build_yearly_cohorts(df)
        else:
            print(f"⚠️ Granularidad '{self.granularidad}' no soportada. Usando quarterly.")
            df = self._build_quarterly_cohorts(df)

        if df['cohort'].isnull().any():
            raise ValueError("Error Crítico: Se detectaron valores nulos en la generación de cohortes.")

        min_date = df['order_date'].min().date()
        max_date = df['order_date'].max().date()
        unique_cohorts = sorted(df['cohort'].unique())
        
        print(f"📅 Rango de fechas: {min_date} al {max_date}")
        print(f"✅ Se han generado {len(unique_cohorts)} cohortes.")
        print(f"📊 Listado de cohortes: {unique_cohorts[:10]}{'...' if len(unique_cohorts) > 10 else ''}")
        print("-" * 60)

        return df
    
    def _build_quarterly_cohorts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera cohortes trimestrales RELATIVAS al año base del país.
        Q1 = primera cohorte del país (ej: CR: 2022-Q1 → Q1)
        """
        years = df['order_date'].dt.year
        quarters = df['order_date'].dt.quarter
        
        # Fórmula relativa: (año - start_year) * 4 + trimestre
        df['cohort_index'] = ((years - self.start_year) * 4) + quarters
        
        # Cohortes < 1 se asignan a Q1 (no negativas, no Q0)
        df['cohort_index'] = df['cohort_index'].clip(lower=1)
        
        df['cohort'] = df['cohort_index'].apply(lambda x: f"Q{int(x)}")
        df = df.drop(columns=['cohort_index'])
        
        # Validación: no debe haber cohortes negativas ni Q0
        negative_cohorts = df[df['cohort'].str.replace('Q', '').astype(float) < 1]
        if not negative_cohorts.empty:
            print(f"⚠️ Se encontraron {len(negative_cohorts)} cohortes < Q1. Forzando a Q1.")
            df.loc[df['cohort'].str.replace('Q', '').astype(float) < 1, 'cohort'] = 'Q1'
        
        return df
    
    def _build_monthly_cohorts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera cohortes mensuales formato YYYY-MM (absoluto)"""
        df['cohort'] = df['order_date'].dt.strftime('%Y-%m')
        return df
    
    def _build_weekly_cohorts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera cohortes semanales formato YYYY-Wxx (absoluto)"""
        df['cohort'] = df['order_date'].dt.strftime('%Y-W%W')
        return df
    
    def _build_semiannual_cohorts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera cohortes semestrales formato YYYY-H1, YYYY-H2 (absoluto)"""
        year = df['order_date'].dt.year
        half = df['order_date'].dt.month.apply(lambda m: 1 if m <= 6 else 2)
        df['cohort'] = year.astype(str) + '-H' + half.astype(str)
        return df
    
    def _build_yearly_cohorts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera cohortes anuales formato YYYY (absoluto)"""
        df['cohort'] = df['order_date'].dt.year.astype(str)
        return df