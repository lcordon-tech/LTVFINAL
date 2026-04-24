import pandas as pd
import numpy as np
from datetime import datetime
from Run.Services.time_granularity_adapter import format_cohort_for_granularity


class CohortBuilder:
    """
    Responsabilidad: Asignar a cada orden una cohorte según granularidad seleccionada.
    
    Granularidades soportadas:
    - quarterly: Q1, Q2, Q3... (default, comportamiento original)
    - monthly: YYYY-MM
    - weekly: YYYY-Wxx
    - semiannual: YYYY-H1, YYYY-H2
    - yearly: YYYY
    """
    
    def __init__(self, granularidad: str = 'quarterly'):
        """
        Args:
            granularidad: Tipo de cohorte ('quarterly', 'monthly', 'weekly', 'semiannual', 'yearly')
        """
        self.granularidad = granularidad
        self.START_YEAR = 2021  # Año base para quarterly (mantiene compatibilidad)
    
    def build_cohort(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula la cohorte basada en la granularidad seleccionada.
        """
        print("\n" + "="*60)
        print(f" INICIANDO CONSTRUCCIÓN DE COHORTES ({self.granularidad.upper()}) ".center(60))
        print("="*60)

        if df.empty:
            print("⚠️ El DataFrame está vacío. No se pueden generar cohortes.")
            return df

        # Preparar fechas
        if not pd.api.types.is_datetime64_any_dtype(df['order_date']):
            df['order_date'] = pd.to_datetime(df['order_date'])

        # Generar cohortes según granularidad
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

        # Validación
        if df['cohort'].isnull().any():
            raise ValueError("Error Crítico: Se detectaron valores nulos en la generación de cohortes.")

        min_date = df['order_date'].min().date()
        max_date = df['order_date'].max().date()
        unique_cohorts = sorted(df['cohort'].unique())
        
        print(f"📅 Rango de fechas: {min_date} al {max_date}")
        print(f"✅ Se han generado {len(unique_cohorts)} cohortes.")
        print(f"📊 Listado de cohortes: {unique_cohorts[:10]}{'...' if len(unique_cohorts) > 10 else ''}")
        print("-" * 60)
        print(f"✅ FASE DE CONSTRUCCIÓN DE COHORTES COMPLETADA")
        print("-" * 60)

        return df
    
    def _build_quarterly_cohorts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Genera cohortes trimestrales (comportamiento original).
        Q1 = 2021-Q1, Q5 = 2022-Q1, etc.
        """
        years = df['order_date'].dt.year
        quarters = df['order_date'].dt.quarter
        
        # Fórmula original: (año - 2021) * 4 + trimestre
        df['cohort_index'] = ((years - self.START_YEAR) * 4) + quarters
        df['cohort'] = df['cohort_index'].apply(lambda x: f"Q{int(x)}")
        df = df.drop(columns=['cohort_index'])
        
        return df
    
    def _build_monthly_cohorts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera cohortes mensuales formato YYYY-MM"""
        df['cohort'] = df['order_date'].dt.strftime('%Y-%m')
        return df
    
    def _build_weekly_cohorts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera cohortes semanales formato YYYY-Wxx"""
        df['cohort'] = df['order_date'].dt.strftime('%Y-W%W')
        return df
    
    def _build_semiannual_cohorts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera cohortes semestrales formato YYYY-H1, YYYY-H2"""
        year = df['order_date'].dt.year
        half = df['order_date'].dt.month.apply(lambda m: 1 if m <= 6 else 2)
        df['cohort'] = year.astype(str) + '-H' + half.astype(str)
        return df
    
    def _build_yearly_cohorts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera cohortes anuales formato YYYY"""
        df['cohort'] = df['order_date'].dt.year.astype(str)
        return df