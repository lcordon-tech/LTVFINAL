import pandas as pd
import numpy as np
from Run.Services.time_granularity_adapter import TimeGranularityAdapter


class RetentionApplier:
    """
    Responsabilidad: Identificar recurrencia y aplicar retención.
    
    AHORA: Soporta múltiples granularidades mediante TimeGranularityAdapter.
    """
    
    def __init__(self, granularidad: str = 'quarterly'):
        """
        Args:
            granularidad: Tipo de cohorte ('quarterly', 'monthly', etc.)
        """
        self.granularidad = granularidad
        self.adapter = TimeGranularityAdapter(granularidad)
        
        # Fallback values (solo quarterly, se transforman según necesidad)
        self.FALLBACK_RETENTION_2020 = {
            "Q-3": 0.0, "Q-2": 4.64, "Q-1": 3.84, "Q0": 9.29
        }
        
        self.FALLBACK_RETENTION_LIST = [
            4.35, 5.05, 2.59, 6.17,   # Q1-Q4  (2021)
            2.99, 2.00, 1.01, 2.30,   # Q5-Q8  (2022)
            2.47, 3.24, 2.03, 1.71,   # Q9-Q12 (2023)
            1.42, 1.51, 1.50, 1.81,   # Q13-Q16 (2024)
            2.75, 2.37, 3.53, 2.88,   # Q17-Q20 (2025)
            2.72                       # Q21 (2026-Q1)
        ]
    
    def _load_retention_from_assumptions(self, assumptions_dict: dict) -> dict:
        """Carga retention desde CUALQUIER pestaña que tenga la columna."""
        retention_data = {}
        
        if not assumptions_dict:
            return retention_data
        
        for sheet_name, df_sheet in assumptions_dict.items():
            if 'cohort' in df_sheet.columns and 'retention' in df_sheet.columns:
                df_sheet['cohort'] = df_sheet['cohort'].astype(str).str.strip().str.upper()
                
                for _, row in df_sheet.iterrows():
                    cohort = row['cohort']
                    try:
                        value = float(row['retention'])
                        if cohort not in retention_data:
                            retention_data[cohort] = value
                    except (ValueError, TypeError):
                        pass
        
        return retention_data
    
    def _build_fallback_map(self) -> dict:
        """Construye el mapa de fallback quarterly (base)."""
        fallback_map = {}
        fallback_map.update(self.FALLBACK_RETENTION_2020)
        
        for i, val in enumerate(self.FALLBACK_RETENTION_LIST):
            fallback_map[f"Q{i+1}"] = val
        
        return fallback_map
    
    def apply(self, df: pd.DataFrame, assumptions_dict: dict = None) -> pd.DataFrame:
        """
        Aplica retención usando supuestos transformados según granularidad.
        """
        print("\n" + "="*60)
        print(f" APLICANDO RETENCIÓN ({self.granularidad.upper()}) ".center(60))
        print("="*60)

        if df.empty:
            return df
        
        # 1. Cargar retention base (formato quarterly desde Excel)
        retention_from_excel = {}
        if assumptions_dict:
            retention_from_excel = self._load_retention_from_assumptions(assumptions_dict)
        
        # 2. Mapa base quarterly (Excel + fallback)
        fallback_map = self._build_fallback_map()
        base_retention_map = {**fallback_map, **retention_from_excel}
        
        # 3. Cargar COGS base (para transformación, si es necesario)
        base_cogs_map = self._load_cogs_from_assumptions(assumptions_dict) if hasattr(self, '_load_cogs_from_assumptions') else {}
        
        # 4. Transformar según granularidad
        transformed_retention, _ = self.adapter.transform(base_retention_map, base_cogs_map)
        
        # 5. Detectar cohortes sin supuestos
        df_cohorts = df['cohort'].unique()
        missing_cohorts = [c for c in df_cohorts if c not in transformed_retention]
        
        if missing_cohorts:
            print(f"\n⚠️ ADVERTENCIA: {len(missing_cohorts)} cohortes sin retention definida:")
            for c in missing_cohorts[:10]:
                print(f"   • {c}")
            if len(missing_cohorts) > 10:
                print(f"   ... y {len(missing_cohorts) - 10} más")
            print(f"   → Usando valores por defecto (0)\n")

        # --- LÓGICA ORIGINAL DE RETENCIÓN ---
        # Necesitamos orden numérico para comparar cohortes
        df = self._add_cohort_numeric_order(df)
        
        df['birth_cohort_order'] = df.groupby('customer_id')['cohort_order'].transform('min')
        df['is_recurrence'] = df['cohort_order'] > df['birth_cohort_order']

        df['retention_budget'] = df['cohort'].map(transformed_retention).fillna(0.0)
        df.loc[~df['is_recurrence'], 'retention_budget'] = 0.0

        orders_in_cohort = df[df['is_recurrence']].groupby(['customer_id', 'cohort'])['order_id'].transform('nunique')
        items_in_order = df.groupby('order_id')['order_id'].transform('count')

        df['retention_cost_$'] = 0.0
        mask = df['is_recurrence']
        
        df.loc[mask, 'retention_cost_$'] = (
            df.loc[mask, 'retention_budget'] / orders_in_cohort
        ) / items_in_order

        if df['retention_cost_$'].isna().any():
            df['retention_cost_$'] = df['retention_cost_$'].fillna(0.0)

        total_spent = df['retention_cost_$'].sum()
        print(f"\n✅ Análisis de retención finalizado.")
        print(f"📈 Total invertido: ${total_spent:,.2f}")
        
        cols_to_drop = ['cohort_order', 'birth_cohort_order', 'is_recurrence', 'retention_budget']
        return df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    def _add_cohort_numeric_order(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega un orden numérico a las cohortes para comparación.
        """
        # Obtener cohortes únicas y ordenarlas
        unique_cohorts = sorted(df['cohort'].unique())
        cohort_to_order = {cohort: i for i, cohort in enumerate(unique_cohorts)}
        df['cohort_order'] = df['cohort'].map(cohort_to_order)
        return df
    
    def _load_cogs_from_assumptions(self, assumptions_dict: dict) -> dict:
        """Carga COGS base para transformación (delegado)."""
        cogs_data = {}
        if not assumptions_dict:
            return cogs_data
        
        for sheet_name, df_sheet in assumptions_dict.items():
            if 'cohort' in df_sheet.columns and 'cogs' in df_sheet.columns:
                df_sheet['cohort'] = df_sheet['cohort'].astype(str).str.strip().str.upper()
                for _, row in df_sheet.iterrows():
                    cohort = row['cohort']
                    try:
                        value = float(row['cogs'])
                        if cohort not in cogs_data:
                            cogs_data[cohort] = value
                    except (ValueError, TypeError):
                        pass
        return cogs_data