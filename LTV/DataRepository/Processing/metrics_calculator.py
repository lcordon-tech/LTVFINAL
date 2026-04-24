import pandas as pd
import numpy as np
import time
from typing import Dict, Optional

from Run.Services.time_granularity_adapter import TimeGranularityAdapter


class MetricsCalculator:
    """
    Responsabilidad: Calcular métricas de Unit Economics.
    
    AHORA: Soporta COGS transformados según granularidad Y TIPO DE CAMBIO DINÁMICO.
    """
    
    IVA_FACTOR = 1.12
    
    def __init__(self, granularidad: str = 'quarterly', 
                 country_context=None, 
                 fx_engine=None):
        """
        Args:
            granularidad: Tipo de cohorte para transformación de COGS
            country_context: Contexto del país (para tasa default)
            fx_engine: Motor de tipo de cambio (para tasa por cohorte)
        """
        self.granularidad = granularidad
        self.adapter = TimeGranularityAdapter(granularidad)
        self.country_context = country_context
        self.fx_engine = fx_engine
        
        # Tasa de cambio por defecto (fallback si no hay FXEngine)
        self.default_fx_rate = country_context.default_fx_rate if country_context else 7.66

    def _get_fx_rate(self, cohort: str) -> float:
        """
        Obtiene tipo de cambio para una cohorte específica.
        
        Prioridad:
        1. FXEngine.get_rate() si existe (tasa por cohorte)
        2. Tasa default del país
        3. 7.66 (fallback histórico)
        """
        if self.fx_engine is not None:
            try:
                rate = self.fx_engine.get_rate(cohort, self.granularidad)
                if rate > 0:
                    return rate
            except Exception as e:
                print(f"⚠️ Error obteniendo tasa para {cohort}: {e}")
        
        # Fallback a tasa default del país
        if self.country_context is not None:
            return self.country_context.default_fx_rate
        
        # Último fallback (Guatemala histórico)
        return 7.66

    def _load_cogs_from_assumptions(self, assumptions_dict: dict) -> dict:
        """Carga COGS desde CUALQUIER pestaña que tenga la columna."""
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
                        print(f"⚠️ COGS inválido para {cohort}: {row['cogs']}")
        
        return cogs_data

    def _build_fallback_cogs_map(self) -> dict:
        """Mapa de fallback para COGS quarterly."""
        return {
            "Q8": -0.8986, "Q9": -0.8658, "Q10": -0.8085, "Q11": -0.825,
            "Q12": -0.8308, "Q13": -0.8326, "Q14": -0.8301, "Q15": -0.8432,
            "Q16": -0.8575, "Q17": -0.8408, "Q18": -0.8704, "Q19": -0.8817,
            "Q20": -0.8895, "Q21": -0.8895,
        }

    def run(self, df: pd.DataFrame, assumptions_dict: dict = None) -> pd.DataFrame:
        print("\n" + "="*60)
        country_name = self.country_context.name if self.country_context else "Desconocido"
        print(f" INICIANDO CÁLCULO DE MÉTRICAS ({self.granularidad.upper()}) - {country_name} ".center(60))
        print("="*60)
        
        start_total = time.time()

        # --- SUB-FASE 1: BLINDAJE ---
        t0 = time.time()
        
        # Cargar COGS base desde Excel
        cogs_from_excel = self._load_cogs_from_assumptions(assumptions_dict) if assumptions_dict else {}
        fallback_cogs = self._build_fallback_cogs_map()
        base_cogs_map = {**fallback_cogs, **cogs_from_excel}
        
        # Transformar COGS según granularidad
        _, transformed_cogs = self.adapter.transform({}, base_cogs_map)
        
        if cogs_from_excel:
            print(f"✅ COGS transformados para {self.granularidad}: {len(transformed_cogs)} cohortes")
        else:
            print(f"⚠️ Usando COGS fallback transformados para {self.granularidad}")
        
        # Columnas de supuestos (YA VIENEN EN USD - NO CONVERTIR)
        assumptions_cols = [
            'shipping_cost', 'shipping_revenue', 'credit_card_payment', 
            'cash_on_delivery_comision', 'fraud', 'infrastructure',
            'fc_variable_headcount', 'cs_variable_headcount', 'commission_percent'
        ]

        for col in assumptions_cols:
            if col not in df.columns:
                df[col] = 0.0
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        # SOIS ya está en USD - solo dividir por IVA
        if 'sois' not in df.columns:
            df['sois'] = 0.0
        else:
            df['sois'] = (pd.to_numeric(df['sois'], errors='coerce').fillna(0.0)) / self.IVA_FACTOR

        if 'retention_cost_$' not in df.columns:
            df['retention_cost_$'] = 0.0
        else:
            df['retention_cost_$'] = pd.to_numeric(df['retention_cost_$'], errors='coerce').fillna(0.0)

        # --- APLICAR TIPO DE CAMBIO DINÁMICO (DB → USD) ---
        # Calcular tasa por cohorte
        df['_fx_rate'] = df['cohort'].apply(self._get_fx_rate)
        
        # Validar que no haya tasas 0 o nulas
        mask_zero_rate = (df['_fx_rate'] <= 0) | (df['_fx_rate'].isna())
        if mask_zero_rate.any():
            print(f"⚠️ {mask_zero_rate.sum()} filas con tasa inválida. Usando default.")
            df.loc[mask_zero_rate, '_fx_rate'] = self.default_fx_rate
        
        # Mostrar resumen de tasas (debug)
        print(f"💱 Tipos de cambio aplicados: min={df['_fx_rate'].min():.4f}, max={df['_fx_rate'].max():.4f}, unique={df['_fx_rate'].nunique()}")
        
        # Calcular revenue en USD
        df['revenue'] = ((df['quantity'] * df['price']) / self.IVA_FACTOR) / df['_fx_rate']
        
        print(f"⏱️  Sub-fase 1 (Blindaje + FX): {time.time() - t0:.2f}s")

        # Normalización de commission_percent
        mask_to_fix = (df['commission_percent'] > 1) & (df['commission_percent'] <= 100)
        if mask_to_fix.any():
            df.loc[mask_to_fix, 'commission_percent'] /= 100

        mask_error = (df['commission_percent'] > 1) | (df['commission_percent'] < 0)
        if mask_error.any():
            df.loc[mask_error, 'commission_percent'] = 0.9

        # Ajuste item_cost faltante (usando la tasa de cambio)
        mask_missing_cost = (
            df['b_unit'].isin(['DS', '1P', 'OTROS']) & 
            ((df['item_cost'].isna()) | (df['item_cost'] <= 0))
        )
        if mask_missing_cost.any():
            df.loc[mask_missing_cost, 'item_cost'] = (df.loc[mask_missing_cost, 'price'] * 0.9) / self.IVA_FACTOR / df.loc[mask_missing_cost, '_fx_rate']
            print(f"⚠️ AJUSTE ITEM_COST: {mask_missing_cost.sum()} filas corregidas")

        # --- SUB-FASE 2: BASE COST (COGS) ---
        t1 = time.time()
        df['base_cost'] = 0.0
        
        # TM - Usa COGS transformados (sobre revenue USD)
        mask_tm = df['b_unit'] == 'TM'
        df.loc[mask_tm, 'base_cost'] = df['revenue'] * df['cohort'].map(transformed_cogs).fillna(0).abs()

        # 3P - Comisión sobre revenue USD
        mask_3p = df['b_unit'] == '3P'
        mask_3p_zero = mask_3p & (df['commission_percent'] == 0)
        df.loc[mask_3p_zero, 'commission_percent'] = 0.1
        df.loc[mask_3p, 'base_cost'] = df['revenue'] * (1 - df['commission_percent'])

        # Estándar (1P, FBP, DS) - item_cost se convierte usando fx_rate
        mask_std_cost = df['b_unit'].isin(['FBP', '1P', 'DS'])
        df.loc[mask_std_cost, 'base_cost'] = ((df['quantity'] * df['item_cost']) / self.IVA_FACTOR) / df.loc[mask_std_cost, '_fx_rate']

        # Otros
        mask_others = ~df['b_unit'].isin(['TM', '3P', 'FBP', '1P', 'DS'])
        df.loc[mask_others, 'base_cost'] = df['revenue'] * 0.90
        print(f"⏱️  Sub-fase 2 (Base Cost): {time.time() - t1:.2f}s")

        # --- SUB-FASE 3: DISTRIBUCIÓN FIJA ---
        t2 = time.time()
        
        orders_per_cohort = df.groupby('cohort')['order_id'].transform('nunique')
        items_per_order = df.groupby('order_id')['order_id'].transform('count')

        # Estos campos ya están en USD (vienen de supuestos)
        ops_cols = {
            'shipping_cost': 'shipping_cost_usd',
            'shipping_revenue': 'shipping_revenue_usd',
            'fc_variable_headcount': 'fc_variable_usd',
            'cs_variable_headcount': 'cs_variable_usd'
        }
        
        for raw_col, final_col in ops_cols.items():
            df['temp_raw_total'] = df[raw_col] * df['quantity']
            total_cohort_cost = df.groupby('cohort')['temp_raw_total'].transform('sum')
            cost_per_order = total_cohort_cost / orders_per_cohort
            df[final_col] = cost_per_order / items_per_order
            
        df = df.drop(columns=['temp_raw_total'])
        
        # Verificar que las columnas de distribución no sean todas cero
        for col in ops_cols.values():
            if col in df.columns:
                non_zero = (df[col] != 0).sum()
                if non_zero == 0:
                    print(f"⚠️ ADVERTENCIA: {col} está completamente en cero")
        
        print(f"⏱️  Sub-fase 3 (Distribución): {time.time() - t2:.2f}s")

        # --- SUB-FASE 4: CP ---
        t3 = time.time()

        # Estos son porcentajes sobre revenue USD
        df['credit_card_cost'] = df['revenue'] * df['credit_card_payment']
        df['cod_cost'] = df['revenue'] * df['cash_on_delivery_comision']
        df['fraud_cost'] = df['revenue'] * df['fraud']
        df['infra_cost'] = df['revenue'] * df['infrastructure']

        # Contribution Profit (TODO EN USD)
        df['contribution_profit'] = (
            df['revenue'] + df['shipping_revenue_usd'] + df['sois']
            - df['base_cost'] + df['shipping_cost_usd']
            + df['credit_card_cost'] + df['cod_cost'] + df['fraud_cost'] + df['infra_cost']
            + df['fc_variable_usd'] + df['cs_variable_usd']
            - df['retention_cost_$']
        )

        print(f"⏱️  Sub-fase 4 (CP): {time.time() - t3:.2f}s")

        # Limpiar columnas auxiliares
        cols_to_drop = ['_fx_rate']
        for col in cols_to_drop:
            if col in df.columns:
                df = df.drop(columns=[col])

        total_time = time.time() - start_total
        print("-" * 60)
        print(f"✅ METRICS CALCULATOR FINALIZADO EN {total_time:.2f}s")
        print(f"📊 CP Promedio: ${df['contribution_profit'].mean():.2f} USD")
        print(f"📊 Revenue Promedio: ${df['revenue'].mean():.2f} USD")
        print(f"📊 Base Cost Promedio: ${df['base_cost'].mean():.2f} USD")
        print("-" * 60)
        
        return df