import pandas as pd
import numpy as np

class AssumptionApplier:
    """
    Responsabilidad: Sincronizar órdenes con supuestos financieros (1P, 3P, FBP, TM, DS).
    AHORA: También carga y valida supuestos de cohortes desde Excel.
    """

    def apply(self, df: pd.DataFrame, assumptions_dict: dict) -> pd.DataFrame:
        print("\n" + "="*60)
        print(" APLICANDO SUPUESTOS Y NORMALIZACIÓN HISTÓRICA ".center(60))
        print("="*60)

        # 0. PREPARACIÓN DE DATOS
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['cohort'] = df['cohort'].astype(str).str.strip().str.upper()

        # --- PASO A: NORMALIZACIÓN PRE-MODELO (FECHAS < 2021) ---
        mask_pre_2021 = df['order_date'] < '2021-01-01'
        
        if mask_pre_2021.any():
            mask_1p_ds = mask_pre_2021 & df['b_unit'].isin(['1P', 'DS'])
            df.loc[mask_1p_ds, 'cohort'] = 'Q1'
            mask_3p_fbp = mask_pre_2021 & df['b_unit'].isin(['3P', 'FBP'])
            df.loc[mask_3p_fbp, 'cohort'] = 'Q9'
            mask_tm = mask_pre_2021 & (df['b_unit'] == 'TM')
            df.loc[mask_tm, 'cohort'] = 'Q8'
            print(f"⚠️  AJUSTE PRE-MODELO: {mask_pre_2021.sum()} registros (<2021) reasignados.")

        # --- PASO B: AJUSTES ESTRATÉGICOS POR UNIDAD ---
        cohort_num = df['cohort'].str.replace('Q', '', regex=False).replace('', '0').astype(int)

        mask_early_3p = (df['b_unit'] == '3P') & (cohort_num < 9)
        if mask_early_3p.any():
            valid_3p = (df['b_unit'] == '3P') & (cohort_num >= 9)
            avg_comm = df.loc[valid_3p, 'commission_percent'].mean()
            if pd.isna(avg_comm): avg_comm = 0.9
            df.loc[mask_early_3p, 'commission_percent'] = avg_comm
            df.loc[mask_early_3p, 'cohort'] = 'Q9'
            print(f"⚠️  AJUSTE 3P: {mask_early_3p.sum()} filas normalizadas a Q9.")

        mask_early_fbp = (df['b_unit'] == 'FBP') & (cohort_num < 9)
        if mask_early_fbp.any():
            df.loc[mask_early_fbp, 'cohort'] = 'Q9'
            print(f"⚠️  AJUSTE FBP: {mask_early_fbp.sum()} filas normalizadas a Q9.")

        mask_early_tm = (df['b_unit'] == 'TM') & (cohort_num < 8)
        if mask_early_tm.any():
            df.loc[mask_early_tm, 'cohort'] = 'Q8'
            print(f"⚠️  AJUSTE TM: {mask_early_tm.sum()} filas normalizadas a Q8.")

        # 1. Configuración de Unidades y Columnas
        required_units = ["1P", "3P", "FBP", "TM", "DS"]
        required_cols = [
            "cohort", "shipping_cost", "shipping_revenue", 
            "credit_card_payment", "cash_on_delivery_comision", 
            "fc_variable_headcount", "cs_variable_headcount", 
            "fraud", "infrastructure"
        ]

        df_final_list = []
        
        # 2. BUCLE DE PROCESAMIENTO POR UNIDAD
        for unit in required_units:
            df_unit = df[df['b_unit'] == unit].copy()
            if df_unit.empty: continue

            if unit not in assumptions_dict:
                print(f"⚠️  ADVERTENCIA: Pestaña '{unit}' no encontrada. Valores en 0.")
                df_final_list.append(df_unit)
                continue
            
            df_assump_unit = assumptions_dict[unit].copy()
            df_assump_unit['cohort'] = df_assump_unit['cohort'].astype(str).str.strip().str.upper()
            
            cols_to_merge = [c for c in required_cols if c in df_assump_unit.columns]
            df_assump_unit = df_assump_unit[cols_to_merge].sort_values('cohort').ffill()

            df_merged = pd.merge(df_unit, df_assump_unit, on='cohort', how='left')
            numeric_cols = df_merged.select_dtypes(include=[np.number]).columns
            df_merged[numeric_cols] = df_merged[numeric_cols].fillna(0.0)

            df_final_list.append(df_merged)

        # 3. CONSOLIDACIÓN FINAL Y RESCATE DE "OTROS"
        df_final = pd.concat(df_final_list, ignore_index=True) if df_final_list else df
        
        df_others = df[~df['b_unit'].isin(required_units)].copy()
        if not df_others.empty:
            print(f"⚠️  RESIDUOS: {len(df_others)} filas de 'OTROS' heredan supuestos de 1P.")
            df_assump_base = assumptions_dict.get('1P', pd.DataFrame()).copy()
            if not df_assump_base.empty:
                df_assump_base['cohort'] = df_assump_base['cohort'].astype(str).str.strip().str.upper()
                df_others = pd.merge(df_others, df_assump_base[required_cols], on='cohort', how='left')
                num_cols_others = df_others.select_dtypes(include=[np.number]).columns
                df_others[num_cols_others] = df_others[num_cols_others].fillna(0.0)
            df_final = pd.concat([df_final, df_others], ignore_index=True)

        # --- NUEVO: VALIDACIÓN DE SUPUESTOS DE COHORTES ---
        print("\n" + "🔍 VALIDACIÓN DE SUPUESTOS POR COHORTE".center(60, "-"))
        
        # Detectar cohortes en los datos
        cohorts_in_data = sorted(df_final['cohort'].unique())
        
        # Verificar qué cohortes tienen supuestos en el Excel
        cohorts_with_assumptions = set()
        for sheet_name, df_sheet in assumptions_dict.items():
            if 'cohort' in df_sheet.columns:
                for cohort in df_sheet['cohort'].astype(str).str.strip().str.upper():
                    cohorts_with_assumptions.add(cohort)
        
        missing_cohorts = [c for c in cohorts_in_data if c not in cohorts_with_assumptions]
        
        if missing_cohorts:
            print(f"⚠️ Cohortes sin supuestos definidos en Excel ({len(missing_cohorts)}):")
            for c in missing_cohorts[:10]:
                print(f"   • {c}")
            if len(missing_cohorts) > 10:
                print(f"   ... y {len(missing_cohorts) - 10} más")
            print("   → Usando valores por defecto o forward fill")
        else:
            print(f"✅ Todas las cohortes ({len(cohorts_in_data)}) tienen supuestos definidos")
        
        print("-" * 60)
        print(f"✨ Pipeline de Supuestos finalizado exitosamente.")
        return df_final