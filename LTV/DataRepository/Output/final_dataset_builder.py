import pandas as pd
import numpy as np

class FinalDatasetBuilder:
    """
    Responsabilidad: Formatear el DataFrame final para cumplir con el esquema 
    estricto del sistema downstream.
    - REVISIÓN v5.8: Se cambia el sufijo 'qtz' por '$' para claridad visual.
    """

    def build(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            print("⚠️ Advertencia: No hay datos para estructurar.")
            return pd.DataFrame()

        # 1. RENOMBRADO DE COLUMNAS (Aquí hacemos el cambio a $)
        rename_map = {
            'b_unit': 'business_unit',
            'fc_variable_qtz': 'fc_variable_$',
            'cs_variable_qtz': 'cs_variable_$',
            'shipping_cost_qtz': 'shipping_cost_$',
            'shipping_revenue_qtz': 'shipping_revenue_$',
            'product_pid': 'prod_pid'
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # --- FIX CRÍTICO PROD_PID ---
        if 'prod_pid' not in df.columns:
            df['prod_pid'] = 'UNKNOWN'

        # 2. CREACIÓN DE COLUMNAS FALTANTES
        if 'category' not in df.columns:
            df['category'] = 'UNKNOWN'
        if 'subcategory' not in df.columns:
            df['subcategory'] = 'UNKNOWN'
        
        # NUEVO: Asegurar existencia de brand y name
        if 'brand' not in df.columns:
            df['brand'] = 'UNKNOWN'
        if 'name' not in df.columns:
            df['name'] = 'UNKNOWN'

        # Actualizamos la lista de numéricos con los nuevos nombres ($)
        expected_numeric_cols = [
            'quantity', 'price', 'item_cost', 'revenue', 'base_cost', 
            'shipping_cost_$', 'credit_card_cost', 'cod_cost', 'fraud_cost', 
            'infra_cost', 'shipping_revenue_$', 'sois', 'contribution_profit',
            'commission_percent', 'fc_variable_$', 'cs_variable_$',
            'retention_cost_$'
        ]
        
        for col in expected_numeric_cols:
            if col not in df.columns:
                df[col] = 0.0

        # 3. ELIMINACIÓN DE COLUMNAS NO REQUERIDAS
        columns_to_drop = ['cohort_index', 'raw_soi', 'temp_raw_total']
        for col in columns_to_drop:
            if col in df.columns:
                df = df.drop(columns=[col])

        # 4. DEFINICIÓN DEL ORDEN EXACTO (Contrato actualizado con brand y name)
        columns_order = [
            'business_unit',
            'category',
            'subcategory',
            'brand',        # NUEVO
            'name',         # NUEVO
            'order_id',
            'customer_id',
            'order_date',
            'prod_pid',
            'quantity',
            'price',
            'item_cost',
            'commission_percent',
            'revenue',
            'base_cost',
            'shipping_cost_$',
            'credit_card_cost',
            'cod_cost',
            'fraud_cost',
            'infra_cost',
            'fc_variable_$',
            'cs_variable_$',
            'retention_cost_$',
            'shipping_revenue_$',
            'sois',
            'contribution_profit',
            'cohort'
        ]

        # 5. BLINDAJE Y FORMATEO
        for col in columns_order:
            if col not in df.columns:
                df[col] = 0.0 if col not in ['order_date', 'cohort', 'business_unit', 'brand', 'name'] else pd.NaT

        df_final = df[columns_order].copy()

        # Tipos de datos
        for col in ['order_id', 'customer_id', 'prod_pid']:
            df_final[col] = df_final[col].astype(str).str.replace('.0', '', regex=False)
        
        # NUEVO: Asegurar tipos string para brand y name
        df_final['brand'] = df_final['brand'].astype(str)
        df_final['name'] = df_final['name'].astype(str)

        for col in expected_numeric_cols:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0.0)

        df_final['order_date'] = pd.to_datetime(df_final['order_date'], errors='coerce')

        # 6. ORDENAMIENTO FINAL
        df_final = df_final.sort_values(by=['order_date', 'order_id'], ascending=[True, True])
        df_final = df_final.reset_index(drop=True)

        # NUEVO: Validación de integridad
        print(f"📊 Validación brand: {df_final['brand'].isnull().sum()} nulos")
        print(f"📊 Validación name: {df_final['name'].isnull().sum()} nulos")

        print("="*60)
        print("✅ Estructura final completada (Sufijos '$' aplicados).")
        print(f"🏷️  Columnas agregadas: brand, name")
        print(f"📊 CP Total: ${df_final['contribution_profit'].sum():,.2f}")
        print("="*60)

        return df_final