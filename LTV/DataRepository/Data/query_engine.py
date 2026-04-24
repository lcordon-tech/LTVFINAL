import pandas as pd
from sqlalchemy import create_engine
from typing import Optional


class QueryEngine:
    """
    Responsabilidad: Gestionar la conexión a MySQL y extraer la data 
    cruda de órdenes aplicando la lógica de negocio de Pacifiko.
    
    AHORA: Soporta queries diferentes por país.
    """
    
    # Query para Guatemala
    QUERY_GT = """
        WITH order_base AS (  
            SELECT
                o1.payment_code,
                o1.shipping_method,
                o1.order_id,
                o1.parent_order_id,
                o1.order_status_id,
                o1.customer_id,
                o1.bank
            FROM db_pacifiko.oc_order o1
            WHERE o1.parent_order_id IS NULL OR o1.parent_order_id != 0
        ),
        fecha_real AS (
            SELECT
                order_id,
                MIN(date_added) AS fecha_colocada
            FROM db_pacifiko.oc_order_history
            WHERE order_status_id IN (1, 2, 13)
            GROUP BY 1
        ),
        product_ref AS (      
            SELECT DISTINCT
                p.product_id,
                p.product_pid,
                p.cost
            FROM db_pacifiko.oc_product p
            WHERE p.product_merchant_code = 'PAC1'
            OR (p.product_merchant_code = '' AND p.product_merchant_type = 'S')
        ),
        order_status_desc AS (
            SELECT DISTINCT
                os.order_status_id,
                os.name AS order_status_name
            FROM db_pacifiko.oc_order_status os
            WHERE os.language_id = 2
        ),
        vendor_commission_dedup AS (
            SELECT
                pvc.order_id,
                pvc.product_id,
                MAX(pvc.commission_percent) AS commission_percent
            FROM db_pacifiko.oc_purpletree_vendor_commissions pvc
            GROUP BY pvc.order_id, pvc.product_id
        )
        SELECT
            fr.fecha_colocada,
            ob.customer_id,
            ob.payment_code,
            ob.shipping_method,
            op.order_id,
            op.product_id,
            pr.product_pid,
            op.quantity,
            op.cost AS cost_order_table,
            op.price,
            pr.cost AS cost_product_table,
            CASE
                WHEN op.cost IS NOT NULL AND op.cost > 0 THEN op.cost
                ELSE pr.cost
            END AS cost_item,
            ob.bank,
            osd.order_status_name,
            COALESCE(vcd.commission_percent, 0) AS commission_percent
        FROM db_pacifiko.oc_order_product op
        JOIN order_base ob ON ob.order_id = op.order_id
        LEFT JOIN fecha_real fr ON fr.order_id = ob.order_id
        LEFT JOIN product_ref pr ON pr.product_id = op.product_id
        JOIN order_status_desc osd ON osd.order_status_id = ob.order_status_id
        LEFT JOIN vendor_commission_dedup vcd ON vcd.order_id = op.order_id 
            AND vcd.product_id = op.product_id
        WHERE ob.order_status_id IN (1, 2, 3, 5, 9, 14, 15, 17, 18, 19, 20, 21, 29, 30, 34, 50)
        AND op.order_product_status_id NOT IN (9, 15, 2, 4, 19, 33, 35, 36, 37, 38, 39, 43, 44, 45)
        AND fr.fecha_colocada BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY fr.fecha_colocada ASC;
    """
    
    # Query para Costa Rica
    QUERY_CR = """
        WITH order_base AS (  
            SELECT
                o1.payment_code,
                o1.shipping_method,
                o1.order_id,
                o1.order_status_id,
                o1.customer_id,
                o1.bank
            FROM oc_order o1
        ),
        fecha_real AS (
            SELECT
                oh.order_id,
                MIN(oh.date_added) AS fecha_colocada
            FROM oc_order_history oh
            WHERE oh.order_status_id IN (1, 2, 13)
            GROUP BY 1
        ),
        product_ref AS (      
            SELECT DISTINCT
                p.product_id,
                p.product_pid,
                p.cost,
                p.cabys
            FROM oc_product p
        ),
        order_status_desc AS (
            SELECT DISTINCT
                os.order_status_id,
                os.name AS order_status_name
            FROM oc_order_status os
            WHERE os.language_id = 2
        ),
        cabys_db AS (
            SELECT pc.cabys, pc.tax_rate FROM pac_cabys pc
        )
        SELECT
            fr.fecha_colocada,
            ob.customer_id,
            ob.payment_code,
            ob.shipping_method,
            op.order_id,
            op.product_id,
            pr.product_pid,
            op.quantity,
            osd.order_status_name,
            op.price / (1 + COALESCE(pc1.tax_rate, 0.13)) AS price,
            CASE 
                WHEN op.cost IS NOT NULL AND op.cost > 0 
                THEN op.cost / (1 + COALESCE(pc1.tax_rate, 0.13))
                ELSE pr.cost / (1 + COALESCE(pc1.tax_rate, 0.13))
            END AS cost_item
        FROM oc_order_product op
        JOIN order_base ob ON ob.order_id = op.order_id
        LEFT JOIN fecha_real fr ON fr.order_id = ob.order_id
        LEFT JOIN product_ref pr ON pr.product_id = op.product_id
        LEFT JOIN cabys_db pc1 ON pc1.cabys = pr.cabys
        JOIN order_status_desc osd ON osd.order_status_id = ob.order_status_id
        WHERE 
            ob.order_status_id IN (1, 2, 3, 5, 9, 14, 15, 17, 18, 19, 20, 21, 29, 30, 34, 50)
            AND op.order_product_status_id NOT IN (9, 15, 2, 4, 19, 33, 34, 35, 36, 37, 38, 39, 43, 44, 45)
            AND fr.fecha_colocada BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY fr.fecha_colocada ASC;
    """
    
    # Mapeo de país a query
    QUERIES = {
        "GT": QUERY_GT,
        "CR": QUERY_CR,
    }
    
    def __init__(self, user, password, host, db, country_code: str = "GT"):
        """
        Args:
            user: Usuario de BD
            password: Contraseña
            host: Host de BD
            db: Nombre de la base de datos
            country_code: Código del país (GT, CR)
        """
        print(f"\n🔧 [QueryEngine] INICIALIZANDO:")
        print(f"   País: {country_code}")
        print(f"   Host: {host}")
        print(f"   Database: {db}")
        print(f"   User: {user}")
        
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db}")
        self.country_code = country_code.upper()
        self.database = db  # ← Guardar la base de datos
        
        # Seleccionar la query según el país
        if self.country_code not in self.QUERIES:
            print(f"⚠️ País '{self.country_code}' no tiene query definida. Usando query de Guatemala.")
            self.query = self.QUERIES["GT"]
        else:
            self.query = self.QUERIES[self.country_code]
            print(f"📋 Usando query específica para {self.country_code}")
        
        # 🔧 Reemplazar el nombre de la base de datos en la query si es necesario
        if self.country_code == "GT" and self.database != "db_pacifiko":
            self.query = self.query.replace("db_pacifiko.", f"{self.database}.")
            print(f"   🔄 Query adaptada a base de datos: {self.database}")

    def fetch_orders(self, start_date=None, end_date=None) -> pd.DataFrame:
        """
        Ejecuta la conexión y descarga la data en un DataFrame.
        
        Args:
            start_date: datetime o str 'YYYY-MM-DD' (filtro inicio)
            end_date: datetime o str 'YYYY-MM-DD' (filtro fin)
        """
        try:
            # Valores por defecto según país
            if start_date is None:
                if self.country_code == "CR":
                    start_date = '2022-01-01'  # CR empieza en 2022
                else:
                    start_date = '2020-01-01'  # GT desde 2020
            
            if end_date is None:
                if self.country_code == "CR":
                    end_date = '2026-03-30'   # CR termina en 2026
                else:
                    end_date = '2030-12-31'   # GT futuro extensible
            
            # Convertir a string si es datetime
            if hasattr(start_date, 'strftime'):
                start_date = start_date.strftime('%Y-%m-%d')
            if hasattr(end_date, 'strftime'):
                end_date = end_date.strftime('%Y-%m-%d')
            
            params = {
                'start_date': start_date,
                'end_date': end_date
            }
            
            print(f"🔍 Conectando a la Base de Datos... ({self.country_code})")
            print(f"📅 Rango consultado: {start_date} → {end_date}")
            df = pd.read_sql(self.query, self.engine, params=params)
            
            if df.empty:
                print("⚠️ La consulta se ejecutó pero no devolvió filas.")
                return pd.DataFrame()
            
            print(f"✅ Descarga exitosa: {len(df)} filas obtenidas.")
            return df
        except Exception as e:
            print(f"❌ Error crítico en QueryEngine: {e}")
            return pd.DataFrame()