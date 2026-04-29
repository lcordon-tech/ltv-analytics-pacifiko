import pandas as pd
from sqlalchemy import create_engine
from typing import Optional
from datetime import datetime, timedelta


class QueryEngine:
    """
    Responsabilidad: Gestionar la conexión a MySQL y extraer la data 
    cruda de órdenes aplicando la lógica de negocio de Pacifiko.
    
    AHORA: 
    - GT: Query ORIGINAL (sin IVA). El IVA se aplica en MetricsCalculator.
    - CR: Aplica neteo de IVA dinámico por tax_rate (13% default) en la query.
    
    FECHAS:
    - end_date por defecto = hoy - 7 días (margen para actualización de DB)
    """
    
    # Query para Guatemala (SIN IVA - se aplica en MetricsCalculator)
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
    
    # Query para Costa Rica (CON IVA neteado por tax_rate dinámico)
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
    
    QUERIES = {
        "GT": QUERY_GT,
        "CR": QUERY_CR,
    }
    
    # Offset por defecto para end_date (días hacia atrás desde hoy)
    DEFAULT_DAYS_OFFSET = -7  # 7 días antes de hoy
    
    def __init__(self, user: str, password: str, host: str, db: str, country_code: str = "GT"):
        """
        Args:
            user: Usuario de BD
            password: Contraseña
            host: Host de BD
            db: Nombre de la base de datos
            country_code: Código del país (GT, CR)
        """
        # Guardar credenciales para posible recreación del engine
        self.user = user
        self.password = password
        self.host = host
        self.database = db
        self.country_code = country_code.upper()
        
        print(f"\n🔧 [QueryEngine] INICIALIZANDO:")
        print(f"   País: {self.country_code}")
        print(f"   Host: {self.host}")
        print(f"   Database: {self.database}")
        print(f"   📅 end_date por defecto: hoy {abs(self.DEFAULT_DAYS_OFFSET)} días")
        
        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{db}")
        
        # Seleccionar la query según el país
        if self.country_code not in self.QUERIES:
            print(f"⚠️ País '{self.country_code}' no tiene query definida. Usando query de Guatemala.")
            self.query = self.QUERIES["GT"]
        else:
            self.query = self.QUERIES[self.country_code]
            print(f"📋 Usando query específica para {self.country_code}")
            if self.country_code == "GT":
                print(f"   🇬🇹 Query ORIGINAL (sin IVA). El IVA se aplicará en MetricsCalculator")
            elif self.country_code == "CR":
                print(f"   🇨🇷 Aplicando neteo de IVA dinámico por tax_rate")
        
        # Adaptar nombre de base de datos si es necesario (solo para GT)
        if self.country_code == "GT" and self.database != "db_pacifiko":
            self.query = self.query.replace("db_pacifiko.", f"{self.database}.")
            print(f"   🔄 Query adaptada a base de datos: {self.database}")

    def fetch_orders(self, start_date=None, end_date=None) -> pd.DataFrame:
        """
        Ejecuta la conexión y descarga la data en un DataFrame.
        
        Args:
            start_date: datetime o str 'YYYY-MM-DD' (filtro inicio)
                       Si es None, usa default según país:
                       - GT: '2020-01-01'
                       - CR: '2022-01-01'
            end_date: datetime o str 'YYYY-MM-DD' (filtro fin)
                      Si es None, usa hoy - DEFAULT_DAYS_OFFSET días
        
        Returns:
            pd.DataFrame con datos de órdenes
        """
        try:
            # 🔧 LIMPIAR FECHAS - GARANTIZAR QUE SEAN STRINGS
            def clean_date(date_val):
                if date_val is None:
                    return None
                if isinstance(date_val, dict):
                    return str(date_val.get('start_date', date_val.get('date', date_val.get('end_date', '2020-01-01'))))
                if hasattr(date_val, 'strftime'):
                    return date_val.strftime('%Y-%m-%d')
                return str(date_val)
            
            start_date_clean = clean_date(start_date)
            end_date_clean = clean_date(end_date)
            
            # Valores por defecto para start_date según país
            if start_date_clean is None:
                start_date_clean = '2022-01-01' if self.country_code == "CR" else '2020-01-01'
            
            # 🔧 NUEVO: end_date por defecto = hoy + DEFAULT_DAYS_OFFSET (ej: hoy - 7 días)
            if end_date_clean is None:
                target_date = datetime.now() + timedelta(days=self.DEFAULT_DAYS_OFFSET)
                end_date_clean = target_date.strftime('%Y-%m-%d')
                print(f"📅 Usando end_date por defecto: {end_date_clean} (hoy {self.DEFAULT_DAYS_OFFSET:+d} días)")
            
            params = {
                'start_date': start_date_clean,
                'end_date': end_date_clean
            }
            
            print(f"🔍 Conectando a la Base de Datos... ({self.country_code})")
            print(f"📅 Rango consultado: {start_date_clean} → {end_date_clean}")
            
            df = pd.read_sql(self.query, self.engine, params=params)
            
            if df.empty:
                print("⚠️ La consulta se ejecutó pero no devolvió filas.")
                return pd.DataFrame()
            
            print(f"✅ Descarga exitosa: {len(df)} filas obtenidas.")
            return df
            
        except Exception as e:
            print(f"❌ Error crítico en QueryEngine: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def get_date_range_info(self) -> dict:
        """
        Retorna información sobre el rango de fechas por defecto.
        Útil para debugging.
        """
        today = datetime.now()
        default_end = today + timedelta(days=self.DEFAULT_DAYS_OFFSET)
        
        return {
            "country_code": self.country_code,
            "default_start_gt": "2020-01-01",
            "default_start_cr": "2022-01-01",
            "default_end": default_end.strftime('%Y-%m-%d'),
            "default_end_description": f"hoy {self.DEFAULT_DAYS_OFFSET:+d} días",
            "offset_days": self.DEFAULT_DAYS_OFFSET,
        }