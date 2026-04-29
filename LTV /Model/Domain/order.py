from datetime import datetime
from typing import Union

class Order:
    """
    Representa una transacción individual con lógica de Unit Economics avanzada.
    Versión tolerante a valores negativos y costos desglosados de pago.
    Incluye segmentación por Business Unit (BU) y soporte para Costo de Retención.
    
    Dimensiones soportadas para análisis:
    - category (mode=1)
    - subcategory (mode=2)
    - brand (mode=3)
    - name / product (mode=4)
    """

    def __init__(
        self,
        order_id: Union[str, int],
        customer_id: Union[str, int],
        order_date: str,
        revenue: float,
        cost: float,
        sois: float,
        shipping_cost: float, 
        shipping_revenue: float,
        quantity: float = 1.0,
        prod_pid: str = "N/A",
        credit_card_cost: float = 0.0,
        cod_cost: float = 0.0,
        category: str = "General",
        subcategory: str = "General",
        business_unit: str = "N/A",  
        fc_variable: float = 0.0,
        cs_variable: float = 0.0,
        fraud_cost: float = 0.0,
        infrastructure_cost: float = 0.0,
        retention_cost: float = 0.0,
        # NUEVAS COLUMNAS PARA BRAND Y PRODUCTO
        brand: str = "N/A",
        name: str = "N/A",
        **kwargs 
    ):
        # --- VALIDACIONES DE FECHA ---
        try:
            # Limpieza de fecha por si viene con timestamp (YYYY-MM-DD HH:MM:SS)
            clean_date = str(order_date).split(' ')[0]
            self.order_date = datetime.strptime(clean_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"ID {order_id}: formato de fecha inválido '{order_date}'. Use AAAA-MM-DD.")

        # Atributos de Identificación
        self.order_id = order_id
        self.customer_id = customer_id
        self.prod_pid = str(prod_pid)
        self.quantity = float(quantity) if quantity else 1.0
        self.category = str(category).strip() if category else "General"
        self.subcategory = str(subcategory).strip() if subcategory else "General"
        self.business_unit = str(business_unit).strip() if business_unit else "N/A"
        
        # NUEVOS ATRIBUTOS PARA DIMENSIONES ESTRATÉGICAS
        self.brand = str(brand).strip() if brand else "N/A"
        self.name = str(name).strip() if name else "N/A"
        
        # Atributos Financieros Base
        self.revenue = float(revenue)
        self.cost = float(cost)
        self.sois = float(sois)
        self.shipping_cost = float(shipping_cost)
        self.shipping_revenue = float(shipping_revenue)

        # Lógica de Pagos
        self.credit_card_cost = float(credit_card_cost)
        self.cod_cost = float(cod_cost)
        self.payment_cost = self.credit_card_cost + self.cod_cost
        
        # Inferencia de método de pago
        if self.credit_card_cost > 0:
            self.payment_method = "credit_card"
        elif self.cod_cost > 0:
            self.payment_method = "cash_on_delivery"
        else:
            self.payment_method = "other/adj"

        # Atributos Operativos y de Retención
        self.fc_variable = float(fc_variable)
        self.cs_variable = float(cs_variable)
        self.fraud_cost = float(fraud_cost)
        self.infrastructure_cost = float(infrastructure_cost)
        self.retention_cost = float(retention_cost)

    def calculate_cp(self) -> float:
        """
        Calcula el Contribution Profit (CP) Neto en $.
        Resta todos los costos operativos, incluyendo el nuevo costo de retención.
        """
        # El costo neto de envío (lo pagado vs lo recuperado)
        shipping_net = self.shipping_cost + self.shipping_revenue
        
        # Sumatoria de costos operativos variables
        total_opx_variable = (
            self.fc_variable + 
            self.cs_variable + 
            self.fraud_cost + 
            self.infrastructure_cost -
            self.retention_cost
        )
        
        # CP Final: Revenue - Cost + SOIS + Envío Neto + Costo de Pago + Operativos/Retención
        cp = (
            self.revenue - 
            self.cost + 
            self.sois +
            shipping_net + 
            self.payment_cost +
            total_opx_variable
        )
        
        return round(cp, 2)
    
    # En Model/Domain/order.py

    @property
    def subcategory_brand(self) -> str:
        """Retorna combinación de subcategoría y marca en formato: Subcategoría (Marca)"""
        subcat = str(self.subcategory).strip() if self.subcategory else ""
        brand = str(self.brand).strip() if self.brand else ""
        
        # Limpiar valores nulos o vacíos
        if not subcat or subcat.lower() in ["nan", "none", "n/a", "null", ""]:
            subcat = ""
        if not brand or brand.lower() in ["nan", "none", "n/a", "null", ""]:
            brand = ""
        
        if subcat and brand:
            return f"{subcat} ({brand})"  # ← NUEVO FORMATO
        elif brand:
            return brand
        elif subcat:
            return subcat
        else:
            return "N/A"

    def __repr__(self):
        return (f"<Order {self.order_id} | Qty: {self.quantity} | BU: {self.business_unit} | "
                f"Cat: {self.category} | Sub: {self.subcategory} | Brand: {self.brand} | "
                f"Product: {self.name} | CP: ${self.calculate_cp()} | Retención: ${self.retention_cost}>")