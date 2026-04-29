from .order import Order
from .customer import Customer
import pandas as pd


class LTVController:
    def __init__(self):
        self.customers = {}
        # Usamos un set de huellas digitales para permitir múltiples items por orden
        self._processed_fingerprints = set()

    def process_raw_data(self, raw_data_list):
        """
        Orquesta la creación de modelos y asegura la integridad de la base.
        Soporta múltiples productos bajo una misma orden (Order ID).
        """
        for data in raw_data_list:
            try:
                # 1. IDENTIFICACIÓN Y HUELLA DIGITAL (Mejorada)
                o_id = data.get('order_id')
                revenue = data.get('revenue', 0)
                # Agregamos category o un identificador de producto a la huella 
                # para que si una orden tiene 2 items con mismo precio, NO se borren.
                item_id = data.get('category', 'Gen') 
                fingerprint = f"{o_id}_{revenue}_{item_id}" 
                
                if fingerprint in self._processed_fingerprints:
                    continue 

                # 2. LIMPIEZA Y SEGURIDAD (Blindaje de Datos)
                # Quitamos payment_cost para evitar conflictos con los nuevos campos
                if 'payment_cost' in data:
                    data.pop('payment_cost')
                
                # Aseguramos que 'quantity' exista para que el Exporter no falle
                if 'quantity' not in data or pd.isna(data['quantity']):
                    data['quantity'] = 1.0

                # 3. INSTANCIAR EL MODELO
                # Al pasar **data, enviamos automáticamente:
                # quantity, business_unit, credit_card_cost, cod_cost, etc.
                new_order = Order(**data)
                
                # 4. ASIGNACIÓN AL CLIENTE
                c_id = str(new_order.customer_id)
                if c_id not in self.customers:
                    self.customers[c_id] = Customer(c_id)
                
                self.customers[c_id].add_order(new_order)
                
                # 5. REGISTRO DE PROCESAMIENTO
                self._processed_fingerprints.add(fingerprint)

            except ValueError as ve:
                print(f"[Controlador] Saltando fila inválida {data.get('order_id', 'S/ID')}: {ve}")
            except Exception as e:
                print(f"[Controlador] Error inesperado en orden {data.get('order_id', 'S/ID')}: {e}")

    def get_customers(self):
        """Retorna la lista de objetos Customer procesados."""
        return list(self.customers.values())

    def get_total_clients(self):
        """Retorna el conteo único de clientes captados."""
        return len(self.customers)