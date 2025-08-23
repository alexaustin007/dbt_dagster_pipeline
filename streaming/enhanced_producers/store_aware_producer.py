"""
Enhanced Kafka producer that generates realistic sales events using actual store/department data
"""
import json
import random
import time
import uuid
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
from typing import Dict, List

class StoreAwareProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        
        # Load actual store and sales data
        self.stores_df = pd.read_csv("data/raw/stores.csv")
        self.sales_df = pd.read_csv("data/raw/sales.csv")
        
        # Extract unique store IDs and department IDs
        self.store_ids = self.stores_df['Store'].unique().tolist()
        self.dept_ids = self.sales_df['Dept'].unique().tolist()
        
        # Store type mappings for different behavior patterns
        self.store_types = dict(zip(self.stores_df['Store'], self.stores_df['Type']))
        
        # Business hours configuration
        self.business_hours = {
            'weekday': {'start': 6, 'end': 22},  # 6 AM - 10 PM
            'weekend': {'start': 8, 'end': 23}   # 8 AM - 11 PM
        }
        
        # Store type transaction patterns
        self.store_patterns = {
            'A': {'volume_multiplier': 3.0, 'avg_transaction': 45.0},  # Large stores
            'B': {'volume_multiplier': 2.0, 'avg_transaction': 35.0},  # Medium stores
            'C': {'volume_multiplier': 1.0, 'avg_transaction': 25.0}   # Small stores
        }
        
        # Department-specific price ranges
        self.dept_price_ranges = {
            1: (5, 25),     # Grocery
            2: (10, 50),    # Entertainment
            3: (15, 75),    # Home & Garden
            # Add more departments as needed
        }
        
        print(f"Initialized producer with {len(self.store_ids)} stores and {len(self.dept_ids)} departments")

    def is_business_hours(self) -> bool:
        """Check if current time is within business hours"""
        now = datetime.now()
        day_type = 'weekend' if now.weekday() >= 5 else 'weekday'
        hours = self.business_hours[day_type]
        return hours['start'] <= now.hour <= hours['end']

    def get_transaction_probability(self, store_id: int) -> float:
        """Calculate transaction probability based on store type and time"""
        if not self.is_business_hours():
            return 0.1  # Very low probability outside business hours
        
        store_type = self.store_types.get(store_id, 'C')
        base_prob = self.store_patterns[store_type]['volume_multiplier'] * 0.3
        
        # Add time-of-day patterns
        hour = datetime.now().hour
        if 11 <= hour <= 13 or 17 <= hour <= 19:  # Lunch and dinner rush
            base_prob *= 2.0
        elif 20 <= hour <= 22:  # Evening shopping
            base_prob *= 1.5
        
        return min(base_prob, 1.0)

    def generate_realistic_price(self, dept_id: int, store_type: str) -> float:
        """Generate realistic price based on department and store type"""
        price_range = self.dept_price_ranges.get(dept_id, (10, 100))
        base_price = random.uniform(price_range[0], price_range[1])
        
        # Store type pricing adjustments
        type_multipliers = {'A': 1.1, 'B': 1.0, 'C': 0.9}
        return round(base_price * type_multipliers.get(store_type, 1.0), 2)

    def generate_sales_event(self) -> Dict:
        """Generate a realistic sales event"""
        store_id = random.choice(self.store_ids)
        store_type = self.store_types[store_id]
        
        # Check if transaction should occur based on probability
        if random.random() > self.get_transaction_probability(store_id):
            return None
        
        dept_id = random.choice(self.dept_ids)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
        unit_price = self.generate_realistic_price(dept_id, store_type)
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": datetime.utcnow().isoformat(),
            "store_id": store_id,
            "dept_id": dept_id,
            "product_id": f"{store_id}_{dept_id}_{random.randint(1000, 9999)}",
            "customer_id": str(uuid.uuid4()),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(quantity * unit_price, 2),
            "transaction_type": random.choices(
                ["SALE", "RETURN", "EXCHANGE"], 
                weights=[85, 10, 5]
            )[0],
            "payment_method": random.choices(
                ["CARD", "CASH", "MOBILE"], 
                weights=[60, 25, 15]
            )[0],
            "promotion_applied": random.choice([True, False])
        }
        
        return event

    def run(self):
        """Main producer loop"""
        print("Starting store-aware sales event producer...")
        print(f"Business hours: {self.business_hours}")
        
        while True:
            try:
                event = self.generate_sales_event()
                
                if event:  # Only send if event was generated (based on probability)
                    self.producer.send("sales_events", event)
                    print(f"Sent: Store {event['store_id']}, Dept {event['dept_id']}, ${event['total_amount']}")
                else:
                    print("No transaction (outside business hours or low probability)")
                
                # Variable sleep time based on business hours
                sleep_time = 0.5 if self.is_business_hours() else 5.0
                time.sleep(sleep_time)
                
            except KeyboardInterrupt:
                print("Stopping producer...")
                break
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(1)
        
        self.producer.close()

if __name__ == "__main__":
    producer = StoreAwareProducer()
    producer.run()