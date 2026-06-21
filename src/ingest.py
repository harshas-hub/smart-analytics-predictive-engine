import os
import csv
from typing import List, Dict, Set, Generator, Any

class DataIngestionEngine:
    def __init__(self, input_path: str):
        self.input_path = input_path
        self.seen_transactions: Set[str] = set()  # O(1) lookup speed to track duplicates

    def read_raw_data(self) -> Generator[Dict[str, str], None, None]:
        """Reads CSV data line by line using a Generator to save system RAM."""
        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Target data file not found at: {self.input_path}")
            
        with open(self.input_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                yield row

    def clean_row(self, row: Dict[str, str]) -> Dict[str, Any] | None:
        """Applies validation and data-cleaning transformations matrix."""
        # Strip trailing/leading hidden spaces from column keys and values
        tx_id = row.get("transaction_id", "").strip()
        cust_id = row.get("customer_id", "").strip()
        amount_raw = row.get("amount", "").strip()
        category = row.get("product_category", "").strip()
        status = row.get("status", "").strip()

        # 1. Validation: Drop duplicates or records missing critical identification IDs
        if not tx_id or tx_id in self.seen_transactions:
            return None
        if not cust_id:
            return None  # Drop orphaned records

        # 2. Type Casting & Anomaly Handling
        try:
            amount = float(amount_raw)
            if amount <= 0:
                return None  # Filter out negative numbers or accidental zero entries
        except ValueError:
            return None  # Drop rows where amounts are corrupted text strings or blank

        # 3. Standardization & State Management
        self.seen_transactions.add(tx_id)
        return {
            "transaction_id": tx_id,
            "customer_id": cust_id,
            "timestamp": row.get("timestamp", "").strip(),
            "amount": amount,
            "product_category": category.capitalize(),  # Standardizes 'ELECTRONICS' -> 'Electronics'
            "status": status.upper()                     # Standardizes case uniformity
        }

    def process_pipeline(self) -> List[Dict[str, Any]]:
        """Orchestrates stream extraction and returns validated datasets."""
        print("🚀 Starting Data Ingestion Pipeline...")
        cleaned_dataset: List[Dict[str, Any]] = []
        
        for raw_row in self.read_raw_data():
            processed_row = self.clean_row(raw_row)
            if processed_row:
                cleaned_dataset.append(processed_row)
                
        print(f"✅ Pipeline Completed! Successfully ingested {len(cleaned_dataset)} clean records.")
        return cleaned_dataset


if __name__ == "__main__":
    # Define relative paths to step back out from src/ and look inside data/
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_file_path = os.path.join(base_dir, "data", "raw_transactions.csv")
    
    try:
        engine = DataIngestionEngine(data_file_path)
        clean_data = engine.process_pipeline()
        
        print("\n--- Ingested Sample Records ---")
        for record in clean_data[:3]:
            print(record)
            
    except Exception as e:
        print(f"❌ Pipeline Execution Failed: {e}")