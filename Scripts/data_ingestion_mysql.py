import pandas as pd
import os
from sqlalchemy import create_engine


user = '****'
password = '****'
host = '****'
port = ****
database = 'inventory_db'

engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}')

def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con = engine, if_exists = 'append', index = False)

path = r'C:\Users\****\Desktop\data'

for file in os.listdir(path):
    if file.endswith('.csv'):
        try:
            full_path = os.path.join(path, file)
            df = pd.read_csv(full_path, low_memory=False)
            
            print(f"📤 Uploading: {file}")
            df.to_sql(file[:-4], con = engine, if_exists='replace', index=False, chunksize=10000)
            print(f"✅ Done: {file}\n")

        except Exception as e:
            print(f"❌ Error with {file}:\n{e}")
            with engine.connect() as conn:
                conn.rollback()
                
📤 Uploading: begin_inventory.csv
✅ Done: begin_inventory.csv

📤 Uploading: end_inventory.csv
✅ Done: end_inventory.csv

📤 Uploading: purchases.csv
✅ Done: purchases.csv

📤 Uploading: purchase_prices.csv
✅ Done: purchase_prices.csv

📤 Uploading: sales.csv
✅ Done: sales.csv

📤 Uploading: vendor_invoice.csv
✅ Done: vendor_invoice.csv
