"""
Script Name: vendor_sales_summary.py
Description: Extracts, cleans, and enriches vendor-level sales and purchase data from MySQL.
Author: Gaurav Kumar Mahato
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Database Configuration
DB_CONFIG = {
    'user': '****',
    'password': '****',
    'host': '****',
    'port': ****,
    'database': 'inventory_db'
}

# Create SQLAlchemy Engine
engine = create_engine(
    f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)


def create_vendor_summary(engine):
    """
    This function merges vendor, purchase, and sales data to create a vendor-level summary.
    It executes a SQL query using Common Table Expressions (CTEs).
    """
    query = '''
    WITH freight_summary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),

    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Volume, pp.Price
    ),

    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )

    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.Volume,
        ps.ActualPrice,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN freight_summary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    '''

    df = pd.read_sql_query(query, engine)
    return df


def clean_vendor_data(df):
    """
    This function cleans and enriches the vendor summary data.
    It adds new KPIs such as GrossProfit, ProfitMargin, etc.
    """
    # Convert types
    df['Volume'] = df['Volume'].astype('float64')

    # Fill nulls
    df.fillna(0, inplace=True)

    # Clean string columns
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Create new columns
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollars']) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity']
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars']

    # Replace inf/-inf
    df.replace([np.inf, -np.inf], 0, inplace=True)

    return df


# Main Execution
if __name__ == "__main__":
    print("\n Running Vendor Summary Pipeline...")

    # Step 1: Create summary from SQL
    vendor_summary = create_vendor_summary(engine)

    # Step 2: Clean and enrich data
    vendor_summary_cleaned = clean_vendor_data(vendor_summary)

    # vendor_summary_cleaned.to_sql("vendor_sales_summary", engine, if_exists="replace", index=False)
    # vendor_summary_cleaned.to_csv("vendor_sales_summary.csv", index=False)

    print("Done. Here's a preview:\n")
    print(vendor_summary_cleaned.head())
