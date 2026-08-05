# utils/export_to_csv.py
"""
One-shot script: reads all Parquet files from output/ and exports
three clean CSV files for Power BI Desktop import.

Usage:
    python utils/export_to_csv.py

Output:
    powerbi_export/lifestyle.csv
    powerbi_export/personal.csv
    powerbi_export/profession.csv

In Power BI Desktop:
    Get Data → Text/CSV → select each file → Load
    Then build your visuals and save as .pbix
"""

import os
import pandas as pd

BASE       = os.environ.get("OUTPUT_BASE", os.path.join(os.path.dirname(os.path.dirname(__file__)), "output"))
EXPORT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "powerbi_export")


def read_parquet_folder(folder: str) -> pd.DataFrame:
    if not os.path.exists(folder):
        print(f"  [SKIP] folder not found: {folder}")
        return pd.DataFrame()
    files = [os.path.join(folder, f)
             for f in os.listdir(folder) if f.endswith(".parquet")]
    if not files:
        print(f"  [SKIP] no parquet files in: {folder}")
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    # Parse timestamp for Power BI compatibility
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def main():
    os.makedirs(EXPORT_DIR, exist_ok=True)
    print(f"Exporting Parquet → CSV to: {EXPORT_DIR}\n")

    datasets = {
        "lifestyle":  os.path.join(BASE, "lifestyle"),
        "personal":   os.path.join(BASE, "personal"),
        "profession": os.path.join(BASE, "profession"),
    }

    for name, path in datasets.items():
        print(f"  Reading {name} …")
        df = read_parquet_folder(path)
        if df.empty:
            print(f"  [WARN] {name} is empty — skipping.\n")
            continue
        out = os.path.join(EXPORT_DIR, f"{name}.csv")
        df.to_csv(out, index=False)
        print(f"  ✓  {len(df):,} rows → {out}\n")

    print("Done. Import these CSVs into Power BI Desktop:")
    print("  Get Data → Text/CSV → select each file → Load")
    print("  Save your visuals as a .pbix file for your portfolio.")


if __name__ == "__main__":
    main()
