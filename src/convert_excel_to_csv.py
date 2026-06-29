import pandas as pd

excel_path = "data/raw/Online Retail.xlsx"
csv_path = "data/landing/online_retail.csv"

df = pd.read_excel(excel_path)

df.to_csv(csv_path, index=False)

print("CSV created successfully")
print(df.shape)