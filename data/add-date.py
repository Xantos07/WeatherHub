import os
import pandas as pd

def convert_date(sheet_name):
    if len(sheet_name) == 6 and sheet_name.isdigit():
        day = sheet_name[:2]
        month = sheet_name[2:4]
        year = '20' + sheet_name[4:]
        return f"{year}-{month}-{day}"
    return None

data_dir = "data"
for filename in os.listdir(data_dir):
    if filename.endswith(".xlsx") and ("Weather" in filename):
        file_path = os.path.join(data_dir, filename)
        xls = pd.ExcelFile(file_path)
        writer = pd.ExcelWriter(file_path.replace(".xlsx", "_with_date.xlsx"), engine="openpyxl")
        for sheet in xls.sheet_names:
            date_str = convert_date(sheet)
            df = pd.read_excel(xls, sheet_name=sheet)
            if date_str:
                df["date"] = date_str
            df.to_excel(writer, sheet_name=sheet, index=False)
        writer.close()
        print(f"{file_path.replace('.xlsx', '_with_date.xlsx')} créé avec la colonne date ajoutée à chaque feuille.")