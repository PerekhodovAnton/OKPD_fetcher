import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

def find_columns_in_df(df):
    """Scan through the DataFrame to find columns matching our criteria"""
    # Check all cells for 'Наименование', 'Код ОКП/ОКПД2', etc.
    item_col = None
    code_col = None
    doc_col = None
    num_col = None
    
    # Look in the first 10 rows for header-like content
    for row_idx in range(min(10, df.shape[0])):
        for col_idx in range(df.shape[1]):
            cell_value = str(df.iloc[row_idx, col_idx]).lower() if not pd.isna(df.iloc[row_idx, col_idx]) else ""
            
            # Check for relevant column headers
            if '№' in cell_value and 'п/п' in cell_value:
                num_col = col_idx
            elif 'наименование' in cell_value:
                item_col = col_idx
            elif ('код' in cell_value) and ('окп' in cell_value or 'окпд' in cell_value):
                code_col = col_idx
            elif 'первич' in cell_value and ('докум' in cell_value or 'договор' in cell_value):
                doc_col = col_idx
                
    return num_col, item_col, code_col, doc_col

def extract_table_data(df, num_col, item_col, code_col, doc_col, start_row):
    """Extract actual data rows from the DataFrame"""
    # Skip to start_row
    df = df.iloc[start_row:].reset_index(drop=True)
    
    # Extract each column by index
    data = []
    
    for idx in range(len(df)):
        row = df.iloc[idx]
        
        # Get values using indices
        num_value = row.iloc[num_col] if num_col is not None and num_col < len(row) else None
        item_value = row.iloc[item_col] if item_col is not None and item_col < len(row) else None
        code_value = row.iloc[code_col] if code_col is not None and code_col < len(row) else None
        doc_value = row.iloc[doc_col] if doc_col is not None and doc_col < len(row) else None
        
        # Only include rows with an item name
        if not pd.isna(item_value) and str(item_value).strip() and not str(item_value).startswith(('ВСЕГО', 'Итого')):
            data.append({
                'num': num_value,
                'item': item_value,
                'code': code_value,
                'doc': doc_value
            })
    
    return data

def print_sheet_structure(sheet_name):
    print(f"\n=== Sheet: {sheet_name} ===")
    
    # Load the full sheet
    df = pd.read_excel('4_1.xlsx', sheet_name=sheet_name)
    
    # Find our target columns
    num_col, item_col, code_col, doc_col = find_columns_in_df(df)
    
    if item_col is not None:
        print(f"Found columns:")
        print(f"№ п/п column index: {num_col}")
        print(f"Наименование column index: {item_col}")
        print(f"Код ОКП/ОКПД2 column index: {code_col}")
        print(f"Первичные документы column index: {doc_col}")
        
        # Find the row where real data starts (after headers)
        start_row = 0
        for i in range(df.shape[0]):
            if not pd.isna(df.iloc[i, item_col]) and str(df.iloc[i, item_col]).strip().lower() != 'наименование':
                if not str(df.iloc[i, item_col]).startswith(('ВСЕГО', 'Итого')):
                    start_row = i
                    break
        
        print(f"Data starts at row {start_row}")
        
        # Extract table data
        data = extract_table_data(df, num_col, item_col, code_col, doc_col, start_row)
        
        print(f"\nFound {len(data)} items with names. Sample data (first 5 rows):")
        for i, row in enumerate(data[:5]):
            print(f"\nItem {i+1}:")
            print(f"  № п/п: {row['num']}")
            print(f"  Наименование: {row['item']}")
            print(f"  Код ОКП/ОКПД2: {row['code']}")
            print(f"  Первичные документы: {row['doc']}")
            
            # Check for "Приложения" in documents field
            if row['doc'] is not None and isinstance(row['doc'], str) and 'приложени' in row['doc'].lower():
                print("  ** This item would be SKIPPED due to 'Приложения' in documents field **")
        
        return True
    else:
        print(f"Could not find required columns in sheet {sheet_name}")
        return False

# Get all sheet names
excel_file = pd.ExcelFile('4_1.xlsx')
sheet_names = excel_file.sheet_names

print(f"Found {len(sheet_names)} sheets: {sheet_names}")

# Process first few sheets
for sheet_name in sheet_names[:5]:
    print_sheet_structure(sheet_name) 