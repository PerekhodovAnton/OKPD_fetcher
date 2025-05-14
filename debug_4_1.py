import pandas as pd
import openpyxl
import os
import logging
from processors.format_4_1_processor import Format41Processor

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('debug_test')

# Test file
test_file = '4_1.xlsx'

# Create mock file object (processors expect a file-like object with a name attribute)
class MockFile:
    def __init__(self, name):
        self.name = name

if os.path.exists(test_file):
    logger.info(f"Testing Format41Processor with file: {test_file}")
    
    # Create a processor instance
    processor = Format41Processor(MockFile(test_file), checkpoint_name="debug_checkpoint.xlsx")
    
    # Set the number of header rows (default is 5)
    processor.NUM_HEADER_ROWS = 5
    
    # Test column detection
    success = processor._find_columns_in_excel()
    logger.info(f"Column detection success: {success}")
    if success:
        logger.info(f"Name column: {processor.name_column_index}, Code column: {processor.code_column_index}")
    
    # Test the file reading
    logger.info("Reading with pandas for comparison:")
    df = pd.read_excel(test_file)
    logger.info(f"pandas read: {df.shape[0]} rows, {df.shape[1]} columns")
    logger.info(f"First 5 column names: {list(df.columns)[:5]}")
    
    # First few rows
    logger.info("\nFirst 5 rows data from pandas:")
    for idx, row in df.iloc[:5].iterrows():
        logger.info(f"Row {idx}: {dict(row)[:3]}")  # Show just first 3 items
        
    # Test the processing (comment out if you don't want to run full processing)
    logger.info("\nProcessing file...")
    result = processor._process_file()
    logger.info(f"Processing result: {result}")
    
    if hasattr(processor, 'results_to_update') and processor.results_to_update:
        logger.info(f"Found {len(processor.results_to_update)} items to update with OKPD codes")
    
else:
    logger.error(f"Test file {test_file} not found!")

# Read the file without header
df = pd.read_excel('4_1.xlsx', header=None)
print(f"File shape: {df.shape}")

# Look for document column by finding cells containing "прил"
print("\nSearching for cells containing 'прил'...")
for row in range(df.shape[0]):
    for col in range(df.shape[1]):
        if not pd.isna(df.iloc[row, col]) and 'прил' in str(df.iloc[row, col]).lower():
            print(f"Found 'прил' at row {row}, col {col}: {df.iloc[row, col]}")
            
            # Print the items in the same row
            print(f"Items in the same row:")
            for c in range(min(5, df.shape[1])):
                if not pd.isna(df.iloc[row, c]):
                    print(f"  Col {c}: {df.iloc[row, c]}")

# Look specifically at rows 8-15 to see what columns might contain document info
print("\nExamining rows 8-15:")
for row in range(8, 15):
    print(f"\nRow {row}:")
    for col in range(min(20, df.shape[1])):
        if not pd.isna(df.iloc[row, col]):
            print(f"  Col {col}: {df.iloc[row, col]}")

# Check the headers around row 2-5
print("\nExamining potential header rows 2-5:")
for row in range(2, 6):
    print(f"\nRow {row}:")
    for col in range(min(20, df.shape[1])):
        if not pd.isna(df.iloc[row, col]):
            print(f"  Col {col}: {df.iloc[row, col]}") 