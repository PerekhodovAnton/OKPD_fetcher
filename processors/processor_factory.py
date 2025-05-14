"""
Фабрика для создания процессоров различных форматов
"""

import os
import pandas as pd
from logger import setup_logger
from .standard_processor import StandardProcessor
from .format_4_1_processor import Format41Processor
from .multi_sheet_processor import MultiSheetProcessor

logger = setup_logger('processor_factory')

def create_processor(input_file, checkpoint_name="checkpoint.xlsx", save_interval=10, progress=None):
    """
    Фабричный метод для создания подходящего процессора на основе типа файла
    
    Args:
        input_file: Объект файла
        checkpoint_name: Имя файла чекпоинта
        save_interval: Интервал сохранения чекпоинтов
        progress: Объект прогресса Gradio
        
    Returns:
        Подходящий процессор для данного файла
    """
    # Проверка по имени файла
    if "4_1" in os.path.basename(input_file.name):
        logger.info(f"Creating Format41Processor based on filename")
        return Format41Processor(input_file, checkpoint_name, save_interval, progress)
    
    # Проверка наличия нескольких листов
    try:
        excel_file = pd.ExcelFile(input_file.name)
        sheet_names = excel_file.sheet_names
        if len(sheet_names) > 1:
            logger.info(f"Creating MultiSheetProcessor (found {len(sheet_names)} sheets)")
            return MultiSheetProcessor(input_file, checkpoint_name, save_interval, progress)
    except Exception as e:
        logger.warning(f"Error checking for multiple sheets: {e}")
    
    # Проверка структуры файла
    try:
        # Чтение первых строк файла для определения структуры
        df_test = pd.read_excel(input_file.name, nrows=10)
        
        # Если стандартных колонок нет, это может быть формат 4_1
        if 'Наименование' not in df_test.columns:
            # Пробуем проверить, похоже ли на формат 4_1 (ищем специфические паттерны)
            df_test_noheader = pd.read_excel(input_file.name, header=None, nrows=10)
            
            # Проверяем, присутствуют ли типичные для 4_1 колонки/контент
            has_section_header = False
            has_complex_structure = False
            
            # Ищем заголовки разделов типа "Сырье и основные материалы:"
            for idx, row in df_test_noheader.iterrows():
                for col in range(df_test_noheader.shape[1]):
                    if not pd.isna(row[col]) and isinstance(row[col], str):
                        if ('материалы' in str(row[col]).lower() and ':' in str(row[col])) or 'наименование' in str(row[col]).lower():
                            has_section_header = True
                            break
            
            # Проверяем, большинство колонок не имеют имен (типично для формата 4_1)
            unnamed_cols = sum(1 for col in df_test.columns if 'Unnamed:' in str(col))
            if unnamed_cols > df_test.shape[1] / 2:  # Более половины колонок без имен
                has_complex_structure = True
                
            if has_section_header or has_complex_structure:
                logger.info(f"Creating Format41Processor based on file structure")
                return Format41Processor(input_file, checkpoint_name, save_interval, progress)
                
        # По умолчанию используем стандартный процессор
        logger.info(f"Creating StandardProcessor")
        return StandardProcessor(input_file, checkpoint_name, save_interval, progress)
        
    except Exception as e:
        # При ошибке используем стандартный процессор
        logger.warning(f"Error detecting file format: {e}. Using StandardProcessor")
        return StandardProcessor(input_file, checkpoint_name, save_interval, progress) 