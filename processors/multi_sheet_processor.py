"""
Обработчик для многолистовых Excel-файлов
"""

import pandas as pd
import os
import shutil
from main import Processor, group_similar
from src.morphology import normalize_term
from src.okpd_fetch import fetch_okpd2_batch
from .base_processor import BaseProcessor

class MultiSheetProcessor(BaseProcessor):
    """Процессор для Excel-файлов с несколькими листами"""
    
    def _process_file(self):
        """Обработка многолистового файла"""
        try:
            # Получение пути к входному файлу
            input_path = self.input_path
            
            # Чтение файла для получения имен листов
            excel_file = pd.ExcelFile(input_path)
            sheet_names = excel_file.sheet_names
            self.logger.info(f"Found {len(sheet_names)} sheets in the file")
            
            # Копирование входного файла в выходной
            shutil.copy2(input_path, self.output_path)
            
            # Инициализация индикатора прогресса для всех листов
            total_sheets = len(sheet_names)
            if self.progress is not None:
                self.progress(0, desc="Initializing...", total=total_sheets)
            
            # Обработка каждого листа
            for sheet_idx, sheet_name in enumerate(sheet_names):
                if self.stop_event.is_set():
                    self.logger.info("Processing stopped by user")
                    break
                
                try:
                    self.logger.info(f"Processing sheet {sheet_idx+1}/{total_sheets}: {sheet_name}")
                    if self.progress is not None:
                        self.progress(sheet_idx, desc=f"Processing sheet: {sheet_name}", total=total_sheets)
                    
                    # Чтение листа
                    df = pd.read_excel(input_path, sheet_name=sheet_name)
                    
                    # Поиск индексов колонок
                    item_col_idx, code_col_idx, doc_col_idx = self._find_columns(df)
                    
                    # Если нашли колонку наименования, продолжаем обработку
                    if item_col_idx is not None:
                        self.logger.info(f"Found columns - Item: {item_col_idx}, Code: {code_col_idx}, Doc: {doc_col_idx}")
                        
                        # Поиск фактических строк данных (после заголовков)
                        data_rows = self._find_data_rows(df, item_col_idx, doc_col_idx)
                        
                        # Обработка элементов
                        total_items = len(data_rows)
                        self.logger.info(f"Found {total_items} items to process in sheet {sheet_name}")
                        
                        # Группировка элементов для пакетной обработки
                        items_to_process = []
                        for row_idx in data_rows:
                            if pd.isna(df.iloc[row_idx, item_col_idx]):
                                continue
                            item_value = str(df.iloc[row_idx, item_col_idx]).strip()
                            if item_value and item_value != '-':
                                items_to_process.append((row_idx, item_value))
                        
                        # Обработка элементов пакетами
                        batch_size = min(10, len(items_to_process))
                        for batch_start in range(0, len(items_to_process), batch_size):
                            if self.stop_event.is_set():
                                self.logger.info("Processing stopped by user")
                                break
                                
                            batch = items_to_process[batch_start:batch_start+batch_size]
                            batch_items = [item[1] for item in batch]
                            batch_rows = [item[0] for item in batch]
                            
                            self.logger.info(f"Processing batch {batch_start//batch_size + 1} with {len(batch)} items")
                            
                            try:
                                # Нормализация терминов
                                normalized_terms = [normalize_term(item) for item in batch_items]
                                
                                # Получение упрощенных терминов
                                simplified_terms = []
                                for term in normalized_terms:
                                    prompt = [{"role": "user", "content": term}]
                                    resp = self.model.generate(prompt)
                                    simplified_terms.append(resp['content'])
                                
                                # Получение кодов ОКПД пакетом
                                okpd_data = fetch_okpd2_batch(simplified_terms)
                                
                                # Обработка каждого элемента в пакете
                                for i, (row_idx, item) in enumerate(batch):
                                    normalized = normalized_terms[i]
                                    simplified = simplified_terms[i]
                                    entries = okpd_data.get(simplified, [])
                                    
                                    # Получение кода
                                    try:
                                        # Использование _decide из Processor
                                        code, name, comment = Processor._decide(None, entries, item, simplified)
                                        self.logger.info(f"Item: {item}, Code: {code}")
                                        
                                        # Обновление листа Excel
                                        if code_col_idx is not None:
                                            df.iloc[row_idx, code_col_idx] = code
                                        
                                    except Exception as e:
                                        self.logger.exception(f"Error processing item {item}: {e}")
                                
                                # Сохранение чекпоинта после каждого пакета
                                with pd.ExcelWriter(self.output_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                                
                                # Также сохраняем файл чекпоинта
                                with pd.ExcelWriter(self.checkpoint_name, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                                    
                                self.logger.info(f"Saved checkpoint after batch {batch_start//batch_size + 1}")
                                
                            except Exception as e:
                                self.logger.exception(f"Error processing batch: {e}")
                        
                    else:
                        self.logger.warning(f"Could not find required columns in sheet {sheet_name}")
                    
                except Exception as e:
                    self.logger.exception(f"Error processing sheet {sheet_name}: {e}")
            
            self.logger.info(f"Processing completed. Saved to {self.output_path}")
            return True
            
        except Exception as e:
            self.logger.exception(f"Error in multi-sheet processing: {e}")
            return False
    
    def _find_columns(self, df):
        """Поиск индексов колонок в DataFrame"""
        item_col_idx = None
        code_col_idx = None
        doc_col_idx = None
        
        # Поиск в первых 10 строках
        for row_idx in range(min(10, df.shape[0])):
            for col_idx in range(df.shape[1]):
                if pd.isna(df.iloc[row_idx, col_idx]):
                    continue
                
                cell_value = str(df.iloc[row_idx, col_idx]).lower()
                
                # Проверка на идентификаторы колонок
                if '№' in cell_value and 'п/п' in cell_value:
                    pass  # Колонка с номерами, игнорируем
                elif 'наименование' in cell_value:
                    item_col_idx = col_idx
                elif 'код' in cell_value and ('окп' in cell_value or 'окпд' in cell_value):
                    code_col_idx = col_idx
                elif ('первич' in cell_value and 'докум' in cell_value) or 'договор' in cell_value:
                    doc_col_idx = col_idx
        
        return item_col_idx, code_col_idx, doc_col_idx
    
    def _find_data_rows(self, df, item_col_idx, doc_col_idx):
        """Поиск строк с данными для обработки"""
        data_rows = []
        for row_idx in range(df.shape[0]):
            if pd.isna(df.iloc[row_idx, item_col_idx]):
                continue
                
            item_value = str(df.iloc[row_idx, item_col_idx]).strip()
            
            # Пропускаем заголовки строк и строки c итогами
            if (item_value.lower() == 'наименование' or 
                item_value.startswith('ВСЕГО') or 
                item_value.startswith('Итого')):
                continue
            
            # Пропускаем строки с "Приложение" в колонке документов
            should_skip = False
            if doc_col_idx is not None and not pd.isna(df.iloc[row_idx, doc_col_idx]):
                doc_value = str(df.iloc[row_idx, doc_col_idx]).lower()
                if 'прил' in doc_value:
                    self.logger.info(f"Skipping row {row_idx} due to 'Приложения' in doc field")
                    should_skip = True
                    
            if not should_skip:
                data_rows.append(row_idx)
                
        return data_rows 