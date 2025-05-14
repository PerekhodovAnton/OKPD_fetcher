"""
Обработчик для файлов формата 4_1.xlsx - упрощенная версия
"""

import pandas as pd
import os
import re
import openpyxl
import shutil
import time
from copy import copy
from main import Processor, group_similar
from src.morphology import normalize_term
from src.okpd_fetch import fetch_okpd2_batch
from .base_processor import BaseProcessor

class Format41Processor(BaseProcessor):
    """
    Процессор для формата 4_1.
    
    Просто извлекает данные из колонки 'Наименование' и записывает коды в 'Код ОКП/ОКПД2',
    при этом пропускает служебные строки.
    """
    
    # Количество строк сверху, которые нужно пропустить (заголовки таблицы)
    # Определяется как атрибут класса, но может быть переопределен для экземпляра
    _DEFAULT_HEADER_ROWS = 5
    
    # Шаблоны для строк, которые нужно пропустить
    SKIP_PATTERNS = [
        r'ВСЕГО\s+по\s+разделу',
        r'ИТОГО\s+по\s+разделу',
        r'ВСЕГО\s+\d+',
        r'ИТОГО\s+\d+',
        r'Сырье\s+и\s+основные\s+материалы',
        r'Вспомогательные\s+материалы',
        r'Возвратные\s+отходы',
        r'Приобретение\s+комплектующих\s+изделий',
        r'Покупные\s+комплектующие\s+изделия',
        r'Возвратные\s+отходы\s+\(вычитаются\)'
    ]
    
    # Компилируем регулярные выражения для быстрой проверки
    SKIP_PATTERNS_COMPILED = [re.compile(pattern, re.IGNORECASE) for pattern in SKIP_PATTERNS]
    
    def __init__(self, input_file=None, checkpoint_name="checkpoint.xlsx", save_interval=10, progress=None):
        super().__init__(input_file, checkpoint_name, save_interval, progress)
        # Инициализируем напрямую без использования свойства
        self._num_header_rows = self._DEFAULT_HEADER_ROWS
        self.skipped_rows = 0
        self.skipped_service_rows = 0
        self.processed_items = 0
        
        # Создаем резервную копию входного файла сразу
        if input_file and input_file.name:
            self._create_backup_file()
            
        # Коды ОКПД для обновления в файле
        self.results_to_update = {}
        
        # Данные о файле
        self.workbook = None
        self.sheet_name = None
        self.name_column_index = None
        self.code_column_index = None
    
    # Свойство для доступа к атрибуту _num_header_rows экземпляра
    @property
    def NUM_HEADER_ROWS(self):
        return self._num_header_rows
        
    @NUM_HEADER_ROWS.setter
    def NUM_HEADER_ROWS(self, value):
        self._num_header_rows = int(value)
        self.logger.info(f"Установлено пропуск {self._num_header_rows} строк заголовка")
        
    def _create_backup_file(self):
        """Создает резервную копию исходного файла"""
        try:
            original_path = self.input_path
            if not original_path or not os.path.exists(original_path):
                return
                
            # Формируем имя для бэкапа
            base_name, ext = os.path.splitext(original_path)
            backup_path = f"{base_name}_original{ext}"
            
            # Создаем копию только если её ещё нет
            if not os.path.exists(backup_path):
                shutil.copy2(original_path, backup_path)
                self.logger.info(f"Создана резервная копия исходного файла: {backup_path}")
                return backup_path
        except Exception as e:
            self.logger.warning(f"Не удалось создать резервную копию: {e}")
        return None
        
    def _find_columns_in_excel(self):
        """
        Находит колонки с наименованием и кодами ОКПД в файле Excel
        непосредственно используя openpyxl
        """
        if not self.input_path or not os.path.exists(self.input_path):
            self.logger.error("Не указан путь к входному файлу")
            return False
            
        try:
            # Открываем Excel файл
            self.workbook = openpyxl.load_workbook(self.input_path)
            self.logger.info(f"Excel файл открыт: {self.input_path}")
            
            # Определяем активный лист (или первый, если активный не задан)
            if self.workbook.active:
                sheet = self.workbook.active
                self.sheet_name = sheet.title
            else:
                sheet = self.workbook.worksheets[0]
                self.sheet_name = sheet.title
            
            self.logger.info(f"Анализируем лист: {self.sheet_name}")
            
            # Ищем в первых нескольких строках заголовки колонок
            name_column = None
            code_column = None
            
            # Проверяем первые 15 строк (обычно заголовки там)
            max_check_row = min(15, sheet.max_row)
            
            for row in range(1, max_check_row + 1):
                for col in range(1, min(20, sheet.max_column + 1)):
                    cell_value = sheet.cell(row=row, column=col).value
                    
                    if not cell_value:
                        continue
                        
                    cell_text = str(cell_value).strip().lower()
                    
                    # Ищем колонку с наименованием
                    if cell_text == "наименование" or "наименов" in cell_text:
                        name_column = col
                        self.logger.info(f"Найдена колонка 'Наименование': строка {row}, колонка {col}")
                    
                    # Ищем колонку с кодом ОКПД
                    if "код" in cell_text and ("окп" in cell_text or "окпд" in cell_text):
                        code_column = col
                        self.logger.info(f"Найдена колонка 'Код ОКП/ОКПД2': строка {row}, колонка {col}")
                    
                    # Если нашли обе колонки, завершаем поиск
                    if name_column and code_column:
                        self.name_column_index = name_column
                        self.code_column_index = code_column
                        return True
            
            # Если не нашли колонку кода, но нашли наименование
            if name_column and not code_column:
                self.name_column_index = name_column
                # Ищем первую пустую колонку после наименования для кодов ОКПД
                for col in range(name_column + 1, min(name_column + 5, sheet.max_column + 1)):
                    is_empty = True
                    for row in range(self._num_header_rows + 1, min(self._num_header_rows + 10, sheet.max_row + 1)):
                        if sheet.cell(row=row, column=col).value:
                            is_empty = False
                            break
                    
                    if is_empty:
                        code_column = col
                        self.code_column_index = col
                        self.logger.info(f"Не найдена колонка для кодов ОКПД, будем использовать колонку {col}")
                        
                        # Добавляем заголовок в ячейку для кодов ОКПД
                        header_row = self._num_header_rows
                        for row in range(1, header_row + 1):
                            if sheet.cell(row=row, column=name_column).value:
                                sheet.cell(row=row, column=col).value = "Код ОКП/ОКПД2"
                                self.logger.info(f"Добавлен заголовок 'Код ОКП/ОКПД2' в строку {row}, колонку {col}")
                                break
                        return True
            
            # Не нашли нужные колонки
            self.logger.error("Не удалось найти колонки 'Наименование' и 'Код ОКП/ОКПД2'")
            return False
            
        except Exception as e:
            self.logger.exception(f"Ошибка при анализе Excel файла: {e}")
            return False
    
    def _process_file(self):
        """Обработка файла формата 4_1"""
        try:
            self.logger.info(f"Начало обработки файла формата 4_1: {self.input_path}")
            self.logger.info(f"Пропускаем первые {self._num_header_rows} строк заголовка")
            
            # Находим колонки в Excel файле
            if not self._find_columns_in_excel():
                return False
            
            # Теперь читаем данные с pandas для более удобной обработки
            self.df = pd.read_excel(self.input_path)
            self.logger.info(f"Файл прочитан для анализа, обнаружено {self.df.shape[0]} строк, {self.df.shape[1]} столбцов")
            
            # Определяем имена колонок
            if self.name_column_index is not None:
                # Преобразуем индекс колонки openpyxl (с 1) в индекс pandas (с 0)
                pandas_name_col = self.name_column_index - 1
                pandas_code_col = self.code_column_index - 1 if self.code_column_index else None
                
                # Получаем имя колонки, если заголовки есть
                if 'Наименование' in self.df.columns:
                    item_column_name = 'Наименование'
                elif pandas_name_col < len(self.df.columns):
                    item_column_name = self.df.columns[pandas_name_col]
                else:
                    # Используем индекс, если имя недоступно
                    item_column_name = pandas_name_col
                
                # Аналогично для колонки кода
                if 'Код ОКП/ОКПД2' in self.df.columns:
                    code_column_name = 'Код ОКП/ОКПД2'
                elif 'Код ОКПД2' in self.df.columns:
                    code_column_name = 'Код ОКПД2'
                elif pandas_code_col is not None and pandas_code_col < len(self.df.columns):
                    code_column_name = self.df.columns[pandas_code_col]
                else:
                    # Используем индекс, если имя недоступно
                    code_column_name = pandas_code_col
                
                self.logger.info(f"Используем колонки: '{item_column_name}' для наименований и '{code_column_name}' для кодов")
            else:
                # Если не нашли колонки через openpyxl, пробуем найти через pandas
                if 'Наименование' in self.df.columns:
                    item_column_name = 'Наименование'
                    self.logger.info("Найдена колонка 'Наименование'")
                else:
                    # Пробуем искать по содержимому
                    for col in self.df.columns:
                        col_values = self.df[col].astype(str).str.lower()
                        if col_values.str.contains('наименование').any():
                            item_column_name = col
                            self.logger.info(f"Найдена колонка с наименованиями: {col}")
                            break
                    else:
                        self.logger.error("Не удалось найти колонку с наименованиями")
                        return False
                
                # Ищем колонку с кодом ОКПД2
                if 'Код ОКП/ОКПД2' in self.df.columns:
                    code_column_name = 'Код ОКП/ОКПД2'
                    self.logger.info("Найдена колонка 'Код ОКП/ОКПД2'")
                elif 'Код ОКПД2' in self.df.columns:
                    code_column_name = 'Код ОКПД2'
                    self.logger.info("Найдена колонка 'Код ОКПД2'")
                else:
                    # Пробуем искать по содержимому
                    for col in self.df.columns:
                        col_name = str(col).lower()
                        if 'код' in col_name and ('окп' in col_name or 'окпд' in col_name):
                            code_column_name = col
                            self.logger.info(f"Найдена колонка с кодами ОКПД: {col}")
                            break
                    else:
                        # Создаем новую колонку
                        self.logger.info("Колонка с кодом не найдена, используем индекс колонки из openpyxl")
                        if self.code_column_index:
                            code_column_name = self.code_column_index - 1  # преобразуем в индекс pandas
                        else:
                            code_column_name = len(self.df.columns)  # крайний случай - новая колонка в конце
                            self.df[code_column_name] = ''
            
            self.logger.info(f"Определены колонки: {item_column_name} (наименования) и {code_column_name} (коды)")
            
            # Собираем все наименования, кроме служебных строк
            items_to_process = []
            
            # Счетчики для анализа пропущенных строк
            headers_skipped = 0
            empty_skipped = 0
            patterns_skipped = 0
            total_rows = 0
            
            # Проходим по всем строкам, пропуская указанное количество строк заголовка
            for idx, row in enumerate(self.df.iterrows()):
                idx, row = row  # Распаковываем кортеж (индекс, Series)
                total_rows += 1
                
                # Пропускаем первые N строк (заголовки таблицы)
                if idx < self._num_header_rows:
                    headers_skipped += 1
                    self.logger.debug(f"Пропускаем строку {idx} (часть заголовка)")
                    continue
                
                # Пропускаем пустые ячейки
                if pd.isna(row[item_column_name]):
                    empty_skipped += 1
                    continue
                    
                item_text = str(row[item_column_name]).strip()
                
                # Пропускаем пустые строки и одиночные символы
                if not item_text or len(item_text) <= 1:
                    empty_skipped += 1
                    continue
                    
                # Пропускаем, если это число или короткое число
                if item_text.isdigit() and len(item_text) <= 3:
                    empty_skipped += 1
                    continue
                
                # Проверяем по шаблонам служебных строк
                skip_item = False
                for pattern in self.SKIP_PATTERNS_COMPILED:
                    if pattern.search(item_text):
                        skip_item = True
                        break
                        
                if skip_item:
                    patterns_skipped += 1
                    self.logger.info(f"Пропускаем служебную строку [{idx}]: '{item_text}'")
                    continue
                    
                # Добавляем элемент для обработки
                items_to_process.append((idx, item_text))
            
            # Обновляем счетчики для логирования
            self.skipped_rows = headers_skipped + empty_skipped
            self.skipped_service_rows = patterns_skipped
            
            self.logger.info(f"Всего строк в файле: {total_rows}")
            self.logger.info(f"Пропущено строк заголовка: {headers_skipped}")
            self.logger.info(f"Пропущено пустых строк: {empty_skipped}")
            self.logger.info(f"Пропущено служебных строк: {patterns_skipped}")
                
            if not items_to_process:
                self.logger.error("Не найдено элементов для обработки")
                return False
                
            self.logger.info(f"Найдено {len(items_to_process)} элементов для обработки")
            
            # Инициализируем модель перед группировкой для экономии времени
            if not self.init_model():
                self.logger.error("Ошибка инициализации модели")
                return False
                
            # Группируем похожие элементы
            self.logger.info("Группируем похожие элементы...")
            item_texts = [item[1] for item in items_to_process]
            groups = group_similar(item_texts)
            
            # Создаем отображение текста на индекс строки
            item_to_idx = {items_to_process[i][1]: items_to_process[i][0] for i in range(len(items_to_process))}
            
            # Обработка каждой группы
            total_groups = len(groups)
            self.logger.info(f"Сгруппировано в {total_groups} групп")
            
            # Безопасная работа с прогрессом
            if self.progress is not None:
                self.progress(0, desc="Обработка наименований...", total=total_groups)
                progress_iter = self.progress.tqdm(groups, desc="Обработка групп")
            else:
                progress_iter = groups
                
            processed_groups = 0
            processed_items = 0
            success_items = 0
            error_items = 0
            
            # Очищаем словарь результатов перед обработкой
            self.results_to_update = {}
            
            for idx, group in enumerate(progress_iter, start=1):
                if self.stop_event.is_set():
                    self.logger.info("Обработка остановлена пользователем")
                    break
                    
                if not group:
                    continue
                    
                processed_groups += 1
                
                # Берем представителя группы
                rep = group[0]
                
                try:
                    # Нормализуем термин
                    normalized = normalize_term(rep)
                    self.logger.info(f"Обработка группы {idx}/{total_groups}: {normalized}")
                    
                    # Получаем упрощенный термин
                    prompt = [{"role": "user", "content": normalized}]
                    simplified = self.model.generate(prompt)['content']
                    self.logger.info(f"Упрощено до: {simplified}")
                    
                    # Запрашиваем коды ОКПД
                    okpd_data = fetch_okpd2_batch([simplified])
                    entries = okpd_data.get(simplified, [])
                    
                    # Выбираем подходящий код
                    code, name, comment = Processor._decide(None, entries, rep, simplified)
                    self.logger.info(f"Выбран код: {code} - {name}")
                    
                    # Сохраняем коды для каждого элемента в группе
                    for item in group:
                        processed_items += 1
                        if item in item_to_idx:
                            row_idx = item_to_idx[item]
                            # Сохраняем результат в словаре для последующего обновления Excel
                            self.results_to_update[row_idx] = {
                                'code': code,
                                'column_name': code_column_name,
                                'item': item
                            }
                            success_items += 1
                            
                    # Сохраняем промежуточный результат
                    if idx % int(self.save_interval) == 0:
                        self._update_excel_with_codes()
                        self.logger.info(f"Сохранен промежуточный результат после {idx}/{total_groups} групп ({processed_items} элементов)")
                        
                except Exception as e:
                    self.logger.exception(f"Ошибка при обработке группы {idx}: {e}")
                    error_items += len(group)
            
            # Обновляем счетчики для финального отчета
            self.processed_items = processed_items
            
            # Обновляем Excel-файл с кодами ОКПД
            result = self._update_excel_with_codes()
            
            if result:
                self.logger.info(f"Обработка завершена, результаты сохранены в исходный файл с сохранением форматирования")
                self.logger.info(f"Итого: обработано {processed_groups}/{total_groups} групп, {success_items} элементов успешно, {error_items} с ошибками")
                
                # Копируем исходный файл с обновлениями в output_path для интерфейса
                try:
                    shutil.copy2(self.input_path, self.output_path)
                    self.logger.info(f"Копия результата сохранена в {self.output_path}")
                except Exception as e:
                    self.logger.warning(f"Не удалось создать копию результата: {e}")
            else:
                self.logger.error("Ошибка при сохранении результатов в исходный файл")
                
            return result
            
        except Exception as e:
            self.logger.exception(f"Ошибка в Format41Processor: {e}")
            return False
    
    def _update_excel_with_codes(self):
        """
        Обновляет коды ОКПД в исходном Excel-файле, сохраняя форматирование
        
        Returns:
            bool: True если успешно, False в случае ошибки
        """
        if not self.results_to_update:
            self.logger.warning("Нет данных для обновления Excel файла")
            return False
            
        if not self.input_path or not os.path.exists(self.input_path):
            self.logger.error("Не найден исходный Excel файл")
            return False
            
        try:
            # Если есть открытый workbook, используем его
            if not self.workbook:
                self.workbook = openpyxl.load_workbook(self.input_path)
                
            # Используем активный лист или первый лист
            if self.sheet_name:
                if self.sheet_name in self.workbook.sheetnames:
                    sheet = self.workbook[self.sheet_name]
                else:
                    sheet = self.workbook.active
            else:
                sheet = self.workbook.active
                
            self.logger.info(f"Обновление данных в листе '{sheet.title}'")
            
            # Счетчик обновлений
            updated = 0
            
            for row_idx, data in self.results_to_update.items():
                code = data['code']
                column_name = data['column_name']
                
                # Определяем номер колонки для кода ОКПД
                if isinstance(column_name, int):
                    # Если индекс pandas (с 0), преобразуем в индекс openpyxl (с 1)
                    col_idx = column_name + 1
                else:
                    # Если это название колонки, найдем соответствующий индекс
                    col_idx = self.code_column_index
                
                if not col_idx:
                    self.logger.warning(f"Не удалось определить колонку для кода ОКПД")
                    continue
                
                # Номер строки в Excel (строка в pandas + header_rows)
                excel_row = int(row_idx) + 1
                
                # Записываем код в ячейку
                try:
                    cell = sheet.cell(row=excel_row, column=col_idx)
                    old_value = cell.value
                    cell.value = code
                    updated += 1
                    self.logger.debug(f"Обновлена ячейка ({excel_row}, {col_idx}): '{old_value}' -> '{code}'")
                except Exception as e:
                    self.logger.warning(f"Ошибка при обновлении ячейки ({excel_row}, {col_idx}): {e}")
            
            # Сохраняем изменения
            self.workbook.save(self.input_path)
            self.logger.info(f"Файл Excel обновлен: {updated} кодов ОКПД добавлено")
            
            return True
            
        except Exception as e:
            self.logger.exception(f"Ошибка при обновлении Excel файла: {e}")
            return False 