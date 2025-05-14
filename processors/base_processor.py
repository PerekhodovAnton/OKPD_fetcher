"""
Базовый класс процессора, определяющий общий интерфейс для всех обработчиков файлов
"""

import os
import tempfile
import pandas as pd
import threading
import logging
import time
from abc import ABC, abstractmethod
import gradio as gr
from src.model import Model
from logger import setup_logger

class BaseProcessor(ABC):
    """Базовый абстрактный класс для всех процессоров файлов"""
    
    def __init__(self, input_file=None, checkpoint_name="checkpoint.xlsx", save_interval=10, progress=None):
        self.input_file = input_file
        self.input_path = input_file.name if input_file else None
        self.checkpoint_name = checkpoint_name
        self.save_interval = int(save_interval)
        self.progress = progress  # Сохраняем ссылку на объект прогресса
        self.logger = setup_logger(self.__class__.__name__)
        self.stop_event = threading.Event()
        
        # Создаем временный файл для результата
        self.temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        self.temp_output.close()
        self.output_path = self.temp_output.name
        
        self.model = None
        self.df = None
        
        # Добавляем метки времени для отслеживания прогресса
        self.start_time = None
        self.end_time = None
        
    def init_model(self):
        """Инициализация модели"""
        try:
            self.logger.info("Инициализация модели...")
            start_time = time.time()
            self.model = Model()
            elapsed = time.time() - start_time
            self.logger.info(f"Модель инициализирована успешно за {elapsed:.1f} сек.")
            return True
        except Exception as e:
            self.logger.exception(f"Ошибка инициализации модели: {e}")
            return False
    
    def cancel(self):
        """Отменить обработку"""
        self.stop_event.set()
        self.logger.info("Обработка остановлена пользователем")
        return "Обработка остановлена пользователем"
    
    def save_checkpoint(self, idx=None, total=None, sheet_name='Sheet1'):
        """Сохранить чекпоинт"""
        try:
            if hasattr(self, 'df') and self.df is not None:
                self.logger.info(f"Сохранение промежуточного результата в {self.checkpoint_name}...")
                
                # Проверяем, существует ли файл чекпоинта
                file_exists = os.path.exists(self.checkpoint_name)
                
                try:
                    if file_exists:
                        # Если файл существует, записываем в него
                        with pd.ExcelWriter(self.checkpoint_name, engine='openpyxl', mode='a', 
                                        if_sheet_exists='replace') as writer:
                            self.df.to_excel(writer, sheet_name=sheet_name, index=False)
                    else:
                        # Если файл не существует, создаем новый
                        with pd.ExcelWriter(self.checkpoint_name, engine='openpyxl', mode='w') as writer:
                            self.df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    if idx and total:
                        self.logger.info(f"Сохранен промежуточный результат {idx}/{total}")
                    else:
                        self.logger.info(f"Сохранен промежуточный результат")
                    return True
                except Exception as e:
                    self.logger.error(f"Ошибка при записи в файл {self.checkpoint_name}: {e}")
                    
                    # Пробуем сохранить с другим именем
                    try:
                        backup_name = f"{self.checkpoint_name}.backup.xlsx"
                        self.logger.info(f"Пробуем сохранить с другим именем: {backup_name}")
                        self.df.to_excel(backup_name, index=False)
                        self.logger.info(f"Резервная копия сохранена в {backup_name}")
                        return True
                    except Exception as e2:
                        self.logger.error(f"Не удалось создать резервную копию: {e2}")
                        return False
        except Exception as e:
            self.logger.exception(f"Ошибка сохранения промежуточного результата: {e}")
        return False
    
    def process(self):
        """
        Обработка файла. Основной метод, который должен быть реализован всеми подклассами.
        
        Returns:
            generator: Генератор, возвращающий путь к результирующему файлу
        """
        self.stop_event.clear()
        self.start_time = time.time()
        
        if not self.input_file:
            self.logger.error("Не указан входной файл")
            yield None
            return
            
        self.logger.info(f"Начало обработки файла: {self.input_path}")
        
        try:
            # Инициализация модели
            if not self.init_model():
                self.logger.error("Не удалось инициализировать модель для обработки")
                if os.path.exists(self.checkpoint_name):
                    self.logger.info(f"Возвращаем последний промежуточный результат из-за ошибки: {self.checkpoint_name}")
                    yield self.checkpoint_name
                else:
                    self.logger.error("Промежуточный файл не найден")
                    yield None
                return
                
            # Выполнение обработки
            if self._process_file():
                self.end_time = time.time()
                elapsed = self.end_time - self.start_time
                self.logger.info(f"Обработка завершена за {elapsed:.1f} сек. Результат сохранен в {self.output_path}")
                yield self.output_path
            else:
                self.end_time = time.time()
                elapsed = self.end_time - self.start_time
                self.logger.error(f"Обработка завершилась с ошибками за {elapsed:.1f} сек.")
                if os.path.exists(self.checkpoint_name):
                    self.logger.info(f"Возвращаем последний промежуточный результат: {self.checkpoint_name}")
                    yield self.checkpoint_name
                else:
                    self.logger.error("Промежуточный файл не найден")
                    yield None
                
        except Exception as e:
            self.end_time = time.time()
            elapsed = self.end_time - self.start_time
            self.logger.exception(f"Критическая ошибка при обработке за {elapsed:.1f} сек: {e}")
            if os.path.exists(self.checkpoint_name):
                self.logger.info(f"Возвращаем последний промежуточный результат из-за ошибки: {self.checkpoint_name}")
                yield self.checkpoint_name
            else:
                self.logger.error("Промежуточный файл не найден")
                yield None
    
    @abstractmethod
    def _process_file(self):
        """
        Реализация обработки конкретного формата файла
        
        Returns:
            bool: True если обработка успешна, False в противном случае
        """
        pass 