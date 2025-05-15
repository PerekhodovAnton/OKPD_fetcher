import gradio as gr
import pandas as pd
import logging
import os
from logger import setup_logger
from gradio_log import Log
from processors.standard_processor import StandardProcessor
from processors.format_4_1_processor import Format41Processor

# Настройка логирования
logger = setup_logger("app")

def process_standard_file(
    input_file,
    checkpoint_name="checkpoint.xlsx",
    save_interval=10,
    progress=None
):
    """
    Обработка стандартного файла Excel с колонкой 'Наименование'
    """
    logger.info(f"Starting process with standard file: {input_file.name}")
    
    # Обновляем статус
    yield None, "Запуск обработки..."
    
    # Создание стандартного процессора
    processor = StandardProcessor(input_file, checkpoint_name, save_interval, progress)
    
    # Выполнение обработки и передача результата
    for result in processor.process():
        if result is None:
            yield None, "Ошибка обработки. Проверьте лог для подробностей."
        else:
            yield result, "Обработка завершена успешно."

def process_format_41_file(
    input_file,
    checkpoint_name="checkpoint.xlsx",
    save_interval=10,
    header_rows=5,
    progress=None
):
    """
    Обработка файла Excel в формате 4_1
    """
    logger.info(f"Starting process with Format 4_1 file: {input_file.name}")
    
    # Обновляем статус
    yield None, f"Запуск обработки файла формата 4_1. Пропускаем {header_rows} строк заголовка..."
    
    # Создание процессора для формата 4_1
    processor = Format41Processor(input_file, checkpoint_name, save_interval, progress)
    
    # Устанавливаем количество строк заголовка для пропуска
    processor.NUM_HEADER_ROWS = int(header_rows)
    logger.info(f"Set to skip {header_rows} header rows")
    
    # Выполнение обработки и передача результата с обновлением статуса
    step = 0
    for result in processor.process():
        step += 1
        if result is None:
            if step == 1:
                yield None, "Ошибка при инициализации обработки. Проверьте формат файла."
            else:
                yield None, "Ошибка во время обработки. Проверьте лог для подробностей."
        else:
            yield result, "Обработка завершена успешно."

def cancel_process():
    """
    Отмена обработки (общая функция для всех процессоров)
    """
    # В новой архитектуре каждый процессор имеет свой флаг остановки,
    # но мы можем использовать глобальную переменную, чтобы отменить все запущенные процессы
    from processors.base_processor import BaseProcessor
    
    logger = logging.getLogger("app")
    logger.info("Stop requested by user")
    return "Обработка остановлена пользователем"

# Стили и цвета для интерфейса
# primary_color = "#4CAF50"
# secondary_color = "#2196F3"
header_style = "font-size: 28px; font-weight: 600; margin-bottom: 10px"
subheader_style = "font-size: 18px; font-weight: 500; margin-bottom: 5px"
description_style = "font-size: 14px; margin-bottom: 20px"

with gr.Blocks(theme=gr.themes.Soft(primary_hue=gr.themes.colors.green)) as demo:
    gr.Markdown(f"<h1 style='{header_style}'>ОКПД2 Обработчик Файлов</h1>")
    gr.Markdown(f"<p style='{description_style}'>Инструмент для автоматического присвоения кодов ОКПД2 товарам и услугам из Excel файлов.</p>")
    
    # Добавляем отображение лога для удобства отладки
    with gr.Accordion("Журнал событий (лог)", open=False):
        Log('processor.log', dark=True, xterm_font_size=14)

    with gr.Tab("Стандартный формат"):
        gr.Markdown(f"<h2 style='{subheader_style}'>Обработка файла с колонкой 'Наименование'</h2>")
        gr.Markdown(f"<p style='{description_style}'>Этот режим подходит для Excel файлов, в которых есть колонка с именем 'Наименование', содержащая список товаров/услуг для классификации.</p>")
        
        with gr.Row():
            with gr.Column(scale=3):
                std_file_input = gr.File(
                    label="Загрузить Excel файл (.xlsx)", 
                    file_types=[".xlsx"],
                    interactive=True
                )
                
            with gr.Column(scale=2):
                std_checkpoint_name = gr.Textbox(
                    label="Имя файла для сохранения промежуточных результатов", 
                    value="checkpoint_std.xlsx",
                    info="При длительной обработке файла, промежуточные результаты будут сохраняться в этот файл"
                )
                std_save_interval = gr.Slider(
                    label="Интервал сохранения (групп)", 
                    minimum=1, 
                    maximum=50, 
                    value=10, 
                    step=1,
                    info="Как часто сохранять промежуточные результаты"
                )
        
        with gr.Row():
            std_run_btn = gr.Button("Запустить обработку", variant="primary", scale=2)
            std_stop_btn = gr.Button("Остановить", variant="stop", scale=1)
            
        std_status = gr.Textbox(
            label="Статус", 
            value="Готов к работе",
            interactive=False
        )
        
        std_result_file = gr.File(
            label="Скачать результат",
            visible=True,
            elem_id="std_result"
        )

        # Запуск обработки
        std_run_event = std_run_btn.click(
            fn=process_standard_file,
            inputs=[std_file_input, std_checkpoint_name, std_save_interval],
            outputs=[std_result_file, std_status],
            queue=True
        )
        
        # Отмена обработки
        std_stop_btn.click(
            fn=cancel_process,
            inputs=[],
            outputs=std_status
        )

    with gr.Tab("Формат 4_1"):
        gr.Markdown(f"<h2 style='{subheader_style}'>Обработка файла формата 4_1</h2>")
        gr.Markdown(f"<p style='{description_style}'>Этот режим предназначен для обработки файлов формата 4_1, содержащих спецификации с колонкой 'Наименование' и пропуском служебных строк (заголовки разделов, итоги, отходы и т.д.)</p>")
        
        with gr.Row():
            with gr.Column(scale=3):
                f41_file_input = gr.File(
                    label="Загрузить Excel файл формата 4_1 (.xlsx)", 
                    file_types=[".xlsx"],
                    interactive=True
                )
                
            with gr.Column(scale=2):
                f41_checkpoint_name = gr.Textbox(
                    label="Имя файла для сохранения промежуточных результатов", 
                    value="checkpoint_41.xlsx",
                    info="При длительной обработке файла, промежуточные результаты будут сохраняться в этот файл"
                )
                f41_save_interval = gr.Slider(
                    label="Интервал сохранения (групп)", 
                    minimum=1, 
                    maximum=50, 
                    value=10, 
                    step=1,
                    info="Как часто сохранять промежуточные результаты"
                )
                f41_header_rows = gr.Slider(
                    label="Количество строк заголовка", 
                    minimum=0, 
                    maximum=20, 
                    value=5, 
                    step=1,
                    info="Пропустить указанное количество начальных строк файла (название таблицы, шапка и т.д.)"
                )
        
        with gr.Row():
            f41_run_btn = gr.Button("Запустить обработку", variant="primary", scale=2)
            f41_stop_btn = gr.Button("Остановить", variant="stop", scale=1)
        
        f41_status = gr.Textbox(
            label="Статус", 
            value="Готов к работе", 
            interactive=False
        )
        
        f41_result_file = gr.File(
            label="Скачать результат",
            visible=True,
            elem_id="f41_result"
        )

        # Запуск обработки
        f41_run_event = f41_run_btn.click(
            fn=process_format_41_file,
            inputs=[f41_file_input, f41_checkpoint_name, f41_save_interval, f41_header_rows],
            outputs=[f41_result_file, f41_status],
            queue=True
        )
        
        # Отмена обработки
        f41_stop_btn.click(
            fn=cancel_process,
            inputs=[],
            outputs=f41_status
        )

    # Информация о программе
    with gr.Accordion("О программе", open=False):
        gr.Markdown("""
        ### ОКПД2 Обработчик Файлов
        
        Программа для автоматического присвоения кодов ОКПД2 товарам и услугам из Excel файлов.
        
        **Возможности:**
        - Обработка файлов Excel со стандартной структурой (колонка 'Наименование')
        - Обработка файлов формата 4_1 с автоматическим определением колонок и пропуском служебных строк
        - Поддержка многолистовых Excel-файлов
        - Автоматический поиск и присвоение кодов ОКПД2
        - Сохранение промежуточных результатов в процессе работы
        - Пропуск заголовков таблиц (настраиваемое количество строк)
        
        **Инструкция:**
        1. Выберите подходящий формат файла на соответствующей вкладке
        2. Загрузите Excel-файл
        3. При необходимости измените имя файла для сохранения промежуточных результатов
        4. Для формата 4_1 укажите количество строк заголовка для пропуска
        5. Нажмите кнопку "Запустить обработку"
        6. Дождитесь завершения процесса и скачайте результат
        
        **При возникновении ошибок:**
        - Проверьте, что выбран правильный формат файла (стандартный или 4_1)
        - Для стандартного формата - в файле должна быть колонка 'Наименование'
        - Для формата 4_1 - настройте количество пропускаемых строк заголовка
        - Следите за обновлениями статуса обработки, там будет отображаться текущий этап
        """)

    demo.queue()  # Включаем очередь для фоновой обработки

if __name__ == "__main__":
    demo.launch()