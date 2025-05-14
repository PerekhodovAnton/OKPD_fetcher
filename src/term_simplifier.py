from typing import Dict, Any, Callable
import threading
import _thread
import time

def run_with_timeout(func: Callable, args=(), kwargs={}, timeout=30, default=None):
    """Run a function with a timeout"""
    result = [default]
    def worker():
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            result[0] = default
    
    t = threading.Thread(target=worker)
    t.daemon = True
    t.start()
    t.join(timeout)
    if t.is_alive():
        _thread.interrupt_main()  # Interrupt the main thread
        raise TimeoutError(f"Function {func.__name__} timed out after {timeout} seconds")
    return result[0]

class TermSimplifier:
    def __init__(self, model):
        self.model = model

    def simplify(self, term: str) -> str:
        prompt = [
        {"role": "system", "content": 'Ты помогаешь выбрать один код для военной компании, которая занимается производством и работает с различным металом, где производят Системы термостатирования и контроля температурно влажностного режима.'},
        {"role": "user", "content": f"Перефразируй название товара, удалив все размеры и числовые параметры, преобразовав тип товара.\nЕсли встречаешь металические изделия, то прибавляй алюминевый. \n \
         Если слово 'лист' -> 'профиль алюминевый', если слово 'круг' -> 'профиль алюминевый, если слово 'болт' или 'винт -> 'болты и винты', если слово 'гвоздь' -> 'гвоздь', если слово 'доска' или 'брусок' -> 'пиломатериалы', если слово 'жгут' -> 'жгуты синтетические', если слово 'бензин' -> 'бензин', если слово 'бензин' -> 'бензин'.\n \
         Если встречаешь слово на английском языке - ничего не меняй. Напрмиер: если слово 'Isolontape 500 3005 VB D LM' -> 'Isolontape' \
         Если встречаешь слово которого нет в примерах, ориентируйся и сделай на подобии. \
         \nНазвание: {term}\nВыведи только товар:"}
        ]
        
        try:
            response = run_with_timeout(
                self.model.generate,
                kwargs={"content": prompt},
                timeout=90
            )
            return response.get('content', '').strip().lower()
        except Exception:
            res = run_with_timeout(
                self.model.generate,
                args=(),
                kwargs={
                    "content": prompt,
                    "temperature": 0.3,
                    "top_p": 0.9,
                    "repeat_penalty": 1.0
                },
                timeout=200
            ) or {}
        simplified = res.get('content', '').strip().lower()
        return simplified