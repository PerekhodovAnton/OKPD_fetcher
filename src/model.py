import os
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from typing import List, Dict, Union
import logging

logger = logging.getLogger(__name__)

os.environ["TOKENIZERS_PARALLELISM"] = "false"
torch.set_num_threads(1)


# --- TRANSFORMERS ----
class Model:
    def __init__(
        self,
        model_name: str = "Qwen/Qwen3-0.6B",
        device: str = "cpu",
        temperature: float = 0.1,
        top_p: float = 0.9,
        max_new_tokens: int = 50,
    ):
        # Инициализация токенизатора и модели
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype="auto",
            device_map=device
        )
        # Параметры генерации по умолчанию
        self.temperature = temperature
        self.top_p = top_p
        self.max_new_tokens = max_new_tokens

    def generate(
        self,
        prompt: Union[str, List[Dict[str, str]]],
        temperature: float = None,
        top_p: float = None,
        max_new_tokens: int = None,
    ) -> dict:
        # Параметры генерации
        temperature = temperature if temperature is not None else self.temperature
        top_p = top_p if top_p is not None else self.top_p
        max_new_tokens = max_new_tokens if max_new_tokens is not None else self.max_new_tokens

        # Если prompt — не список, оборачиваем в формат чата
        if isinstance(prompt, list):
            messages = prompt
        else:
            messages = [{"role": "user", "content": prompt}]

        # Генерация текста с учётом thinking
        text = self.tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
            enable_thinking=False
        )


        # Токенизация и перенос на устройство модели
        model_inputs = self.tokenizer([text], return_tensors="pt").to(self.model.device)

        # Генерация токенов
        generated_ids = self.model.generate(
            **model_inputs,
            do_sample=True,
            temperature=temperature,
            top_p=top_p,
            max_new_tokens=max_new_tokens
        )

        # Отделяем сгенерированные токены от prompt'а
        output_ids = generated_ids[0][len(model_inputs.input_ids[0]):].tolist()

        # Пытаемся найти границу мыслительного блока
        try:
            # Токен `</think>` имеет id 151668 в Qwen3
            index = len(output_ids) - output_ids[::-1].index(151668)
        except ValueError:
            index = 0

        # Декодируем обе части: thinking и основной контент
        thinking = self.tokenizer.decode(output_ids[:index], skip_special_tokens=True).strip("\n")
        content = self.tokenizer.decode(output_ids[index:], skip_special_tokens=True).strip("\n")

        return {
            "thinking": thinking,
            "content": content
        }
