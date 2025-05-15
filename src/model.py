import os
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from typing import List, Dict, Union
import logging


logger = logging.getLogger(__name__)

os.environ["TOKENIZERS_PARALLELISM"] = "false"
torch.set_num_threads(1)

###---LLAMA.CPP---###
# from llama_cpp import Llama
# class Model:
#     def __init__(
#         self,
#         model_path: str = "models/Qwen3-4B-Q4_0.gguf",
#         temperature: float = 0.1,
#         top_p: float = 0.9,
#         repeat_penalty: float = 1.2,
#         max_new_tokens: int = 100,
#         n_ctx: int = 4096,
#         n_threads: int = 4,
#         system_prompt: str = None,
#     ):
#         self.tokenizer = AutoTokenizer.from_pretrained("Qwen/Qwen3-4B")
#         self.client = Llama(
#             model_path=model_path,
#             n_ctx=n_ctx,
#             n_threads=n_threads,
#             use_mmap=True,
#             verbose=False,       # <— Отключаем внутренние логи llama.cpp
#             log_level='error', 
#         )
#         self.defaults = {
#             "temperature": temperature,
#             "top_p": top_p,
#             "repeat_penalty": repeat_penalty,
#             "max_new_tokens": max_new_tokens,
#         }
#         self.system_prompt = system_prompt or (
#             "Ты помогаешь выбрать один код для военной компании, которая "
#             "занимается производством и работает с различным металом."
#         )

#     def generate(
#         self,
#         content,
#         system_prompt: str = None,
#         temperature: float = None,
#         top_p: float = None,
#         repeat_penalty: float = None,
#         max_new_tokens: float = None,
#     ) -> dict:
#         # Determine messages: if content is already a list of messages, use it
#         if isinstance(content, list):
#             messages = content
#         else:
#             messages = [
#                 {"role": "system", "content": system_prompt or self.system_prompt},
#                 {"role": "user", "content": content},
#             ]
        
#         # Fixed: Accessing locals directly causes issues when parameters aren't in scope
#         # Merge defaults with overrides
#         temp = temperature if temperature is not None else self.defaults["temperature"]
#         tp = top_p if top_p is not None else self.defaults["top_p"]
#         rp = repeat_penalty if repeat_penalty is not None else self.defaults["repeat_penalty"]
#         mnt = max_new_tokens if max_new_tokens is not None else self.defaults["max_new_tokens"]
        
#         # Render conversation
#         text = self.tokenizer.apply_chat_template(
#             messages,
#             tokenize=False,
#             add_generation_prompt=True,
#             enable_thinking=False,
#         )
#         # Call Llama
#         resp = self.client.create_completion(
#             prompt=text,
#             max_tokens=mnt,
#             temperature=temp,
#             top_p=tp,
#             repeat_penalty=rp,
#             stop=["</think>"],
#             echo=True,
#         )
#         full = resp["choices"][0]["text"]
#         generated = full[len(text):]
#         # Split thinking vs content
#         marker = "</think>"
#         if marker in generated:
#             thinking, content = generated.split(marker, 1)
#             thinking, content = thinking.strip(), content.strip()
#         else:
#             thinking, content = "", generated.strip()
#         return {"thinking": thinking, "content": content}

# # --- TRANSFORMERS ----
class Model:
    def __init__(
        self,
        model_name: str = "Qwen/Qwen3-0.6B",
        # model_name: str = "Qwen/Qwen3-1.7B",
        device: str = "cpu",
        temperature: float = 0.1,
        top_p: float = 0.9,
        max_new_tokens: int = 2096,
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
            enable_thinking=True
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
