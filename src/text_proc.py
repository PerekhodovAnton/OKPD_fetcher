import re
import pymorphy3
from collections import defaultdict
from typing import List

class TextProcessor:
    def __init__(self):
        self.morph = pymorphy3.MorphAnalyzer()

    @staticmethod
    def extract_code(text: str) -> str:
        match = re.search(r"\b\d+(?:\.\d+){1,}\b", text)
        return match.group(0) if match else ""

    def normalize_term(self, term: str) -> str:
        norma = re.findall(r"[a-zA-Zа-яА-ЯёЁ]+", term)
        normed = [n for n in norma if len(n) > 2]
        clean = re.sub(r'[Тт]овар:? ?', '', ' '.join(normed))
        return clean

    @staticmethod
    def remove_links(text: str) -> str:
        return re.sub(r'https?://\S+', '', text)

    @staticmethod
    def group_by_first_word(elements: List[str]) -> List[List[str]]:
        groups = defaultdict(list)
        for el in elements:
            first_word = el.split()[0] if el else ''
            groups[first_word].append(el)
        return list(groups.values())