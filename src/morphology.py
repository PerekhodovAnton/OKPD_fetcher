import re
import logging
import pymorphy3

logger = logging.getLogger(__name__)
morph = pymorphy3.MorphAnalyzer()

def normalize_term(term: str) -> str:
    words = re.findall(r"[a-zA-Zа-яА-ЯёЁ]+", term)
    clean = [w for w in words if len(w) > 2]
    joined = ' '.join(clean)
    joined = re.sub(r'[Тт]овар:? ?', '', joined)
    logger.info(f"Normalized term: {joined}")
    return joined