import re

def extract_code(text: str) -> str:
    match = re.search(r"\b\d+(?:\.\d+){1,}\b", text)
    return match.group(0) if match else ''


def remove_links(text: str) -> str:
    return re.sub(r'https?://\S+', '', text)