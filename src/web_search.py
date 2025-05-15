import os
import json
import time
from typing import List
import logging

logger = logging.getLogger(__name__)
CACHE_PATH = 'duckduckgo_cache.json'
REQUEST_DELAY = 5
MAX_RETRIES = 3

def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            return json.load(open(CACHE_PATH, encoding='utf-8'))
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
    return {}


def save_cache(cache: dict):
    try:
        json.dump(cache, open(CACHE_PATH, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving cache: {e}")

CACHE = load_cache()

def web_search(query: str, max_results: int = 2) -> List[str]:
    """
    Search the web for information about a query
    Returns a list of search results or fallbacks if search fails
    """
    # Check cache first
    if query in CACHE:
        logger.info(f"Web search cache hit: {query}")
        return CACHE[query]

    # Try to use duckduckgo_search if available
    try:
        from duckduckgo_search import DDGS
        from fake_useragent import UserAgent
        
        try:
            ua = UserAgent()
            headers = {'User-Agent': ua.random}
        except Exception:
            logger.warning("Failed to create UserAgent, using default headers")
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        
        results = []
        
        for i in range(MAX_RETRIES):
            try:
                with DDGS(headers=headers) as ddgs:
                    search_results = ddgs.text(query, region='wt-wt', safesearch='Moderate', max_results=max_results)
                    for r in search_results:
                        results.append(f"{r['title']} {r['body']} {r['href']}")
                
                if results:
                    CACHE[query] = results
                    save_cache(CACHE)
                    time.sleep(REQUEST_DELAY)
                    return results
                else:
                    logger.warning(f"No results found for query: {query}")
            except Exception as e:
                logger.warning(f"Search failed (attempt {i+1}/{MAX_RETRIES}): {e}")
                time.sleep(REQUEST_DELAY * (i+1))
        
        logger.error(f"All retries failed for query: {query}")
    except ImportError as e:
        logger.warning(f"Could not import search libraries: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in web search: {e}")
    
    # If we reach here, use general fallback
    logger.info(f"Using generic fallback for: {query}")
    fallback = [f"Информация о '{query}' для промышленного и военного применения"]
    CACHE[query] = fallback
    save_cache(CACHE)
    return fallback