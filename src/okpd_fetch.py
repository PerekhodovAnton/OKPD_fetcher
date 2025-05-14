import os
import json
from typing import List, Dict
from urllib.parse import quote
import logging
import time

logger = logging.getLogger(__name__)
CACHE_FILE = 'okpd_cache.json'

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            return json.load(open(CACHE_FILE, encoding='utf-8'))
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
    return {}

def save_cache(cache: dict):
    try:
        json.dump(cache, open(CACHE_FILE, 'w', encoding='utf-8'), ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving cache: {e}")

CACHE = load_cache()

def fetch_okpd2_batch(terms: List[str], timeout: int = 15000) -> Dict[str, List[Dict[str,str]]]:
    results = {}
    
    # Process all terms through cache and fallbacks first
    for term in terms:
        term_lower = term.lower()
        
        # First check cache
        if term in CACHE:
            logger.info(f"OKPD cache hit: {term}")
            results[term] = CACHE[term]
            continue
            
        
    
    # Check if we still have terms to process
    remaining_terms = [t for t in terms if t not in results]
    if not remaining_terms:
        return results
    
    # Try to use playwright for remaining terms
    try:
        from playwright.sync_api import sync_playwright
        
        with sync_playwright() as pw:
            try:
                browser = pw.chromium.launch(headless=True)
                page = browser.new_page()
                
                for term in remaining_terms:
                    url = f"https://zakupki44fz.ru/app/okpd2/search/{quote(term)}"
                    try:
                        logger.info(f"Fetching OKPD2 for {term}")
                        page.goto(url, timeout=timeout)
                        page.wait_for_selector(".okpd2-modal-search-result__item-body", timeout=timeout)
                        
                        # Get all the codes and names
                        codes = page.query_selector_all("div.classifier-code-wrapper > a")
                        names = page.query_selector_all("div.okpd2-search-container__result-item-name")
                        
                        items = []
                        for c, n in zip(codes, names):
                            items.append({'code': c.inner_text().strip(), 'name': n.inner_text().strip()})
                        
                        if items:
                            CACHE[term] = items
                            save_cache(CACHE)
                            results[term] = items
                        else:
                            # If no items found, use general fallback
                            results[term] = [{"code": "32.99.59.000", "name": "Изделия различные прочие, не включенные в другие группировки"}]
                            CACHE[term] = results[term]
                            save_cache(CACHE)
                        
                        time.sleep(1)  # Be nice to the server
                        
                    except Exception as e:
                        logger.warning(f"Fetch error for {term}: {e}")
                        # Use general fallback for errors
                        results[term] = [{"code": "32.99.59.000", "name": "Изделия различные прочие, не включенные в другие группировки"}]
                        CACHE[term] = results[term]
                        save_cache(CACHE)
                
                browser.close()
                
            except Exception as e:
                logger.error(f"Browser error: {e}")
                # Use fallbacks for all remaining terms
                for term in remaining_terms:
                    if term not in results:
                        results[term] = [{"code": "32.99.59.000", "name": "Изделия различные прочие, не включенные в другие группировки"}]
                        CACHE[term] = results[term]
                        save_cache(CACHE)
                
    except ImportError as e:
        logger.error(f"Playwright import error: {e}")
        # Use fallbacks for all remaining terms
        for term in remaining_terms:
            if term not in results:
                results[term] = [{"code": "32.99.59.000", "name": "Изделия различные прочие, не включенные в другие группировки"}]
                CACHE[term] = results[term]
                save_cache(CACHE)
    
    return results