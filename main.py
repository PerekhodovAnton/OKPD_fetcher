import pandas as pd
from collections import defaultdict
from typing import List, Dict
from tqdm import tqdm
import time
import logging

from logger import setup_logger
from src.model import Model
from src.utils import extract_code, remove_links
from src.morphology import normalize_term
from src.web_search import web_search
from src.okpd_fetch import fetch_okpd2_batch

logger = setup_logger(__name__)

def group_similar(elements: List[str]) -> List[List[str]]:
    """Group similar items by their first word"""
    by_key = defaultdict(list)
    for el in elements:
        if not el or not isinstance(el, str):
            continue
        words = el.split()
        if not words:
            continue
        key = words[0]
        by_key[key].append(el)
    return list(by_key.values())

class Processor:
    def __init__(self, input_excel: str, output_excel: str, checkpoint: str = 'checkpoint.xlsx', save_interval: int = 10):
        try:
            self.df = pd.read_excel(input_excel)
            if 'Наименование' not in self.df.columns:
                logger.error(f"Input file {input_excel} doesn't have a 'Наименование' column")
                raise ValueError(f"Input file {input_excel} doesn't have a 'Наименование' column")
                
            self.terms = self.df['Наименование'].dropna().tolist()
            
            self.model = Model()
            
            # Initialize result columns
            for col in ['ОКПД код','Название кода','Комментарий']:
                if col not in self.df.columns:
                    self.df[col] = ''
                
            self.checkpoint = checkpoint
            self.save_interval = save_interval
            self.output_excel = output_excel
            
            logger.info(f"Successfully initialized processor with {len(self.terms)} items")
            
        except Exception as e:
            logger.error(f"Error initializing processor: {e}")
            raise

    def run(self):
        try:
            logger.info("Starting processing pipeline")
            
            # Group similar terms
            groups = group_similar(self.terms)
            logger.info(f"Grouped {len(self.terms)} items into {len(groups)} groups")
            
            # Get representative term from each group
            reps = [g[0] for g in groups if g]
            
            # Process terms
            normalized = [normalize_term(r) for r in reps]
            simplified = []
            
            logger.info("Simplifying terms...")
            for norm in normalized:
                try:
                    prompt = [{"role": "user", "content": norm}]
                    resp = self.model.generate(prompt)
                    simplified.append(resp['content'])
                except Exception as e:
                    logger.error(f"Error simplifying term '{norm}': {e}")
                    simplified.append(norm)  # Use normalized as fallback

            # Fetch OKPD data in batch
            logger.info("Fetching OKPD data...")
            okpd_data = fetch_okpd2_batch(simplified)

            # Process each group
            logger.info("Processing groups and assigning codes...")
            for idx, grp in enumerate(tqdm(groups, desc='Processing')):
                if not grp:
                    continue
                    
                rep, simp = reps[idx], simplified[idx]
                entries = okpd_data.get(simp, [])
                
                try:
                    code, name, comment = self._decide(entries, rep, simp)
                    mask = self.df['Наименование'].isin(grp)
                    self.df.loc[mask, ['ОКПД код','Название кода','Комментарий']] = [code, name, comment]
                except Exception as e:
                    logger.error(f"Error processing group {idx} ({rep}): {e}")

                # Save checkpoint at intervals
                if (idx+1) % self.save_interval == 0:
                    self.df.to_excel(self.checkpoint, index=False)
                    logger.info(f"Saved checkpoint at group {idx+1}/{len(groups)}")

            # Save final result
            self.df.to_excel(self.output_excel, index=False)
            logger.info(f"Saved to {self.output_excel}")
            
        except Exception as e:
            logger.error(f"Error in processor run: {e}")
            # Try to save what we have
            try:
                self.df.to_excel(self.checkpoint, index=False)
                logger.info(f"Saved emergency checkpoint due to error")
            except:
                pass
            raise

    def _decide(self, entries, original, simplified):
        """Decide which OKPD code to use for a given term"""
        if not entries:
            return '32.99.59.000', 'Изделия различные прочие, не включенные в другие группировки', ''

        options = '\n'.join(f"{e['code']} — {e['name']}" for e in entries)
        
        try:
            raw = web_search(original)
            context = remove_links(' '.join(raw))
        except Exception as e:
            logger.warning(f"Web search failed for {original}: {e}")
            context = f"Информация о '{original}' для промышленного применения"

        prompt = [
            {"role": "system", "content": 'Ты помогаешь выбрать один код для военной компании, которая занимается производством и работает с различным металом.'},
            {"role": "user", "content": f"Ты составляешь таблицу закупок товаров для военной компании, которая занимается производством и работает с различным металом. \
            Твоя задача выбрать подходящий код для товара отталкиваясь от специфики военного предприятия, где используются различные ЧЕРНЫЕ МЕТАЛЫ, АЛЮМИНИЙ.\n\
            #Тебе ЗАПРЕЩЕНО указывать коды: медицина, Мебель медицинская, гипс. \n \
            \nТовар: {simplified}\nКонтекст: {context}\nВарианты:\n{options}\nВыведи только код:"}
        ]
        
        resp = self.model.generate(prompt)
        code = extract_code(resp['content'])
        
        # Find name for the selected code
        name = ''
        comment = ''
        for e in entries:
            if e['code'] == code:
                name = e['name']
                break
        
        # If code not found in entries, use first entry as fallback
        if not name and entries:
            name = entries[0]['name']
            code = entries[0]['code']
            comment = '(fallback)'
        
        # Final fallback
        if not name:
            name = 'Изделия различные прочие, не включенные в другие группировки'
            code = '32.99.59.000'
            comment = '(no matching code)'
            
        return code, name, comment

if __name__ == '__main__':
    start = time.time()
    try:
        p = Processor(
            input_excel='/Users/anper/oboronka_zakaz/okpd_fetcher/OKPD_fetcher/4_1_test.xlsx',  # Use test input by default
            output_excel='output.xlsx',
            checkpoint='checkpoint.xlsx',
            save_interval=10
        )
        p.run()
        print(f"Done in {time.time() - start:.1f}s")
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        print(f"Error: {e}")
    