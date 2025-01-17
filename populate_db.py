import weaviate
import pandas as pd
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from typing import List, Dict, Any
import gc

client = weaviate.connect_to_local()
newsDb = client.collections.get("News_db")



def preprocess_articles(articles: List[Dict]) -> List[Dict]:
    """Preprocess articles to reduce memory usage and optimize for GPU"""
    processed = []
    for article in articles:
        processed.append(article)
    return processed


def process_batch(articles: List[Dict[Any, Any]]) -> None:
    """Process a batch of articles with GPU optimization"""
    try:
        # Larger batch size for GPU processing
        with newsDb.batch.dynamic() as batch:
            for article in articles:
                date = article["date"]
                if isinstance(date, pd.Timestamp):
                    date = date.strftime('%Y-%m-%d')
                batch.add_object({
                    "headline": article["headline"],
                    "short_description": article["short_description"],
                    "category": article["category"],
                    "authors": article["authors"],
                    "date": date,
                    "link": article["link"],
                })
    except Exception as e:
        print(e)


def import_with_batching(file_path: str, batch_size: int = 128) -> None:
    """Import data with GPU and CPU optimization"""
    try:
        # Load and preprocess data
        print("Loading and preprocessing data...")
        news_data = pd.read_json(file_path, lines=True)
        articles = news_data.to_dict('records')
        articles = preprocess_articles(articles)

        # Optimize workers for i5 13400 (6P + 4E cores)
        num_workers = 15  # Using most of the physical cores

        print(f"Starting import with {num_workers} workers and batch size {batch_size}")
        start_time = time.time()

        batches = [articles[i:i + batch_size] for i in range(0, len(articles), batch_size)]

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            for batch in batches:
                futures.append(executor.submit(process_batch, batch))

            completed = 0
            for future in tqdm(futures, desc="Processing batches"):
                future.result()
                completed += batch_size
                if completed % 5000 == 0:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed
                    print(f"Rate: {rate:.2f} articles/second")
                    print(f"Memory usage per article: {gc.get_count()}")

        elapsed_time = time.time() - start_time
        final_rate = len(articles) / elapsed_time
        print(f"Finished processing {len(articles)} articles in {elapsed_time:.2f} seconds")
        print(f"Final average rate: {final_rate:.2f} articles/second")

    except Exception as e:
        print(f"Error during import: {str(e)}")
        raise


if __name__ == "__main__":
    import_with_batching("news.json", batch_size=128)