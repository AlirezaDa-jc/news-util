import weaviate
import pandas as pd
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any


def process_batch(articles: List[Dict[Any, Any]], newsDb) -> None:
    """Process a batch of articles"""
    with newsDb.batch.dynamic() as batch:  # Using the correct batch API
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


def import_with_batching(file_path: str, batch_size: int = 100, num_workers: int = 4) -> None:
    """Import data with batching and parallel processing"""
    try:
        # Load data
        news_data = pd.read_json(file_path, lines=True)
        articles = news_data.to_dict('records')
        print(f"Loaded {len(articles)} articles from {file_path}")

        # Create batches
        batches = [articles[i:i + batch_size] for i in range(0, len(articles), batch_size)]

        # Initialize client for the main thread
        client = weaviate.connect_to_local()
        newsDb = client.collections.get("News_db")

        start_time = time.time()

        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            for batch in batches:
                futures.append(executor.submit(process_batch, batch, newsDb))

            # Monitor progress
            for i, future in enumerate(tqdm(futures, desc="Processing batches")):
                future.result()  # This will raise any exceptions that occurred
                if (i + 1) % 10 == 0:  # Print progress every 10 batches
                    elapsed = time.time() - start_time
                    print(f"Processed {(i + 1) * batch_size} articles in {elapsed:.2f} seconds")

        elapsed_time = time.time() - start_time
        final_rate = len(articles) / elapsed_time
        print(f"Finished processing {len(articles)} articles in {elapsed_time:.2f} seconds")
        print(f"Final average rate: {final_rate:.2f} articles/second")
    except Exception as e:
        print(f"Error during import: {str(e)}")
        raise
    finally:
        client.close()


if __name__ == "__main__":

    # Get number of CPU cores for optimal worker count
    import multiprocessing

    recommended_workers = max(1, multiprocessing.cpu_count() - 1)
    print(recommended_workers)
    print(f"Starting import with {recommended_workers} workers...")
    import_with_batching(
        file_path="news.json",
        batch_size=50,
        num_workers=recommended_workers
    )