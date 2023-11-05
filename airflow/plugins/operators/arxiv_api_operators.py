from plugins.extractors.arxiv_api_extractor import ArxivAPIExtractor

import time
from datetime import datetime, timedelta
from urllib.parse import quote

def check_arxiv_api_responsiveness():
    extractor = ArxivAPIExtractor(timestamp_cutoff= datetime.now())
    start_time = time.time()

    while True:
        query_url = f"{extractor.BASE_URL}search_query={extractor.search_query}&max_results=1"
        encoded_url = quote(query_url, safe=':/?=&()')

        feed = extractor._get_feed_data(encoded_url)

        if feed is not None and len(feed.entries) > 0:
            break

        elapsed_time = time.time() - start_time
        if elapsed_time > 60:  # 1 minute in seconds
            raise Exception("Arxiv API check failed after 1 minute of retries.")
        
        time.sleep(5)

    return "Arxiv API is responsive."
