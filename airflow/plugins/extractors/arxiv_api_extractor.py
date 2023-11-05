import time
import feedparser
import requests
from datetime import datetime, timedelta
from urllib.parse import quote


class ArxivAPIExtractor:

    # Base API query URL
    BASE_URL = 'http://export.arxiv.org/api/query?'
    # Search parameters
    DEFAULT_SEARCH_QUERY = "cat:(cs.AI OR cs.CL OR cs.CV OR cs.LG OR cs.MA OR cs.NE OR cs.RO)"
    SORT_BY = 'submittedDate'
    SORT_ORDER = 'descending'
    MAX_RESULTS = 2000

    def __init__(self, timestamp_cutoff, search_query=None):
        # Set initial ingestion parameters
        self.search_query = search_query or self.DEFAULT_SEARCH_QUERY
        self.start = 0 # Starting index for paper retrieval                                                           
        self.timestamp_cutoff = timestamp_cutoff  # Initialize timestamp based on given value
        self.published_on = None # Publication date of the last paper in the batch
        self.cutoff_reached = False # Has the cutoff timestamp been reached?
        self.papers = [] # Initialize list of papers to ingest

    def _get_feed_data(self, url):
        """Retrieve and parse the data from the given URL."""
        try:
            feed = feedparser.parse(url)
            if len(feed.entries) == 0:
                response = requests.get(url)
                if response.status_code != 200:
                    raise ValueError(f"Unsuccessful response with status code {response.status_code}.")
                return None
            return feed
        except Exception as e:
            print(f"Error occurred during data retrieval: {str(e)}")
            raise

    def _process_paper(self, entry):
        """Extract and append data for a single paper from the feed"""

        self.published_on = datetime.strptime(entry.published, "%Y-%m-%dT%H:%M:%SZ")
        if self.published_on <= self.timestamp_cutoff:
            self.cutoff_reached = True
            return

        paper_data = {
            'id': entry.id.split('/abs/')[-1],
            'title': entry.title,
            'abstract': entry.summary,
            'authors': [author.name for author in entry.authors],
            'categories': [tag.term for tag in entry.tags],
            'published_on': self.published_on.isoformat(),
            'last_update': entry.updated
        }
        self.papers.append(paper_data)

    def _wait_and_retry(self, start_time, wait_time=3):
        if datetime.now() - start_time > timedelta(minutes=10):
            print("Waited for 10 minutes without a successful response, exiting...")
            return None

        time.sleep(wait_time)
        return min(wait_time + 3, 30)

    def fetch_papers(self):
            """Fetch papers from arXiv based on search parameters."""
            start_time = datetime.now()  
            wait_time = 3 

            while not self.cutoff_reached:
                print("-----------------------------------------------------------------")
                print(f"Current batch starts at index: {self.start}")

                query_url = f"{self.BASE_URL}search_query={self.search_query}&sortBy={self.SORT_BY}&sortOrder={self.SORT_ORDER}&max_results={self.MAX_RESULTS}&start={self.start}"
                encoded_url = quote(query_url, safe=':/?=&()')

                feed = self._get_feed_data(encoded_url)

                if feed is None:
                    print(f"No entries found in the current batch. Waiting for {wait_time} seconds before retrying...")
                    wait_time = self._wait_and_retry(start_time, wait_time)  # Pass the start_time here
                    if wait_time is None:
                        break 
                    else:
                        continue

                wait_time = 3

                # Process each paper in the response
                for entry in feed.entries:
                    self._process_paper(entry)

                print(f"List of papers is now of length: {len(self.papers)}")
                print(f"Last paper of batch was published on: {self.published_on}")

                self.start += len(feed.entries)

            return self.papers  
