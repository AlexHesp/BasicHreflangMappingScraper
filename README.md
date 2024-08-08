# Hreflang Crawler

This project is a Python script that crawls URLs extracted from a sitemap, retrieves `hreflang` links, and writes the results to a CSV file. It includes features for adaptive rate limiting, error handling, and session management.

## Features

- Extracts URLs from a sitemap.
- Retrieves `hreflang` links from each URL.
- Writes the results to a CSV file.
- Adaptive rate limiting based on request success/failure.
- Error handling and logging.
- Uses custom headers and cookies for session management.

## Requirements

- Python 3.x
- `requests` library
- `beautifulsoup4` library
- `lxml` library (for `xml.etree.ElementTree`)
- `concurrent.futures` library (comes with Python 3.2 and above)

## Installation

1. Clone this repository.
2. Install the required libraries:

    ```sh
    pip install requests beautifulsoup4 lxml
    ```

## Usage:

### Define your sitemap URL in the script:

```python
sitemap_url = 'https://example.com/sitemap.xml'
```

### Add your personal User-Agent & cookie session id:

```python
custom_headers = {
    'User-Agent': 'Your User Agent'
}

custom_cookies = {
    'session_id': 'Your Session ID'
}
```

## Script Explanation:

### Functions

- **`is_valid_url(url)`**: Checks if a URL is valid.
- **`get_all_hreflangs(url, headers=None, cookies=None, retries=3, backoff_factor=0.3)`**: Retrieves hreflang links from a webpage.
- **`get_urls_from_sitemap(sitemap_url, headers=None, cookies=None)`**: Extracts URLs from a sitemap.
- **`crawl_site(start_urls, output_csv, headers=None, cookies=None, max_workers=10, initial_rate_limit=0.5, max_rate_limit=5.0)`**: Crawls the extracted URLs, retrieves hreflang links, and writes the results to a CSV file.

### Adaptive Rate Limiting

- **Initial rate limit** is set with `initial_rate_limit`.
- **Rate limit decreases** on successful requests and increases on failures.
- Ensures polite crawling and reduces chances of being blocked.

### Error Handling

- **Errors in retrieving pages** are logged.
- If a page retrieval fails, the corresponding row in the CSV file is marked with **ERROR**.

Thanks, Alex Hesp-Gollins
