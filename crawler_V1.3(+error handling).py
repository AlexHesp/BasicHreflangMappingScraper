import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import csv
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import xml.etree.ElementTree as ET
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_valid_url(url):
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)

def get_all_hreflangs(url, headers=None, cookies=None, retries=3, backoff_factor=0.3):
    """
    Returns a dictionary of hreflang URLs mapped by their language codes from a given webpage
    Retries the request in case of failure.
    """
    hreflangs = {}
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    try:
        response = session.get(url, headers=headers, cookies=cookies, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        for link in soup.find_all("link", rel="alternate", hreflang=True):
            hreflang_code = link.get('hreflang')
            href = urljoin(url, link.get('href'))
            if href and is_valid_url(href):
                hreflangs[hreflang_code] = href
    except requests.RequestException as e:
        logging.error(f"Failed to retrieve {url}: {str(e)}")
        return 'ERROR'
    
    return hreflangs

def get_urls_from_sitemap(sitemap_url, headers=None, cookies=None):
    """
    Extracts URLs from a sitemap
    """
    urls = []
    try:
        response = requests.get(sitemap_url, headers=headers, cookies=cookies, timeout=10)
        response.raise_for_status()
        sitemap_content = response.text
        root = ET.fromstring(sitemap_content)
        for url in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
            loc = url.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc").text
            if is_valid_url(loc):
                urls.append(loc)
    except requests.RequestException as e:
        logging.error(f"Failed to retrieve sitemap {sitemap_url}: {str(e)}")
    except ET.ParseError as e:
        logging.error(f"Failed to parse sitemap {sitemap_url}: {str(e)}")
    return urls

def crawl_site(start_urls, output_csv, headers=None, cookies=None, max_workers=10, initial_rate_limit=0.5, max_rate_limit=5.0):
    """
    Crawls a list of start URLs, compiles hreflang mappings, and writes them to a CSV file.
    """
    all_languages = set()
    page_data = {}
    rate_limit = initial_rate_limit

    def crawl(url):
        nonlocal rate_limit
        time.sleep(rate_limit + random.uniform(0, 1))  # Rate limiting with random delay
        logging.info(f"Crawling: {url} with rate limit {rate_limit:.2f} seconds")
        try:
            result = get_all_hreflangs(url, headers=headers, cookies=cookies)
            if result == 'ERROR':
                rate_limit = min(max_rate_limit, rate_limit * 1.5)  # Increase rate limit on failure
                return url, 'ERROR'
            rate_limit = max(initial_rate_limit, rate_limit * 0.9)  # Reduce rate limit on success
            return url, result
        except Exception as e:
            rate_limit = min(max_rate_limit, rate_limit * 1.5)  # Increase rate limit on failure
            logging.error(f"Error crawling {url}: {str(e)}")
            return url, 'ERROR'

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(crawl, url): url for url in start_urls}
        for future in as_completed(future_to_url):
            url, hreflangs = future.result()
            page_data[url] = hreflangs
            if hreflangs != 'ERROR':
                all_languages.update(hreflangs.keys())

    all_languages = sorted(all_languages)  # Sort languages for consistent header order

    with open(output_csv, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        headers = ['URL'] + all_languages
        writer.writerow(headers)

        for url, langs in page_data.items():
            if langs == 'ERROR':
                row = [url] + ['ERROR'] * len(all_languages)
            else:
                row = [url] + [langs.get(lang, '') for lang in all_languages]
            writer.writerow(row)

# Example usage
sitemap_url = 'https://example.com/post-sitemap2.xml'  # Replace with your sitemap URL

# Define custom headers and cookies if needed

#Example: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36
custom_headers = {
    'User-Agent': 'Example User agent',
}

#Example: 123456789
custom_cookies = {
    'session_id': '1234567890' 
}

# Get URLs from the sitemap
start_urls = get_urls_from_sitemap(sitemap_url, headers=custom_headers, cookies=custom_cookies)

# Crawl the extracted URLs
output_file_path = 'hreflang_map.csv'
crawl_site(start_urls, output_file_path, headers=custom_headers, cookies=custom_cookies, max_workers=10, initial_rate_limit=1.0, max_rate_limit=5.0)  # Adjust max_workers and rate_limit as needed
logging.info("Crawling completed. Check the CSV file for results.")
