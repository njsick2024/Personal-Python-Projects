# %%
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from functools import wraps
from typing import List, Dict

BASE_URL = "https://www.theswiftcodes.com/china/"


def retry(max_retries: int = 3, delay: float = 1.0):

    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    time.sleep(delay)
            # all retries failed
            raise last_exc

        return wrapped

    return decorator


def get_page_url(page_number: int) -> str:
    if page_number == 1:
        return BASE_URL
    return f"{BASE_URL}page/{page_number}"


@retry(max_retries=3, delay=2)
def fetch_page(session: requests.Session, page_number: int) -> str:
    url = get_page_url(page_number)
    resp = session.get(url, timeout=10, verify=False)
    resp.raise_for_status()
    return resp.text


def parse_table(html: str, table_class: str = "swift-country") -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_=table_class)
    if not table:
        return []
    # Extract headers
    headers = [th.get_text(strip=True) for th in table.thead.find_all("th")]
    rows: List[Dict[str, str]] = []
    for tr in table.tbody.find_all("tr"):
        cols = tr.find_all("td")
        if len(cols) != len(headers):
            continue
        row = {headers[i]: cols[i].get_text(strip=True) for i in range(len(headers))}
        rows.append(row)
    return rows


def scrape_all_pages(total_pages: int = 59) -> pd.DataFrame:
    all_rows: List[Dict[str, str]] = []
    with requests.Session() as session:
        for page_num in range(1, total_pages + 1):
            html = fetch_page(session, page_num)
            page_rows = parse_table(html)
            all_rows.extend(page_rows)
    # Vectorize into DataFrame in one shot
    return pd.DataFrame(all_rows)


# %%

df = scrape_all_pages(total_pages=59)
print(df.head())
df.to_csv("china_swift_codes.csv", index=False)
