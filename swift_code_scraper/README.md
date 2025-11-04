# China SWIFT Codes Scraper
A simple Python script to scrape Chinese banking institutions and their SWIFT codes from [theswiftcodes.com/china](https://www.theswiftcodes.com/china/). It paginates through all 59 pages, extracts the `<table class="swift-country">`, and aggregates the results into a pandas DataFrame (and CSV).
## Features
- Handles page 1 (`/china/`) and pages 2–59 (`/china/page/<n>`) automatically  
- Retries failed requests up to 3 times with a delay  
- Parses the table header and rows into a list of dictionaries  
- Converts the final list into a single pandas DataFrame  
- Saves output to `china_swift_codes.csv`
## Requirements
- Python 3.7 or higher  
- [requests](https://pypi.org/project/requests/)  
- [beautifulsoup4](https://pypi.org/project/beautifulsoup4/)  
- [pandas](https://pypi.org/project/pandas/)
## Installation
1. Clone or download this repository.  
2. Install dependencies:
  ```bash
  pip install requests beautifulsoup4 pandas
