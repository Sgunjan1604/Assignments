# RSS News Feed Scraper

This project scrapes news headlines and summaries from various countries using publicly available RSS feeds from major news agencies. It can also attempt to gather historical data where available.

## Features

- Scrapes news from 30 different RSS feeds across 20+ countries
- Extracts news titles, publication dates, sources, countries, summaries, and URLs
- Saves data in JSON, CSV, or SQLite database format
- Attempts to gather historical data where available
- Handles various encoding formats and date formats
- Detects language of articles
- Generates summary reports of scraped data

## Requirements

- Python 3.8 or higher
- Dependencies listed in `requirements.txt`

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/rss-news-scraper.git
   cd rss-news-scraper
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage

Run the script with default settings (JSON output format):

```
python rss_scraper.py
```

### Output Format Options

You can specify the output format with the `--format` option:

```
# Save to JSON (default)
python rss_scraper.py --format json

# Save to CSV
python rss_scraper.py --format csv

# Save to SQLite database
python rss_scraper.py --format db
```

