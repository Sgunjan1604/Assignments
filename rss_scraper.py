import feedparser
import pandas as pd
import requests
import time
import sqlite3
import json
import datetime
import os
import logging
import re
from bs4 import BeautifulSoup
from langdetect import detect
from urllib.parse import urlparse
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create directory for data if it doesn't exist
os.makedirs('data', exist_ok=True)

class RSSFeedScraper:
    def __init__(self, db_file='news_data.db', user_agent="NewsScraperBot/1.0", data_format="json"):
        """
        Initialize the RSS Feed Scraper.
        
        Args:
            db_file (str): SQLite database file name
            user_agent (str): User agent for HTTP requests
            data_format (str): Output format - "json", "csv", or "db"
        """
        self.headers = {"User-Agent": user_agent}
        self.feeds_list = []
        self.data_format = data_format.lower()
        self.db_file = db_file
        
        # Initialize database if format is db
        if self.data_format == "db":
            self._init_db()
        
        # Load RSS feeds from the feeds.json file
        self._load_feeds()

    def _init_db(self):
        """Initialize SQLite database with required schema"""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        # Create table if it doesn't exist
        c.execute('''
            CREATE TABLE IF NOT EXISTS news_articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                publication_date TEXT,
                source TEXT,
                country TEXT,
                language TEXT,
                summary TEXT,
                url TEXT UNIQUE,
                content TEXT,
                keywords TEXT,
                scraped_date TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_file}")

    def _load_feeds(self):
        """Load RSS feeds from feeds.json file"""
        try:
            with open('feeds.json', 'r', encoding='utf-8') as file:
                self.feeds_list = json.load(file)
                logger.info(f"Loaded {len(self.feeds_list)} RSS feeds from feeds.json")
        except FileNotFoundError:
            logger.error("feeds.json not found. Please create the file with RSS feed URLs.")
            exit(1)
        except json.JSONDecodeError:
            logger.error("Invalid JSON format in feeds.json.")
            exit(1)

    def _detect_language(self, text):
        """Detect language of the text"""
        try:
            return detect(text)
        except:
            return "unknown"

    def _parse_date(self, date_str):
        """Parse various date formats to ISO format"""
        if not date_str:
            return datetime.now().isoformat()
        
        try:
            # Try to parse with feedparser's date handler
            parsed_date = feedparser._parse_date(date_str)
            if parsed_date:
                return time.strftime("%Y-%m-%dT%H:%M:%SZ", parsed_date)
        except:
            pass
            
        try:
            # Try standard datetime parsing
            return datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z").isoformat()
        except:
            try:
                return datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z").isoformat()
            except:
                return datetime.now().isoformat()

    def _extract_content(self, url):
        """Extract full article content from the URL (if possible)"""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Remove script and style elements
                for element in soup(["script", "style"]):
                    element.decompose()
                    
                # Try to extract main article content (this is very site-specific)
                article = soup.find("article") or soup.find("div", class_=re.compile("(article|content|story)"))
                
                if article:
                    return article.get_text().strip()
                else:
                    # Fallback to extract paragraphs
                    paragraphs = soup.find_all("p")
                    content = " ".join([p.get_text().strip() for p in paragraphs])
                    return content
            return ""
        except Exception as e:
            logger.warning(f"Error extracting content from {url}: {e}")
            return ""

    def scrape_feed(self, feed_info):
        """
        Scrape a single RSS feed and return articles data.
        
        Args:
            feed_info (dict): Dictionary with feed information
            
        Returns:
            list: List of dictionaries with article data
        """
        url = feed_info["url"]
        country = feed_info["country"]
        source = feed_info["source"]
        
        logger.info(f"Scraping feed: {source} ({country}) - {url}")
        
        articles = []
        try:
            # Add delay to respect rate limits
            time.sleep(1)
            
            # Parse RSS feed
            feed = feedparser.parse(url)
            
            for entry in feed.entries:
                try:
                    # Extract data from feed entry
                    title = entry.get("title", "").strip()
                    if not title:  # Skip entries without title
                        continue
                        
                    publication_date = self._parse_date(entry.get("published", ""))
                    link = entry.get("link", "")
                    
                    # Extract summary/description
                    summary = ""
                    if "summary" in entry:
                        summary = entry.summary
                    elif "description" in entry:
                        summary = entry.description
                    
                    # Clean HTML from summary
                    if summary:
                        soup = BeautifulSoup(summary, 'html.parser')
                        summary = soup.get_text().strip()
                    
                    # Detect language based on title and summary
                    language = self._detect_language(title + " " + summary)
                    
                    # Extract content (optional)
                    content = ""
                    if feed_info.get("extract_content", False):
                        content = self._extract_content(link)
                    
                    # Extract keywords (if available)
                    keywords = []
                    if "tags" in entry:
                        keywords = [tag.term for tag in entry.tags if hasattr(tag, 'term')]
                    
                    # Create article dictionary
                    article = {
                        "title": title,
                        "publication_date": publication_date,
                        "source": source,
                        "country": country,
                        "language": language,
                        "summary": summary,
                        "url": link,
                        "content": content,
                        "keywords": ",".join(keywords),
                        "scraped_date": datetime.now().isoformat()
                    }
                    
                    articles.append(article)
                except Exception as e:
                    logger.warning(f"Error processing entry in {source}: {e}")
            
            logger.info(f"Scraped {len(articles)} articles from {source}")
            return articles
            
        except Exception as e:
            logger.error(f"Error scraping feed {url}: {e}")
            return []

    def scrape_historical_data(self, feed_info, months_back=12):
        """
        Attempt to scrape historical data by modifying URLs or using archives.
        Note: This is highly dependent on the news site structure.
        
        Args:
            feed_info (dict): Dictionary with feed information
            months_back (int): Number of months to go back in time
            
        Returns:
            list: List of dictionaries with historical article data
        """
        all_articles = []
        source = feed_info["source"]
        
        # This is a simplified approach - many news sites don't expose historical data via RSS
        # A more comprehensive approach would involve site-specific scrapers
        
        logger.info(f"Attempting to gather historical data for {source}")
        
        # For sites that might have archive RSS feeds with year/month parameters
        domain = urlparse(feed_info["url"]).netloc
        
        # Try common archive patterns (this is speculative and will only work for some sites)
        current_date = datetime.now()
        
        for i in range(months_back):
            target_date = current_date - timedelta(days=30*i)
            year = target_date.year
            month = target_date.month
            
            # Try some common archive URL patterns
            archive_urls = [
                f"https://{domain}/archive/{year}/{month:02d}/rss.xml",
                f"https://{domain}/archives/{year}/{month:02d}/feed",
                f"https://{domain}/{year}/{month:02d}/feed",
                f"https://{domain}/feed/archive/{year}/{month:02d}"
            ]
            
            for archive_url in archive_urls:
                try:
                    time.sleep(2)  # Respectful delay
                    
                    # Try to parse the archive feed
                    archive_feed = feedparser.parse(archive_url)
                    
                    if len(archive_feed.entries) > 0:
                        logger.info(f"Found archive feed: {archive_url} with {len(archive_feed.entries)} entries")
                        
                        # Create a temporary feed_info with the archive URL
                        temp_feed_info = feed_info.copy()
                        temp_feed_info["url"] = archive_url
                        
                        # Scrape the archive feed
                        articles = self.scrape_feed(temp_feed_info)
                        all_articles.extend(articles)
                except Exception as e:
                    logger.debug(f"Failed to fetch archive {archive_url}: {e}")
        
        logger.info(f"Scraped {len(all_articles)} historical articles for {source}")
        return all_articles

    def save_to_database(self, articles):
        """Save articles to SQLite database"""
        if not articles:
            return

        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        for article in articles:
            try:
                c.execute('''
                    INSERT OR IGNORE INTO news_articles 
                    (title, publication_date, source, country, language, 
                     summary, url, content, keywords, scraped_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    article["title"],
                    article["publication_date"],
                    article["source"],
                    article["country"],
                    article["language"],
                    article["summary"],
                    article["url"],
                    article["content"],
                    article["keywords"],
                    article["scraped_date"]
                ))
            except sqlite3.Error as e:
                logger.error(f"SQLite error: {e} for article {article['title']}")
        
        conn.commit()
        conn.close()
        logger.info(f"Saved {len(articles)} articles to database")

    def save_to_json(self, articles, filename="data/news_data.json"):
        """Save articles to JSON file"""
        if not articles:
            return
            
        try:
            # Read existing data if file exists
            existing_data = []
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as file:
                    try:
                        existing_data = json.load(file)
                    except json.JSONDecodeError:
                        existing_data = []
            
            # Combine existing data with new articles, avoiding duplicates
            url_set = set(item["url"] for item in existing_data)
            
            for article in articles:
                if article["url"] not in url_set:
                    existing_data.append(article)
                    url_set.add(article["url"])
            
            # Write combined data back to file
            with open(filename, 'w', encoding='utf-8') as file:
                json.dump(existing_data, file, ensure_ascii=False, indent=2)
                
            logger.info(f"Saved {len(articles)} articles to JSON file {filename}")
            
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")

    def save_to_csv(self, articles, filename="data/news_data.csv"):
        """Save articles to CSV file"""
        if not articles:
            return
            
        try:
            # Convert to DataFrame
            df = pd.DataFrame(articles)
            
            # Check if file exists to append or create new
            if os.path.exists(filename):
                # Read existing CSV
                existing_df = pd.read_csv(filename)
                
                # Combine and remove duplicates based on URL
                combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['url'])
                
                # Write back to CSV
                combined_df.to_csv(filename, index=False, encoding='utf-8')
            else:
                # Create new CSV
                df.to_csv(filename, index=False, encoding='utf-8')
                
            logger.info(f"Saved {len(articles)} articles to CSV file {filename}")
            
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")

    def generate_report(self):
        """Generate a summary report of scraped data"""
        report = {"countries": {}}
        
        if self.data_format == "db":
            # Generate report from database
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            # Get total count
            cursor.execute("SELECT COUNT(*) FROM news_articles")
            total_count = cursor.fetchone()[0]
            
            # Get counts by country and source
            cursor.execute("""
                SELECT country, source, COUNT(*) as count,
                MIN(publication_date) as earliest_date
                FROM news_articles
                GROUP BY country, source
                ORDER BY country, source
            """)
            
            results = cursor.fetchall()
            
            for row in results:
                country, source, count, earliest_date = row
                
                if country not in report["countries"]:
                    report["countries"][country] = {"total": 0, "sources": {}}
                
                report["countries"][country]["sources"][source] = {
                    "count": count,
                    "earliest_date": earliest_date
                }
                report["countries"][country]["total"] += count
            
            conn.close()
            
        elif self.data_format == "json":
            # Generate report from JSON file
            try:
                with open("data/news_data.json", 'r', encoding='utf-8') as file:
                    articles = json.load(file)
                    
                total_count = len(articles)
                
                # Collect country and source statistics
                for article in articles:
                    country = article["country"]
                    source = article["source"]
                    pub_date = article["publication_date"]
                    
                    if country not in report["countries"]:
                        report["countries"][country] = {"total": 0, "sources": {}}
                    
                    if source not in report["countries"][country]["sources"]:
                        report["countries"][country]["sources"][source] = {
                            "count": 0,
                            "earliest_date": pub_date
                        }
                    
                    report["countries"][country]["sources"][source]["count"] += 1
                    report["countries"][country]["total"] += 1
                    
                    # Update earliest date if needed
                    if pub_date < report["countries"][country]["sources"][source]["earliest_date"]:
                        report["countries"][country]["sources"][source]["earliest_date"] = pub_date
                        
            except Exception as e:
                logger.error(f"Error generating report from JSON: {e}")
                
        elif self.data_format == "csv":
            # Generate report from CSV file
            try:
                df = pd.read_csv("data/news_data.csv")
                total_count = len(df)
                
                # Group by country and source
                grouped = df.groupby(["country", "source"]).agg({
                    "title": "count",
                    "publication_date": "min"
                }).reset_index()
                
                for _, row in grouped.iterrows():
                    country = row["country"]
                    source = row["source"]
                    count = row["title"]
                    earliest_date = row["publication_date"]
                    
                    if country not in report["countries"]:
                        report["countries"][country] = {"total": 0, "sources": {}}
                    
                    report["countries"][country]["sources"][source] = {
                        "count": int(count),
                        "earliest_date": earliest_date
                    }
                    report["countries"][country]["total"] += int(count)
                    
            except Exception as e:
                logger.error(f"Error generating report from CSV: {e}")
        
        # Add total count to report
        report["total_articles"] = total_count
        
        # Save report to file
        try:
            with open("data/report.json", 'w', encoding='utf-8') as file:
                json.dump(report, file, ensure_ascii=False, indent=2)
                
            # Also create a markdown report
            self._create_markdown_report(report)
                
            logger.info("Generated report successfully")
            
        except Exception as e:
            logger.error(f"Error saving report: {e}")
            
        return report

    def _create_markdown_report(self, report):
        """Create a markdown report from the report data"""
        with open("data/report.md", 'w', encoding='utf-8') as file:
            file.write("# News Scraping Report\n\n")
            file.write(f"Total Articles: {report['total_articles']}\n\n")
            
            file.write("## Articles by Country\n\n")
            file.write("| Country | News Agency | Total Articles | Historical Data |\n")
            file.write("|---------|-------------|----------------|----------------|\n")
            
            for country, data in sorted(report["countries"].items()):
                for source, source_data in sorted(data["sources"].items()):
                    file.write(f"| {country} | {source} | {source_data['count']} | Since {source_data['earliest_date'].split('T')[0]} |\n")

    def run(self, include_historical=True):
        """
        Run the RSS feed scraper for all feeds.
        
        Args:
            include_historical (bool): Whether to attempt scraping historical data
        """
        all_articles = []
        
        for feed_info in self.feeds_list:
            # Scrape current feed
            articles = self.scrape_feed(feed_info)
            all_articles.extend(articles)
            
            # Attempt to scrape historical data if requested
            if include_historical:
                historical_articles = self.scrape_historical_data(feed_info)
                all_articles.extend(historical_articles)
        
        # Save data according to format
        if self.data_format == "db":
            self.save_to_database(all_articles)
        elif self.data_format == "json":
            self.save_to_json(all_articles)
        elif self.data_format == "csv":
            self.save_to_csv(all_articles)
        else:
            logger.warning(f"Unknown data format: {self.data_format}")
        
        # Generate report
        report = self.generate_report()
        
        return report


if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description="RSS Feed News Scraper")
    parser.add_argument("--format", choices=["json", "csv", "db"], default="json",
                      help="Output format (default: json)")
    parser.add_argument("--no-historical", action="store_true",
                      help="Skip historical data scraping")
    parser.add_argument("--db-file", default="news_data.db",
                      help="SQLite database file (for db format)")
    
    args = parser.parse_args()
    
    # Run the scraper
    scraper = RSSFeedScraper(db_file=args.db_file, data_format=args.format)
    report = scraper.run(include_historical=not args.no_historical)
    
    print("\nScraping completed. Summary:")
    print(f"Total articles scraped: {report['total_articles']}")
    print(f"Countries covered: {len(report['countries'])}")
    print(f"Report saved to data/report.json and data/report.md")