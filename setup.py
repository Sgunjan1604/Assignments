from setuptools import setup, find_packages

setup(
    name="rss_news_scraper",
    version="1.0.0",
    description="A tool to scrape news from RSS feeds across different countries",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        "feedparser>=6.0.10",
        "beautifulsoup4>=4.12.2",
        "bs4>=0.0.1",
        "pandas>=2.0.3",
        "requests>=2.31.0",
        "langdetect>=1.0.9",
        "python-dateutil>=2.8.2",
        "flask>=2.3.3",
    ],
    entry_points={
        "console_scripts": [
            "rss-scraper=rss_scraper:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)