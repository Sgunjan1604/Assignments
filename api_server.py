from flask import Flask, jsonify, request
import json
import sqlite3
import pandas as pd
import os

app = Flask(__name__)

def get_db_connection():
    """Create a connection to the SQLite database"""
    conn = sqlite3.connect('news_data.db')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/api/news', methods=['GET'])
def get_news():
    """Get all news articles with optional filtering"""
    # Get query parameters
    country = request.args.get('country')
    source = request.args.get('source')
    language = request.args.get('language')
    since = request.args.get('since')  # Date filter
    limit = request.args.get('limit', default=100, type=int)
    offset = request.args.get('offset', default=0, type=int)
    
    # Load data based on format
    if os.path.exists('news_data.db'):
        # Use SQLite database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build query
        query = "SELECT * FROM news_articles WHERE 1=1"
        params = []
        
        if country:
            query += " AND country = ?"
            params.append(country)
        
        if source:
            query += " AND source = ?"
            params.append(source)
        
        if language:
            query += " AND language = ?"
            params.append(language)
        
        if since:
            query += " AND publication_date >= ?"
            params.append(since)
        
        query += " ORDER BY publication_date DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        # Execute query
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
    elif os.path.exists('data/news_data.json'):
        # Use JSON file
        with open('data/news_data.json', 'r', encoding='utf-8') as file:
            all_news = json.load(file)
        
        # Filter results
        results = all_news
        
        if country:
            results = [item for item in results if item['country'] == country]
        
        if source:
            results = [item for item in results if item['source'] == source]
        
        if language:
            results = [item for item in results if item['language'] == language]
        
        if since:
            results = [item for item in results if item['publication_date'] >= since]
        
        # Sort by publication date (descending)
        results.sort(key=lambda x: x['publication_date'], reverse=True)
        
        # Apply limit and offset
        results = results[offset:offset+limit]
        
    elif os.path.exists('data/news_data.csv'):
        # Use CSV file
        df = pd.read_csv('data/news_data.csv')
        
        # Apply filters
        if country:
            df = df[df['country'] == country]
        
        if source:
            df = df[df['source'] == source]
        
        if language:
            df = df[df['language'] == language]
        
        if since:
            df = df[df['publication_date'] >= since]
        
        # Sort by publication date
        df = df.sort_values(by='publication_date', ascending=False)
        
        # Apply limit and offset
        df = df.iloc[offset:offset+limit]
        
        # Convert to list of dictionaries
        results = df.to_dict('records')
    
    else:
        return jsonify({"error": "No data files found"}), 404
    
    return jsonify({
        "count": len(results),
        "offset": offset,
        "limit": limit,
        "results": results
    })

@app.route('/api/countries', methods=['GET'])
def get_countries():
    """Get list of available countries"""
    if os.path.exists('news_data.db'):
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT country, COUNT(*) as count FROM news_articles GROUP BY country ORDER BY count DESC")
        countries = [{"country": row["country"], "count": row["count"]} for row in cursor.fetchall()]
        conn.close()
    
    elif os.path.exists('data/news_data.json'):
        with open('data/news_data.json', 'r', encoding='utf-8') as file:
            all_news = json.load(file)
        
        # Count articles by country
        country_counts = {}
        for item in all_news:
            country = item['country']
            if country in country_counts:
                country_counts[country] += 1
            else:
                country_counts[country] = 1
        
        countries = [{"country": country, "count": count} for country, count in country_counts.items()]
        countries.sort(key=lambda x: x["count"], reverse=True)
    
    elif os.path.exists('data/news_data.csv'):
        df = pd.read_csv('data/news_data.csv')
        country_counts = df['country'].value_counts().reset_index()
        country_counts.columns = ['country', 'count']
        countries = country_counts.to_dict('records')
    
    else:
        return jsonify({"error": "No data files found"}), 404
    
    return jsonify(countries)

@app.route('/api/sources', methods=['GET'])
def get_sources():
    """Get list of available news sources"""
    country = request.args.get('country')
    
    if os.path.exists('news_data.db'):
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = "SELECT DISTINCT source, country, COUNT(*) as count FROM news_articles"
        params = []
        
        if country:
            query += " WHERE country = ?"
            params.append(country)
        
        query += " GROUP BY source, country ORDER BY count DESC"
        
        cursor.execute(query, params)
        sources = [{"source": row["source"], "country": row["country"], "count": row["count"]} for row in cursor.fetchall()]
        conn.close()
    
    elif os.path.exists('data/news_data.json'):
        with open('data/news_data.json', 'r', encoding='utf-8') as file:
            all_news = json.load(file)
        
        # Filter by country if specified
        if country:
            all_news = [item for item in all_news if item['country'] == country]
        
        # Count articles by source
        source_counts = {}
        for item in all_news:
            source = item['source']
            country = item['country']
            key = (source, country)
            
            if key in source_counts:
                source_counts[key] += 1
            else:
                source_counts[key] = 1
        
        sources = [{"source": source, "country": country, "count": count} 
                  for (source, country), count in source_counts.items()]
        sources.sort(key=lambda x: x["count"], reverse=True)
    
    elif os.path.exists('data/news_data.csv'):
        df = pd.read_csv('data/news_data.csv')
        
        if country:
            df = df[df['country'] == country]
        
        source_counts = df.groupby(['source', 'country']).size().reset_index(name='count')
        sources = source_counts.to_dict('records')
    
    else:
        return jsonify({"error": "No data files found"}), 404
    
    return jsonify(sources)

@app.route('/api/report', methods=['GET'])
def get_report():
    """Get summary report"""
    if os.path.exists('data/report.json'):
        with open('data/report.json', 'r', encoding='utf-8') as file:
            report = json.load(file)
        return jsonify(report)
    else:
        return jsonify({"error": "Report file not found"}), 404

@app.route('/', methods=['GET'])
def home():
    """Simple home page with API documentation"""
    return """
    <html>
    <head>
        <title>RSS News API</title>
        <style>
            body { font-family: Arial, sans-serif; line-height: 1.6; margin: 0; padding: 20px; max-width: 800px; margin: 0 auto; }
            h1 { color: #333; }
            h2 { color: #555; }
            code { background-color: #f4f4f4; padding: 2px 5px; border-radius: 3px; }
            pre { background-color: #f4f4f4; padding: 10px; border-radius: 5px; overflow-x: auto; }
            .endpoint { margin-bottom: 20px; }
        </style>
    </head>
    <body>
        <h1>RSS News API</h1>
        <p>Welcome to the RSS News API. Use the following endpoints to access scraped news data:</p>
        
        <div class="endpoint">
            <h2>Get News Articles</h2>
            <code>GET /api/news</code>
            <p>Returns a list of news articles with optional filtering.</p>
            <h3>Parameters:</h3>
            <ul>
                <li><code>country</code> - Filter by country</li>
                <li><code>source</code> - Filter by news source</li>
                <li><code>language</code> - Filter by article language</li>
                <li><code>since</code> - Filter by publication date (ISO format)</li>
                <li><code>limit</code> - Maximum number of results (default: 100)</li>
                <li><code>offset</code> - Result offset for pagination (default: 0)</li>
            </ul>
            <h3>Example:</h3>
            <pre>GET /api/news?country=USA&limit=10</pre>
        </div>
        
        <div class="endpoint">
            <h2>Get Countries</h2>
            <code>GET /api/countries</code>
            <p>Returns a list of countries with article counts.</p>
            <h3>Example:</h3>
            <pre>GET /api/countries</pre>
        </div>
        
        <div class="endpoint">
            <h2>Get News Sources</h2>
            <code>GET /api/sources</code>
            <p>Returns a list of news sources with article counts.</p>
            <h3>Parameters:</h3>
            <ul>
                <li><code>country</code> - Filter sources by country</li>
            </ul>
            <h3>Example:</h3>
            <pre>GET /api/sources?country=UK</pre>
        </div>
        
        <div class="endpoint">
            <h2>Get Report</h2>
            <code>GET /api/report</code>
            <p>Returns a summary report of all scraped data.</p>
            <h3>Example:</h3>
            <pre>GET /api/report</pre>
        </div>
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)