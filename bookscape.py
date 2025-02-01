import requests
import pymysql
import pandas as pd
import streamlit as st
import mysql.connector
from mysql.connector import Error


# import mysql.connector

# connection = mysql.connector.connect(
#   host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
#   port = 4000,
#   user = "21Zb8bmfzqCTMN9.root",
#   password = "q7U5DSJ84feP7qW4",
#   database = "Nature",
#   ssl_ca = "<CA_PATH>",
#   ssl_verify_cert = True,
#   ssl_verify_identity = True
# )

api_key = "AIzaSyD2QY8JHySi20zqjfo2zWvm4vMrejkvfAM"   # API key

# Google Books API Constants
API_URL = "https://www.googleapis.com/books/v1/volumes"
MAX_RESULTS = 40
TOTAL_BOOKS = 1500

# TiDB Connection Constants (Replace these with your own values)
TIDB_HOST = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com"
TIDB_PORT = 4000
TIDB_USER = "21Zb8bmfzqCTMN9.root"
TIDB_PASSWORD = "q7U5DSJ84feP7qW4"
TIDB_DB = "Nature"

# Function to fetch books from Google Books API with pagination
def fetch_books_from_api(query):
    all_books = []
    start_index = 0

    while len(all_books) < TOTAL_BOOKS:
        params = {
            'q': query,
            'startIndex': start_index,
            'maxResults': MAX_RESULTS
        }
        response = requests.get(API_URL, params=params)
        data = response.json()

        if 'items' in data:
            all_books.extend(data['items'])
            start_index += MAX_RESULTS
        else:
            break

    return all_books[:TOTAL_BOOKS]

# Create the schema in TiDB
def create_db_schema():
    conn = pymysql.connect(
        host=TIDB_HOST,
        port=TIDB_PORT,
        user=TIDB_USER,
        password=TIDB_PASSWORD,
        db=TIDB_DB,
        ssl={'ssl_ca': '/content/cert/ca.pem'}
    )
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS books;")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS books (
        bookid INT PRIMARY KEY AUTO_INCREMENT,
        searchkey TEXT,
        booktitle TEXT,
        booksubtitle TEXT,
        bookauthors TEXT,
        bookdescription TEXT,
        publisher TEXT,
        industryIdentifiers TEXT,
        textreadingModes TEXT,
        imagereadingModes TEXT,
        pageCount INT,
        categories TEXT,
        language VARCHAR(255),
        imageLinks TEXT,
        ratingsCount INT,
        averageRating DECIMAL(3,2),
        country VARCHAR(50),
        saleability TEXT,
        isEbook BOOLEAN,
        amountlistPrice DECIMAL(10,2),
        currencyCode_listPrice VARCHAR(255),
        amountretailPrice DECIMAL(10,2),
        currencyCoderetailPrice VARCHAR(255),
        buyLink TEXT,
        year INT
    );
    """)
    conn.commit()
    cursor.close()
    conn.close()

# Store book data into TiDB
def store_books_in_tidb(books, query):
    conn = pymysql.connect(
        host=TIDB_HOST,
        port=TIDB_PORT,
        user=TIDB_USER,
        password=TIDB_PASSWORD,
        db=TIDB_DB,
        ssl={'ssl_ca': '/content/cert/ca.pem'}
    )
    cursor = conn.cursor()
    for item in books:
        volume_info = item.get('volumeInfo', {})
        sale_info = item.get('saleInfo', {})      

        volume_info = item['volumeInfo']
        title = volume_info.get('title', 'N/A')
        subtitle = volume_info.get('subtitle', 'N/A')
        authors = ', '.join(volume_info.get('authors', []))
        description = volume_info.get('description', 'N/A')
        publisher = volume_info.get('publisher', 'N/A')
        industry_identifiers = ', '.join(str(x) for x in volume_info.get('industryIdentifiers', []))
        text_reading_modes = volume_info.get('readingModes', {}).get('text', 'N/A')
        image_reading_modes = volume_info.get('readingModes', {}).get('image', 'N/A')
        page_count = volume_info.get('pageCount', 0)
        categories = ', '.join(volume_info.get('categories', []))
        language = volume_info.get('language', 'en')
        image_links = ', '.join(volume_info.get('imageLinks', {}).values())
        ratings_count = volume_info.get('ratingsCount', 0)
        average_rating = volume_info.get('averageRating', 0)
        country = sale_info.get('country', 'US')
        saleability = sale_info.get('saleability', False)
        is_ebook = sale_info.get('isEbook', False)
        list_price = sale_info.get('listPrice', {}).get('amount', 0)
        currency_code_list_price = sale_info.get('listPrice', {}).get('currencyCode', 'USD')
        retail_price = sale_info.get('retailPrice', {}).get('amount', 0)
        currency_code_retail_price = sale_info.get('retailPrice', {}).get('currencyCode', 'USD')
        buy_link = sale_info.get('buyLink', 'N/A')
        # year = int(volume_info.get('publishedDate', '0000')[:4])
        published_date = volume_info.get('publishedDate', '0000')
        try:
            year = int(''.join(filter(str.isdigit, published_date[:4])))
        except ValueError:
            year = 0

        cursor.execute("""
            INSERT INTO books (searchkey, booktitle, booksubtitle, bookauthors, bookdescription, publisher,
            industryIdentifiers, textreadingModes, imagereadingModes, pageCount, categories, language, imageLinks,
            ratingsCount, averageRating, country, saleability, isEbook, amountlistPrice, currencyCode_listPrice,
            amountretailPrice, currencyCoderetailPrice, buyLink, year)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (query, title, subtitle, authors, description, publisher, industry_identifiers, text_reading_modes,
              image_reading_modes, page_count, categories, language, image_links, ratings_count, average_rating,
              country, saleability, is_ebook, list_price, currency_code_list_price, retail_price,
              currency_code_retail_price, buy_link, year))
    conn.commit()
    cursor.close()
    conn.close()

# Streamlit App
st.title('📚📥 Book Scraping Application')
keyword = st.text_input("**Enter a keyword:**")
if st.button("🔍 Search"):
    if keyword:
        with st.spinner("Fetching data..."):
            books = fetch_books_from_api(keyword)
        if books:
            create_db_schema()
            store_books_in_tidb(books, keyword)
            st.success(f"✅ Found {len(books)} books for keyword: {keyword}")
            book_data = pd.DataFrame(books)
            #st.dataframe(book_data)
        else:
            st.error("No data found for the given query.")
    else:
        st.error("Please enter a search term.")

# TiDB Connection Constants
TIDB_HOST = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com"
TIDB_PORT = 4000
TIDB_USER = "21Zb8bmfzqCTMN9.root"
TIDB_PASSWORD = "q7U5DSJ84feP7qW4"
TIDB_DB = "Nature"

# SQL Queries
queries = {
    "1. Check Availability of eBooks vs Physical Books":
        "SELECT isEbook, COUNT(*) AS book_count FROM books GROUP BY isEbook;",
    "2. Find the Publisher with the Most Books Published":
        "SELECT publisher, COUNT(*) AS book_count FROM books GROUP BY publisher ORDER BY book_count DESC LIMIT 1;",
    "3. Identify the Publisher with the Highest Average Rating":
        "SELECT publisher, AVG(averageRating) AS avg_rating FROM books GROUP BY publisher ORDER BY avg_rating DESC LIMIT 1;",
    "4. Get the Top 5 Most Expensive Books by Retail Price":
        "SELECT booktitle, amountretailPrice FROM books ORDER BY amountretailPrice DESC LIMIT 5;",
    "5. Find Books Published After 2010 with at Least 500 Pages":
        "SELECT booktitle, pageCount, year FROM books WHERE year > 2010 AND pageCount >= 500;",
    "6. List Books with Discounts Greater than 20%":
        """SELECT booktitle, (amountlistPrice - amountretailPrice) / amountlistPrice AS discount_percentage 
           FROM books WHERE
           (amountlistPrice - amountretailPrice) / amountlistPrice > 0.02;""",
    "7. Find the Average Page Count for eBooks vs Physical Books":
        "SELECT isEbook, AVG(pageCount) AS avg_page_count FROM books GROUP BY isEbook;",
    "8. Find the Top 3 Authors with the Most Books":
        "SELECT bookauthors, COUNT(*) AS book_count FROM books GROUP BY bookauthors ORDER BY book_count DESC LIMIT 3;",
    "9. List Publishers with More than 10 Books":
        "SELECT publisher, COUNT(*) AS book_count FROM books GROUP BY publisher HAVING book_count > 10;",
    "10. Find the Average Page Count for Each Category":
        "SELECT categories, AVG(pageCount) AS avg_page_count FROM books GROUP BY categories;",
    "11. Retrieve Books with More than 3 Authors":
        """SELECT booktitle, bookauthors FROM books 
           WHERE LENGTH(bookauthors) - LENGTH(REPLACE(bookauthors, ',', '')) >= 3;""",
    "12. Books with Ratings Count Greater Than the Average":
        """SELECT booktitle, ratingsCount FROM books 
           WHERE ratingsCount > (SELECT AVG(ratingsCount) FROM books);""",
    "13. Books with the Same Author Published in the Same Year":
        """SELECT bookauthors, year, COUNT(*) AS book_count FROM books 
           GROUP BY bookauthors, year HAVING book_count > 1;""",
    "14. Books with a Specific Keyword in the Title": "", # Placeholder for dynamic query
        
    "15. Year with the Highest Average Book Price":
        """SELECT year, AVG(amountretailPrice) AS avg_price FROM books 
           GROUP BY year ORDER BY avg_price DESC LIMIT 1;""",
    "16. Count Authors Who Published 3 Consecutive Years":
        """SELECT bookauthors, COUNT(DISTINCT year) AS consecutive_years FROM books 
           GROUP BY bookauthors HAVING consecutive_years >= 3;""",
    "17. Authors with Books Published in the Same Year by Different Publishers":
        """SELECT bookauthors, year, COUNT(DISTINCT publisher) AS publisher_count FROM books 
           GROUP BY bookauthors, year HAVING publisher_count > 1;""",
    "18. Average Retail Price of eBooks and Physical Books":
        "SELECT isEbook, AVG(amountretailPrice) AS avg_retail_price FROM books GROUP BY isEbook;",
    "19. Identify Books That Are Outliers":
        """SELECT booktitle, amountretailPrice FROM books 
           WHERE amountretailPrice > (SELECT AVG(amountretailPrice) + 3 * STDDEV(amountretailPrice) FROM books) 
           OR amountretailPrice < (SELECT AVG(amountretailPrice) - 3 * STDDEV(amountretailPrice) FROM books);""",
    "20. Publisher with the Highest Average Rating (More than 10 Books)":
        """SELECT publisher, AVG(averageRating) AS avg_rating FROM books 
           GROUP BY publisher HAVING COUNT(*) > 10 ORDER BY avg_rating DESC LIMIT 1;"""
}

# Streamlit App
st.title('📗 Analysis Book')

# User selection
selected_question = st.selectbox("Select a question:", list(queries.keys()))

if selected_question == "14. Books with a Specific Keyword in the Title":
    keyword = st.text_input("Enter the keyword to search in book titles:")

if st.button("🚀Run Query"):
    query = queries[selected_question]

    if selected_question == "14. Books with a Specific Keyword in the Title" and keyword:
        query = f"SELECT * FROM books WHERE booktitle LIKE '%{keyword}%'"
    else:
        query = queries.get(selected_question, "")
    
    try:
        conn = pymysql.connect(
            host=TIDB_HOST,
            port=TIDB_PORT,
            user=TIDB_USER,
            password=TIDB_PASSWORD,
            db=TIDB_DB,
            ssl={'ssl_ca': '/content/cert/ca.pem'}
        )
        # Execute the query
        data = pd.read_sql(query, conn)
        conn.close()
        
        if not data.empty:
            st.write(f"Results for: {selected_question}")
            st.dataframe(data)
        else:
            st.warning("No results found!")
    except Exception as e:
        st.error(f"Error running query: {e}")
