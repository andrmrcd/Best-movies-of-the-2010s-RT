print('Importing Libaries')
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import psycopg2

url = "https://editorial.rottentomatoes.com/guide/the-200-best-movies-of-the-2010s/"

def start_request(url):
    try:
        print('Start url request')
        time.sleep(1)
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an exception for any HTTP errors
        print('Request successful')
        time.sleep(1)
        return response
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the request: {e}")
        exit(1)

def save_to_csv(moviedf):
    try:
        moviedf.to_csv('Top200Movies.csv', encoding='utf-8')
        print("Saved to .csv file successfully.")
    except Exception as e:
        print(f"An error occurred while saving the CSV file: {e}")
        exit(1)

def connect_to_server():
    try:
        #Connect to db
        conn_string = "dbname = 'rt_movies' user = 'postgres'  host = 'localhost' port='5432' password ='root'"
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        print('Connected to database successfully')
        return conn, cursor

    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return None, None
        exit(1)

def extract_movie_info(response):
    try:
        # Create BeautifulSoup object to parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all the divs containing movie information
        divs = soup.find_all('div', {'class': 'col-sm-18 col-full-xs countdown-item-content'})

        # Initialize lists to store movie information
        movie_title = []
        year = []
        tmeterscore = []
        movielink = []
        directors = []
        starring = []

        print('Extracting...')
        # Extract information for each movie
        for div in divs:
            # Title
            heading = div.find('h2')
            movie_title.append(heading.find('a').string if heading else None)

            # Year
            year.append(heading.find('span', class_='subtle start-year').string[1:-1] if heading else None)

            # Tomato Score
            tmeterscore.append(heading.find('span', class_='tMeterScore').string[:-1] if heading else None)

            # Movie Rotten Tomatoes Link
            movielink.append(heading.find('a').get('href') if heading else None)

            # Director
            director_heading = div.find('div', class_='info director')
            directors.append(director_heading.find('a').string if director_heading else None)

            # Casting-Info
            cast_info = div.find('div', class_='info cast')
            if cast_info:
                starring_links = cast_info.find_all('a')
                cast_names = [link.string for link in starring_links]
                starring.append(', '.join(cast_names))
            else:
                starring.append(None)
        print('Extract Successful')
        time.sleep(1)

        # Create a DataFrame to store the movie information
        moviedf = pd.DataFrame()
        moviedf['title'] = movie_title
        moviedf['year'] = year
        moviedf['score'] = tmeterscore
        moviedf['starring_cast'] = starring
        moviedf['director'] = directors

        time.sleep(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)
    return moviedf


def insert_to_db(df):
    try:
        table_name = 'bestmovies200'
        # SQL query to create the table if it doesn't exist
        create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                                        "title" TEXT, 
                                        year int, 
                                        "score" int, 
                                        "starring_cast" TEXT, director TEXT)"""
        cursor.execute(create_table_query)
        conn.commit()

        # Convert dataframe columns and values to string for the INSERT query
        columns = ', '.join(df.columns)
        values = ', '.join(['%s' for _ in df])

        # SQL query to insert data into table
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        data = [tuple(row) for row in df.values]
        cursor.executemany(insert_query, data)
        conn.commit()
        print('Insert into database successful')

    except psycopg2.Error as e:
        print(f"An error occured: {e}")
        exit(1)


response = start_request(url)
conn, cursor = connect_to_server()
moviedf = extract_movie_info(response)
save_to_csv(moviedf)
insert_to_db(moviedf)


# Close cursor and connection
cursor.close()
conn.close()