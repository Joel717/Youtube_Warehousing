# YouTube Data Harvesting & Warehousing

This script is designed to collect data from YouTube channels using the YouTube API, store it in MongoDB, and migrate it to a PostgreSQL database. Additionally, it includes a Streamlit app for interacting with the data and executing SQL queries.

## Features

- **Data Collection:**
  - Utilizes the YouTube API to retrieve channel information, playlist details, video data, and comments.
  - Stores the collected data in MongoDB for initial storage.

- **Database Migration:**
  - Migrates the collected data from MongoDB to a PostgreSQL database using SQLalchemy.

- **Streamlit App:**
  - Provides a user-friendly interface using Streamlit to interact with the collected data.
  - Allows users to collect and load data to MongoDB, migrate data to SQL, and view tables.

- **SQL Queries:**
  - Executes SQL queries to analyze the stored data and answer specific questions.

## How to Use

1. **Collect and Load Data to MongoDB:**
   - Enter the YouTube channel ID in the input box and click the "Collect and load data to MongoDB" button.

2. **Migrate Data to SQL:**
   - Click the "Migrate Data to SQL" button to transfer data from MongoDB to PostgreSQL.

3. **Streamlit App:**
   - Run the script and open the Streamlit app in a web browser.
   - Interact with the app to view tables, execute SQL queries, and explore the collected data.

4. **SQL Queries:**
   - Choose a question from the dropdown list to execute specific SQL queries on the PostgreSQL database.

## Requirements

- Python 3.x
- Required Python packages can be installed using:
