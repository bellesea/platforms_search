import os
import logging
from collections import deque
import csv
import numpy as np
import pandas as pd
import streamlit as st
from itertools import combinations
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import requests

def find():
    files = os.listdir('./../gtrends_politics/political_queries')
    files.sort()  # Sort files alphabetically
    query_array = []

    for filename in files:
        filepath = os.path.join('./../gtrends_politics/political_queries', filename)
        
        # Step 3: Check if the file is a CSV file
        if filename.endswith('.csv'):
            logging.info(f'Processing {filename}...')
            # Step 4: Read the CSV file
            with open(filepath, 'r', newline='', encoding='utf-8') as file:
                reader = csv.reader(file)
                for row in reader:
                    query = ', '.join(row)
                    time = filename.replace('.csv', '')
                    q = {
                        'time': time,
                        'query': query
                    }
                    query_array.append(q)

    return query_array

def writeDSToCSV(q):
    fieldnames = q[0].keys()

    with open('./political_queries.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Write the data rows
        for x in q:
            writer.writerow(x)

def readCSV():
    # URL of the CSV file
    csv_url = 'https://raw.githubusercontent.com/bellesea/data/main/political_queries.csv'

    # Fetch the CSV file
    response = requests.get(csv_url, verify=False)
    response.raise_for_status()  # Ensure we raise an error if the request failed

    # Split the response text into lines and read it using csv.reader
    lines = response.text.splitlines()
    reader = csv.reader(lines)
    
    # Create the array of objects
    data_array = []
    
    # Skip the header row if it exists
    next(reader, None)
    
    for row in reader:
        if len(row) >= 2:  # Ensure there are enough columns in the row
            data_array.append({'time': row[0], 'query': row[1]})
    
    return data_array

def createPandas(arr):
    df = pd.DataFrame(arr)
    return df

def countDuplicates(df):
    result_df = df.groupby('query')['time'].apply(list).reset_index()
    result_df['count'] = result_df['time'].apply(len)
    result_df = result_df[['query', 'count', 'time']]
    sorted_df = result_df.sort_values(by='count', ascending=False)

    return sorted_df

def extract_common_words(query1, query2):
    words1 = set(query1.split(', '))
    words2 = set(query2.split(', '))
    common_words = words1.intersection(words2)
    if len(common_words) >= 3:
        return ', '.join(sorted(common_words)[:3])  # Pick the first 3 common words sorted alphabetically
    else:
        return None  # If less than 3 common words, return None

def strictlyCountDuplicates(df):
    # Group by query and aggregate times

    # Add a new column with simplified name based on common words
    df['simplified_query'] = df['query']

    # Iterate over pairs of queries to find common words
    for idx1, idx2 in combinations(df.index, 2):
        query1 = df.at[idx1, 'query']
        query2 = df.at[idx2, 'query']
        common_name = extract_common_words(query1, query2)
        if common_name:
            df.loc[idx1, 'simplified_query'] = common_name
            df.loc[idx2, 'simplified_query'] = common_name

    result_df = df.groupby('simplified_query')['time'].apply(list).reset_index()
    result_df['count'] = result_df['time'].apply(len)

    # Reorder columns
    result_df = result_df[['simplified_query', 'count', 'time']]
    sorted_df = result_df.sort_values(by='count', ascending=False)

    # Display the result DataFrame
    return sorted_df

def createWordCloud(df):
    # Concatenate all queries into a single string
    text = ' '.join(df['query'])

    # Create word cloud
    wc = WordCloud(width=800, height=400, background_color='white').generate(text)

    # Display the word cloud using matplotlib
    fig = plt.figure(figsize=(10, 6))
    plt.imshow(wc, interpolation='bilinear')
    plt.axis('off')
    # plt.show()

    return fig

def countLenQueries(df):
    queries = df['query']
    len_array = []
    for q in queries:
        length = len(q.split(', '))
        len_array.append(length)

    len_obj = {'length': len_array}
    d = pd.DataFrame(len_obj)
    d = d['length'].value_counts()
    
    return d

def countNumQueries(df):
    result_df = df.groupby('time')['query'].apply(list).reset_index()
    result_df['count'] = result_df['query'].apply(len)
    result_df = result_df[['time', 'count']]

    return result_df

##############################
# Call functions
##############################

array = readCSV()
df = createPandas(array)
dup = countDuplicates(df)
new_dup = strictlyCountDuplicates(df)
word_cloud = createWordCloud(df)
len_queries = countLenQueries(df)
num_queries = countNumQueries(df)

median = num_queries['count'].median()
mean = num_queries['count'].mean()

##############################
# Streamlit Stuff
##############################

st.header("Analyzing Political Google Top Trends")

if st.checkbox('show raw data'):
    st.write(df)

st.subheader("How often does a query appear?")
st.write(f"From {len(df)} queries we find {len(dup)} unique queries")
st.write(dup)

st.write(f"But this only counts queries that strictly repeat... what about those that are about the same thing? This further narros it down to {len(new_dup)} topics")
st.caption("I calculate this by counting queries with at least 3 similar words as the same")
st.write(new_dup)

st.subheader("What are the top queries?")
st.pyplot(word_cloud)

st.subheader("How many words is a query group?")
st.write(len_queries)

st.subheader("How many political queries are collected each hour?")
st.bar_chart(num_queries, x='time', y='count')

left_column, right_column = st.columns(2)

with left_column:
    st.metric(label="Mean", value=mean)
with right_column:
    st.metric(label="Median", value=median)

# countDuplicates(df)
