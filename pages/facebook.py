import streamlit as st
import pandas as pd
import os
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import requests
import io
import re
from urllib.parse import urlparse, parse_qs
from datetime import datetime

#####################
# Get Data
#####################
def createObjects(csv_content):
    metadata = getMetaData(csv_content)
    df = pd.read_csv(csv_content)
    if df.empty:
        return []
    entries = []
    for index, row in df.iterrows():
        entry = {
            'pos': index,
            'link': row['url'],
            'type': row['type'],
            'name': row['name'],
            'likes': row['likes'],
            'views': row['views'],
            'shares': row['shares'],
            'comments': row['comments'],
            'description': row['description'],
            'time': row['time'],
            'trendingTime': metadata[0],
            'collectedTime': metadata[1].replace(".csv", ""),
            'searchTerm': metadata[2]
        }
        entries.append(entry)
    return entries

@st.cache_data
def getData():
    fileNames = []
    all_entries = []
    directory = '/Users/belle/Desktop/analysis/data_analysis/fb'
    for root, dirs, files in os.walk(directory):
        if files:
            for file_name in files:
                if file_name.endswith('.csv'):
                    fileNames.append(os.path.join(root, file_name))

    for file in fileNames:
        if(os.stat(file).st_size == 0):
            print('empty file')
        else:
            entry = createObjects(file)
            all_entries.extend(entry)

    return all_entries

@st.cache_data
def getMetaData(file_path):
    path_parts = file_path.split('/')
    trending_time = ''
    collected_time = ''
    search_term = ''
    # Find the segment containing the trending and collected times
    for part in path_parts:
        if 'trending@' in part and 'collected@' in part:
            # Split the segment by '_' and then extract the dates
            segments = part.split('_')
            for segment in segments:
                if segment.startswith('trending@'):
                    trending_time = segment.split('trending@')[1]
                elif segment.startswith('collected@'):
                    collected_time = segment.split('collected@')[1]
        if '.csv' in part:
            search_term = part.split('.')[0]
            search_term = search_term.split('_')[0]

    return [trending_time, collected_time, search_term]

def getUserCategory(df):
    file_path = '/Users/belle/Desktop/analysis/data_analysis/labelledUsers.csv'
    csv_data = pd.read_csv(file_path)
    user_data = []
    for index, row in csv_data.iterrows():
        entry = {
            'author_nickname': row['author_nickname'],
            'cat': row['cat']
        }
        user_data.append(entry)

    user_df = pd.DataFrame(user_data)
    df = df.merge(user_df, on='author_nickname', how='left')
    return df

def cleanUpData(df):
    df = df[~df['link'].str.contains('login/?', regex=False)]
    df = df[~df['type'].str.contains('unknown', regex=False)]
    df = df[~df['name'].str.contains('name', regex=False)]
    return df
# #########################
# # Process Data
# #########################
def createPandas(arr):
    df = pd.DataFrame(arr)
    df = df.drop_duplicates()
    df['id'] = df['link'].apply(extract_facebook_id)
    trending_time_grouped = df.groupby('id')['trendingTime'].apply(list).reset_index()
    df['likes'] = df['likes'].apply(convert_likes).astype(int)
    df['views'] = df['views'].apply(convert_likes).astype(int)
    df['shares'] = df['shares'].apply(convert_likes).astype(int)
    df['time'] = df['time'].apply(convertTime)
    df = df.drop(columns='trendingTime').drop_duplicates().merge(trending_time_grouped, on='id')
    df['name'] = df['name'].str.replace(' ', '_')
    return df

def extract_facebook_id(url):
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    # Pattern to extract the ID based on known URL formats
    patterns = [
        r"videos/[^/]+/(\d+)",                       # Pattern for video ID in URL path
        r"story_fbid=(\d+)",                   # Pattern for story_fbid in query parameters
        r"id=(\d+)",                            # Pattern for id in query parameters
        r"v=(\d+)",                      # Pattern for v in query parameters
        r"reel/(\d+)"     
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)

    # If no pattern matches, return empty string
    return ''


def convert_likes(likes):
    if likes is None or pd.isna(likes):
        return 0
    elif isinstance(likes, str):
        if 'likes' in likes or 'i' in likes or 'views' in likes or 'comment' in likes or 'shares' in likes or 'of' in likes:
            return 0
        if 'play' in likes.lower():
            likes = likes.lower().replace('play', '')
            
        likes = likes.lower().replace(',', '')
        if 'k' in likes:
            return float(likes.replace('k', '')) * 1000
        elif 'm' in likes:
            return float(likes.replace('m', '')) * 1000000
    try:
        return pd.to_numeric(likes, errors='coerce')
    except (TypeError, ValueError):
        return 0  # Handle any remaining issues with conversion

def countNumOfLikes(df):
    result_df = df.groupby('likes')['link'].apply(list).reset_index()
    result_df['count'] = result_df['link'].apply(len)
    result_df = result_df[['likes', 'count']].sort_values(by='count', ascending=False)

    return result_df

def convertTime(date_string):
    try:
        # Define the format matching the input string
        date_format = '%A, %B %d, %Y at %I:%M %p'

        # Parse the string into a datetime object
        datetime_obj = datetime.strptime(date_string, date_format)
        return datetime_obj
    except:
        return datetime(2050, 2, 1)

def countNumResults(df):
    result_df = df.groupby(['searchTerm', 'link'])['collectedTime'].apply(lambda x: list(set(x))).reset_index()
    result_df['count'] = result_df['collectedTime'].apply(len)
    result_df = result_df[['link', 'count', 'searchTerm', 'collectedTime']].sort_values(by='count', ascending=False)

    return result_df

def sameVideoDifQuery(df):
    result_df = df.groupby('link')['searchTerm'].apply(lambda x: list(set(x))).reset_index()
    result_df['queries count'] = result_df['searchTerm'].apply(len)
    result_df = result_df[['link', 'queries count', 'searchTerm']].sort_values(by='queries count', ascending=False)

    return result_df

def countNumOfSameVideoOccurences(df):
    result_df = df.groupby('queries count')['link'].apply(list).reset_index()
    result_df['videos'] = result_df['link'].apply(len)
    result_df = result_df[['queries count', 'videos']]

    return result_df

def countNumUniqueVideos(df):
    result_df = df.groupby('id')['time'].apply(list).reset_index()
    result_df = result_df[['id']]

    return result_df

def countNumOfVideoOccurences(df):
    result_df = df.groupby('count')['link'].apply(list).reset_index()
    result_df['accounts'] = result_df['link'].apply(len)
    result_df = result_df[['count', 'accounts']].sort_values(by='count', ascending=False)

    return result_df

def createAccountsDistribution(df):
    result_df = df.groupby('name')['id'].apply(list).reset_index()
    result_df['frequency'] = result_df['id'].apply(len)
    result_df = result_df[['name', 'frequency']].sort_values(by='frequency', ascending=False)

    return result_df

def createWordCloud(df, column, width=400, height=500):
    # Concatenate all queries into a single string
    text = ' '.join(df[column])

    # Create word cloud
    wc = WordCloud(width=width, height=height, background_color='white').generate(text)

    # Display the word cloud using matplotlib
    fig = plt.figure(figsize=(10, 6))
    plt.imshow(wc, interpolation='bilinear')
    plt.axis('off')

    return fig
    # plt.show()

def createBoxPlot(df):
    fig, ax = plt.subplots()
    df.boxplot(column=['views', 'likes', 'comments'], ax=ax)
    y_ticks = range(0, len(df.columns) * 1000, 1000)
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_ticks)

    return fig

def parse_collected_time(time_str):
    month, day, hour = time_str.split('-')
    return pd.Timestamp(year=2024, month=int(month), day=int(day), hour=int(hour))


def checkFreshnessOfData(df):
    # Convert 'collectedTime' to datetime, assuming the year is 2024 for consistency

    # Apply the function to the 'collectedTime' column
    df['collectedTime'] = df['collectedTime'].apply(parse_collected_time)
    df['time']
    # Convert 'time' to datetime
    df['time'] = pd.to_datetime(df['time'])
    cutoff = datetime(2050, 1, 1)
    df = df[df['time'] < cutoff]

    # Calculate the difference in hours
    df['time_difference_hours'] = (df['collectedTime'] - df['time']).dt.total_seconds() / 3600
    df = df[['id', 'time_difference_hours', 'views', 'likes', 'name', 'pos']]

    author_df = createAccountsDistribution(df)
    df = df.merge(author_df, on='name')
    return df


def postedLessThanXHoursAgo(df, hours):
    filtered_df = df[df['time_difference_hours'] <= hours]
    return filtered_df

def countLikesOverTime(df):
    results = []
    for i in range(1, 480):
        freshness_by_hours_df = postedLessThanXHoursAgo(df, i)
        likes_hours = round(freshness_by_hours_df['likes'].mean(), 2)
        results.append({'hours': i, 'average likes': likes_hours})
    
    return pd.DataFrame(results)

def countViewsOverTime(df):
    results = []
    for i in range(1, 480):
        freshness_by_hours_df = postedLessThanXHoursAgo(df, i)
        likes_hours = round(freshness_by_hours_df['likes'].mean(), 2)
        views_hours = round(freshness_by_hours_df['views'].mean(), 2)
        results.append({'hours': i, 'average views': views_hours, 'average likes': likes_hours})
    
    return pd.DataFrame(results)

def getQueries(df):
    result_df = df.groupby('searchTerm')
    return result_df

def getAccounts(df):
    result_df = df.groupby('name')
    return result_df

arr = getData()
df = createPandas(arr)
df = cleanUpData(df)
# df = getUserCategory(df)
new_vid_df = countNumResults(df)
unique_df = countNumUniqueVideos(df)
# vid_freq_df = countNumOfVideoOccurences(new_vid_df)
author_freq_df = createAccountsDistribution(df)
author_wordcloud = createWordCloud(df, column='name')
freshness_df = checkFreshnessOfData(df)
likes_time_df = countLikesOverTime(freshness_df)
views_time_df = countViewsOverTime(freshness_df)
# video_query_df = sameVideoDifQuery(df)
# video_query_freq_df = countNumOfSameVideoOccurences(video_query_df)
description_wordcloud = createWordCloud(df, column='description')
queries_df = getQueries(df)
accounts_df = getAccounts(df)
# engagement_box_plt = createBoxPlot(df)
average_likes = round(df['likes'].median(), 0)
average_views = round(df['views'].median(), 0)
range_likes = df['likes'].max()
likes_df = countNumOfLikes(df)

########################
# Streamlit stuff
########################
st.header("Learning about political content on Facebook")
if st.checkbox('All: Show raw data'):
    st.write(df)

one, two, three = st.columns(3)
with one:
    st.metric(label="Collected videos", value=len(df))
with two:
    st.metric(label="Unique videos", value=len(unique_df))
with three:
    st.metric(label="Queries searched", value=len(queries_df))

st.subheader("Engagement on posts")
one, two = st.columns(2)
with one:
    st.metric(label="Avg likes", value=average_likes)
with two:
    st.metric(label="Range of likes", value=f"0 - {range_likes}")

st.bar_chart(likes_df, x="likes", y="count")

st.subheader("Who is posting the accounts?")
left_column, right_column = st.columns(2)

with left_column:
    st.write(author_freq_df)
with right_column:
    st.pyplot(author_wordcloud)

st.subheader("How fresh is the content?")
if st.checkbox('Freshness: show raw data'):
    st.write(freshness_df)

number = st.number_input("How popular are posts when we collect them?", value=24)

freshness_by_hours_df = postedLessThanXHoursAgo(freshness_df, number)
mean_hours = round(freshness_by_hours_df['pos'].mean(), 2)
median_hours = freshness_by_hours_df['pos'].median()
percentage_hours = round(len(freshness_by_hours_df) / len(df) * 100, 2)
likes_hours = round(freshness_by_hours_df['likes'].mean(), 2)
views_hours = round(freshness_by_hours_df['views'].mean(), 2)
regular_producer = freshness_by_hours_df[freshness_by_hours_df['frequency'] >= 5]
regular_hours = round(len(regular_producer) / len(freshness_by_hours_df) * 100, 2)

st.write(f"Stats of videos posted less than {number} hours before it was searched")

left_column, center_column, right_column = st.columns(3)

with left_column:
    st.metric(label="Fresh videos", value=f"{percentage_hours} %")
with center_column:
    st.metric(label="Mean Rank", value=mean_hours)
with right_column:
    st.metric(label="Median Rank", value=median_hours)

left_column, center_column, right_column = st.columns(3)
with left_column:
    st.metric(label="Average views", value=views_hours)
with center_column:
    st.metric(label="Average likes", value=likes_hours)
with right_column:
    st.metric(label="Percentage of regular accounts", value=f"{regular_hours} %")

st.write(freshness_by_hours_df)

st.subheader("What happens to posts x hours after posting?")
st.write("Average views of posts based on time passed before collection")
st.line_chart(views_time_df, x="hours", y="average views", color="#09AFB4")

st.write("Average likes of posts based on time passed before collection")
st.line_chart(likes_time_df, x="hours", y="average likes", color="#FF5C00")

st.subheader("What are the posts about?")
st.pyplot(description_wordcloud)