import streamlit as st
import pandas as pd
import os
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import requests
import io
#####################
# Get Data
#####################

@st.cache_data
def createObjects(csv_content):
    metadata = getMetaData(csv_content)
    df = pd.read_csv(csv_content)
    entries = []
    for index, row in df.iterrows():
        entry = {
            'pos': index,
            'id': row['id'],
            'likes': row['diggCount'],
            'shares': row['shareCount'],
            'comments': row['commentCount'],
            'views': row['playCount'],
            'collectCount': row['collectCount'],
            'repostCount': row['repostCount'],
            'time': row['createTime'],
            'isAd': row['isAd'],
            'music': row['music'],
            'diversificationLabels': row['diversificationLabels'],
            'suggestedWords': row['suggestedWords'],
            'keywordTags': row['keywordTags'],
            'IsAigc': row['IsAigc'],
            'AIGCDescription': row['AIGCDescription'],
            'locationCreated': row['locationCreated'],
            'videoDuration': row['videoDuration'],
            'description': row['videoDescription'],
            'author_id': row['author_id'],
            'author_uniqueId': row['author_uniqueId'],
            'author_nickname': row['author_nickname'],
            'author_signature': row['author_signature'],
            'author_verified': row['author_verified'],
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
    directory = '/Users/belle/Desktop/analysis/data_analysis/tt'
    for root, dirs, files in os.walk(directory):
        if files:
            for file_name in files:
                if file_name.endswith('.csv'):
                    fileNames.append(os.path.join(root, file_name))

    for file in fileNames:
        entry = createObjects(file)
        all_entries.extend(entry)

    return all_entries

def getMetaData(file_path):
    path_parts = file_path.split('/')

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

# #########################
# # Process Data
# #########################
# @st.cache_data
def createPandas(arr):
    df = pd.DataFrame(arr)
    df = df.drop_duplicates()
    df = df[df['id'].apply(lambda x: 'https://www.tiktok.com' not in str(x))]
    df['author_nickname'] = df['author_nickname'].astype(str).str.replace(" ", "_")
    df['diversificationLabels'] = df['diversificationLabels'].astype(str).str.replace("'", "")
    df['suggestedWords'] = df['suggestedWords'].astype(str).str.replace("'", "")
    df['description'] = df['description'].astype(str)
    # Group by 'id' and apply list function to 'trendingTime'
    trending_time_grouped = df.groupby('id')['trendingTime'].apply(list).reset_index()
    df['link'] = df['id'].apply(lambda x: f"https://www.tiktok.com/share/video/{x}")

    # Merge the grouped 'trendingTime' back with the original DataFrame
    df = df.drop(columns='trendingTime').drop_duplicates().merge(trending_time_grouped, on='id')

    # df['likes'] = df['likes'].apply(convert_likes)
    # df['display_name'] = df['display_name'].str.replace(' ', '_')
    return df

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
    result_df = df.groupby('author_nickname')['id'].apply(list).reset_index()
    result_df['frequency'] = result_df['id'].apply(len)
    result_df = result_df[['author_nickname', 'frequency']].sort_values(by='frequency', ascending=False)

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

    # Convert 'time' to datetime
    df['time'] = pd.to_datetime(df['time'])
    df['time'] = df['time'] - pd.Timedelta(hours=4)

    # Calculate the difference in hours
    df['time_difference_hours'] = (df['collectedTime'] - df['time']).dt.total_seconds() / 3600
    df = df[['id', 'time_difference_hours', 'views', 'likes', 'author_verified', 'pos', 'author_nickname']]

    author_df = createAccountsDistribution(df)
    df = df.merge(author_df, on='author_nickname')
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
    result_df = df.groupby('author_id')
    return result_df

arr = getData()
df = createPandas(arr)
new_vid_df = countNumResults(df)
unique_df = countNumUniqueVideos(df)
vid_freq_df = countNumOfVideoOccurences(new_vid_df)
author_freq_df = createAccountsDistribution(df)
author_wordcloud = createWordCloud(df, column='author_nickname')
freshness_df = checkFreshnessOfData(df)
likes_time_df = countLikesOverTime(freshness_df)
views_time_df = countViewsOverTime(freshness_df)
video_query_df = sameVideoDifQuery(df)
video_query_freq_df = countNumOfSameVideoOccurences(video_query_df)
labels_wordcloud = createWordCloud(df, column='diversificationLabels', width=300, height=200)
words_wordcloud = createWordCloud(df, column='suggestedWords')
description_wordcloud = createWordCloud(df, column='description')
queries_df = getQueries(df)
accounts_df = getAccounts(df)
engagement_box_plt = createBoxPlot(df)

percentage_verified = round((df['author_verified'].sum() / len(df)) * 100, 2)
average_follower = round(df['collectCount'].sum() / len(df), 2)
average_likes = round(df['likes'].sum() / len(df), 0)
average_views = round(df['views'].sum() / len(df), 0)
average_comments = round(df['comments'].sum() / len(df), 0)
average_shares = round(df['shares'].sum() / len(df), 0)
########################
# Streamlit stuff
########################
st.header("Learning about political content on TikTok")
if st.checkbox('All: Show raw data'):
    st.write(df)

one, two, three, four = st.columns(4)
with one:
    st.metric(label="Collected videos", value=len(df))
with two:
    st.metric(label="Unique videos", value=len(unique_df))
with three:
    st.metric(label="Queries searched", value=len(queries_df))
with four:
    st.metric(label="Days of collection", value="12")


one, two, three = st.columns(3)
with one:
    st.metric(label="Number of accounts", value=len(accounts_df))
with two:
    st.metric(label="Average follower count", value=average_follower)
with three:
    st.metric(label="Verified accounts", value=f"{percentage_verified} %")

one, two, three = st.columns(3)
with one:
    st.metric(label="Avg views", value=average_views)
with two:
    st.metric(label="Avg likes", value=average_likes)
with three:
    st.metric(label="Avg comments", value=average_comments)

# st.pyplot(engagement_box_plt)
st.markdown("""---""")

st.subheader("How often is a video shown again when searched at a later time?")
left_column, right_column = st.columns(2)

with left_column: 
    st.write(new_vid_df)
with right_column:
    st.bar_chart(vid_freq_df, x='count', y='accounts', color='#09AFB4')

st.subheader("How many queries does a video show under?")
left_column, right_column = st.columns(2)

with left_column: 
    st.write(video_query_df)
with right_column:
    st.bar_chart(video_query_freq_df, x='queries count', y='videos', color='#09AFB4')

st.subheader("Who is posting the accounts?")
left_column, right_column = st.columns(2)

with left_column:
    st.write(author_freq_df)
with right_column:
    st.pyplot(author_wordcloud)

st.subheader("How fresh is the content?")
if st.checkbox('Freshness: show raw data'):
    st.write(freshness_df)

number = st.number_input("How many hours ago do you want to check?", value=24)

freshness_by_hours_df = postedLessThanXHoursAgo(freshness_df, number)
mean_hours = round(freshness_by_hours_df['pos'].mean(), 2)
median_hours = freshness_by_hours_df['pos'].median()
percentage_hours = round(len(freshness_by_hours_df) / len(df) * 100, 2)
likes_hours = round(freshness_by_hours_df['likes'].mean(), 2)
views_hours = round(freshness_by_hours_df['views'].mean(), 2)
regular_producer = freshness_by_hours_df[freshness_by_hours_df['frequency'] >= 5]
regular_hours = round(len(regular_producer) / len(freshness_by_hours_df) * 100, 2)

st.write(f"Percentage of videos posted less than {number} hours before it was searched")

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
st.write("Average views over time")
st.line_chart(views_time_df, x="hours", y="average views", color="#09AFB4")

st.write("Average likes over time")
st.line_chart(likes_time_df, x="hours", y="average likes", color="#FF5C00")

st.subheader("What are the videos about?")
st.write("Most commonly mentioned words in the video description")
st.pyplot(description_wordcloud)
st.write("Most commonly mentioned words from the suggested words")
st.pyplot(words_wordcloud)
st.write("What TikTok thinks the videos are about")
st.pyplot(labels_wordcloud)
