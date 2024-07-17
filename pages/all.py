import streamlit as st
import pandas as pd
import os
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import requests
import io
from datetime import datetime
import helper
import re


@st.cache_data
def getYouTubeData():
    fileNames = []
    main_df = pd.DataFrame()
    directory = '/Users/belle/Desktop/analysis/data_analysis/yt'
    fileNames = helper.commonTools.getFilesToCheck(directory, "YOUTUBE")
    for root, dirs, files in os.walk(directory):
        if files:
            for file_name in files:
                if file_name.endswith('.csv'):
                    file_path = os.path.join(root, file_name)
                    df = helper.read_post_csv(file_path, "youtube")
                    metadata = getMetaData(file_path)
                    df['url'] = df['text']
                    df['trendingTime'] =  metadata[0]
                    df['collectedTime'] = metadata[1].replace(".csv", "")
                    df['searchTerm'] = metadata[2]
                    df['platform'] = "YOUTUBE"
                    new_df = pd.concat([main_df, df], axis=0, ignore_index=True)
                    main_df = new_df
    return main_df

@st.cache_data
def getD():
    fileNames = []
    main_df = pd.DataFrame()
    directory = '/Users/belle/Desktop/analysis/data_analysis/data'
    fileList = helper.commonTools.getFilesToCheck(directory, platform='all')
    for files in fileList:
        df = helper.read_post_csv(files)
        data = helper.getDataCollectionParameters(files)
        df['searchTerm'] = data['query']
        df['platform'] = data['platform']
        df['trendingTime'] =  data['trending_time'][0]
        df['collectedTime'] = data['collection_time']
        if data['platform'] == 'instagram':
            df['url'] = df['id']
        new_df = pd.concat([main_df, df], axis=0, ignore_index=True)
        main_df = new_df
    return main_df

@st.cache_data
def getAllTrump():
    fileNames = []
    main_df = pd.DataFrame()
    directory = '/Users/belle/Desktop/analysis/data_analysis/trump_assassination'
    fileList = helper.commonTools.getFilesToCheck(directory, platform='all')
    for files in fileList:
        df = helper.read_post_csv(files)
        data = helper.getDataCollectionParameters(files)
        df['searchTerm'] = data['query']
        df['platform'] = data['platform']
        df['trendingTime'] =  data['trending_time'][0]
        df['collectedTime'] = data['collection_time']
        if data['platform'] == 'instagram':
            df['url'] = df['id']
        new_df = pd.concat([main_df, df], axis=0, ignore_index=True)
        main_df = new_df
    return main_df

@st.cache_data
def getFacebookData():
    fileNames = []
    main_df = pd.DataFrame()
    directory = '/Users/belle/Desktop/analysis/data_analysis/fb'
    for root, dirs, files in os.walk(directory):
        if files:
            for file_name in files:
                if file_name.endswith('.csv'):
                    file_path = os.path.join(root, file_name)
                    df = helper.read_post_csv(file_path, "facebook")
                    metadata = getMetaData(file_path)
                    df['trendingTime'] =  metadata[0]
                    df['collectedTime'] = metadata[1].replace(".csv", "")
                    df['searchTerm'] = metadata[2]
                    df['platform'] = "FB"
                    new_df = pd.concat([main_df, df], axis=0, ignore_index=True)
                    main_df = new_df
    main_df = facebookCleanUpData(main_df)
    return main_df

@st.cache_data
def getTikTokData():
    fileNames = []
    main_df = pd.DataFrame()
    directory = '/Users/belle/Desktop/analysis/data_analysis/tt'
    for root, dirs, files in os.walk(directory):
        if files:
            for file_name in files:
                if file_name.endswith('.csv'):
                    file_path = os.path.join(root, file_name)
                    df = helper.read_post_csv(file_path, "tiktok")
                    metadata = getMetaData(file_path)
                    df['trendingTime'] =  metadata[0]
                    df['collectedTime'] = metadata[1].replace(".csv", "")
                    df['searchTerm'] = metadata[2]
                    df['platform'] = "TIKTOK"
                    df['url'] = df['id'].apply(lambda x: f"https://www.tiktok.com/share/video/{x}")
                    new_df = pd.concat([main_df, df], axis=0, ignore_index=True)
                    main_df = new_df

    return main_df


@st.cache_data
def readData(file_url):
    response = requests.get(file_url, verify=False)  
    response.raise_for_status()  

    df = pd.read_csv(io.StringIO(response.text))
    df['trendingTime'] = df['trendingTime'].apply(parseTime)
    df['collectedTime'] = df['collectedTime'].apply(parseTime)
    return df

def parseTime(timestamp_str):
    # Convert the string to a datetime object
    try:
        timestamp_obj = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        try:
            timestamp_obj = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            date_str = timestamp_str.split("'")[1]
            timestamp_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

    return timestamp_obj

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
            search_term = search_term.split('-')[0]

            if(trending_time == ''):

                if "Facebook" in part:
                    part = part.split('_')[1]
                search_term = part.split('-')[0]
                # date_hour_match = re.search(r'(\d{2}-\d{2}) (\d{2})', part)
                # print(date_hour_match)
                # if date_hour_match:
                #     collected_time = f"{date_hour_match.group(1)}-{date_hour_match.group(2)}"
                month = part.split('-')[1]
                day = part.split('-')[2]
                last = part.split('-1')[-1]
                hour = last.split(' ')[1].split(":")[0]
                collected_time = f"{month}-{day}-{hour}"

                trending_time = None

    return [trending_time, collected_time, search_term]

def getUserCategory(df):
    file_path = '/Users/belle/Desktop/analysis/data_analysis/labelledUsers.csv'
    csv_data = pd.read_csv(file_path)
    user_data = []
    for index, row in csv_data.iterrows():
        entry = {
            'user name': row['user name'],
            'cat': row['cat']
        }
        user_data.append(entry)

    user_df = pd.DataFrame(user_data)
    df = df.merge(user_df, on='user name', how='left')
    return df

# #########################
# # Process Data
# #########################

# @st.cache_data
def getUniqueVideos(df):
    df = df.drop_duplicates(subset='url', keep='last')
    return df

def addMoreData(df):
    df['likes'] = pd.to_numeric(df['likes'], errors='coerce')
    df['text'] = df['text'].astype(str)
    trending_time_grouped = df.groupby('url')['trendingTime'].apply(list).reset_index()
    df = df.drop(columns='trendingTime').drop_duplicates().merge(trending_time_grouped, on='url')
    return df

def tiktokAddData(df):
    df['diversificationLabels'] = df['diversificationLabels'].astype(str).str.replace("'", "")
    df['suggestedWords'] = df['suggestedWords'].astype(str).str.replace("'", "")
    return df

def facebookCleanUpData(df):
    df = df[~df['url'].str.contains('login/?', regex=False)]
    df = df[~df['type'].str.contains('unknown', regex=False)]
    df = df[df['upload time'].notna()]
    df = df[df['collectedTime'].notna()]
    return df

def countNumResults(df):
    result_df = df.groupby(['searchTerm', 'url'])['collectedTime'].apply(lambda x: list(set(x))).reset_index()
    result_df['count'] = result_df['collectedTime'].apply(len)
    result_df = result_df[['url', 'count', 'searchTerm', 'collectedTime']].sort_values(by='count', ascending=False)

    return result_df

def sameVideoDifQuery(df):
    result_df = df.groupby('url')['searchTerm'].apply(lambda x: list(set(x))).reset_index()
    result_df['queries count'] = result_df['searchTerm'].apply(len)
    result_df = result_df[['url', 'queries count', 'searchTerm']].sort_values(by='queries count', ascending=False)

    return result_df

def countNumOfSameVideoOccurences(df):
    result_df = df.groupby('queries count')['url'].apply(list).reset_index()
    result_df['videos'] = result_df['url'].apply(len)
    result_df = result_df[['queries count', 'videos']]

    return result_df

def videosPerQuery(df):
    result_df = df.groupby('searchTerm')['url'].apply(list).reset_index()
    result_df['videos'] = result_df['url'].apply(len)
    result_df = result_df[['searchTerm', 'videos']].sort_values(by='videos', ascending=False)

    return result_df

def countNumUniqueVideos(df):
    result_df = df.groupby('url')['upload time'].apply(list).reset_index()
    result_df = result_df[['url']]

    return result_df

def countNumOfVideoOccurences(df):
    result_df = df.groupby('count')['url'].apply(list).reset_index()
    result_df['accounts'] = result_df['url'].apply(len)
    result_df = result_df[['count', 'accounts']].sort_values(by='count', ascending=False)

    return result_df

def countViewsPerAccount(df):
    df = df[df['user name'].notna()]
    df = df[df['likes'].notna()]
    df = df[df['user name'] != 'None']
    df = df[df['likes'] != 'None']
    result_df = df.groupby('user name')['likes'].sum().reset_index()
    result_df = result_df.sort_values(by='likes', ascending=False)
    return result_df.head(10)

def createAccountsDistribution(df):
    df = df[df['user name'] != 'None']
    result_df = df.groupby('user name')['url'].apply(list).reset_index()
    result_df['frequency'] = result_df['url'].apply(len)
    result_df = result_df[['user name', 'frequency']].sort_values(by='frequency', ascending=False)
    return result_df.head(10)

def countFreshContent(df, platform):
    if platform == "TIKTOK":
        freshness_df = checkFreshnessOfDataTikTok(df)
    else:
        freshness_df = checkFreshnessOfData(df)
    freshness_by_hours_df = postedLessThanXHoursAgo(freshness_df, 24)
    mean_hours = round(freshness_by_hours_df['rank'].mean(), 2)
    median_hours = freshness_by_hours_df['rank'].median()
    percentage_hours = round(len(freshness_by_hours_df) / len(df) * 100, 2)
    
    return {
        "platform": platform,
        "percentage": percentage_hours,
    }

def createPlatformStats(df, function):
    platforms = df['platform'].unique()
    platform_stats = []
    
    for platform in platforms:
        platform_df = df[df['platform'] == platform]
        stats = function(platform_df, platform)
        platform_stats.append(stats)
    
    result_df = pd.DataFrame(platform_stats)
    result_df = result_df.set_index('platform')
    return result_df
    

def forEachPlatform(df, function, metric1, metric2, top10=""):
    platforms = df['platform'].unique()
    platform_distributions = {}

    for platform in platforms:
        platform_df = df[df['platform'] == platform]
        platform_distributions[platform] = function(platform_df)
    
    # Create a DataFrame to store the ranked accounts
    all_ranks = pd.DataFrame()

    for platform, distribution in platform_distributions.items():
        distribution = distribution.reset_index(drop=True)
        distribution = distribution[[metric1, metric2]]
        distribution.columns = [f'{platform}_{top10}', f'{platform}_{metric2}_{top10}']
        if all_ranks.empty:
            all_ranks = distribution
        else:
            all_ranks = pd.concat([all_ranks, distribution], axis=1)
    
    return all_ranks


def forEachPlatform2(df, function, metric1, metric2, top10=""):
    platforms = df['platform'].unique()
    platform_distributions = {}

    for platform in platforms:
        platform_df = df[df['platform'] == platform]
        platform_distributions[platform] = function(platform_df)
    
    # Create a DataFrame to store the ranked accounts
    all_ranks = pd.DataFrame()

    for platform, distribution in platform_distributions.items():
        distribution = distribution.reset_index(drop=True)
        distribution = distribution[[metric1, metric2]]
        distribution.columns = [f'{metric1}', f'{platform}_{metric2}_{top10}']
        if all_ranks.empty:
            all_ranks = distribution
        else:
            all_ranks = pd.merge(all_ranks, distribution, on=metric1, how='outer')

    
    all_ranks.fillna(0, inplace=True)
    # Calculate the total excluding the query column
    all_ranks['total'] = all_ranks.drop(columns=[metric1]).sum(axis=1)    
    return all_ranks

def top10Posts(df):
    df = df[df['rank'] <= 10]
    return df

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
    if isinstance(time_str, pd.Timestamp):
        return time_str
    try:
        # Assume the format is 'MM-DD-HH'
        month, day, hour = time_str.split('-')
        return pd.Timestamp(year=2024, month=int(month), day=int(day), hour=int(hour))
    except Exception as e:
        print(f"Error parsing time_str '{time_str}': {e}")
        return None

def checkFreshnessOfData(df):
    # Convert 'collectedTime' to datetime, assuming the year is 2024 for consistency

    # Apply the function to the 'collectedTime' column
    df['collectedTime'] = df['collectedTime'].apply(parse_collected_time)

    # Convert 'upload time' to datetime
    df['upload time'] = df.apply(convert_upload, axis=1)
    # Calculate the difference in hours
    df['time_difference_hours'] = df.apply(calculate_time_difference, axis=1)
    df = df[df['time_difference_hours'].notna()]
    df = df[df['time_difference_hours'] != 'None']

    df = df[['url', 'time_difference_hours', 'collectedTime', 'upload time', 'likes', 'rank', 'user name']]
    return df

def checkFreshnessOfDataTikTok(df):
    # Convert 'collectedTime' to datetime, assuming the year is 2024 for consistency

    # Apply the function to the 'collectedTime' column
    df['collectedTime'] = df['collectedTime'].apply(parse_collected_time)

    # Convert 'upload time' to datetime
    df['upload time'] = df.apply(convert_upload, axis=1)
    df['upload time'] = df['upload time'] - pd.Timedelta(hours=4) #check if i should add hours for this
    # Calculate the difference in hours
    df['time_difference_hours'] = df.apply(calculate_time_difference, axis=1)
    df = df[df['time_difference_hours'].notna()]
    df = df[df['time_difference_hours'] != 'None']

    df = df[['url', 'time_difference_hours', 'collectedTime', 'upload time', 'likes', 'rank', 'user name']]
    return df

def convert_upload(row):
    try:
        return pd.to_datetime(row['upload time'])
    except:
        return None

def calculate_time_difference(row):
    try:
        return (row['collectedTime'] - row['upload time']).total_seconds() / 3600
    except:
        return None

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

def getQueries(df):
    result_df = df.groupby('searchTerm')
    return result_df

def getAccounts(df):
    result_df = df.groupby('user name')
    return result_df

def countNumOfOrg(df):
   org = df['cat'].str.contains('ORG').sum()
   ind = df['cat'].str.contains('IND').sum()

   return org / (org + ind)

def exportToCSV(df):
    df.to_csv('../data.csv')

def statsPerPlatform(df, platform):
    numOfVid = len(df)
    unique_df = getUniqueVideos(df)
    numOfUniqueVid = len(unique_df)
    accounts_df = getAccounts(df)
    numOfAccounts = len(accounts_df)

    return {
        "platform": platform,
        "total videos": numOfVid,
        "unique videos": numOfUniqueVid,
        "number of accounts": numOfAccounts
    }

# df = pd.concat([getAllTrump(), getD()])
df = readData('https://raw.githubusercontent.com/bellesea/platforms_search/main/data.csv')
df = addMoreData(df)
unique_df = getUniqueVideos(df)
author_freq_df = createAccountsDistribution(df)
author_views_freq_df = forEachPlatform(df, countViewsPerAccount, 'user name', 'likes')
platform_ranks = forEachPlatform(df, createAccountsDistribution, 'user name', 'frequency')
query_ranks = forEachPlatform2(df, videosPerQuery, 'searchTerm','videos')

top_10_df = top10Posts(df)
platform_ranks_top_10 = forEachPlatform(top_10_df, createAccountsDistribution, 'user name', 'frequency', top10="top10")
query_ranks_top_10 = forEachPlatform2(top_10_df, videosPerQuery, 'searchTerm','videos')


new_ranks = pd.concat([platform_ranks_top_10, platform_ranks], axis=1)
new_ranks = new_ranks[['facebook_', 'facebook_frequency_', 'facebook_top10', 'facebook_frequency_top10', 'instagram_', 'instagram_frequency_', 'instagram_top10', 'instagram_frequency_top10', 'youtube_', 'youtube_frequency_', 'youtube_top10', 'youtube_frequency_top10']]

freshness_df = checkFreshnessOfData(df)
likes_time_df = countLikesOverTime(freshness_df)
description_wordcloud = createWordCloud(df, column='text')
queries_df = getQueries(df)
accounts_df = getAccounts(df)
# ########################
# # Streamlit stuff
# ########################
st.header("Learning about political content on Social Media")
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
    st.metric(label="Number of accounts", value=len(accounts_df))


st.write(df.head(10))
 
# st.pyplot(engagement_box_plt)
st.markdown("""---""")

st.subheader("Summary of data collection")
summary_data = createPlatformStats(df, statsPerPlatform)
summary_data

st.subheader("How many videos are there per query?")
query_ranks

st.subheader("Who is posting the accounts?")

platform_ranks
author_views_freq_df

st.subheader("How fresh is the content?")
freshness_platform_df = createPlatformStats(df, countFreshContent)
freshness_platform_df


st.subheader("Analyzing top 10 posts")
platform_ranks_top_10