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
import numpy as np
import seaborn as seaborn
import csv
from collections import Counter
import altair as alt

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
            df['url'] = df['id'].apply(lambda x: f"https://instagram.com/p/{x}")
        if data['platform'] == 'tiktok':
            df['url'] = df['id'].apply(lambda x: f"https://www.tiktok.com/@/video/{x}")
        if data['platform'] == 'youtube' and 'url' not in df.columns:
            df['url'] = df['text']
        new_df = pd.concat([main_df, df], axis=0, ignore_index=True)
        main_df = new_df
    return main_df

@st.cache_data
def getAllTrump():
    fileNames = []
    main_df = pd.DataFrame()
    directory = '/Users/belle/Desktop/analysis/data_analysis/trump_assassination'
    fileList = helper.getFilesToCheck(directory, platform='all')
    for files in fileList:
        df = helper.read_post_csv(files)
        data = helper.getDataCollectionParameters(files)
        df['searchTerm'] = data['query']
        df['platform'] = data['platform']
        df['trendingTime'] =  data['trending_time'][0]
        df['collectedTime'] = data['collection_time']
        if data['platform'] == 'instagram':
            df['url'] = df['id'].apply(lambda x: f"https://instagram.com/p/{x}")
        if data['platform'] == 'tiktok':
            df['url'] = df['id'].apply(lambda x: f"https://www.tiktok.com/@/video/{x}")
        if data['platform'] == 'youtube':
             if 'url' not in df.columns or df['url'].isnull().all() or df['url'].eq('').all():
                df['url'] = df['text']
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

@st.cache_data
def cleanData1(df):
    df = df.loc[df.astype(str).drop_duplicates().index]
    df['id'] = df['id'].astype(str)
    df['url'] = df['url'].astype(str)
    # df['success'] = df['success'].astype(str)
    df = df[~df['id'].str.contains('https://www.tiktok', regex=False)]
    df = df[~df['url'].str.contains('login/?', regex=False)]
    df = df[~df['url'].str.contains('https://www.tiktok.com/@/video/could not collect', regex=False)]
    # df = df[~df['success'].str.contains('-1', regex=False)]
    df = df[~df['url'].str.contains('https://www.tiktok.com/share/video/could not collect', regex=False)]
    df['url'] = df.apply(lambda row: clean_fb_url(row['url']) if row['platform'] == 'facebook' else row['url'], axis=1)
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
                month = part.split('-')[1]
                day = part.split('-')[2]
                last = part.split('-1')[-1]
                hour = last.split(' ')[1].split(":")[0]
                collected_time = f"{month}-{day}-{hour}"

                trending_time = None

    return [trending_time, collected_time, search_term]

def getUserCategory(df):
    file_path = './labelledUsers.csv'
    csv_data = pd.read_csv(file_path)
    user_data = []
    for index, row in csv_data.iterrows():
        if "IND" in row['cat']:
            cat = "IND"
        else:
            cat = "ORG"

        entry = {
            'user name': row['user_name'],
            'cat': cat
        }
        user_data.append(entry)

    user_df = pd.DataFrame(user_data)
    user_df = user_df.drop_duplicates(subset='user name', keep='last')
    df = df.merge(user_df, on='user name', how='left')
    return df

def cleanQueries(df):
    approvedQueries = [
    "Trump assassination",
    "Menendez Senator",
    "Trump attempted assassination",
    "Trump gunman",
    "Biden Philly Strike Union",
    "Trump shooting",
    "SCOTUS Biden Social Media",
    "US Supreme Court Biden Social Media",
    "Trump secret service",
    "SCOTUS Social Media Biden",
    "Trump rally",
    "US Supreme Court Biden Social",
    "US Supreme Court Social Media Biden",
    "Abbott Taiwan Governor Texas",
    "Giuliani Disbarment Trump NYC",
    "Trump deep state",
    "US Supreme Court Corruption",
    "SCOTUS Chevron Case Law",
    "Trump shooting staged",
    "Trump shooting conspiracy",
    "Trump",
    "Trump shooter",
    "Biden Hunter White House",
    "Illinois GOP Trump Convention",
    "Trump Antifa",
    "SCOTUS Chevron NRDC Case",
    "Trump assassination",
    "US Supreme Court",
    "SCOTUS Chevron NRDC Law",
    "Supreme Court Chevron Case",
    "USPS United States",
    "SCOTUS Chevron Natural Resources Case",
    "Iowa GOP Trump Convention",
    "Biden EV GM Industry",
    "Trump Sons GOP",
    "Menendez Senator Corruption",
    "Trump Secret Service Fact-check",
    "Thomas Cook Trump",
    "Trump BlackRock",
    "Trump 2024",
    "Pennsylvania Budget Shapiro",
    "Sheila Jackson Lee Texas House",
    "Trump Immigration",
    "Epstein Grand Jury Florida",
    "Iran Nuclear Program Blinken",
    "Biden DNC Israel",
    "Trump FBI Investigation",
    "Sasse UF President Resignation",
    "Ivana New York Trump",
    "Thomas Garland Senate Whitehouse",
    "Russia Ukraine War Institute",
    "Rieckhoff IAVA Independence Day",
    "Baldwin Hutchins Manslaughter",
    "Iowa Abortion Supreme Court",
    "RNC Trump Family GOP",
    "Trump Wisconsin",
    "Afghanistan US Kabul Biden",
    "Biden Abortion First Lady",
    "Trump Conspiracy Theory",
    "Rubio Trump VP Florida",
    "Kelley Hubbard RNC Florida",
    "Iowa Abortion SCOTUS",
    "SCOTUS Trump Riot",
    "Biden Austin Defense",
    "Trump attempted assassination",
    "Biden Apprenticeship Higher Education",
    "SCOTUS Corruption",
    "Trump Jr Eric GOP",
    "Trump RNC Lara GOP",
    "Florida Abortion DeSantis",
    "RNC Milwaukee GOP Police",
    "Sheila Jackson Lee Congress Texas",
    "Dobbs Trump Fox CNN",
    "Trump Corrido",
    "Ukraine Biden",
    "RNC Protest Milwaukee",
    "Biden Trump Election Democratic",
    "Trump Classified Supreme Court",
    "San Diego Trump RNC",
    "RNC Milwaukee GOP Law",
    "Biden Student Loan Forgiveness",
    "Iran Nuclear Israel Biden",
    "RNC Milwaukee Protest",
    "Zelenskyy Putin Ukraine Conflict",
    "RNC Protest",
    "Trump Vance VP GOP",
    "Melania Donald Trump Republican",
    "Eric Trump RNC Republican",
    "Zelenskyy Putin Ukraine Reuters",
    "Trump Iran",
    "Pennsylvania Vehicle Plates Shapiro",
    "Menendez Corruption Closing Argument",
    "Trump FBI DHS",
    "Trump Judge Alaska Misconduct",
    "Hamas Israel Herzog",
    "DOJ Housing Abuse Southwest Key",
    "Biden Democratic Israel Platform",
    "Trump China Tariff",
    "Philippines Biden US China",
    "RNC Republican Party Milwaukee",
    "RNC Republican Milwaukee",
    "RNC Milwaukee GOP",
    "Menendez Corruption Closing Senator",
    "Inhofe Senate Oklahoma Republican",
    "Zelenskyy Ukraine Russia NATO",
    "Trump July 15",
    "RFK Jr North Carolina Ballot",
    "RFK Jr NC Ballot Access",
    "Kennedy North Carolina Ballot Access",
    "Kennedy NC Ballot Access",
    "Harris Biden Dallas AKA",
    "Newsom Oakland CHP DA",
    "Luxon New Zealand Biden NATO",
    "Blinken Iran Nuclear Agreement",
    "Virginia Election Recount GOP",
    "Trump UAV",
    "Trump Human Injury",
    "Trump Campaign Social Media",
    "North Korea Kim Drama",
    "Wisconsin Recall Vos Republican",
    "Utah Redistricting Supreme Court",
    "Trump Secret Service",
    "Trump RNC Wisconsin",
    "Trump Electoral Roll Registration",
    "Pennsylvania Budget Shapiro Education",
    "Giuliani NYC Mayor",
    "GOP Convention 2024",
]

    filtered_df = df[df['searchTerm'].isin(approvedQueries)]

    return filtered_df

@st.cache_data
def pickOneDataframe(df):
    grouped_df = df.groupby(['platform', 'searchTerm', 'collectedTime'])['url'].apply(list).reset_index()
    grouped_df = grouped_df.drop_duplicates(subset=['platform', 'searchTerm'], keep='first')
    result_rows = []
    
    for _, row in grouped_df.iterrows():
        platform = row['platform']
        searchTerm = row['searchTerm']
        collectedTime = row['collectedTime']
        urls = row['url']
        
        for url in urls:
            filtered_rows = df[
                (df['url'] == url) &
                (df['collectedTime'] == collectedTime) &
                (df['platform'] == platform) &
                (df['searchTerm'] == searchTerm)
            ]
            result_rows.append(filtered_rows)
            
    # Concatenate the result rows into a single dataframe
    result_df = pd.concat(result_rows, ignore_index=True)
    # Drop duplicates from the resulting dataframe
    result_df = result_df.loc[result_df.astype(str).drop_duplicates().index]

    return result_df


# #########################
# # Process Data
# #########################

# @st.cache_data

from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

def clean_fb_url(full_url):
    if 'watch' in full_url:
        return full_url
    
    parsed_url = urlparse(full_url)
    query_params = parse_qs(parsed_url.query)

    # Retain only the 'story_fbid' parameter
    clean_query_params = {k: v for k, v in query_params.items() if k == 'story_fbid'}

    # Rebuild the query string
    clean_query_string = urlencode(clean_query_params, doseq=True)

    # Rebuild the URL
    clean_url = urlunparse(parsed_url._replace(query=clean_query_string))
    
    return clean_url

def getUniqueVideos(df):
    df = df.drop_duplicates(subset='url', keep='last')
    return df

@st.cache_data
def addMoreData(df):
    df['likes'] = pd.to_numeric(df['likes'], errors='coerce')
    df['text'] = df['text'].astype(str)
    trending_time_grouped = df.groupby('url')['trendingTime'].apply(list).reset_index()
    df = df.drop(columns='trendingTime').drop_duplicates().merge(trending_time_grouped, on='url')
    df['firstTrending'] = df['trendingTime'].apply(getFirstTrending)
    return df

def getFirstTrending(trendingTime):
    try:
        firstTrending = min(trendingTime)
    except:
        timefmt = "%m-%d-%H"
        firstTrending = datetime.strptime("01-01-00", timefmt).replace(year=2024)
    
    return firstTrending

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
    # if platform == "instagram":
    #     # f = checkFreshnessOfDataInstagram(df)
    #     # f = f[f['fresh'] == 1]
    #     # total_posts = len(f)
    #     # fresh_posts = f['fresh'].sum()
    #     # fresh_percentage = (fresh_posts / total_posts) * 100
    #     # st.write(f"NEW WAY {fresh_posts}")
    #     # st.write(f)

    #     # print("HIII")
    #     freshness_df = checkFreshnessOfData(df)
    #     freshness_by_hours_df = postedLessThanXHoursAgo(freshness_df, 24)
    #     st.write(freshness_by_hours_df)
    #     st.write(f"all: {len(freshness_df)}, fresh: {len(freshness_by_hours_df)}")
    #     mean_hours = round(freshness_by_hours_df['rank'].mean(), 2)
    #     median_hours = freshness_by_hours_df['rank'].median()
    #     percentage_hours = round(len(freshness_by_hours_df) / len(df) * 100, 2)
    # else:
    # #     return {
    # #     "platform": platform,
    # #     "percentage": fresh_percentage,
    # # }


    df['upload time'] = df.apply(convert_upload, axis=1)

    if platform == "tiktok":
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
        #if not (platform == 'youtube'):
        platform_df = df[df['platform'] == platform]
        stats = function(platform_df, platform)
        platform_stats.append(stats)
    
    stats = function(df, "all")
    platform_stats.append(stats)
    
    result_df = pd.DataFrame(platform_stats)
    # result_df = result_df.set_index('platform')
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
        distribution.columns = [f'{platform}', f'{platform[0]}likes']
        if all_ranks.empty:
            all_ranks = distribution
        else:
            all_ranks = pd.concat([all_ranks, distribution], axis=1)
    
    return all_ranks

def forEachPlatform3(df):
    platforms = df['platform'].unique()
    all_ranks = pd.DataFrame()

    for platform in platforms:
        platform_df = df[df['platform'] == platform]
        d = countAccountCategory2(platform_df)
        
        rank = pd.DataFrame({
            'platform': [platform] * 1,
            'account': d['accounts'],
            'likes': d['likes'],
            'posts': d['videos']
        })
        
        all_ranks = pd.concat([all_ranks, rank], axis=0)

    return all_ranks.reset_index(drop=True)

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

def createWordCloud(df, column, width=400, height=200, filter = None):
    # Concatenate all queries into a single string
    text = ' '.join(df[column])
    #if we give filter words, will make new set of words without the one in the filter
    if filter:
        filtered = ' '.join([word for word in text.split() if not any(f.lower() in word.lower() for f in filter)])
    else:
        filtered = text
    # Create word cloud
    wc = WordCloud(width=width, height=height, background_color='white', colormap="winter").generate(filtered)

    # Display the word cloud using matplotlib
    fig = plt.figure(figsize=(10, 6))
    plt.imshow(wc, interpolation='bilinear')
    plt.axis('off')

    return fig

def boxPlot(df):
    f2 = plt.figure(figsize=(15, 10))
    seaborn.boxplot(x='platform', y='likes', data=df, hue='platform', palette="Blues_d")

    plt.title('Likes per Platform')
    plt.xlabel('Platform')
    plt.ylabel('Number of Likes (logarithmic)')
    plt.yscale('log')
    
    return f2

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

    df = df[['url', 'time_difference_hours', 'collectedTime', 'upload time', 'likes', 'rank', 'user name', 'platform']]
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

def checkFreshnessOfDataInstagram(df):
    # Convert 'collectedTime' to datetime, assuming the year is 2024 for consistency
    df['collectedTime'] = df['collectedTime'].apply(parse_collected_time)

    # Convert 'upload time' to datetime
    df['upload time'] = df.apply(convert_upload, axis=1)
    df['upload time'] = df['upload time']

    # Extract month and day
    df['collected_month_day'] = df['collectedTime'].dt.strftime('%m-%d')
    df['upload_month_day'] = df['upload time'].dt.strftime('%m-%d')

    # Calculate 'fresh' column
    df['fresh'] = ((df['collected_month_day'] == df['upload_month_day']) | 
                   (df['collected_month_day'] == (df['upload time'] + pd.Timedelta(days=1)).dt.strftime('%m-%d'))).astype(int)

    df = df[['url', 'collectedTime', 'upload time', 'collected_month_day', 'upload_month_day', 'likes', 'rank', 'user name', 'fresh', 'platform']]

    return df

def checkRecentnessOfContent(df):
    df['collectedTime'] = df['collectedTime'].apply(parse_collected_time)
    # df['firstTrending'] = df['firstTrending'].apply(parse_trending_time)
    df['trendingCollectedDifference'] = df.apply(calculate_time_difference_trending, axis=1)
    df['firstTrending'] = df['firstTrending'].apply(parse_collected_time)
    df = df[df['trendingCollectedDifference'].notna()]
    df = df[df['trendingCollectedDifference'] != 'None']
    timefmt = "%m-%d-%H"
    time = datetime.strptime("01-01-00", timefmt).replace(year=2024)
    df = df[df['firstTrending'] != time]
    return df

def convert_upload(row):
    try:
        return pd.to_datetime(row['upload time'])
    except:
        return None

def calculate_time_difference(row):
    try:
        time_difference = row['collectedTime'] - row['upload time']
        hours_difference = time_difference.total_seconds() / 3600
        return hours_difference
    except:
        return None

def calculate_time_difference_trending(row):
    try:
        return (row['collectedTime'] - row['firstTrending']).total_seconds() / 3600
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

def exportUsers(df):
    users = df['user name'].unique()
    with open('./users.csv', "w") as file:
       writer = csv.writer(file)
       for item in users:
            writer.writerow([item])


def countNumOfOrg(df):
   org = df['cat'].str.contains('ORG').sum()
   ind = df['cat'].str.contains('IND').sum()

   return org / (org + ind)

def exportToCSV(df):
    df.to_csv('../trumpData.csv')

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

def hoursSincePostedPerRank(df):
    freshness_df = checkFreshnessOfData(df)
    freshness_df = freshness_df[freshness_df['time_difference_hours'].notna()]
    freshness_df = freshness_df[freshness_df['time_difference_hours'] != 'None']
    rank_grouped = freshness_df.groupby(['rank', 'platform']).agg({
        'url': list,
    'time_difference_hours': list
    }).reset_index()
    rank_grouped['avg_hours'] = rank_grouped['time_difference_hours'].apply(lambda x: sum(x) / len(x))
    rank_grouped['median_hours'] = rank_grouped['time_difference_hours'].apply(lambda x: np.median(x))
    rank_grouped = rank_grouped[['rank', 'platform', 'avg_hours', 'median_hours']]

    return rank_grouped

def hoursSincePostedPerRank2(df):
    # Check freshness of data
    freshness_df = checkFreshnessOfData(df)
    
    # Filter out rows where 'time_difference_hours' is not available or 'None'
    freshness_df = freshness_df[freshness_df['time_difference_hours'].notna()]
    freshness_df = freshness_df[freshness_df['time_difference_hours'] != 'None']
    freshness_df = freshness_df[(freshness_df['rank'] >= 0) & (freshness_df['rank'] <= 9)]

    # Group by 'rank' and 'platform' and aggregate 'time_difference_hours'
    rank_grouped = freshness_df.groupby(['platform']).agg({
        'url': list,
        'time_difference_hours': list
    }).reset_index()
    
    rank_grouped['median_hours'] = rank_grouped['time_difference_hours'].apply(lambda x: np.median(x))
    
    rank_grouped = rank_grouped[['platform', 'median_hours']]
    
    return rank_grouped

def createHoursPlot(df):
    df = df.head(40)
    fig = plt.figure(figsize=(12, 8))
    seaborn.barplot(data=df, y='median_hours', x='rank', hue='platform', palette="Blues_d") 
    plt.ylabel('Median Hours')
    plt.xlabel('Rank')
    plt.title('Median Hours Since Posted Per Rank and Platform')
    plt.legend(title='Platform')
    # plt.show()
    return fig

def createOverallHoursPlot(df):
    df = df.head(300)
    fig2 = plt.figure(figsize=(12, 8))
    seaborn.barplot(data=df, y='median_hours', x='rank', palette="Blues_d") 
    plt.ylabel('Median Hours')
    plt.xlabel('Rank')
    plt.xticks(rotation=90)
    plt.title('Median Hours Since Posted Per Rank for all platforms')
    plt.legend(title='Platform')
    # plt.show()
    return fig2

def getTopVideos(df):
    df = getUniqueVideos(df)
    result_df = df[['url', 'likes']].sort_values(by='likes', ascending=False)
    result_df = result_df.sample(20)
    return result_df

@st.cache_resource
def createHorizontalBar(df):
    seaborn.set_theme(style="whitegrid")

    f, ax = plt.subplots(figsize=(4, 4))

    seaborn.barplot(x="percentage", y="platform", data=df, palette="Blues_d")

    ax.set_title("Percentage of posts created within 24 Hours of collection", fontsize=14, weight='bold')
    ax.set(xlim=(0, 25), ylabel="Platform", xlabel="Percentage (%)")
    
    ax.xaxis.grid(True, linestyle='--', alpha=0.7)
    ax.yaxis.grid(False)
    seaborn.despine(left=True, bottom=True)

    return f

def createBar(df):
    df = df[df['trendingCollectedDifference'] < 513]
    seaborn.set_theme(style="whitegrid")

    f2, ax = plt.subplots(figsize=(8, 4))

    seaborn.barplot(x="trendingCollectedDifference", y="percentageOrg", data=df, palette="Blues_d")

    ax.set_title("Percentage of posts created within 24 Hours of collection", fontsize=14, weight='bold')
    ax.set(ylabel="percentageOrg", xlabel="trendingCollectedDifference")
    
    ax.xaxis.grid(True, linestyle='--', alpha=0.7)
    ax.yaxis.grid(False)
    seaborn.despine(left=True, bottom=True)

    return f2
def getAccNum(df, category):
    row = df.loc[df['cat'] == category]
    if not row.empty:
        return row['count'].values[0]
    else:
        return f"Category '{category}' not found."

def countAccountCategory(df):
    org = df[df['cat'] == 'ORG']
    ind = df[df['cat'] == 'IND']
    org_likes = org['likes'].sum()
    ind_likes = ind['likes'].sum()
    acc_num = df.groupby('cat')['user name'].apply(lambda x: list(set(x))).reset_index()
    acc_num['count'] = acc_num['user name'].apply(len)

    # data = {
    #     "org": {
    #         "account": getAccNum(acc_num, 'ORG'),
    #         "likes": org_likes,
    #         "videos": len(org),

    #     },
    #     "ind": {
    #         "account": getAccNum(acc_num, 'IND'),
    #         "likes": ind_likes,
    #         "videos": len(ind),

    #     },
    # }

    new_data = {
        "type": ['# of accounts', '# of videos', 'total likes',],
        "percentage" : [getAccNum(acc_num, "ORG") / len(getAccounts(df)) *100 , len(org) / len(df) * 100, org_likes / (org_likes + ind_likes) *100],
        "total": [100, 100, 100]
    }

    new_df = pd.DataFrame(new_data)
    return new_df

def countAccountCategory2(df):
    org = df[df['cat'] == 'ORG']
    ind = df[df['cat'] == 'IND']
    org_likes = org['likes'].sum()
    ind_likes = ind['likes'].sum()
    acc_num = df.groupby('cat')['user name'].apply(lambda x: list(set(x))).reset_index()
    acc_num['count'] = acc_num['user name'].apply(len)

    new_data = {
        'accounts' : getAccNum(acc_num, "ORG") / len(getAccounts(df)) *100,
        'videos' : len(org) / len(df) * 100, 
        'likes': org_likes / (org_likes + ind_likes) *100,
    }

    return new_data

def countLikesPerAccountCat(df):
    # Group by 'cat' and calculate the mean of the 'likes' column
    avg_likes_per_cat = df.groupby('cat')['likes'].mean().reset_index()
    
    # Rename the column for clarity
    avg_likes_per_cat.rename(columns={'likes': 'average_likes'}, inplace=True)
    
    return avg_likes_per_cat

def createAccountCatChart(df):
    seaborn.set_theme(style="whitegrid")

    fig3, ax = plt.subplots(figsize=(8, 4))
    

    ax.set_title("Share of videos posted by ORGS vs INDIVIDUALS", fontsize=14, weight='bold')
    ax.set(xlim=(0, 100), xlabel="Percentage (%)", ylabel=" ")
    seaborn.barplot(x="total", y="type", data=df,
                label="individuals", color="steelblue")

    seaborn.barplot(x="percentage", y="type", data=df, label="organizations", color="lightblue")

    
    ax.xaxis.grid(True, linestyle='--', alpha=0.7)
    ax.yaxis.grid(False)
    seaborn.despine(left=True, bottom=True)

    return fig3

def trackPostOverTime(df, metric):
    trump_ass = df[df['searchTerm'] == "Trump assassination"]
    trump_ass_url = trump_ass.groupby('url')['collectedTime'].apply(list).reset_index()
    trump_ass_url['Count'] = trump_ass_url['collectedTime'].apply(len)
    filtered_trump = trump_ass_url[trump_ass_url['Count'] >= 3]
    url_3 = filtered_trump[['url', 'Count']]
    merged = pd.merge(trump_ass, url_3, on='url', how='inner')
    finaldf = merged.copy()
    sortedurl = url_3.sort_values(by = 'Count', ascending = False)
    top_11_urls = sortedurl.head(11)['url']
    last = trump_ass[trump_ass['url'].isin(top_11_urls)]
    last2 = last[last['url'] != 'could not collect']
    last2['collectedTime'] = pd.to_datetime(last2['collectedTime'])
    last2 = last2.drop_duplicates(subset=["collectedTime", 'url']).sort_values(by = 'collectedTime', ascending = False)
    last2 = last2[last2['likes'].notna()]
    # last2 = last2[last2['collectedTime'] != '2024-07-16 00:00:00'] #
    groups = last2.groupby('url')
    
    fig4 = plt.figure(figsize = (12, 8))

    cmap = plt.get_cmap('BuPu')
    colors = cmap(np.linspace(0, 1, 10))
    i = 0
    for name, group in groups:
        plt.plot(group['collectedTime'], group[metric], marker = 'o', label = name, color=colors[i])
        i += 1
    plt.title(f'Change in rank over time for 10 posts')
    plt.xlabel('Time at collection')
    plt.ylabel('Rank on search results')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.gca().invert_yaxis()
    return fig4

@st.experimental_fragment
def getSample(df):
    query = st.selectbox("Which query do you want to see videos from?", options=df['searchTerm'].unique())
    if query:
        df = df[df['searchTerm'] == query]
        st.write(df.sample(10))

def calculate_org_percentage(group):
    total_count = len(group)
    org_count = 0
    for i in group:
        if (i == 'ORG'):
            org_count += 1
    return (org_count / total_count) * 100

def orgTimePercentage(df):
    #percentage_df = df.groupby(['trendingCollectedDifference']).apply(calculate_org_percentage).reset_index(name='orgDistribution')
    grouped = df.groupby('trendingCollectedDifference').agg({
    'url': list,
    'cat': list  # Aggregate 'cat' as a list
    }).reset_index()
    grouped['percentageOrg'] = grouped['cat'].apply(calculate_org_percentage)
    return grouped

@st.experimental_fragment
def findWordFrequency(df, filter):
    word_to_find = st.text_input("What word do you want to find?")
    text = ' '.join(df['text'])
    filtered = ' '.join([word for word in text.split() if not any(f.lower() in word.lower() for f in filter)])
    words = filtered.split()
    word_counts = Counter(words)
    word_frequency = word_counts[word_to_find]
    ranked_words = word_counts.most_common()
    word_rank = next((rank for rank, (word, count) in enumerate(ranked_words, start=1) if word == word_to_find), None)

    st.write(f"Frequency: {word_frequency}; Rank: {word_rank}")

# df = pd.read_csv('/Users/belle/Desktop/analysis/data.csv')
df = pd.concat([getAllTrump(), getD()])
# df = readData('https://raw.githubusercontent.com/bellesea/platforms_search/main/data.csv')
# df = getAllTrump()
df = addMoreData(df)
df = cleanData1(df)
df = getUserCategory(df)
df = cleanQueries(df)
# exportToCSV(df)

unique_df = getUniqueVideos(df)

author_freq_df = createAccountsDistribution(df)
queries_df = getQueries(df)
accounts_df = getAccounts(df)

author_views_freq_df = forEachPlatform(df, countViewsPerAccount, 'user name', 'likes')
platform_ranks = forEachPlatform(df, createAccountsDistribution, 'user name', 'frequency')

one_df = pickOneDataframe(df)
author_views_freq_df_one = forEachPlatform(one_df, countViewsPerAccount, 'user name', 'likes')
platform_ranks_one = forEachPlatform(one_df, createAccountsDistribution, 'user name', 'frequency')
top_10_df = top10Posts(df)

# query_ranks_top_10 = forEachPlatform2(top_10_df, videosPerQuery, 'searchTerm','videos')
# new_ranks = pd.concat([platform_ranks_top_10, platform_ranks], axis=1)
# new_ranks = new_ranks[['facebook_', 'facebook_frequency_', 'facebook_top10', 'facebook_frequency_top10', 'instagram_', 'instagram_frequency_', 'instagram_top10', 'instagram_frequency_top10', 'youtube_', 'youtube_frequency_', 'youtube_top10', 'youtube_frequency_top10']]

ranked_df = hoursSincePostedPerRank(df)
ranked_2_df = hoursSincePostedPerRank2(df)
pattern = r'Trump\s*assassination|Trump\s*assasination'
time = datetime.strptime("07-12-00", helper.timefmt).replace(year=2024)
trump_queries = df[df['searchTerm'].str.contains(pattern, case=False, regex=True)]
# trump_queries = trump_queries[trump_queries['upload time'] > time]

# trump_queries = getUniqueVideos(trump_queries)

# exportToCSV(trump_queries)
t_youtube = trump_queries[trump_queries['platform'] == 'youtube']
top_10_likes = forEachPlatform(trump_queries, getTopVideos, 'url', 'likes')
# top_10_likes = top_10_likes.sort_values(by='likes', ascending=True)

# hii = checkFreshnessOfDataInstagram(trump_queries)
# hii = hii[hii['fresh'] == 1]
# hii = hii[hii['platform'] == 'instagram']

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

st.subheader("Summary of data collection")
summary_data = createPlatformStats(df, statsPerPlatform)
summary_data

st.subheader("How many videos are there per query?")
query_ranks = forEachPlatform2(df, videosPerQuery, 'searchTerm','videos')
query_ranks

getSample(df)

st.markdown("""---""")

st.header("Looking at freshness across platform")

st.subheader("Analyzing post freshness by rank")
st.caption("Looking at the first 10 posts seen in the search results for each platform")

chart = alt.Chart(ranked_df.head(40)).mark_bar().encode(
    x=alt.X('rank:O'),
    y='median_hours:Q',
    color=alt.Color('platform:N', scale=alt.Scale(scheme='blues'))
).properties(
    width=600,
    height=400
).configure_axis(
    labelFontSize=12,
    titleFontSize=14
).configure_legend(
    titleFontSize=14,
    labelFontSize=12
)

st.altair_chart(chart)

st.caption("Looking at the the correlation in time passed between posting and being seen on the search page and rank")
chart2 = alt.Chart(ranked_df.head(300)).mark_bar().encode(
    x=alt.X('rank:O'),
    y='median_hours:Q',
    color=alt.Color('platform:N', scale=alt.Scale(scheme='blues'))
).properties(
    width=600,
    height=400
).configure_axis(
    labelFontSize=12,
    titleFontSize=14
).configure_legend(
    titleFontSize=14,
    labelFontSize=12
)

st.altair_chart(chart2)

# # hours_chart = createHoursPlot(ranked_df)
# hours_all_chart = createOverallHoursPlot(ranked_df)
# # st.pyplot(hours_chart)
# st.pyplot(hours_all_chart)

st.subheader("Who is creating the videos?")
st.caption("What percentage of X on each platform comes from organizations?")
platform_org_cat = forEachPlatform3(df)
st.write(platform_org_cat)
st.caption("Top 10 accounts by total engagement received by the account")
author_views_freq_df_one
st.caption("Top 10 accounts by number of posts created")
platform_ranks_one

st.header("Case Study: Posts about Trump Assassination")
st.write("Looking at data about trump")
trump_summary = createPlatformStats(trump_queries, statsPerPlatform)
freshness_platform_df_t = createPlatformStats(trump_queries, countFreshContent)
unique_df_t = getUniqueVideos(trump_queries)
queries_df_t = getQueries(trump_queries)
accounts_df_t = getAccounts(trump_queries)

if st.checkbox('Trump: Show raw data'):
    st.write(trump_queries)

one, two, three, four = st.columns(4)
with one:
    st.metric(label="Collected videos", value=len(trump_queries))
with two:
    st.metric(label="Unique videos", value=len(unique_df_t))
with three:
    st.metric(label="Queries searched", value=len(queries_df_t))
with four:
    st.metric(label="Number of accounts", value=len(accounts_df_t))

st.write(trump_summary)
# st.write(freshness_platform_df_t)
# st.bar_chart(freshness_platform_df_t, x='platform', y='percentage',  color='#5481A6')
chart3 = alt.Chart(freshness_platform_df_t).mark_bar().encode(
    x=alt.X('platform:N', title='Platform'),
    y=alt.Y('percentage:Q', title='Percentage'),
    color=alt.Color('platform:N', scale=alt.Scale(scheme='blues'))
).properties(
    width=600,
    height=400
).configure_axis(
    labelFontSize=12,
    titleFontSize=14
).configure_legend(
    titleFontSize=14,
    labelFontSize=12
)
st.altair_chart(chart3)

st.subheader("Engagement for videos")
box_plot = boxPlot(trump_queries)
st.pyplot(box_plot)

st.subheader("How do posts do over time?")
posts_over_time = trackPostOverTime(df, "rank")
st.pyplot(posts_over_time)


st.subheader("What are people saying?")
bad_words = ["https", 'bit ly', 'nan', 'bit', 'ly', 'youtube', 'twitter', 'facebook', 'instagram', 'https twitter', 'Facebook https', 'App https', 'app', 'Subscribe', 'bio', 'link', 'follow CBS', 'CBS New', 'NBC New', 'None None', 'None'] #'Trump', 'trump', 'assassination attempt', 'assassination', 'attempted assassination', 'donald', 'Donald', 'former President', 'attempt'
description1_wordcloud = createWordCloud(trump_queries, column='text', filter = bad_words)
st.pyplot(description1_wordcloud)

findWordFrequency(trump_queries, filter = bad_words)
