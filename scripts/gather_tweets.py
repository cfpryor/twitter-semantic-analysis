import datetime
import json
import os
import re
import sys
import time

from TwitterSearch import *

# Directory Paths
RELATIVE_PATH = os.path.dirname(__file__)
PRIVATE_FILES_PATH = os.path.join(RELATIVE_PATH, "..", "private_files")
DATA_PATH = os.path.join(RELATIVE_PATH, "..", "data")
CACHE_PATH = os.path.join(DATA_PATH, "cache")

# File Paths Keys
KEYWORD_PATH = os.path.join(PRIVATE_FILES_PATH, "keywords.txt")
TWITTER_KEYS_PATH = os.path.join(PRIVATE_FILES_PATH, "twitter_keys.json")
CURRENT_ID_PATH = os.path.join(PRIVATE_FILES_PATH, "current_id_path.json")

# Tweet Gathering Rate
TWEET_MAX = 17000
QUERY_MAX = 170
DEFAULT_COUNT = 100

# Sleeptime
QUERIES_PER_SLEEP = 12
WAIT_TIME = 61

# Number of days looking back
DEFAULT_LOOKBACK = 7

def initiate_query_times(lookback = DEFAULT_LOOKBACK):
    query_times = [0] * lookback
    current_time = datetime.date.today()
    
    # Initiate query ranges 
    for i in range(0, len(query_times)):
        query_times[len(query_times) - i - 1] = current_time - datetime.timedelta(days = i)
    
    return query_times

def write_query(query_dict, path, human_readable = False):
    if query_dict != None and query_dict:
        with open(path, "w") as file:
            if human_readable:
                json.dump(query_dict, file, indent=4)
            else:
                json.dump(query_dict, file)

def my_callback_closure(current_ts_instance):
        queries, tweets_seen = current_ts_instance.get_statistics()
        if queries > 0 and (queries % QUERIES_PER_SLEEP) == 0:
            print("Waiting for %d seconds." % (WAIT_TIME))
            sys.stdout.flush()
            time.sleep(WAIT_TIME)

def query_data(keywords, count = DEFAULT_COUNT, since_id = None, or_oper = True, start = None, stop = None, result_type = None, language = None, entities = None):
    if not os.path.isfile(TWITTER_KEYS_PATH):
        print("Twitter Keys Does Not Exist: %s" % (TWITTER_KEYS_PATH))
        sys.exit(1)

    with open(TWITTER_KEYS_PATH) as file:
        twitter_keys = json.load(file)
    
    try:
        tso = TwitterSearchOrder()
        tso.set_keywords(keywords, or_operator = or_oper)

        if language != None:
            tso.set_language(language)
        if result_type != None:
            tso.set_result_type(result_type)
        if start != None:
            tso.set_since(start)
        if stop != None:
            tso.set_until(stop)
        if entities != None:
            tso.set_include_entities(entities)
        if since_id != None:
            tso.set_since_id(since_id)

        ts = TwitterSearch(
            consumer_key = twitter_keys["consumer_key"],
            consumer_secret = twitter_keys["consumer_secret"],
            access_token = twitter_keys["access_token"],
            access_token_secret = twitter_keys["access_token_secret"],
        )

        print("Starting Query. Key: %s\tQuery URL: %s" % (keywords[0], tso.create_search_url()))
        sys.stdout.flush()

        current_query_dict = {}
        for tweet in ts.search_tweets_iterable(tso, my_callback_closure):
            current_query_dict[tweet['id']] = tweet

        return current_query_dict
    
    except TwitterSearchException as e:
        print(e)
        sys.exit(1)

def load_current_id():
    # If the last id is stored use it
    if os.path.isfile(CURRENT_ID_PATH):
        with open(CURRENT_ID_PATH, 'r') as file:
            return json.load(file)[-1]
    
    # Find the most recent id otherwise
    id_list = []
    for filename in os.listdir(CACHE_PATH):
        with open(os.path.join(CACHE_PATH, filename), 'r') as file:
            data = json.load(file)
            for key in data:
                id_list.append(int(key))
    
    # Store the most recent id
    with open(CURRENT_ID_PATH, 'w') as file:
        json.dump([max(id_list)], file, indent=4)
    
    return max(id_list)

def load_keywords(path):
    keyword_list = []
    with open(path, 'r') as file:
        for line in file:
            keyword_list.append([line.strip()])
    return keyword_list

def continuous_tweet_collection(keyword_path = KEYWORD_PATH, tweet_max = TWEET_MAX, query_max = QUERY_MAX, count = DEFAULT_COUNT, wait_time = WAIT_TIME):
    # Initiate the times to starting value
    keywords = load_keywords(keyword_path)
    current_id = load_current_id()
    query_count = 0

    while(True):
        id_list = []
        for keyword in keywords:
            query_dict = query_data(keyword, since_id = current_id)
            for key in query_dict:
                id_list.append(int(key))

            print("Writing. Tweet Count: %d\tKey: %s\tCurrent time: %s" % (len(query_dict), keyword[0], str(datetime.datetime.now())))
            write_query(query_dict, os.path.join(CACHE_PATH, re.sub('[#@\- ]', '', keyword[0]) + "_" + str(datetime.datetime.now().strftime("%s")) + ".txt"))

            query_count += (int(len(query_dict) / count) + 1) % QUERIES_PER_SLEEP
            if query_count >= QUERIES_PER_SLEEP:
                print("Waiting for %d seconds." % (WAIT_TIME))
                sys.stdout.flush()
                time.sleep(wait_time)
                query_count = query_count - QUERIES_PER_SLEEP
        
        with open(CURRENT_ID_PATH, 'r') as file:
            curr_id_data = json.load(file)
        with open(CURRENT_ID_PATH, 'w') as file:
            curr_id_data.append(max(id_list))
            json.dump(curr_id_data, file, indent = 4)
        break
    return

def past_tweet_collection(keyword_path = KEYWORD_PATH, tweet_max = TWEET_MAX, query_max = QUERY_MAX, lookback = DEFAULT_LOOKBACK, count = DEFAULT_COUNT, wait_time = WAIT_TIME):
    # Initiate the times to starting value
    query_times = initiate_query_times()
    keywords = load_keywords(keyword_path)
    query_count = 0

    # Create Cache Directory if it doesn't exist
    if not os.path.exists(CACHE_PATH):
        os.makedirs(CACHE_PATH)

    # Start to grab tweets for the past "lookback" number of days
    for times in range(2, len(query_times) - 1):
        for keyword in keywords:
            # Call and write the query
            query_dict = query_data(keyword, start = query_times[times], stop = query_times[times + 1])
            print("Writing. Tweet Count: %d\tKey: %s\tCurrent time: %s" % (len(query_dict), keyword[0], str(datetime.datetime.now())))
            write_query(query_dict, os.path.join(CACHE_PATH, re.sub('[#@\- ]', '', keyword[0]) + "_" + str(datetime.datetime.now().strftime("%s")) + ".txt"))
            query_count += (int(len(query_dict) / count) + 1) % QUERIES_PER_SLEEP

            if query_count >= QUERIES_PER_SLEEP:
                print("Waiting for %d seconds." % (WAIT_TIME))
                sys.stdout.flush()
                time.sleep(wait_time)
                query_count = query_count - QUERIES_PER_SLEEP

if (__name__ == '__main__'):
    continuous_tweet_collection()
    #past_tweet_collection()
