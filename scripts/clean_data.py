import datetime
import json
import os
import time
import re
import sys

from nltk.corpus import stopwords
from pycorenlp import StanfordCoreNLP
from textblob import TextBlob 
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Directory Paths
RELATIVE_PATH = os.path.dirname(__file__)
PRIVATE_FILES_PATH = os.path.join(RELATIVE_PATH, "..", "private_files")
DATA_PATH = os.path.join(RELATIVE_PATH, "..", "data")

# Cache Paths
CACHE_PATH = os.path.join(DATA_PATH, "cache")
PROCESSED_CACHE_PATH = os.path.join(DATA_PATH, "processed_cache")

# File Paths Keys
DESIRED_KEYWORDS_PATH = os.path.join(PRIVATE_FILES_PATH, "desired_keywords.json")

# Core NLP server setup
nlp = StanfordCoreNLP('http://localhost:9000')

def clean_tweet(tweet): 
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split()) 

def remove_stop_words(tweet):
    return " ".join([word for word in tweet.split() if word not in stopwords.words('english')])

def core_nlp(tweet): 
    res = nlp.annotate(tweet, properties={ 'annotators': 'sentiment', 'outputFormat': 'json', 'timeout': 10000,})
    return res  

def load_files(date):
    ts = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(date,'%a %b %d %H:%M:%S +0000 %Y'))
    date_of_tweet, time_of_tweet = str(ts).split()
    hours, minutes, seconds = time_of_tweet.split(":")
    if not os.path.exists(os.path.join(PROCESSED_CACHE_PATH, date_of_tweet)):
        os.makedirs(os.path.join(PROCESSED_CACHE_PATH, date_of_tweet))
        for i in range(0, 24):
            with open(os.path.join(PROCESSED_CACHE_PATH, date_of_tweet, str(i).zfill(2)), "w") as file_out:
                json.dump({}, file_out)

    return (os.path.join(PROCESSED_CACHE_PATH, date_of_tweet, hours), hours)

def process_files(filenames):
    if not os.path.isfile(DESIRED_KEYWORDS_PATH):
        print("Twitter Keys Does Not Exist: %s" % (DESIRED_KEYWORDS_PATH))
        sys.exit(1)

    with open(DESIRED_KEYWORDS_PATH) as file:
        twitter_keys = json.load(file)

    analyzer = SentimentIntensityAnalyzer()
    total = 0
    for filename in filenames:
        if filename[0:-15] not in twitter_keys["keywords"]:
            continue
        
        print(filename[0:-15], datetime.datetime.now())
        sys.stdout.flush()
        
        with open(os.path.join(CACHE_PATH, filename), 'r') as file:
            data = json.load(file)
            flush = False
            previous_hour = None
            previous_path = None
            processed_data = {}

            for entity in data:
                processed_data_path, hour = load_files(data[entity]["created_at"])
                
                # Load the correct file to dump tweet
                with open(processed_data_path, "r") as file_in:
                    if previous_hour != hour or previous_hour == None:
                        if previous_hour != None:
                            if processed_data == {} or processed_data == None:
                                print("DUMPING NONE")
                                sys.stdout.flush()
                            print("Dumping: %s" % (hour))
                            sys.stdout.flush()
                            # Write to the correct file
                            with open(previous_path, "w") as file_out:
                                # Dump new tweet entity
                                json.dump(processed_data, file_out)
                        print(processed_data_path)
                        processed_data = json.load(file_in)
                
                previous_hour = hour
                previous_path = processed_data_path
                

                if entity in processed_data:
                    if "keyword_set" not in processed_data[entity]:
                        processed_data[entity]["keyword_set"] = {}
                    if filename[0:-15] not in processed_data[entity]["keyword_set"]:
                        processed_data[entity]["keyword_set"][filename[0:-15]] = 0
                    processed_data[entity]["keyword_set"][filename[0:-15]] += 1
                    continue
                if data[entity]["lang"] != "en":
                    continue

                processed_data[entity] = {}
                
                if "keyword_set" not in processed_data[entity]:
                    processed_data[entity]["keyword_set"] = {}
                if filename[0:-15] not in processed_data[entity]["keyword_set"]:
                    processed_data[entity]["keyword_set"][filename[0:-15]] = 0
                processed_data[entity]["keyword_set"][filename[0:-15]] += 1
                
                # General info
                processed_data[entity]["in_reply_to_status_id"] = data[entity]["in_reply_to_status_id"]
                processed_data[entity]["in_reply_to_user_id"] = data[entity]["in_reply_to_user_id"]
                processed_data[entity]["in_reply_to_screen_name"] = data[entity]["in_reply_to_screen_name"]
                processed_data[entity]["created_at"] = data[entity]["created_at"]
                processed_data[entity]["id"] = data[entity]["id"]
                processed_data[entity]["truncated"] = data[entity]["truncated"]

                # User info
                processed_data[entity]["user"] = {}
                processed_data[entity]["user"]["id"] = data[entity]["user"]["id"]
                processed_data[entity]["user"]["screen_name"] = data[entity]["user"]["screen_name"]
                
                # Retweeted info
                processed_data[entity]["retweeted_status"] = {}
                if ("retweeted_status" in data[entity]):
                    processed_data[entity]["retweeted_status"]["created_at"] = data[entity]["retweeted_status"]["created_at"]
                    processed_data[entity]["retweeted_status"]["id"] = data[entity]["retweeted_status"]["id"]
                
                # Analysis on Full Text
                processed_data[entity]["text"] = {}
                processed_data[entity]["text"]["text"] = data[entity]["text"]
                # Vader
                vs = analyzer.polarity_scores(processed_data[entity]["text"]["text"])
                processed_data[entity]["text"]["Vader_Pos"] = vs["pos"]
                processed_data[entity]["text"]["Vader_Neg"] = vs["neg"]
                processed_data[entity]["text"]["Vader_Neu"] = vs["neu"]
                processed_data[entity]["text"]["Vader_Compound"] = vs["compound"]
                
                # Analysis on Clean Text
                processed_data[entity]["clean_text"] = {}
                processed_data[entity]["clean_text"]["text"] = clean_tweet(data[entity]["text"])
                # TextBlob
                analysis = TextBlob(processed_data[entity]["clean_text"]["text"])
                processed_data[entity]["clean_text"]["TextBlob_Polarity"] = analysis.sentiment.polarity
                processed_data[entity]["clean_text"]["TextBlob_Subjectivity"] = analysis.sentiment.subjectivity
                # Core NLP
                res = core_nlp(processed_data[entity]["clean_text"]["text"])
                if res != None:
                    for sentiment in res["sentences"]:
                        processed_data[entity]["clean_text"]["CoreNLP_Value"] = sentiment["sentimentValue"]
                        processed_data[entity]["clean_text"]["CoreNLP_Sentiment"] = sentiment["sentiment"]
                # Vader
                vs = analyzer.polarity_scores(processed_data[entity]["clean_text"]["text"])
                processed_data[entity]["clean_text"]["Vader_Pos"] = vs["pos"]
                processed_data[entity]["clean_text"]["Vader_Neg"] = vs["neg"]
                processed_data[entity]["clean_text"]["Vader_Neu"] = vs["neu"]
                processed_data[entity]["clean_text"]["Vader_Compound"] = vs["compound"]

                # Analysis on No Stop Word Text
                processed_data[entity]["no_stop_text"] = {}
                processed_data[entity]["no_stop_text"]["text"] = remove_stop_words(processed_data[entity]["clean_text"]["text"])
                # TextBlob
                analysis = TextBlob(processed_data[entity]["no_stop_text"]["text"])
                processed_data[entity]["no_stop_text"]["TextBlob_Polarity"] = analysis.sentiment.polarity
                processed_data[entity]["no_stop_text"]["TextBlob_Subjectivity"] = analysis.sentiment.subjectivity
                # Core NLP
                res = core_nlp(processed_data[entity]["no_stop_text"]["text"])
                if res != None:
                    for sentiment in res["sentences"]:
                        processed_data[entity]["no_stop_text"]["CoreNLP_Value"] = sentiment["sentimentValue"]
                        processed_data[entity]["no_stop_text"]["CoreNLP_Sentiment"] = sentiment["sentiment"]
                vs = analyzer.polarity_scores(processed_data[entity]["no_stop_text"]["text"])
                processed_data[entity]["no_stop_text"]["Vader_Pos"] = vs["pos"]
                processed_data[entity]["no_stop_text"]["Vader_Neg"] = vs["neg"]
                processed_data[entity]["no_stop_text"]["Vader_Neu"] = vs["neu"]
                processed_data[entity]["no_stop_text"]["Vader_Compound"] = vs["compound"]

            # Write to the correct file
            print("Dumping: %s" % (hour))
            sys.stdout.flush()
            with open(processed_data_path, "w") as file_out:
                # Dump new tweet entity
                json.dump(processed_data, file_out)

            total += len(data)
            print(total, filename)
            sys.stdout.flush()

if (__name__ == '__main__'):
    filenames = os.listdir(CACHE_PATH)
    process_files(filenames)
