import json
import operator
import os
import re

#Directory Paths
RELATIVE_PATH = os.path.dirname(__file__)
DATA_PATH = os.path.join(RELATIVE_PATH, "..", "data")
CACHE_PATH = os.path.join(DATA_PATH, "cache")

THRESHOLD = 50

def print_popular_tweets(hashtag_dict, threshold = THRESHOLD):
    hashtag_sorted = [(key, hashtag_dict[key]) for key in sorted(hashtag_dict, key = hashtag_dict.get, reverse=True)]
    for key, value in hashtag_sorted:
        if int(hashtag_dict[key]) >= threshold:
            print("%s: %s" % (key, hashtag_dict[key]))

def load_hashtags(filepaths):
    dup_dict = {}
    hashtag_dict = {}

    for filepath in filepaths:
        id_count = 0
        with open(os.path.join(CACHE_PATH, filepath), 'r') as file:
            data = json.load(file)
        for id in data:
            if id not in dup_dict:
                dup_dict[id] = 0
                text = data[id]['text']
                parts = text.strip().split()
                for part in parts:
                    part = re.sub(':', '', part)
                    if len(part) == 0:
                        continue
                    if part[0] == "#" or part[0] == "@":
                        if part not in hashtag_dict:
                            hashtag_dict[part] = 0
                        hashtag_dict[part] += 1
    return hashtag_dict
    
if (__name__ == '__main__'):
    filepaths = os.listdir(CACHE_PATH)
    hashtag_dict = load_hashtags(filepaths)
    print_popular_tweets(hashtag_dict)
