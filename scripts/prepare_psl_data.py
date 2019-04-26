import json
import operator
import os

# Directory Paths
RELATIVE_PATH = os.path.dirname(__file__)
DATA_PATH = os.path.join(RELATIVE_PATH, "..", "data")
PROCESSED_CACHE_PATH = os.path.join(DATA_PATH, "processed_cache")
PSL_CACHE_PATH = os.path.join(DATA_PATH, "psl_cache")

# PSL file names
TEXT_POLARITY_TARGETS = "text_polarity_target.txt"
VADER_FILENAME = "local_vader_obs.txt"
BLOB_FILENAME = "local_blob_obs.txt"
CORE_FILENAME = "local_core_obs.txt"
REPLY_FILENAME = "reply_obs.txt"
RETWEETED_FILENAME = "retweeted_obs.txt"
USER_FILENAME = "user_obs.txt"
PARTY_FILENAME = "party_target.txt"
KEYWORD_FILENAME = "has_keyword_obs.txt"

def write_psl(targets, vader, blob, core, reply, retweeted, user, party, keyword):
    # Create Cache Directory if it doesn't exist
    if not os.path.exists(PSL_CACHE_PATH):
        os.makedirs(PSL_CACHE_PATH)

    with open(os.path.join(PSL_CACHE_PATH, TEXT_POLARITY_TARGETS), "w") as file:
        file.write("\n".join(targets))
    with open(os.path.join(PSL_CACHE_PATH, PARTY_FILENAME), "w") as file:
        file.write("\n".join(party))
    with open(os.path.join(PSL_CACHE_PATH, VADER_FILENAME), "w") as file:
        file.write("\n".join(vader))
    with open(os.path.join(PSL_CACHE_PATH, BLOB_FILENAME), "w") as file:
        file.write("\n".join(blob))
    with open(os.path.join(PSL_CACHE_PATH, CORE_FILENAME), "w") as file:
        file.write("\n".join(core))
    with open(os.path.join(PSL_CACHE_PATH, REPLY_FILENAME), "w") as file:
        file.write("\n".join(reply))
    with open(os.path.join(PSL_CACHE_PATH, RETWEETED_FILENAME), "w") as file:
        file.write("\n".join(retweeted))
    with open(os.path.join(PSL_CACHE_PATH, USER_FILENAME), "w") as file:
        file.write("\n".join(user))
    with open(os.path.join(PSL_CACHE_PATH, KEYWORD_FILENAME), "w") as file:
        file.write("\n".join(keyword))

def convert_format(filepaths, threshold = 0.05):
    total_set = set()
    output = []
    vader = []
    blob = []
    core = []
    reply = []
    retweeted = []
    targets = []
    user = []
    party = []
    keyword = []
    for day_dir in filepaths:
        print(day_dir)
        for hour_file in os.listdir(os.path.join(PROCESSED_CACHE_PATH, day_dir)):
            with open(os.path.join(PROCESSED_CACHE_PATH, day_dir, hour_file), 'r') as file:
                data = json.load(file)
            for e in data:
                element = data[e]
                
                # Targets
                if element["id"] not in total_set:
                    total_set.add(element["id"])
                    targets.append(str(element["id"])+"\t"+"Pos")
                    targets.append(str(element["id"])+"\t"+"Neg")
                    targets.append(str(element["id"])+"\t"+"Neu")
                
                # Targets
                party.append(str(element["id"])+"\t"+"Dem")
                party.append(str(element["id"])+"\t"+"Rep")
                party.append(str(element["id"])+"\t"+"Oth")

                # Vader - Full Text
                if element["text"]["Vader_Compound"] >= threshold:
                    vader.append(str(element["id"])+"\t"+"Pos")
                elif element["text"]["Vader_Compound"] <= -1 * threshold:
                    vader.append(str(element["id"])+"\t"+"Neg")
                else:
                    vader.append(str(element["id"])+"\t"+"Neu")
                
                # Blob - Clean Text
                if element["clean_text"]["TextBlob_Polarity"] >= threshold:
                    blob.append(str(element["id"])+"\t"+"Pos")
                elif element["clean_text"]["TextBlob_Polarity"] <= -1 * threshold:
                    blob.append(str(element["id"])+"\t"+"Neg")
                else:
                    blob.append(str(element["id"])+"\t"+"Neu")
                
                # Core - Clean Text
                if "CoreNLP_Sentiment" in element["clean_text"]:
                    if element["clean_text"]["CoreNLP_Sentiment"] == "Positive":
                        core.append(str(element["id"])+"\t"+"Pos")
                    elif element["clean_text"]["CoreNLP_Sentiment"] == "Negative":
                        core.append(str(element["id"])+"\t"+"Neg")
                    else:
                        core.append(str(element["id"])+"\t"+"Neu")
                
                # Reply
                if element["in_reply_to_status_id"] != None:
                    if element["in_reply_to_status_id"] not in total_set:
                        total_set.add(element["in_reply_to_status_id"])
                        targets.append(str(element["in_reply_to_status_id"])+"\t"+"Pos")
                        targets.append(str(element["in_reply_to_status_id"])+"\t"+"Neg")
                        targets.append(str(element["in_reply_to_status_id"])+"\t"+"Neu")
                    
                    reply.append(str(element["id"]) + "\t" + str(element["in_reply_to_status_id"]))
                
                # Retweeted
                if ("id" in element["retweeted_status"]):
                    if element["retweeted_status"]["id"] not in total_set:
                        total_set.add(element["retweeted_status"]["id"])
                        targets.append(str(element["retweeted_status"]["id"])+"\t"+"Pos")
                        targets.append(str(element["retweeted_status"]["id"])+"\t"+"Neg")
                        targets.append(str(element["retweeted_status"]["id"])+"\t"+"Neu")
                    retweeted.append(str(element["id"]) + "\t" + str(element["retweeted_status"]["id"]))
                
                # User
                user.append(str(element["id"]) + "\t" + str(element["user"]["id"]))

                # Keywords
                for key in element["keyword_set"]:
                    keyword.append(str(element["id"]) + "\t" + key)

    print(len(targets) / 3, len(retweeted))
    return targets, vader, blob, core, reply, retweeted, user, party, keyword

if (__name__ == '__main__'):
    filepaths = os.listdir(PROCESSED_CACHE_PATH)
    targets, vader, blob, core, reply, retweeted, user, party, keyword = convert_format(filepaths)
    write_psl(targets, vader, blob, core, reply, retweeted, user, party, keyword)
