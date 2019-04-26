import json
import operator
import os

# Directory Paths
RELATIVE_PATH = os.path.dirname(__file__)
DATA_PATH = os.path.join(RELATIVE_PATH, "..", "data")
PROCESSED_CACHE_PATH = os.path.join(DATA_PATH, "processed_cache")

PSL_OUTPUT_PATH = os.path.join(DATA_PATH, "psl_output", "TEXTPOLARITY.txt")

#[Vader, Textblob, CoreNLP, Vader, Textblob, CoreNLP, Vader, Same with no truncated ...]

ORDERED = ['2019-02-24', '2019-02-25', '2019-02-26', '2019-02-27', '2019-02-28', '2019-03-01', '2019-03-02', '2019-03-05', '2019-03-06', '2019-03-07', '2019-03-08']

SIZE_OF_ARRAY = 45

VADER_FULL_POS = 0
VADER_FULL_NEU = 1
VADER_FULL_NEG = 2

BLOB_CLEAN_POS = 3
BLOB_CLEAN_NEU = 4
BLOB_CLEAN_NEG = 5

CORE_CLEAN_POS = 6
CORE_CLEAN_NEU = 7
CORE_CLEAN_NEG = 8

VADER_CLEAN_POS = 9
VADER_CLEAN_NEU = 10
VADER_CLEAN_NEG = 11

BLOB_STOP_POS = 12
BLOB_STOP_NEU = 13
BLOB_STOP_NEG = 14

CORE_STOP_POS = 15
CORE_STOP_NEU = 16
CORE_STOP_NEG = 17

VADER_STOP_POS = 18
VADER_STOP_NEU = 19
VADER_STOP_NEG = 20

VADER_FULL_POS_T = 21
VADER_FULL_NEU_T = 22
VADER_FULL_NEG_T = 23

BLOB_CLEAN_POS_T = 24
BLOB_CLEAN_NEU_T = 25
BLOB_CLEAN_NEG_T = 26

CORE_CLEAN_POS_T = 27
CORE_CLEAN_NEU_T = 28
CORE_CLEAN_NEG_T = 29

VADER_CLEAN_POS_T = 30
VADER_CLEAN_NEU_T = 31
VADER_CLEAN_NEG_T = 32

BLOB_STOP_POS_T = 33
BLOB_STOP_NEU_T = 34
BLOB_STOP_NEG_T = 35

CORE_STOP_POS_T = 36
CORE_STOP_NEU_T = 37
CORE_STOP_NEG_T = 38

VADER_STOP_POS_T = 39
VADER_STOP_NEU_T = 40
VADER_STOP_NEG_T = 41

PSL_POS = 42
PSL_NEG = 43
PSL_NEU = 44

def print_scores(hours, days, print_hours = False, print_days = False):
    if print_hours:
        for hour in hours:
            print("\t".join([str(i) for i in hour]))
    if print_days:
        for day in days:
            print("\t".join([str(i) for i in day]))

def load_psl(path = PSL_OUTPUT_PATH):
    psl_data = {}
    with open(path, "r") as file:
        for line in file:
            line = line.replace('\'', '')
            parts = line.strip().split("\t")
            if parts[0] not in psl_data:
                psl_data[parts[0]] = {}
            psl_data[parts[0]][parts[1]] = parts[2]
    return psl_data

def count_scores(filepaths, psl_data, threshold = 0.05):
    output = []
    daily_output = []
    for day_dir in filepaths:
        daily = [0] * SIZE_OF_ARRAY
        for hour_file in os.listdir(os.path.join(PROCESSED_CACHE_PATH, day_dir)):
            output.append([0] * SIZE_OF_ARRAY)
            with open(os.path.join(PROCESSED_CACHE_PATH, day_dir, hour_file), 'r') as file:
                data = json.load(file)
            for e in data:
                element = data[e]

                # PSL Results
                id = str(element["id"])
                if (id in psl_data):
                    if psl_data[id]["Pos"] > psl_data[id]["Neg"] and psl_data[id]["Pos"] > psl_data[id]["Neu"]:
                        output[-1][PSL_POS] += 1
                        daily[PSL_POS] += 1
                    elif psl_data[id]["Neg"] > psl_data[id]["Neu"]:
                        output[-1][PSL_NEG] += 1
                        daily[PSL_NEG] += 1
                    else:
                        output[-1][PSL_NEU] += 1
                        daily[PSL_NEU] += 1

                # ====================
                # VADER
                # ====================
                # Full Text
                if element["text"]["Vader_Compound"] >= threshold:
                    output[-1][VADER_FULL_POS] += 1
                    daily[VADER_FULL_POS] += 1
                    if not element["truncated"]:
                        output[-1][VADER_FULL_POS_T] += 1
                        daily[VADER_FULL_POS_T] += 1
                elif element["text"]["Vader_Compound"] <= -1 * threshold:
                    daily[VADER_FULL_NEG] += 1
                    if not element["truncated"]:
                        output[-1][VADER_FULL_NEG_T] += 1
                        daily[VADER_FULL_NEG_T] += 1
                else:
                    output[-1][VADER_FULL_NEU] += 1
                    daily[VADER_FULL_NEU] += 1
                    if not element["truncated"]:
                        output[-1][VADER_FULL_NEU_T] += 1
                        daily[VADER_FULL_NEU_T] += 1
                
                # Clean Text
                if element["clean_text"]["Vader_Compound"] >= threshold:
                    output[-1][VADER_CLEAN_POS] += 1
                    daily[VADER_CLEAN_POS] += 1
                    if not element["truncated"]:
                        output[-1][VADER_CLEAN_POS_T] += 1
                        daily[VADER_CLEAN_POS_T] += 1
                elif element["clean_text"]["Vader_Compound"] <= -1 * threshold:
                    output[-1][VADER_CLEAN_NEG] += 1
                    daily[VADER_CLEAN_NEG] += 1
                    if not element["truncated"]:
                        output[-1][VADER_CLEAN_NEG_T] += 1
                        daily[VADER_CLEAN_NEG_T] += 1
                else:
                    output[-1][VADER_CLEAN_NEU] += 1
                    daily[VADER_CLEAN_NEU] += 1
                    if not element["truncated"]:
                        output[-1][VADER_CLEAN_NEU_T] += 1
                        daily[VADER_CLEAN_NEU_T] += 1
                
                # Stop Text
                if element["no_stop_text"]["Vader_Compound"] >= threshold:
                    output[-1][VADER_STOP_POS] += 1
                    daily[VADER_STOP_POS] += 1
                    if not element["truncated"]:
                        output[-1][VADER_STOP_POS_T] += 1
                        daily[VADER_STOP_POS_T] += 1
                elif element["no_stop_text"]["Vader_Compound"] <= -1 * threshold:
                    output[-1][VADER_STOP_NEG] += 1
                    daily[VADER_STOP_NEG] += 1
                    if not element["truncated"]:
                        output[-1][VADER_STOP_NEG_T] += 1
                        daily[VADER_STOP_NEG_T] += 1
                else:
                    output[-1][VADER_STOP_NEU] += 1
                    daily[VADER_STOP_NEU] += 1
                    if not element["truncated"]:
                        output[-1][VADER_STOP_NEU_T] += 1
                        daily[VADER_STOP_NEU_T] += 1
                
                # ====================
                # TEXT
                # ====================
                # Clean Text
                if element["clean_text"]["TextBlob_Polarity"] >= threshold:
                    output[-1][BLOB_CLEAN_POS] += 1
                    daily[BLOB_CLEAN_POS] += 1
                    if not element["truncated"]:
                        output[-1][BLOB_CLEAN_POS_T] += 1
                        daily[BLOB_CLEAN_POS_T] += 1
                elif element["clean_text"]["TextBlob_Polarity"] <= -1 * threshold:
                    output[-1][BLOB_CLEAN_NEG] += 1
                    daily[BLOB_CLEAN_NEG] += 1
                    if not element["truncated"]:
                        output[-1][BLOB_CLEAN_NEG_T] += 1
                        daily[BLOB_CLEAN_NEG_T] += 1
                else:
                    output[-1][BLOB_CLEAN_NEU] += 1
                    daily[BLOB_CLEAN_NEU] += 1
                    if not element["truncated"]:
                        output[-1][BLOB_CLEAN_NEU_T] += 1
                        daily[BLOB_CLEAN_NEU_T] += 1
                
                # Stop Text
                if element["no_stop_text"]["TextBlob_Polarity"] >= threshold:
                    output[-1][BLOB_STOP_POS] += 1
                    daily[BLOB_STOP_POS] += 1
                    if not element["truncated"]:
                        output[-1][BLOB_STOP_POS_T] += 1
                        daily[BLOB_STOP_POS_T] += 1
                elif element["no_stop_text"]["TextBlob_Polarity"] <= -1 * threshold:
                    output[-1][BLOB_STOP_NEG] += 1
                    daily[BLOB_STOP_NEG] += 1
                    if not element["truncated"]:
                        output[-1][BLOB_STOP_NEG_T] += 1
                        daily[BLOB_STOP_NEG_T] += 1
                else:
                    output[-1][BLOB_STOP_NEU] += 1
                    daily[BLOB_STOP_NEU] += 1
                    if not element["truncated"]:
                        output[-1][BLOB_STOP_NEU_T] += 1
                        daily[BLOB_STOP_NEU_T] += 1
                
                # ====================
                # CORE
                # ====================
                # Clean Text
                if "CoreNLP_Sentiment" in element["clean_text"]:
                    if element["clean_text"]["CoreNLP_Sentiment"] == "Positive":
                        output[-1][CORE_CLEAN_POS] += 1
                        daily[CORE_CLEAN_POS] += 1
                        if not element["truncated"]:
                            output[-1][CORE_CLEAN_POS_T] += 1
                            daily[CORE_CLEAN_POS_T] += 1
                    elif element["clean_text"]["CoreNLP_Sentiment"] == "Negative":
                        output[-1][CORE_CLEAN_NEG] += 1
                        daily[CORE_CLEAN_NEG] += 1
                        if not element["truncated"]:
                            output[-1][CORE_CLEAN_NEG_T] += 1
                            daily[CORE_CLEAN_NEG_T] += 1
                    else:
                        output[-1][CORE_CLEAN_NEU] += 1
                        daily[CORE_CLEAN_NEU] += 1
                        if not element["truncated"]:
                            output[-1][CORE_CLEAN_NEU_T] += 1
                            daily[CORE_CLEAN_NEU_T] += 1
                else:
                    output[-1][CORE_CLEAN_NEU] += 1
                    daily[CORE_CLEAN_NEU] += 1
                    if not element["truncated"]:
                        output[-1][CORE_CLEAN_NEU_T] += 1
                        daily[CORE_CLEAN_NEU_T] += 1
 
                # Stop Text
                if "CoreNLP_Sentiment" in element["no_stop_text"]:
                    if element["no_stop_text"]["CoreNLP_Sentiment"] == "Positive":
                        output[-1][CORE_STOP_POS] += 1
                        daily[CORE_STOP_POS] += 1
                        if not element["truncated"]:
                            output[-1][CORE_STOP_POS_T] += 1
                            daily[CORE_STOP_POS_T] += 1
                    elif element["no_stop_text"]["CoreNLP_Sentiment"] == "Negative":
                        output[-1][CORE_STOP_NEG] += 1
                        daily[CORE_STOP_NEG] += 1
                        if not element["truncated"]:
                            output[-1][CORE_STOP_NEG_T] += 1
                            daily[CORE_STOP_NEG_T] += 1
                    else:
                        output[-1][CORE_STOP_NEU] += 1
                        daily[CORE_STOP_NEU] += 1
                        if not element["truncated"]:
                            output[-1][CORE_STOP_NEU_T] += 1
                            daily[CORE_STOP_NEU_T] += 1
                else:
                    output[-1][CORE_STOP_NEU] += 1
                    daily[CORE_STOP_NEU] += 1
                    if not element["truncated"]:
                        output[-1][CORE_STOP_NEU_T] += 1
                        daily[CORE_STOP_NEU_T] += 1
        
        daily_output.append(daily)
    return output, daily_output

if (__name__ == '__main__'):
    filepaths = os.listdir(PROCESSED_CACHE_PATH)
    psl_data = load_psl()
    hours, days = count_scores(ORDERED, psl_data)
    print_scores(hours, days, print_hours = False, print_days = True)
