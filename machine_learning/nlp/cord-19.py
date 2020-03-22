

# Read the zip file from the https link
import os
import zipfile
import tempfile
import json
import numpy as np
import pandas as pd
import re

# If not in a Kaggle notebook, configure environment to download data from Kaggle API (one time activity)
# Follow instructions here: https://medium.com/@ankushchoubey/how-to-download-dataset-from-kaggle-7f700d7f9198
os.system('kaggle datasets download -d allen-institute-for-ai/CORD-19-research-challenge')
zippath = 'CORD-19-research-challenge.zip'

# Create the temporary directory to store the zip file's content
temp_dir = tempfile.TemporaryDirectory()

# Extract the zip file's content into the temporary directory
with zipfile.ZipFile(zippath, 'r') as zip_ref:
    zip_ref.extractall(temp_dir.name)

# Read the metadata.csv file
md = pd.read_csv(temp_dir.name + '/metadata.csv')




# read all of the text from the research papers
###############################################################################

sources = ['biorxiv_medrxiv',
           'comm_use_subset',
           'noncomm_use_subset',
           'custom_license']

papers_source = []
papers_sha = []
papers_text = []
for h in sources:
    paper_path = '/' + h + '/' + h + '/'
    for i in range(0, len(os.listdir(temp_dir.name + paper_path))):
        # read json file
        sha = os.listdir(temp_dir.name + paper_path)[i]
        json_path = (temp_dir.name + paper_path + sha)
        with open(json_path) as f:
            d = json.load(f)

        # get text
        paper_text = []
        for j in range(0, len(d['body_text'])):
            if len(paper_text) == 0:
                paper_text = d['body_text'][j]['text']
            else:
                paper_text += d['body_text'][j]['text']

        # append to the rest of the extracted papers
        papers_source.append(h)
        papers_sha.append(re.sub('.json', '', sha))
        papers_text.append(paper_text)



# remove stop words
###############################################################################
#!pip3 install nltk
# import nltk
# nltk.download('stopwords')
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import nltk.tokenize

def _create_dictionary_table(text_string) -> dict:

    # Removing stop words
    stop_words = set(stopwords.words("english"))

    words = nltk.word_tokenize(text_string)

    # Reducing words to their root form
    stem = PorterStemmer()

    # Creating dictionary for the word frequency table
    for wd in words:
        wd = stem.stem(wd)
        if wd in stop_words:
            continue
        if wd in frequency_table:
            frequency_table[wd] += 1
        else:
            frequency_table[wd] = 1

    return frequency_table

frequency_table = dict()
for i in papers_text[0:50]:
    _create_dictionary_table(i)


# See the most frequent words, after removing stop words
import operator
sorted_d = dict(sorted(frequency_table.items(),
                       key = operator.itemgetter(1),
                       reverse = True))
sorted_d


# Close the temporary directory
import shutil
shutil.rmtree(temp_dir.name)
