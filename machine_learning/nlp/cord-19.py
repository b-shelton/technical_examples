

import os
import zipfile
import tempfile
import json
import numpy as np
import pandas as pd
import re
from time import process_time
import multiprocessing as mp
import nltk
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import CountVectorizer

# SET THIS APPROPRIATELY to analyze either abstracts or full texts
focus = 'abstract' # 'abstract' or 'body_text'


# If not in a Kaggle notebook, configure environment to download data from Kaggle API (one time activity)
# Follow instructions here: https://medium.com/@ankushchoubey/how-to-download-dataset-from-kaggle-7f700d7f9198
#os.system('kaggle datasets download -d allen-institute-for-ai/CORD-19-research-challenge')
zippath = 'CORD-19-research-challenge.zip'

# Create the temporary directory to store the zip file's content
temp_dir = tempfile.TemporaryDirectory()

# Extract the zip file's content into the temporary directory
with zipfile.ZipFile(zippath, 'r') as zip_ref:
    zip_ref.extractall(temp_dir.name)

# Read the metadata.csv file
md = pd.read_csv(temp_dir.name + '/metadata.csv')



###############################################################################
# Read all of the text from the research papers
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
        for j in range(0, len(d[focus])):
            if len(paper_text) == 0:
                paper_text = d[focus][j]['text']
            else:
                paper_text += d[focus][j]['text']

        # append to the rest of the extracted papers
        papers_source.append(h)
        papers_sha.append(re.sub('.json', '', sha))
        papers_text.append(paper_text)



###############################################################################
# Pre-Processing
###############################################################################
'''
 This section will clean the text to prepare if for analysis, including transformation
 to all lowercase, tokenization, stemming (PortStemmer), and removing stop words.

 This section uses the multiprocessing package, which takes advantage of all the
 operating system's cores. I've hard coded the number of cores to 4, but the user
 can identify how many cores they have available by running `mp.cpu_count()`.

 Even with the multiprocessing package, it takes a long time to stem every word
 in the 30k~ papers.
'''

# remove papers with no abstracts
papers_text1 = list(filter(None, papers_text))

# make every word lowercase
papers_text1 = [x.lower() for x in papers_text1]


# tokenize every paper, using multiprocessing
tokenizer = RegexpTokenizer('[a-zA-Z]\w+\'?\w*')

def token_a_paper(paper_text):
    return tokenizer.tokenize(paper_text)

t1_start = process_time()
pool = mp.Pool(4)
token_papers = list(pool.map(token_a_paper, papers_text1))
t1_end = process_time()
print('Time to tokenize:', round(t1_end - t1_start, 2), 'seconds')
pool.close()


# remove stop words (including customs stop words)
custom_to_exclude = {'et', 'al', 'al.', 'preprint', 'copyright', 'peer-review',
                     'author/fund', 'http', 'licens', 'biorxiv', 'fig', 'figure',
                     'medrxiv', 'i.e.', 'e.g.', 'e.g.,', '\'s', 'doi', 'author',
                     'funder', 'https', 'license'}
stop_words = set(stopwords.words('english')) | custom_to_exclude

st_papers = []
for i in token_papers:
     t = [word for word in i if (not word in stop_words)]
     st_papers.append(t)

# count how many words after stop words are removed
counter = 0
for i in token_papers:
    counter += len(i)
print('Number of total words:', counter)


# stem every remaining word, using multiprocessing
stemmer = PorterStemmer()
def stem_tokens(st_paper):
    return [stemmer.stem(word) for word in st_paper]

t1_start = process_time()
# We double all numbers using map()
pool = mp.Pool(4)
stemmed_papers = pool.map(stem_tokens, st_papers)
t1_end = process_time()
print('Time to stem:', round((t1_end - t1_start) / 60, 2), 'minutes')
pool.close()



###############################################################################
# Exploratory Analysis
###############################################################################

def get_most_freq_words(str, n=None):
    vect = CountVectorizer().fit(str)
    bag_of_words = vect.transform(str)
    sum_words = bag_of_words.sum(axis=0)
    freq = [(word, sum_words[0, idx]) for word, idx in vect.vocabulary_.items()]
    freq =sorted(freq, key = lambda x: x[1], reverse=True)
    return freq[:n]

get_most_freq_words([word for word in stemmed_papers for word in word] , 50)

df = pd.DataFrame({'abstract': papers_text1, 'token_stemmed': stemmed_papers})


# build a dictionary where for each tweet, each word has its own id.

# create a single list of all stemmed words from the papers
flat_words =[]
for i in stemmed_papers:
    flat_words += i

# Creating dictionary for the word frequency table
frequency_table = dict()
for wd in flat_words:
    if wd in frequency_table:
        frequency_table[wd] += 1
    else:
        frequency_table[wd] = 1

# build the corpus i.e. vectors with the number of occurence of each word per tweet
corpus_corpus = [frequency_table.doc2bow(word) for word in stemmed_papers]

from gensim.corpora import Dictionary
from gensim.models.ldamodel import LdaModel
from gensim.models import CoherenceModel

tweets_dictionary = Dictionary(stemmed_papers)


# compute coherence
tweets_coherence = []
for nb_topics in range(1,36):
    lda = LdaModel(tweets_corpus,
                   num_topics = nb_topics,
                   id2word = tweets_dictionary,
                   passes=10)
    cohm = CoherenceModel(model=lda,
                          corpus=tweets_corpus,
                          dictionary=tweets_dictionary,
                          coherence='u_mass')
    coh = cohm.get_coherence()
    tweets_coherence.append(coh)

# visualize coherence
plt.figure(figsize=(10,5))
plt.plot(range(1,36),tweets_coherence)
plt.xlabel("Number of Topics")
plt.ylabel("Coherence Score");




# Close the temporary directory
import shutil
shutil.rmtree(temp_dir.name)
