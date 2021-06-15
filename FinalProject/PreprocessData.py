from bs4 import BeautifulSoup
import re
from collections import OrderedDict
import os
import time
import string
import pickle
import matplotlib.pyplot as plt
from pathlib import Path

# ################ Preprocess Reuters DB ###############################


def ParseReutersDb():
    """
    Processing .sgm files from 'data' folder in current directory.
    Files were taken from reuters db, to be found
    @ https://archive.ics.uci.edu/ml/datasets/reuters-21578+text+categorization+collection
    :return: an ordered dictionary where key=docid and value=document text
    """
    documents = []
    printedbodies = {}
    print('Reading files')
    print('Please wait...')
    t0 = time.time()

    data = ''

    for file in os.listdir("data/"):
        if file.endswith(".sgm"):
            filename = os.path.join("data", file)

            f = open(filename, 'r')
            data = data + f.read()

    print('Reading reuters data took %.2f sec.' % (time.time() - t0))

    print('Transforming data...')
    t0 = time.time()
    soup = BeautifulSoup(data, "html.parser")
    bodies = soup.findAll('body')
    i = 0
    for body in bodies:
        printedbodies[i] = body
        documents.append(
            re.sub(' +', ' ', str(body).replace("<body>", "").replace("</body>", "").translate(string.punctuation)
                   .replace("", "").replace("\n", " ").lower()))
        i = i + 1

    print('Transforming data took %.2f sec.' % (time.time() - t0))

    print('The number of documents read was: ' + str(len(documents)))

    i = 0
    d = OrderedDict()

    t = {}
    t0 = time.time()
    for value in documents:
        # create a dictionary where key=docid and value=document text
        d[i] = value
        # split text into words
        d[i] = re.sub("[^\w]", " ", d[i]).split()

        # remove rows with empty values from dictionary d
        if d[i]:
            i = i + 1
        else:
            del d[i]
            del body[i]  # TODO: what is body?
    return d


# ################ Preprocess text DB ###############################
def ParseTextDb(file):
    """
    Processing .txt files from 'data' folder in current directory.
    Files were taken from Stanford’s Mining of Massive Datasets (“MMDS”) course db
    :return: an ordered dictionary where key=docid and value=document text
    """

    t0 = time.time()
    print('Reading files')
    print('Please wait...')
    d = OrderedDict()
    filename = os.path.join("data", str(file))
    f = open(filename, 'r')
    while True:
        words = f.readline()
        # if line is empty eof reached
        if not words:
            break
        words = words.split(" ")
        docID = words[0]
        del words[0]
        d[docID] = words
    print('Reading text data took %.2f sec.' % (time.time() - t0))
    return d


# ######### AUXILIARY FUNCTIONS #############3


# def time_slow_indexing(files):
#     times = [round(PD.(os.path.join("data", str(str(file) + ".txt"))), 2) for file in files]
#     save_obj(times, "indexing_time_no_shingling")
#     plt.plot(files, times, '-o')
#     plt.xlabel('number of documents')
#     plt.ylabel('time(sec)')
#     plt.title('Indexing time for documents without shingling')
#     plt.show()
#
#
# def time_fast_indexing(files):
#     times = [round(build_index_shingles(os.path.join("data", str(str(file) + ".txt"))), 2) for file in files]
#     save_obj(times, "indexing_time_with_shingling")
#     plt.plot(files, times, '-o')
#     plt.xlabel('number of documents')
#     plt.ylabel('time(sec)')
#     plt.title('Indexing time for documents without shingling')
#     plt.show()


'''
    file_space_no_shingles = [0.2,2.037,5.096,20.400]
    file_space_with_shingles = [0.166,1.650,4.130,16.500]
'''


def plot_graphs():
    file_size = [100, 1000, 2500, 10000]
    file_space_no_shingles = load_obj("indexing_time_no_shingling")
    file_space_with_shingles = load_obj("indexing_time_with_shingling")
    plt.plot(file_size, file_space_no_shingles, '-o')
    plt.plot(file_size, file_space_with_shingles, '-o')
    plt.xlabel('number of documents')
    plt.ylabel('time (sec)')
    plt.legend(["No shingling", "With shingling"])
    plt.title('Indexing time')
    plt.show()


def save_obj(obj, path):
    """
    Saves an object using pickle
    :param obj:
    :param path:
    """
    Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(path):
    """
    Loads an object using pickle
    :param path:
    :return: The pickled object.
    """
    with open(path, 'rb') as f:
        return pickle.load(f)


