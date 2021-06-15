import itertools
from hashlib import sha256
from hashlib import sha512
from hashlib import md5
import string
import random
import time


from collections import Counter
import numpy as np; np.random.seed(0)
import seaborn as sns; sns.set()

W = 4
HASHES = 50

'''
DOCUMENT NAMES MUST BE NUMBER OF ARTICLES IN DOCUMENT
'''


# ############################ Represent Data By Sketches ###################################3


def getShingles(doc):
    """
    :param doc: Document's text
    :return: w-shingle of document doc
    """
    words = doc
    shingles = []
    for i in range(0, len(words) - W + 1):
        shingle = ' '.join(words[i:i + W])
        if shingle not in shingles:
            shingles.append(shingle)
    return shingles


def getRandomStrings():
    """
    Generates random strings.
    :param numStrings: Positive integer
    :return: A list of n random strings of 4 characters each
    """
    # set length of each of the hash words
    length = 4
    randWords = []
    for i in range(0, HASHES):
        randWords.append(''.join(random.choice(string.ascii_letters) for m in range(length)))
    return randWords




def getSketch(doc, randWords):
    """
    :param doc: Document's text.
    :param randWords: Random words.
    :return: Sketch of given document, using given random words for randomizing effect in hashing.
    """
    numWords = len(randWords)
    # shingles = getShingles(doc)
    shingles = getShingles(doc)
    sketch = []
    for i in range(0, numWords):
        m = float('inf')
        for s in shingles:
            h = sha256((s + " " + randWords[i]).encode('raw_unicode_escape'))
            hashVal = int(h.hexdigest(), base=16)
            if hashVal < m:
                m = hashVal
        sketch.append(m)
    return sketch


######## SHINGLING DATA ##################


def sketchData(dict, hashes):
    sketchedDict = {}
    for key, value in dict.items():
        sketchedDict[key] = getSketch(value, hashes)
    return sketchedDict


# ######################### Distance (Similarity) Measuring #########################


def jaccardCoefficient(list1, list2):
    """
    Computes similarity with jaccard coefficient of two given groups (lists)
    :param list1:
    :param list2:
    :return:
    """
    intersection = len(list((Counter(list1) & Counter(list2)).elements()))
    union = (len(list1) + len(list2)) - intersection
    return float(intersection) / union


# ################## MinHash Algorithm ##################################


def minhash(docs_dict):
    hashes = getRandomStrings()
    return sketchData(docs_dict, hashes)


############## ALTERNATE METHODS FOR TESTING #####################

def minhashForTestingShingleFunc(docs_dict, shingleFunc):   #TODO: REMOVE AFTER TESTING SHINGLING IMPLEMENTATION
    hashes = getNrandomStrings(50)
    return sketchDataForCheckingShingleFunc(docs_dict, hashes,shingleFunc)

def minhashForTestingNumHashes(docs_dict, numHashes):   #TODO: REMOVE AFTER TESTING NUMHASH IMPLEMENTATION
    hashes = getNrandomStrings(numHashes)
    return sketchData(docs_dict, hashes), hashes

def minhashForTestingHashSizes(docs_dict,sketchFunc):
        hashes = getRandomStrings()
        sketchedDict = {}
        for key, value in docs_dict.items():
            sketchedDict[key] = sketchFunc(value, hashes)
        return sketchedDict



def getShinglesWithSet(doc):
    """
    :param doc: Document's text
    :return: w-shingle of document doc
      SAME FUNCTION WITH DIFFERENT IMPLEMENTATION
    """
    words = doc
    shingles = [(words[i:i + W]) for i in range(0, len(words) - W + 1)]
    shingles.sort()
    shingleSet = list(shingles for shingles, _ in itertools.groupby(shingles))
    return [' '.join(i) for i in shingleSet]

def getNrandomStrings(numHashes):  # TODO erase after finished checking
    """
    Generates random strings.
    :param numStrings: Positive integer
    :return: A list of n random strings of 4 characters each
    """
    # set length of each of the hash words
    length = 4
    randWords = []
    for i in range(0, numHashes):
        randWords.append(''.join(random.choice(string.ascii_letters) for m in range(length)))
    return randWords

def sketchDataForCheckingShingleFunc(dict, hashes, shingleFunc):  # TODO: REMOVE AFTER TESTING SHINGLING IMPLEMENTATION
    sketchedDict = {}
    for key, value in dict.items():
        sketchedDict[key] = getShingleSketch(value, hashes, shingleFunc)
    return sketchedDict

def getShingleSketch(doc, randWords,shingleFunc):
    """
    :param doc: Document's text.
    :param randWords: Random words.
    :return: Sketch of given document, using given random words for randomizing effect in hashing.
    """
    numWords = len(randWords)
    # shingles = getShingles(doc)
    shingles = shingleFunc(doc) #TODO change back after testing shingleFunc (also in func stamp)
    sketch = []
    for i in range(0, numWords):
        m = float('inf')
        for s in shingles:
            h = sha256((s + " " + randWords[i]).encode('raw_unicode_escape'))
            hashVal = int(h.hexdigest(), base=16)
            if hashVal < m:
                m = hashVal
        sketch.append(m)
    return sketch

def get64bitSketch(doc, randWords):
    """
    :param doc: Document's text.
    :param randWords: Random words.
    :return: Sketch of given document, using given random words for randomizing effect in hashing.
    SAME FUNCTION WITH SHA512 HASHING (INSTEAD OF SHA256) THIS MEANS 64-BIT HASH INSTEAD OF 32
    """
    numWords = len(randWords)
    shingles = getShingles(doc)
    sketch = []
    for i in range(0, numWords):
        m = float('inf')
        for s in shingles:
            h = sha512((s + " " + randWords[i]).encode('raw_unicode_escape'))
            hashVal = int(h.hexdigest(), base=16)
            if hashVal < m:
                m = hashVal
        sketch.append(m)
    return sketch

def get16bitSketch(doc, randWords):
    """
    :param doc: Document's text.
    :param randWords: Random words.
    :return: Sketch of given document, using given random words for randomizing effect in hashing.
    SAME FUNCTION WITH SHA512 HASHING (INSTEAD OF SHA256) THIS MEANS 64-BIT HASH INSTEAD OF 32
    """
    numWords = len(randWords)
    shingles = getShingles(doc)
    sketch = []
    for i in range(0, numWords):
        m = float('inf')
        for s in shingles:
            h = md5((s + " " + randWords[i]).encode('raw_unicode_escape'))
            hashVal = int(h.hexdigest(), base=16)
            if hashVal < m:
                m = hashVal
        sketch.append(m)
    return sketch