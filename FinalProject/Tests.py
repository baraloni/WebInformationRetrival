import itertools
import PreprocessData as PD
import MinHash as MH
import SimHash as SH
import time
import matplotlib.pyplot as plt
import os
import random
from pathlib import Path
from scipy.stats import pearsonr
import sys

OUTPUT_DIR = "Graphs"

THRESHOLDS = [0.9, 0.8, 0.7, 0.6]
INIT_BLOCKS = [1, 2, 3, 4]
# DBS = [100, 1000, 2500, 10000]
# THRESHOLDS = [0.9, 0.8, 0.7, 0.6]  # TODO: un comment
# INIT_BLOCKS = [1, 2, 3, 4]  # TODO: un comment


################ MH shingling #################3

###### SHINGLING COMPARISON ############

def compareShinglingImplementations():
    '''
    compares the cost of 2 implementations of shingling data in terms of time and space
    shows the plots
    '''
    originalImplementation = []
    newImplementation = []
    file_size = [100, 1000, 2500, 10000]
    for file in file_size:
        docs_dict = PD.ParseTextDb(str(file) + ".txt")
        t0 = time.time()
        signatures = MH.minhashForTestingShingleFunc(docs_dict, MH.getShingles)
        newImplementation.append(round(time.time() - t0, 3))
        t0 = time.time()
        signatures = MH.minhashForTestingShingleFunc(docs_dict, MH.getShinglesWithSet)
        originalImplementation.append(round(time.time() - t0, 3))
    print('original:')
    print(originalImplementation)
    print('new:')
    print(newImplementation)
    plt.plot(file_size, originalImplementation, '-o')
    plt.plot(file_size, newImplementation, '-o')
    plt.xlabel('number of documents')
    plt.ylabel('time (sec)')
    plt.legend(["Old Implementation", "New Implementation"])
    plt.title('Indexing time for different shingling implementations')
    plt.savefig(os.path.join(OUTPUT_DIR, 'MH_Indexing time for different shingling implementations'))
    plt.show()

######### COMPARING NUMBER OF RANDOM STRINGS ON INDEXING TIME, SPACE, CORRELATIONS

def compareHashIndexingTimesForAllFiles():
    '''
    compares the cost of using different number of random words in hashing process in terms of time
    :return:
    '''
    # files = [100, 1000, 2500, 10000]
    files = [2500, 10000]   #whats left to save signatures
    numStrings = [3,10,20,50,100,200]
    allHashTimes = []
    #create directory
    for file in files:
        hashingTimePerFile = []
        docs_dict = PD.ParseTextDb(str(file) + ".txt")
        for size in numStrings:
            sig_path = os.path.join("hashes", str(file), str(size), "signatures.pkl")
            hash_path = os.path.join("hashes", str(file), str(size), "hashes.pkl")
            t0 = time.time()
            signatures, hashes = MH.minhashForTestingNumHashes(docs_dict,size)
            hashingTimePerFile.append(time.time() - t0)
            Path(os.path.dirname(sig_path)).mkdir(parents=True, exist_ok=True)
            Path(os.path.dirname(hash_path)).mkdir(parents=True, exist_ok=True)
            # os.makedirs(os.path.dirname(sig_path))
            PD.save_obj(signatures, sig_path)
            PD.save_obj(hashes, hash_path)
        print(hashingTimePerFile)
        allHashTimes.append(hashingTimePerFile)
    PD.save_obj(allHashTimes,"allHashTimes")
    plotHashTimesForAllFiles(allHashTimes)

def compareHashSpaceForAllFiles():
    '''
    compares the cost of using different number of random words in hashing process in terms of space
    :return:
    '''
    numStrings = [3, 10, 20, 50, 100, 200]
    files = [100, 1000, 2500, 10000]
    allHashSpaces = []
    for file in files:
        hashingSpacePerFile = []
        for size in numStrings:
            sig_path = os.path.join("hashes", str(file), str(size), "signatures.pkl")
            fileSize = os.path.getsize(sig_path)
            fileSize = round(float(fileSize / 1000000), 2)
            hashingSpacePerFile.append(fileSize)
        print(hashingSpacePerFile)
        allHashSpaces.append(hashingSpacePerFile)
    result_path = os.path.join("hashes", "MH_Index space for different number of random hash strings.pkl")
    PD.save_obj(allHashSpaces,result_path)
    plotHashSpacesForAllFiles(allHashSpaces)
    #     plotGraphComparingNumberOfHashStrings(allHashSpaces,'Space (Mb)','Index space for different number of random hash strings')


def corrIndexCreationTimingWithNumHashes(times):
    numStrings = [3,10,20,50,100,200]
    plotCorrelations(numStrings, times, 'Number of hashes', 'Indexing time (sec)',
                     'MH_Index creation time performance')
    # plt.savefig(os.path.join(OUTPUT_DIR, 'MH_Indexing time for different number of random hash strings'))

######### ACCURACY RELATED METHODS ###############

def getTestSet(setSize, dict):
    '''

    :param setSize: size of test set required
    :return: test set of requested size in the dictionary form: doc_id, doc_text
    '''
    test_set = {}
    for i in range(0, setSize):
        key = random.choice(list(dict.keys()))
        while (key in test_set):
            key = random.choice(list(dict.keys()))
        test_set[key] = dict[key]
    return test_set


def randomizeQuery(query, threshold):
    '''
    given document (query) change it word by word so that similarity to original query stays above threshold
    :param query: to change
    :param threshold: how much to change query
    :return: a document that has similarity to query that is above threshold, but close
    '''
    randomized = query.copy()
    i = 0
    while MH.jaccardCoefficient(query,randomized ) > threshold:
        last = randomized[i]
        randomized[i] = "**"
        i+=1
    # return the last changed word to what it was so similarity will be above threshold and not below
    randomized[i] = last
    return randomized


def getOverThreshold(dict, query, threshold, dist_func):
    '''
    find elements in index that are similar to query at least above threshold, measured by given metric
    :param dict: index
    :param query: new document, given in same format as index
    :param threshold: how high a similarity to look for
    :param dist_func: distance metric
    :return: list containing id of all similar entries in index
    '''
    similars = [key for key, value in dict.items() if dist_func(value, query) >= threshold]
    return similars

def compareAccuracybyDistanceFunction():  #test_set, signatures, distance function
    '''
    checks the accuracy be measuring the actual jaccard distance vs the jaccard distance between the
    skethces of 2 documents. Check the accuracy for different db sizes and different thresholds
    '''
    # TODO build it so it's more generic
    file = 100
    al = 'naive'
    naiveDataPath = os.path.join("obj", str(file), al, "signatures.pkl")
    data = PD.load_obj(naiveDataPath)
    #check accuracy a few times and take average error
    numIterations = 100
    numStrings = [3, 10, 20, 50, 100, 200]
    thresholds = [0.3,0.5,0.7,0.9,1]
    ErrorsForAllThresholds =[]
    for threshold in thresholds:
        errors = []
        for size in numStrings:
            error = 0
            hashes = MH.getRandomStrings(size)
            for i in range(0,numIterations):
                data1 = random.choice(list(data.values()))
                data2 = data1
                if threshold != 1:
                    data2 = randomizeQuery(data1,threshold)
                actualDistance = MH.jaccardCoefficient(data1, data2)
                sketch1 = MH.getSketch(data1,hashes)
                sketch2 = MH.getSketch(data2,hashes)
                mhDistance = MH.jaccardCoefficient(sketch1,sketch2)
                error += abs(actualDistance - mhDistance)
            avg_error = round(float(error) / numIterations,2)
            errors.append(avg_error)
            print('avg error: ' + str(avg_error)+ 'for ' + str(size) + ' hashes')
        ErrorsForAllThresholds.append(errors)
    title = "MH_Accuracy with different number of hash strings"
    errorsPath = os.path.join("hashes", title +".pkl")
    PD.save_obj(ErrorsForAllThresholds, errorsPath)

    plotGraphOfNumHashesAccuracy()

####### COMPARING DIFFERENT SIZE HASHES ##########

def compareHashSizesIndexingTime():
    '''
    compares time efficiency of creating sketches using 16, 32 and 64 byte hash functions
    :return:
    '''
    hashFuncs = [MH.get16bitSketch, MH.getSketch, MH.get64bitSketch]
    hashSizes =[16, 32, 64]
    allHashTimes = []
    file_size = [100, 1000,2500, 10000]
    al = "minhash"
    for i in range(0,3):
        func = hashFuncs[i]
        hashSize = hashSizes[i]
        hashTimesForFile = []
        for file in file_size:
            docs_dict = PD.ParseTextDb(str(file) + ".txt")
            t0 = time.time()
            signatures = MH.minhashForTestingHashSizes(docs_dict, func)
            hashTimesForFile.append(round(time.time() - t0,3))
            # save signatures
            print('saving signatures for file ' + str(file) +'size ' + str(hashSizes[i]))
            sig_path = os.path.join("obj", str(file), al,str(hashSize) + "bit", "signatures.pkl")
            PD.save_obj(signatures, sig_path)
        allHashTimes.append(hashTimesForFile)
    # save the times themselves
    allTimesPath = os.path.join("Tests_data", "MH_All_Hash_Sizes_Indexing_Times.pkl")
    PD.save_obj(allHashTimes, allTimesPath)

    plotDifferentSizeHashesIndexingTimeGraph()

def compareHashSizesIndexSpace():
    '''
    compares space efficiency of creating sketches using 16, 32 and 64 byte hash functions
    :return:
    '''
    hashSizes = [16,32,64]
    allHashSpaces = []

    file_sizes = [100, 1000,2500, 10000]
    al = "minhash"
    for size in hashSizes:
        hashSpace = []
        for file in file_sizes:
            sig_path = os.path.join("obj", str(file), al,str(size) + "bit", "signatures.pkl")
            fileSize = os.path.getsize(sig_path)
            fileSize = round(float(fileSize / 1000000), 2)
            hashSpace.append(fileSize)
        allHashSpaces.append(hashSpace)
    # save the space themselves
    allSpacePath = os.path.join("Tests_data", "MH_All_Hash_Index_Spaces.pkl")
    PD.save_obj(allHashSpaces, allSpacePath)

    plotDifferentSizeHashesIndexSpaceGraph()
#
# def createTestSets(setSize):
#     testSetsPath = os.path.join("Tests_data", "mh_test_sets.pkl")
#     testSets = []
#     for file in DBS:
#         testSet = getTestSet(setSize, file)
#         testSets.append(testSet)
#     PD.save_obj(testSets, testSetsPath)

# def compareAccuracyHashSizes():
#     file = 100
#     hashSizes = [16 ,32]    #hash sizes in bytes
#     threshold = 0.9
#     testSetSize = 100
#     al = "minhash"
#     testSetPath = os.path.join("Tests_data", "mh_test_sets.pkl")
#     testSets = PD.load_obj(testSetPath)
#
#     for testSet in testSets:
#         # for id, val in testSet.items():
#         testSet = getTestSet(testSetSize, file)
#         realSimilar = getRealSimilar(testSet)
#         for hashSize in hashSizes:
#             sig_path = os.path.join("obj", str(file), al, str(hashSize) + "bit", "signatures.pkl")
#             signatures = PD.load_obj(sig_path)
#
#
#     naiveDataPath = os.path.join("obj", str(file), al, "signatures.pkl")
#     data = PD.load_obj(naiveDataPath)
#     ErrorsForAllThresholds =[]
#     for threshold in thresholds:
#         errors = []
#         for size in numStrings:
#             error = 0
#             hashes = MH.getRandomStrings(size)
#             for i in range(0,numIterations):
#                 data1 = random.choice(list(data.values()))
#                 data2 = data1
#                 if threshold != 1:
#                     data2 = randomizeQuery(data1,threshold)
#                 actualDistance = MH.jaccardCoefficient(data1, data2)
#                 sketch1 = MH.getSketch(data1,hashes)
#                 sketch2 = MH.getSketch(data2,hashes)
#                 mhDistance = MH.jaccardCoefficient(sketch1,sketch2)
#                 error += abs(actualDistance - mhDistance)
#             avg_error = round(float(error) / numIterations,2)
#             errors.append(avg_error)
#             print('avg error: ' + str(avg_error)+ 'for ' + str(size) + ' hashes')
#         ErrorsForAllThresholds.append(errors)
#     title = "MH_Accuracy with different number of hash strings"
#     errorsPath = os.path.join("hashes", title +".pkl")
#     PD.save_obj(ErrorsForAllThresholds, errorsPath)
#
#     plotGraphOfNumHashesAccuracy()

# def getAccuracies(test_set, signatures, threshold):
#     for test_id, test_val in test_set.items():
#         t0 = time.time()
#         randomized_test_val = randomizeQuery(test_val, threshold)




######## CORRELATION PLOTTING ########3


def plotCorrelations(data1, data2,xlabel, ylabel,title): # TODO: FIND OUT WHY THE SCATTER PLOT DOESN'T WORK

    # calculate Pearson's correlation
    fig, ax = plt.subplots()
    ax.set(xlabel=xlabel, ylabel=ylabel)
    for dim in data2:
        corr, _ = pearsonr(data1, data2)
        plt.scatter(data1, dim, label=f'r = {corr}')
    plt.title(title)
    plt.legend()
    print(str(corr))

    plt.show()

############ GRAPH PLOTTING MEHTODS ################

def plotAccuracies(accuracies, testSetSize, threshold):
    # x = [100, 1000, 2500, 10000]
    x = [100, 1000]
    plt.plot(x, accuracies, '-o')
    plt.xlabel('number of documents')
    plt.ylabel('accuracy (%)')
    plt.title('Minhash algorith accuracy. Test set of size: %i. Threshold: %i' %(testSetSize, threshold))
    plt.show()


def plotHashTimesForAllFiles(allHashTimes):
    numStrings = [3, 10, 20, 50, 100, 200]
    docs = [100, 1000, 2500, 10000]
    i = 0
    for time in allHashTimes:
        corr, _ = pearsonr(numStrings, time)
        plt.plot(numStrings, time, '-o', label=f'{docs[i]} documents, r = {round(corr,2)}')
        i+=1
    plt.xlabel('number of random strings')
    plt.ylabel('time (sec)')
    plt.legend()
    # plt.legend(["100 documents", "1000 documents", "2500 documents", "10000 documents"])
    plt.title('Indexing time for different number of random hash strings')
    plt.savefig(os.path.join(OUTPUT_DIR, 'MH_Indexing time for different number of random hash strings'))
    plt.show()


def plotHashSpacesForAllFiles(allHashSizes):
    plt.figure()
    numStrings = [3, 10, 20, 50, 100, 200]
    for size in allHashSizes:
        plt.plot(numStrings, size, '-o')
    plt.xlabel('Number of random strings')
    plt.ylabel('Space (Mb)')
    plt.legend(["100 documents", "1000 documents", "2500 documents", "10000 documents"])
    plt.title('Index space for different number of random hash strings')
    plt.savefig(os.path.join(OUTPUT_DIR, 'MH_Index space for different number of random hash strings.png'))
    plt.show()

def plotGraphOfNumHashesAccuracy():
    title = "MH_Accuracy with different number of hash strings"
    errorsPath = os.path.join("hashes", title +".pkl")
    errors = PD.load_obj(errorsPath)
    thresholds = [0.3,0.5,0.7,0.9,1]
    numStrings = [3, 10, 20, 50, 100, 200]
    #PLOT
    i = 0
    for er in errors:
        plt.plot(numStrings, er, '-o',label= f'threshold = {str(thresholds[i])}')
        i += 1
    plt.xlabel('number of hash strings')
    plt.ylabel('average absolute error')
    plt.legend()
    plt.title(title)
    plt.savefig(os.path.join(OUTPUT_DIR, 'MH_accuracy with different number of hash strings.png'))
    plt.show()


def plotGraphComparingNumberOfHashStrings(results, ylabel,title):
    plt.figure()
    numStrings = [3, 10, 20, 50, 100, 200]
    for result in results:
        plt.plot(numStrings, result, '-o')
    plt.xlabel('Number of random strings')
    plt.ylabel(ylabel)
    plt.legend(["100 documents", "1000 documents", "2500 documents", "10000 documents"])
    plt.title(title)
    plt.savefig(os.path.join(OUTPUT_DIR, 'MH_' + title))
    plt.show()

def plotDifferentSizeHashesIndexingTimeGraph():
    # load data
    allTimesPath = os.path.join("Tests_data", "MH_All_Hash_Sizes_Indexing_Times.pkl")
    allTimes = PD.load_obj(allTimesPath)
    file_size = [100, 1000, 2500, 10000]
    for times in allTimes:
        plt.plot(file_size, times, '-o')
    plt.xlabel('number of documents')
    plt.ylabel('time (sec)')
    plt.legend(["16 bit hash", "32 bit hash", "64 bit hash"])
    title = 'Indexing time for different hash sizes'
    plt.title(title)
    plt.savefig(os.path.join(OUTPUT_DIR, 'MH_' + title + '.png'))
    plt.show()

def plotDifferentSizeHashesIndexSpaceGraph():
    # load data
    allSpacesPath = os.path.join("Tests_data", "MH_All_Hash_Index_Spaces.pkl")
    allSpaces = PD.load_obj(allSpacesPath)
    file_size = [100, 1000, 2500, 10000]
    for space in allSpaces:
        plt.plot(file_size, space, '-o')
    plt.xlabel('number of documents')
    plt.ylabel('size (Mb)')
    plt.legend(["16 bit hash", "32 bit hash", "64 bit hash"])
    title = 'Index space for different hash sizes'
    plt.title(title)
    plt.savefig(os.path.join(OUTPUT_DIR, 'MH_' + title + '.png'))
    plt.show()
######### OLD METHODS FOR CHECKING ACCURACY - SOME MAY STILL BE RELEVANT, I'LL WEED OUT LATER #######

def checkAccuracy(test_set, signatures, randomizationFactor,dist_func): # the old function
    '''

    :param test_set: the test set
    :param testSetAdapter: adapt test set to data (e.g - for minhash, turn to sketches)
    :param signatures: the processed data
    :param randomizationFactor: [0,1] equivalent to threshold - how random to make the query. e.g - 0.8
    :param dist_func: distance function to quantify similarity beween query and signatures
    :return:
    '''

    setSize = len(test_set)
    identified = 0
    for test_id, test_sig in test_set.items():
        # randomize query to be similar to original yet under 'randomizationFactor' similarity
        rQuery = randomizeQuery(test_sig, randomizationFactor)
        # get actual similarity between altered and original query
        querySimilarity = dist_func(rQuery, test_sig)
        # search for signatures whose similarity to altered query are 0.1 under similarity to original
        similars = getOverThreshold(signatures, rQuery, querySimilarity - 0.1, dist_func)
        if test_id in similars:
            identified += 1
        # print("The docs with over " + str(querySimilarity - 0.1) + " percent similarity to the query " + str(test_id) + " are:")
        # print(similars)
    print("Overall accuracy: " + str(identified) + " / " + str(setSize))
    return round(float(identified / setSize * 100),2)


def checkAllAccuracies(testSetSize, threshold):
    files = [100, 1000, 2500, 10000]
    accuracies = []
    for file in files:
        sig_path = os.path.join("obj", str(file), "minhash", "signatures.pkl")
        signatures = PD.load_obj(sig_path)
        accuracy = checkAccuracy(testSetSize, signatures, threshold)
        accuracies.append(accuracy)
    plotAccuracies(accuracies, testSetSize, threshold)

def checkAccuraciesOnDifferentHashSizes(testSetSize, threshold):
    distFunc = MH.jaccardCoefficient
    # files = [100, 1000, 2500, 10000]
    files = [100, 1000]
    numStrings = [3, 10, 20, 50, 100, 200]
    allAccuracies=[]
    for file in files:
        #after running with naive can upload naive instead of parsing again
        docs_dict = PD.ParseTextDb(str(file) + ".txt")
        accuraciesPerFile = []
        for size in numStrings:
            sig_path = os.path.join("hashes", str(file), str(size), "signatures.pkl")
            hash_path = os.path.join("hashes", str(file), str(size), "hashes.pkl")
            signatures = PD.load_obj(sig_path)
            hashes = PD.load_obj(hash_path)
            # process the test set so it fits the rest of the data .e.g - if minhash, turn testSet to sketches
            testSet = getTestSet(testSetSize, docs_dict)
            # testSet = adjustQueryToMinhashData(testSet,hashes)
            testSet,_ = adjustQueryToMinhashData(testSet,size)
            accuracyPercent = checkAccuracy(testSet,signatures,threshold,distFunc)
            accuraciesPerFile.append(accuracyPercent)
        print(accuracyPercent)
        allAccuracies.append(accuraciesPerFile)
    print(allAccuracies)
    plotGraphComparingNumberOfHashStrings(allAccuracies, 'Accuracy (%)', 'Comparing accuracy with different number '+
                                                                         'of random hash strings')
def adjustQueryToMinhashData(docs_dict,size):   #if second version, do hashes
    return MH.minhashForTestingNumHashes(docs_dict,size)
    # return MH.sketchData(docs_dict,hashes)


########## MISTAKE COVER-UP METHODS ##########

def saveShinglingTimes():
    #for new graph
    old = [3.349, 35.496, 91.413, 356.376]
    new = [3.41, 35.275, 91.179, 383.806]

def saveHashTimes():
    a = [0.33610081672668457, 0.8217628002166748, 1.576780080795288, 4.668513536453247, 7.833085060119629, 14.520159006118774]
    b = [2.7167341709136963, 7.954723596572876, 17.319631338119507, 39.97117018699646, 91.12819719314575, 165.02873969078064]
    c = [8.889508485794067, 20.00782585144043, 41.738280057907104, 97.58476161956787, 185.03486347198486, 361.19189739227295]
    d = [26.763323307037354, 76.91426277160645, 148.35449695587158, 412.4998049736023, 702.8549799919128, 1426.6138212680817]
    allHashTimes = []
    allHashTimes.append(a)
    allHashTimes.append(b)
    allHashTimes.append(c)
    allHashTimes.append(d)
    result_path = os.path.join("hashes", "MH_Index creation time for different number of random hash strings.pkl")
    # PD.save_obj(allHashTimes,result_path)
    plotHashTimesForAllFiles(allHashTimes)


# ################### SimHash Tests ###################################################


DATA_DIR = "Tests_data"
THRESHOLDS = [0.9, 0.8, 0.7]
INIT_BLOCKS = [1, 2, 3]
DBS = [100, 1000, 2500, 10000]
# DBS = [2500] #1000


def SH_variations_plot_index_size():
    measurements = PD.load_obj(os.path.join(OUTPUT_DIR, 'SH_variations_index_time_space.pkl'))
    sh_sizes = measurements[3]

    plt.figure(3)
    for i in range(len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))):
        plt.plot(DBS, sh_sizes[i::len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))], '-o')
    plt.xlabel('number of documents')
    plt.ylabel('Size (MB)')
    legend = []
    for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
        legend += ["SimHash " + str(t) + " " + str(b)]
    plt.legend(legend)
    plt.title('SimHash variations index size')
    plt.savefig(os.path.join(OUTPUT_DIR, 'SH_variations_index_size.png'))


def SH_variations_plot_index_size_W_NAIVE():
    measurements = PD.load_obj(os.path.join(OUTPUT_DIR, 'SH_variations_index_time_space.pkl'))
    sh_naive_size = measurements[2]
    sh_sizes = measurements[3]

    plt.figure(2)
    plt.plot(DBS, sh_naive_size, '-o')
    for i in range(len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))):
        plt.plot(DBS, sh_sizes[i::len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))], '-o')
    plt.xlabel('number of documents')
    plt.ylabel('Size (MB)')
    legend = ["Naive SimHash"]
    for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
        legend += ["SimHash " + str(t) + " " + str(b)]
    plt.legend(legend)
    plt.title('SimHash variations index size')
    plt.savefig(os.path.join(OUTPUT_DIR, 'SH_variations_index_size_W_NAIVE.png'))


def SH_variations_plot_index_time():
    measurements = PD.load_obj(os.path.join(OUTPUT_DIR, 'SH_variations_index_time_space.pkl'))
    sh_times = measurements[1]

    plt.figure(1)
    for i in range(len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))):
        plt.plot(DBS, sh_times[i::len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))], '-o')
    plt.xlabel('number of documents')
    plt.ylabel('time (sec)')
    legend = []
    for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
        legend += ["SimHash " + str(t) + " " + str(b)]
    plt.legend(legend)
    plt.title('SimHash variations indexing time')
    plt.savefig(os.path.join(OUTPUT_DIR, 'SH_variations_index_time.png'))


def SH_variations_plot_index_time_W_NAIVE():
    measurements = PD.load_obj(os.path.join(OUTPUT_DIR, 'SH_variations_index_time_space.pkl'))
    sh_naive_times = measurements[0]
    sh_times = measurements[1]

    plt.figure(0)
    plt.plot(DBS, sh_naive_times, '-o')
    for i in range(len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))):
        plt.plot(DBS, sh_times[i::len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))], '-o')
    plt.xlabel('number of documents')
    plt.ylabel('time (sec)')
    legend = ["Naive SimHash"]
    for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
        legend += ["SimHash " + str(t) + " " + str(b)]
    plt.legend(legend)
    plt.title('SimHash variations indexing time')
    plt.savefig(os.path.join(OUTPUT_DIR, 'SH_variations_index_time_W_NAIVE.png'))


def SH_variations_plot_query_time_W_NAIVE():
    measurements = PD.load_obj(os.path.join(OUTPUT_DIR, 'SH_variations_query_time.pkl'))
    sh_naive_times = measurements[0]
    sh_times = measurements[1]
    plot_num = 0

    plt.figure(4)
    legend = []
    for i in range(len(THRESHOLDS)):
        plt.plot(DBS, sh_naive_times[i::len(THRESHOLDS)], '-o')
        legend += ["Naive SimHash " + str(THRESHOLDS[i])]

    for i in range(len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))):
        plt.plot(DBS, sh_times[i::len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))], '-o')

    plt.xlabel('number of documents')
    plt.ylabel('time (sec)')
    for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
        legend += ["SimHash " + str(t) + " " + str(b)]
    plt.legend(legend)
    plt.title('SimHash variations indexing time')
    plt.savefig(os.path.join(OUTPUT_DIR, 'SH_variations_query_time_W_NAIVE.png'))


def SH_variations_plot_query_time():
    measurements = PD.load_obj(os.path.join(OUTPUT_DIR, 'SH_variations_query_time.pkl'))
    sh_times = measurements[1]
    plt.figure(5)
    for i in range(len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))):
        plt.plot(DBS, sh_times[i::len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))], '-o')
    plt.xlabel('number of documents')
    plt.ylabel('time (sec)')
    legend = []
    for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
        legend += ["SimHash " + str(t) + " " + str(b)]
    plt.legend(legend)
    plt.title('SimHash variations querying time')
    plt.savefig(os.path.join(OUTPUT_DIR, 'SH_variations_query_time_W_NAIVE.png'))


def SH_variations_plot_index_query_correlation_W_NAIVE():
    index_measurements = PD.load_obj(os.path.join(OUTPUT_DIR, 'SH_variations_index_time_space.pkl'))
    query_measurements = PD.load_obj(os.path.join(OUTPUT_DIR, 'SH_variations_query_time.pkl'))
    naive_index_times = index_measurements[0]
    sh_index_times = index_measurements[1]
    naive_query_times = query_measurements[0]
    sh_query_times = query_measurements[1]

    plt.figure(6)

    legend = []
    for i in range(len(THRESHOLDS)):
        plt.plot(naive_index_times, naive_query_times[i::len(THRESHOLDS)], '-o')
        legend += ["Naive SimHash " + str(THRESHOLDS[i])]

    for i in range(len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))):
        plt.plot(sh_index_times, sh_query_times[i::len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))], '-o')

    plt.xlabel('Indexing time (sec)')
    plt.ylabel('Querying Time (sec)')
    for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
        legend += ["SimHash " + str(t) + " " + str(b)]
    plt.legend(legend)
    plt.title('SimHash variations indexing time')
    plt.savefig(os.path.join(OUTPUT_DIR, 'SH_variations_index_query_correlation_W_NAIVE.png'))


def SH_variations_plot_index_query_correlation():
    index_measurements = PD.load_obj(os.path.join(OUTPUT_DIR, 'SH_variations_index_time_space.pkl'))
    query_measurements = PD.load_obj(os.path.join(OUTPUT_DIR, 'SH_variations_query_time.pkl'))
    sh_index_times = index_measurements[1]
    sh_query_times = query_measurements[1]

    plt.figure(7)
    for i in range(len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))):
        plt.plot(sh_index_times, sh_query_times[i::len(list(itertools.product(THRESHOLDS, INIT_BLOCKS)))], '-o')
    plt.xlabel('Indexing time (sec)')
    plt.ylabel('Querying Time (sec)')
    legend = [""]
    for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
        legend += ["SimHash " + str(t) + " " + str(b)]
    plt.legend(legend)
    plt.title('SimHash variations indexing time')
    plt.savefig(os.path.join(OUTPUT_DIR, 'SH_variations_index_query_correlation.png'))


def SH_variations_measure_querying_time():
    test_set_path = os.path.join("Tests_data", "test_sets.pkl")
    test_sets = PD.load_obj(test_set_path)

    i = 0
    for size in DBS:
        print("DB of size: " + str(size))
        # load test set:
        index_path = os.path.join("obj", str(size))
        times_path = os.path.join(DATA_DIR, 'SH_variations_query_time_' + str(size) + '.pkl')
        similars_path = os.path.join(DATA_DIR, 'SH_variations_similars_' + str(size) + '.pkl')
        test_set = test_sets[i]
        i += 1

        # Naive:
        nt = []
        sims = []
        for t in THRESHOLDS:
            t_time = 0
            t_similars = []
            for test_id, test_text in test_set.items():
                t0 = time.time()
                test_sig = SH.make_signature(test_text)
                similars = SH.compute_all_distances(os.path.join(index_path, "Naive simhash", "signatures_naive.pkl"),
                                                    test_sig, t)
                t1 = round(time.time() - t0, 3)
                t_time += t1
                t_similars.append(similars)
            sims.append(t_similars)
        PD.save_obj([nt, []], times_path)
        PD.save_obj([sims, []], similars_path)

        # Efficient variations:
        for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
            print("SH of params: " + str(t) + " " + str(b))
            t_time = 0
            t_similars = []
            for test in test_set.items():
                test_id, test_text = test
                t0 = time.time()
                test_sig = SH.make_signature(test_text)
                similars = SH.getOverThresholdEfficiently(os.path.join(index_path, "simhash_" + str(t) + "_" + str(b)),
                                                          test_sig, t, b)
                t1 = round(time.time() - t0, 3)
                t_time += t1
                t_similars.append(similars)

            nt, st = PD.load_obj(times_path)
            st.append(t_time)
            PD.save_obj([nt, st], times_path)

            nsims, ssims = PD.load_obj(similars_path)
            ssims.append(t_similars)
            PD.save_obj([nsims, ssims], similars_path)


def SH_variations_measure_index_time_size():
    for size in DBS:
        print("DB of size: " + str(size))
        docs_dict = PD.ParseTextDb(str(size) + ".txt")
        data_path = os.path.join(DATA_DIR, 'SH_variations_index_time_space_' + str(size) + '.pkl')

        # Naive:
        index_path = os.path.join("obj", str(size), "Naive simhash", "signatures_naive.pkl")
        t0 = time.time()
        SH.naive_simhash(docs_dict, index_path)
        t = round(time.time() - t0, 3)
        s = round(float(os.path.getsize(index_path) / 1000000), 2)
        # save time & size:
        PD.save_obj([[t], [], [s], []], data_path)

        # Efficient variations:
        for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
            print("SH of params: " + str(t) + " " + str(b))
            index_path = os.path.join("obj", str(size), "simhash_" + str(t) + "_" + str(b))
            t0 = time.time()
            SH.simhash(docs_dict, t, b, index_path)
            t1 = round(time.time() - t0, 3)

            # save time:
            nt, st, ns, ss = PD.load_obj(data_path)
            st.append(t1)

            # save size:
            index_file_paths = os.listdir(index_path)
            s = 0
            for file_path in index_file_paths:
                s += round(float(os.path.getsize(os.path.join(index_path, file_path)) / 1000), 2)
            ss.append(s)

            PD.save_obj([nt, st, ns, ss], data_path)

def SH_variations_plots():

    # variations indexing time:
    SH_variations_plot_index_time_W_NAIVE()
    SH_variations_plot_index_time()

    # variations indexing size:
    SH_variations_plot_index_size_W_NAIVE()
    SH_variations_plot_index_size()

    # variations querying time:
    SH_variations_plot_query_time_W_NAIVE()
    SH_variations_plot_query_time()

    # indexing time vs. querying time:
    SH_variations_plot_index_query_correlation_W_NAIVE()
    SH_variations_plot_index_query_correlation()


# Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# SH_variations_measure_index_time_size()
test_set_size = 10
# SH_variations_measure_querying_time(test_set_size)
# SH_variations_plots()

# print(PD.load_obj(os.path.join(DATA_DIR, 'test_set_' + str(10000) + '.pkl')))
[('t4954', 43287334915323410124380358313418580609825341435281583030645313025659842465563),
 ('t6402', 6367195511805346706301222161894826857186885748815064928940387128533450337722),
 ('t5923', 28360675519804093642774656152402678017520640838950616472283093392615979633717),
 ('t7928', 59889095375729255214801387293850097180091114927709745820696419875530778320687),
 ('t7151', 71630142347215115416823168165308854797541287792836021806960170701857883462192),
 ('t3953', 17615652751145436086710211752942642910095822920044464596714510165593216127469),
 ('t8280', 80258786070060566200539075926640762585758474553043425170348756359127607015496),
 ('t7415', 81242093521679384774341571000573672692887636994910371097697176480366745550283),
 ('t3257', 46552193128244388498457652365150923200501624211864350027724101806780414002367),
 ('t8534', 36792819731763594915510905022420671375862302404214707642213540790706974785676)]

############ SAVE INDEX TIMES, INDEX SPACE AND QUERY TIMES FOR NAIVE #########

def naive_measure_index_time_size():
    data_path = os.path.join("Tests_data", "naive_measure_index_time_size.pkl")
    PD.save_obj([[], []], data_path)
    for size in DBS:
        print("DB size: ", str(size))
        nt, ns = PD.load_obj(data_path)

        index_path = os.path.join("obj", str(size), "Naive", "signatures.pkl")

        t0 = time.time()
        signatures = PD.ParseTextDb(os.path.join(str(size) + ".txt"))
        PD.save_obj(signatures, index_path)
        nt.append(round(time.time() - t0, 3))

        ns.append(round(float(os.path.getsize(index_path) / 1000000), 2))

        # save time & size:
        PD.save_obj([nt, ns], data_path)


def naive_measure_query_time():
    similars_path = os.path.join("Tests_data","Naive_30", "naive_similars.pkl")
    times_path = os.path.join("Tests_data","Naive_30", "naive_query_time.pkl")
    test_set_path = os.path.join("Tests_data","Naive_30", "test_set.pkl")

    # test_set = PD.load_obj(test_set_path)

    PD.save_obj([], similars_path)
    PD.save_obj([], times_path)
    PD.save_obj([], test_set_path)
    t = 0.9
    for size in DBS:
        print("DB size: ", str(size))

        index_path = os.path.join("obj", str(size), "Naive", "signatures.pkl")

        # create test set and save it:
        signatures = PD.load_obj(index_path)

        test_set = getTestSet(30, signatures)
        tests = PD.load_obj(test_set_path)
        tests.append(test_set)
        PD.save_obj(tests, test_set_path)

        t_time = 0
        t_similars = []
        for query in test_set:
            # query index, time and results:
            t0 = time.time()
            similars = getOverThreshold(signatures, query, t, MH.jaccardCoefficient)
            t1 = round(time.time() - t0, 3)
            t_time += t1
            t_similars.append(similars)

        # save time & results:
        sims = PD.load_obj(similars_path)
        sims.append(t_similars)
        PD.save_obj(sims, similars_path)

        nt = PD.load_obj(times_path)
        nt.append(t_time)
        PD.save_obj(nt, times_path)


############ SAME FOR MINHASH #########

def minhash_measure_index_time_size():
    data_path = os.path.join("Tests_data", "MH_measure_index_time_size.pkl")
    PD.save_obj([[], []], data_path)
    for size in DBS:
        print("DB size: ", str(size))
        nt, ns = PD.load_obj(data_path)

        index_path = os.path.join("obj", str(size), "minhash", "signatures.pkl")

        t0 = time.time()
        signatures = PD.ParseTextDb(os.path.join(str(size) + ".txt"))
        PD.save_obj(signatures, index_path)
        nt.append(round(time.time() - t0, 3))

        ns.append(round(float(os.path.getsize(index_path) / 1000000), 2))

        # save time & size:
        PD.save_obj([nt, ns], data_path)

def minhash_measure_query_time():
    similars_path = os.path.join("Tests_data","MH_30", "MH_similars.pkl")
    times_path = os.path.join("Tests_data","MH_30", "MH_query_time.pkl")
    test_set_path = os.path.join("Tests_data","Naive_30", "test_set.pkl")
    test_set = PD.load_obj(test_set_path)
    test_set = test_set[0]
    t = 0.9
    PD.save_obj([], similars_path)
    PD.save_obj([], times_path)

    for size in DBS:
        print("DB size: ", str(size))
        index_path = os.path.join("obj", str(size), "Naive", "signatures.pkl")

        # create test set and save it:
        signatures = PD.load_obj(index_path)
        t_time = 0
        t_similars = []
        for query in test_set:
            # query index, time and results:
            t0 = time.time()
            similars = getOverThreshold(signatures, query, t, MH.jaccardCoefficient)
            t1 = round(time.time() - t0, 3)
            t_time += t1
            t_similars.append(similars)

        # save time & results:
        sims = PD.load_obj(similars_path)
        sims.append(t_similars)
        PD.save_obj(sims, similars_path)

        nt = PD.load_obj(times_path)
        nt.append(t_time)
        PD.save_obj(nt, times_path)

def SH_chosen_measure_querying_time():
    t = 0.9
    b = 2
    test_set_path = os.path.join("Tests_data","Naive_30", "test_set.pkl")
    times_path = os.path.join(DATA_DIR, "SH_chosen", 'SH_query_time_30_1000_2500' + '.pkl')
    similars_path = os.path.join(DATA_DIR, "SH_chosen", 'SH_similars_30_1000_2500' + '.pkl')

    f_times = []
    f_similars = []

    test_sets = PD.load_obj(test_set_path)

    i = 0
    for size in [1000, 2500]:
        print("DB of size: " + str(size))
        index_path = os.path.join("obj", str(size))
        test_set = test_sets[i]

        t_time = 0
        t_similars = []
        for test_id, test_text in test_set.items():
            t0 = time.time()
            test_sig = SH.make_signature(test_text)
            similars = SH.getOverThresholdEfficiently(os.path.join(index_path, "simhash_" + str(t) + "_" + str(b)),
                                                      test_sig, t, b)
            t1 = round(time.time() - t0, 3)
            t_time += t1
            t_similars.append(similars)

        f_similars.append(t_similars)
        f_times.append(t_time)

    PD.save_obj(f_similars, similars_path)
    PD.save_obj(f_times, times_path)

def getNaiveAndMHSims():
    testSet = PD.load_obj(os.path.join("Tests_data","Naive_30", "test_set.pkl"))
    allNaiveSims = []
    allMHSims = []
    t = 0.9
    i = 0
    randWords = MH.getRandomStrings()
    for size in DBS:
        FiletestSet = testSet[i]
        print("DB size: ", str(size))
        index_path = os.path.join("obj", str(size), "naive", "signatures.pkl")
        # create test set and save it:
        nSims = []
        mhSims = []
        n_signatures = PD.load_obj(index_path)
        mh_signatures = PD.load_obj(os.path.join("obj", str(size), "minhash", "signatures.pkl"))
        for query in FiletestSet.values():
            # query index, time and results:
            n_similars = getOverThreshold(n_signatures, query, t, MH.jaccardCoefficient)
            nSims.append(n_similars)
            mh_similars = getOverThreshold(mh_signatures, MH.getSketch(query,randWords), t, MH.jaccardCoefficient)
            mhSims.append(mh_similars)
        print(nSims)
        print(mhSims)
        allNaiveSims.append(nSims)
        allMHSims.append(mhSims)
        i += 1
    PD.save_obj(allNaiveSims,os.path.join("Tests_data","Naive_30", "naive_similars.pkl"))
    PD.save_obj(allMHSims,os.path.join("Tests_data","MH_30", "MH_similars.pkl"))
    return allNaiveSims, allMHSims





def plot_graph(p_id, x, ys, x_title, y_title, title, legend, name, y_scale):
    plt.figure(p_id)
    for y in ys:
        plt.plot(x, y, '-o')
    plt.xlabel(x_title)
    plt.ylabel(y_title)
    plt.yscale(y_scale)
    plt.legend(legend)
    plt.title(title)
    plt.savefig(os.path.join(OUTPUT_DIR, name + '.png'))
    plt.show()

def plotQueryTimes():
    naiveTimes = PD.load_obj(os.path.join("Tests_data","Naive_30", "naive_query_time.pkl"))
    mhTimes = PD.load_obj(os.path.join("Tests_data","MH_30", "MH_query_time.pkl"))
    shTimesLeah = PD.load_obj(os.path.join(DATA_DIR, "SH_chosen", 'SH_query_time_30_1000_2500' + '.pkl'))

    shTimesBar = PD.load_obj(os.path.join(DATA_DIR, "SH_chosen", 'SH_query_time_100_10000' + '.pkl'))
    shTimes = []
    shTimes.append(shTimesBar[0])
    shTimes.append(shTimesLeah[0])
    shTimes.append(shTimesLeah[1])
    shTimes.append(shTimesBar[1])
    ys = [naiveTimes, mhTimes,shTimes]
    allQueryTimesPath = os.path.join("Tests_data","All_query_times.pkl")
    PD.save_obj(ys,allQueryTimesPath)
    plot_graph(0, DBS, ys, "Number of documents", "Time (seconds)",
               "Querying time of Naive, Minhash and Simhash_0.7_2 Algorithms",
               ["Naive", "Minhash", "Simhash 0.9 2"],
               "Querying_time_all_methods", "linear")

def calculatePrecision():
    nSims = PD.load_obj(os.path.join("Tests_data","Naive_30", "naive_similars.pkl"))
    mhSims = PD.load_obj(os.path.join("Tests_data","MH_30", "MH_similars.pkl"))
    shSimsLeah = PD.load_obj(os.path.join(DATA_DIR, "SH_chosen", 'SH_similars_30_1000_2500' + '.pkl'))
    shSimsBar = PD.load_obj(os.path.join(DATA_DIR, "SH_chosen", 'SH_similars_100_10000' + '.pkl'))
    shSims = []
    shSims.append(shSimsBar[0])
    shSims.append(shSimsLeah[0])
    shSims.append(shSimsLeah[1])
    shSims.append(shSimsBar[1])
    finalSHSims = []
    for file_sim in shSims:
        fileSimList = []
        for simDict in file_sim:
            fileSimList.append(list(simDict.keys()))
        finalSHSims.append(fileSimList)

    print(finalSHSims[1])
    print(nSims[1])
    allSims = [nSims,mhSims,finalSHSims]
    allPrecisions=[]
    for algo in allSims:
        precisions = []
        for i in range(0,4):
            changed = algo[i]
            truth = nSims[i]
            tp = 0
            for id in changed:
                if id in truth:
                    tp+=1
            fn = len(nSims[i]) - tp
            precisions.append(round(float(tp) / (tp + fn)))
        allPrecisions.append(precisions)
    PD.save_obj(allPrecisions,os.path.join("Tests_data","All_query_precisions.pkl"))

    plot_graph(0,DBS,allPrecisions,"Number of documents", "Precision",
               "Precision of Naive, Minhash and Simhash_0.7_2 Algorithms",
               ["Naive", "Minhash", "Simhash 0.9 2"],
               "Precision_all_methods", "linear")


# def calculateRecall():




# ####################### Testing ################################################3


# SH_variations_indexing_time_size()


# def compare32vs64bitHashForMinhash():

# def checkAccuracy():

# MHvsSH_indexingTime()
# createTextIndex()
# getTextIndexSize()
# compareShinglingImplementations()
# checkAllAccuraciesMinhash(100, 0.8)
# compareHashIndexingTimesForAllFiles()

# saveHashTimes()
# hashTimes = PD.load_obj(os.path.join("hashes", 'allHashTimes'))
# plotHashTimesForAllFiles(hashTimes)
# compareHashSpaceForAllFiles()
# checkAccuraciesOnDifferentHashSizes(50,0.8)
# compJaccardAccuracy()
# naiveIndexingTimeTextDB()
# numStrings =[3, 10, 20, 50, 100, 200]
# errors =[0.21, 0.12, 0.07, 0.06, 0.05, 0.03]
# plotCorrelations(numStrings,errors,"bka" )
# compareAccuracybyDistanceFunction()
# plotGraphOfNumHashesAccuracy()

# compareHashSizesIndexingTime()
# plotDifferentSizeHashesIndexingTimeGraph(smallerSize,biggerSize)
# compareHashSizesIndexSpace()
# minhash_measure_index_time_size()
# minhash_measure_query_time()
# SH_variations_measure_index_time_size()
# SH_variations_measure_querying_time()
# text = PD.load_obj(os.path.join("obj","10000","naive","signatures.pkl "))
# length = 0.0
# for val in text.values():
#     length += len(val)
# print('avg = ' + str(round(length / len(text),2)))
# naive_measure_query_time()
# minhash_measure_query_time()
# SH_chosen_measure_querying_time()
# plotQueryTimes()
calculatePrecision()
# n, mh = getNaiveAndMHSims()
# print(n)
# print(mh)