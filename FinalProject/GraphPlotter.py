import itertools
import os
import PreprocessData as PD
import matplotlib.pyplot as plt
import numpy as np

DBS = [100, 1000, 2500, 10000]
THRESHOLDS = [0.9, 0.8, 0.7]
DATA_DIR = "Tests_data"
OUTPUT_DIR = "Graphs"


def createIndexTimeSizePlots():
    index_times = []
    index_sizes = []
    for db_size in DBS:
        nt, st, ns, ss = PD.load_obj(os.path.join(DATA_DIR,
                                                  "SH_variations_index_time_space_" + str(db_size) + ".pkl"))

        index_times.append(nt + st)
        index_sizes.append(ns + ss)
    index_times = np.stack(index_times, axis=1)
    index_sizes = np.stack(index_sizes, axis=1)
    sh_time = index_times[2::3]
    sh_space = index_sizes[2::3]

    print(sh_space)
    # change sh space to Mb
    sh_space[:] = [x / 1000 for x in sh_space]
    print(sh_space)

    legend = []
    for t, b in list(itertools.product(THRESHOLDS, [2])):
        legend += ["SimHash " + str(t) + " " + str(b)]

    timeMeasure = "measure_index_time_size.pkl"
    mh_time, mh_space = PD.load_obj(os.path.join(DATA_DIR, "MH_" + timeMeasure))
    n_time, n_space = PD.load_obj(os.path.join(DATA_DIR, "naive_" + timeMeasure))
    # plotTime(n_time, mh_time, sh_time, legend)
    plotSpace(n_space, mh_space, sh_space,legend)

def plotTime(n_time, mh_time, sh_times, sh_legends):
    plt.plot(DBS, n_time,'-o')
    plt.plot(DBS, mh_time,'-o')
    for time in sh_times:
        plt.plot(DBS,time, '-o')
    legend = ["naive", "Minhash"]
    legend += sh_legends
    plt.title('Indexing time for naive, minhash and simhash')
    plt.xlabel('Number of documents')
    plt.ylabel('Time (sec)')
    plt.legend(legend)
    plt.yscale('log')
    plt.savefig(os.path.join(OUTPUT_DIR, 'Indexing time for all algorithms'))
    plt.show()

def plotSpace(n_space, mh_space, sh_spaces, sh_legends):
    plt.plot(DBS, n_space,'-o')
    plt.plot(DBS, mh_space,'-o')
    for space in sh_spaces:
        plt.plot(DBS, space, '-o')
    legend = ["Naive", "Minhash"]
    legend += sh_legends
    plt.title('Index space for naive, minhash and simhash')
    plt.xlabel('Number of documents')
    plt.ylabel('Size (Mb)')
    plt.yscale('log')
    plt.legend(legend)
    plt.savefig(os.path.join(OUTPUT_DIR, 'Index space for all algorithms'))
    plt.show()



createIndexTimeSizePlots()

