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
import numpy as np

OUTPUT_DIR = "Graphs"
DATA_DIR = "Tests_data"
THRESHOLDS = [0.9, 0.8, 0.7]
INIT_BLOCKS = [1, 2, 3]
DBS = [100, 1000, 2500, 10000]



# ################### get data  ###################################################


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
            print(t_time)
            nt.append(t_time)
            sims.append(t_similars)
        PD.save_obj([nt, []], times_path)
        PD.save_obj([sims, []], similars_path)

        # Efficient variations:
        for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
            print("SH of params: " + str(t) + " " + str(b))
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
            print(t_time)
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


# ################### plot data  ###################################################

def plot_graphs():
    index_times = []
    index_sizes = []

    naive_query_times = []
    query_times = []

    similars = []

    # collecting data:
    for db_size in DBS:
        nt, st, ns, ss = PD.load_obj(os.path.join(DATA_DIR,
                                                  "SH_variations_index_time_space_" + str(db_size) + ".pkl"))
        nnt = [e/60 for e in nt] #convert to mins
        nst = [e/60 for e in st]
        index_times.append(nnt + nst)

        nss = [e/1000 for e in ss]  #convert to mb
        index_sizes.append(ns + nss)

        nt, st = PD.load_obj(os.path.join(DATA_DIR,
                                                  "SH_variations_query_time_" + str(db_size) + ".pkl"))
        nnt = [e / 60 for e in nt] #convert to mins
        nst = [e / 60 for e in st]
        naive_query_times.append(nnt)
        query_times.append(nst)

        # nt, st, ns, ss = PD.load_obj(os.path.join(DATA_DIR,
        #                                           "SH_variations_query_similars_" + str(db_size) + ".pkl"))
        # index_times.append(nt + st)
        # index_sizes.append(ns + ss)


    # orginazing data:
    index_times = np.stack(index_times, axis=1)
    index_sizes = np.stack(index_sizes, axis=1)
    query_times, naive_query_times = np.stack(query_times, axis=1), np.stack(naive_query_times, axis=1)

    # creating titles:
    legend = ["Naive simhash"]
    for t, b in list(itertools.product(THRESHOLDS, INIT_BLOCKS)):
        legend += ["SimHash " + str(t) + " " + str(b)]

    # graphs:
    p_id = 0
    plot_graph(p_id, DBS, index_times, "Number of documents", "Time (minutes)",
                "SimHash variations indexing time", legend, "SH_indexing_time", 'log')
    p_id += 1

    plot_graph(p_id, DBS, index_sizes, "Number of documents", "Size (MB)",
                "SimHash variations index size", legend, "SH_index_size", 'log')
    p_id += 1

    for i in range(len(THRESHOLDS)):
        legend_i = [legend[0]] + legend[3*i + 1: 3*i + 4]
        ys = query_times[3*i:3*i+3]
        ys = np.concatenate(([naive_query_times[i]], ys), axis=0)
        t = THRESHOLDS[i]
        plot_graph(p_id, DBS, ys, "Number of documents", "Time (Minutes)",
                    "SimHash variations querying time_t=" + str(t), legend_i, "SH_query_time_MIN" + str(t), 'linear')
        p_id += 1

    # plot_graph(p_id, DBS, index_sizes, "Number of documents", "Accuracy (Hamming distance)",
    #             "SimHash variations acurracy", legend, "SH_accuracy")
    # p_id += 1


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


Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


def SH_chosen_measure_querying_time():
    t = 0.9
    b = 2
    test_set_path = os.path.join("Tests_data", "test_set_30.pkl")
    times_path = os.path.join(DATA_DIR, "SH_chosen", 'SH_query_time_30' + '.pkl')
    similars_path = os.path.join(DATA_DIR, "SH_chosen", 'SH_similars_30' + '.pkl')

    f_times = []
    f_similars = []

    test_sets = PD.load_obj(test_set_path)

    i = 0
    for size in [100, 10000]:
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


# SH_chosen_measure_querying_time()
a = PD.load_obj(os.path.join("Tests_data", "test_set_30.pkl"))
b = PD.load_obj(os.path.join(DATA_DIR, "SH_chosen", 'SH_similars_30' + '.pkl'))
print(a)
print(b[0])
print(b[1])