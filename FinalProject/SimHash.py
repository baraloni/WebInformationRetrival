import itertools
import os
import time

import numpy as np
from hashlib import sha256
import MinHash as MH
import PreprocessData as PD

# ############## Preprocess Data ###################

HASH_FUNC = sha256
HASH_LEN = 256

index_pie = dict()
query_pie = dict()


def make_signature(doc_text: str) -> int:
    features = create_features(doc_text)

    t0 = time.time()

    hashes = hash_features(features)
    sig = int(compute_signature(hashes), 2)

    t1 = round(time.time() - t0, 3)
    index_pie["signatures"] = index_pie["signatures"] + t1 if "signatures" in index_pie else t1
    return sig


def create_features(doc_text: str) -> list:
    """

    :param doc_text:
    :return: A set of all shingels
    """
    t0 = time.time()

    shingles = MH.getShingles(doc_text)

    t1 = round(time.time() - t0, 3)
    index_pie["shingles"] = index_pie["shingles"] + t1 if "shingles" in index_pie else t1
    return shingles


def hash_features(features: list) -> list:
    hashes = list()
    for feature in features:
        hash_val = HASH_FUNC(feature.encode('raw_unicode_escape'))  # Encodes
        hash_val = int(hash_val.hexdigest(), base=16)  # Converts the hash value to int
        bin_hash_val = bin(hash_val)[2:]  # Converts the integer hash value to binary string
        full_bin_hash_val = bin_hash_val.zfill(HASH_LEN)
        hashes.append(full_bin_hash_val)
    return hashes


def compute_signature(hashes: list) -> str:
    signature = str()
    for col in (zip(*hashes)):
        col = list(map(int, col))
        signature += (str(int(round(np.mean(col)))))
    return signature


def minimal_num_of_blocks(threshold: float) -> int:
    """
    :param threshold:
    :return: The number of signatures blocks that should differ. Fixes the number of signature blocks lower bound.
    """
    return int((1 - threshold) * HASH_LEN)


def partition_to_blocks(num_of_blocks: int) -> list:
    """
    :param num_of_blocks:
    :return: A partition of the sig_len to num_of_blocks
    """
    # q, r = divmod(HASH_LEN, int(num_of_blocks))
    # return [((q + 1) * i, (q + 1) * (i + 1)) for i in range(r)] + \
    #        [((q + 1) * r + q * i, (q+1) * r + q * (i + 1)) for i in range(num_of_blocks - r)]
    q, r = divmod(HASH_LEN, int(num_of_blocks))
    return [q + 1 for i in range(r)] + \
           [q for i in range(num_of_blocks - r)]


def create_permutation_formulas(partition: list, min_num_of_blocks: int) -> list:
    # combinations = [i for i in itertools.combinations(partition, min_num_of_blocks)]
    # permutations = list()
    # for comb in combinations:
    #     permutations.append([i for i in partition if i not in comb] + list(comb))
    # permutations.remove(partition)
    # return permutations
    id_formula = range(len(partition))
    combinations = [i for i in itertools.combinations(id_formula, min_num_of_blocks)]
    permutations = list()
    for comb in combinations:
        permutations.append([i for i in id_formula if i not in comb] + list(comb))
    permutations.remove(list(id_formula))
    return permutations


def permute_sig(partition: list, sig_parts: list, per: list) -> int:
    per = per[::-1]
    res = 0
    sizes = 0
    for p in per:
        res += sig_parts[p] << sizes
        sizes += partition[p]
    return res
    # return int(''.join([sig[block_lims[0]: block_lims[1]] for block_lims in per]), 2)


def part_sig(partition: list, sig: int) -> list:
    partition.append(0)
    blocks = []
    s = sum(partition)
    mask = 0
    for i in range(len(partition) - 1):
        s -= partition[i]
        prefixed_block = sig >> s
        blocks.append(prefixed_block - mask)
        mask = prefixed_block << partition[i + 1]
    partition.pop()
    return blocks


def part_signatures(partition: list, signatures: dict) -> dict:
    parted_sigs = dict()
    for doc_id, doc_sig in signatures.items():
        parted_sigs[doc_id] = part_sig(partition, doc_sig)
    return parted_sigs


def create_and_save_permutation_tables(partition, parted_signatures, permutation_formulas, index_dir):
    # Save the permutations tables:
    table_formula_mapping = dict()
    per_table = dict()
    for per_idx, per in enumerate(permutation_formulas):

        # create the per table:
        for doc_id, doc_sig in parted_signatures.items():
            per_table[doc_id] = permute_sig(partition, doc_sig, per)

        # sort the table and save it:
        save_path = os.path.join(index_dir, "signatures_p" + str(per_idx) + ".pkl")
        PD.save_obj({k: v for k, v in sorted(per_table.items(), key=lambda item: item[1])}, save_path)

        # update the mapping table: (filename, permutation)
        table_formula_mapping[save_path] = per

    # save the mapping table:
    PD.save_obj(table_formula_mapping, os.path.join(index_dir, "mapping.pkl"))


def save_original_table(signatures: dict, index_dir: str) -> None:
    # Save the original table, sorted:
    save_path = os.path.join(index_dir, "signatures.pkl")
    per_table = {k: v for k, v in sorted(signatures.items(), key=lambda item: item[1])}
    PD.save_obj(per_table, save_path)


# ############### SimHash Algorithm ######################


def naive_simhash(docs_dict: dict, index_dir: str) -> None:
    signatures = dict()
    for doc_id in docs_dict.keys():
        signatures[doc_id] = make_signature(docs_dict[doc_id])
    PD.save_obj(signatures, index_dir)
    PD.save_obj(index_pie, os.path.join("Tests_data", "index_pie", str(len(docs_dict)), "simhash_naive.pkl"))
    index_pie.clear()


def simhash(docs_dict: dict, threshold: float, init_blocks: int, index_dir: str) -> None:
    signatures = dict()
    for doc_id in docs_dict.keys():
        signatures[doc_id] = make_signature(docs_dict[doc_id])

    t0 = time.time()

    min_num_of_blocks = minimal_num_of_blocks(threshold)
    num_of_blocks = min(min_num_of_blocks + init_blocks, HASH_LEN)
    partition = partition_to_blocks(num_of_blocks)
    parted_signatures = part_signatures(partition, signatures)

    t1 = round(time.time() - t0, 3)
    index_pie["parting signatures"] = t1

    t0 = time.time()

    permutation_formulas = create_permutation_formulas(partition, min_num_of_blocks)
    save_original_table(signatures, index_dir)
    create_and_save_permutation_tables(partition, parted_signatures, permutation_formulas, index_dir)

    t1 = round(time.time() - t0, 3)
    index_pie["permutation tables"] = t1

    sh_name = os.path.basename(os.path.normpath(index_dir))
    PD.save_obj(index_pie, os.path.join("Tests_data",  "index_pie", str(len(docs_dict)), sh_name + ".pkl"))
    index_pie.clear()

# ############### Measure Similarity ###################


def compute_hamming_distance(sig1: int, sig2: int) -> float:
    t0 = time.time()

    bstr_s1, bstr_s2 = bin(sig1)[2:], bin(sig2)[2:]
    shorter, longer = bstr_s1, bstr_s2
    if len(bstr_s2) < len(bstr_s1):
        shorter, longer = bstr_s2, bstr_s1
    shorter = shorter.zfill(len(longer))
    ham_dist = sum([a == b for a, b in zip(shorter,  longer)]) / HASH_LEN

    t1 = round(time.time() - t0, 3)
    query_pie["hamming"] = query_pie["hamming"] + t1 if "hamming" in query_pie else t1
    return ham_dist


def compute_all_distances(index_path: str, test_sig: int, threshold: float) -> dict:
    signatures = PD.load_obj(index_path)

    distances = dict()
    for doc_id, doc_sig in signatures.items():
        similarity = compute_hamming_distance(doc_sig, test_sig)
        if similarity >= threshold:
            t0 = time.time()

            distances[doc_id] = similarity

            t1 = round(time.time() - t0, 3)
            query_pie["similars"] = query_pie["similars"] + t1 if "similars" in query_pie else t1

    db_size = os.path.basename(os.path.normpath(os.path.dirname(os.path.dirname(index_path))))
    PD.save_obj(query_pie, os.path.join("Tests_data", "query_pie", db_size, "simhash_naive_" + str(threshold) + ".pkl"))
    query_pie.clear()
    return distances


def prefix_comparator(s1: int, s2: int, prefix_len: int) -> int:
    s1_prefix = s1 >> prefix_len
    s2_prefix = s2 >> prefix_len
    if s1_prefix == s2_prefix:
        return 0
    elif s1_prefix > s2_prefix:
        return 1
    else:
        return -1


def end_range_binary_search(array: list, element: int, prefix_len: int) -> int:
    start = 0
    end = len(array)
    step = 0

    while start <= end:
        step = step + 1
        mid = (start + end) // 2

        compare = prefix_comparator(element, array[mid], prefix_len)
        if compare == 0:  # element is equal to the one in the array
            if mid == len(array) - 1 or prefix_comparator(element, array[mid + 1], prefix_len) < 0:
                return mid
            else:
                start = mid + 1

        elif compare < 0:  # element is smaller than the one in the array
            end = mid - 1
        else:
            start = mid + 1
    return -1


def start_range_binary_search(array: list, element: int, prefix_len: int) -> int:
    start = 0
    end = len(array)
    step = 0

    while start <= end:
        step = step + 1
        mid = (start + end) // 2

        compare = prefix_comparator(element, array[mid], prefix_len)
        if compare == 0:  # element is equal to the one in the array
            if mid == 0 or prefix_comparator(element, array[mid - 1], prefix_len) > 0:
                return mid
            else:
                end = mid - 1

        elif compare < 0:  # element is smaller than the one in the array
            end = mid - 1
        else:
            start = mid + 1
    return -1


def getOverThresholdEfficiently(index_path: str, test_sig: int, threshold: float, init_blocks: int) -> dict:
    permutations_filenames = os.listdir(index_path)
    permutations_filenames.remove("signatures.pkl")
    permutations_filenames.remove("mapping.pkl")
    mapping = PD.load_obj(os.path.join(index_path, "mapping.pkl"))

    min_num_of_blocks = minimal_num_of_blocks(threshold)
    num_of_blocks = min(min_num_of_blocks + init_blocks, HASH_LEN)
    partition = partition_to_blocks(num_of_blocks)

    # collect suspects from permutations tables:
    suspects = set()
    for filename in permutations_filenames:
        signatures = PD.load_obj(os.path.join(index_path, filename))

        # prepare test sig:
        t0 = time.time()

        sig_parts = part_sig(partition, test_sig)
        per_formula = mapping[os.path.join(index_path, filename)]
        per_test_sig = permute_sig(partition, sig_parts, per_formula)

        t1 = round(time.time() - t0, 3)
        query_pie["processing query"] = query_pie["processing query"] + t1 if "processing query" in query_pie else t1

        # binary search test sig:
        t0 = time.time()

        p_idxs = per_formula[len(partition) - min_num_of_blocks:]
        p = sum(np.array(partition)[p_idxs])
        vals = list(signatures.values())
        start_idx = start_range_binary_search(vals, per_test_sig, p)
        if start_idx != -1:
            end_idx = end_range_binary_search(vals, per_test_sig, p)
            if end_idx != -1:
                suspects.update(list(signatures.keys())[start_idx:end_idx + 1])

        t1 = round(time.time() - t0, 3)
        query_pie["binary search"] = query_pie["binary search"] + t1 if "binary search" in query_pie else t1

    # collect suspects from original table:
    signatures = PD.load_obj(os.path.join(index_path, "signatures.pkl"))

    # binary search test sig:
    t0 = time.time()

    p = sum(partition[:len(partition) - min_num_of_blocks])
    vals = list(signatures.values())
    start_idx = start_range_binary_search(vals, test_sig, p)
    if start_idx != -1:
        end_idx = end_range_binary_search(vals, test_sig, p)
        if end_idx != -1:
            suspects.update(list(signatures.keys())[start_idx:end_idx + 1])

    t1 = round(time.time() - t0, 3)
    query_pie["binary search"] = query_pie["binary search"] + t1 if "binary search" in query_pie else t1

    # compare to suspects:
    distances = dict()
    for suspect in suspects:
        similarity = compute_hamming_distance(signatures[suspect], test_sig)
        if similarity >= threshold:
            t0 = time.time()

            distances[suspect] = similarity

            t1 = round(time.time() - t0, 3)
            query_pie["similars"] = query_pie["similars"] + t1 if "similars" in query_pie else t1

    db_size = os.path.basename(os.path.normpath(os.path.dirname(index_path)))
    PD.save_obj(query_pie,
                os.path.join("Tests_data", "query_pie", db_size, "simhash_" + str(threshold) + "_" + str(init_blocks) + ".pkl"))
    query_pie.clear()
    return distances


# ######################## Sanity checks ###########################


# def test():
#     threshold = 6 / 7
#     init_blocks = 1
#     index_dir = "simhash_test"
#     signatures = {1: int('1110001', 2), 2: int('1100110', 2), 3: int('1111111', 2), 4: int('0000111', 2)}
#     x = int('1110111', 2)
#
#     # index:
#     min_num_of_blocks = minimal_num_of_blocks(threshold)
#     print(min_num_of_blocks)
#     num_of_blocks = min(min_num_of_blocks + init_blocks, HASH_LEN)
#     partition = partition_to_blocks(num_of_blocks)
#     print("partition: ", partition)
#
#     parted_signatures = part_signatures(partition, signatures)
#     print("parted: ")
#     for k in parted_signatures:
#         print([bin(element)[2:] for element in parted_signatures[k]])
#
#     permutation_formulas = create_permutation_formulas(partition, min_num_of_blocks)
#     print("formulas: ", permutation_formulas)
#
#     save_original_table(signatures, index_dir)
#     create_and_save_permutation_tables(partition, parted_signatures, permutation_formulas, index_dir)
#
#     # Query:
#     similars = getOverThresholdEfficiently(index_dir, x, threshold, init_blocks)
#     print(similars)
#
#     # Naive:
#     print(compute_all_distances(os.path.join(index_dir, "signatures.pkl"), x, threshold))
