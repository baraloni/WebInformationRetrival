package webdata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ReviewSearch {

    private static final int scoreThreshold = 3;
    private double[]  _scoresArrVS;
    private String[] _product_namesArrVS;
    private double[]  _scoresArrLM;
    private String[] _product_namesArrLM;
    private IndexReader __ir;

    /**
     * Constructor
     */
    public ReviewSearch(IndexReader iReader) {
        this.__ir = iReader;
    }


    //######################################## Vector Space #################################

    /**
     * Returns a list of the id-s of the k most highly ranked reviews for the
     * given query, using the vector space ranking function lnn.ltc (using the
     * SMART notation)
     * The list should be sorted by the ranking
     */
    public Enumeration<Integer> vectorSpaceSearch(Enumeration<String> query, int k) {
        ArrayList<String> list = this.normalizeQuery(query);
        Set<String> set = new TreeSet(list);
//        double[] queryVec = __getQueryVector(query);
        double[] queryVec = __getQueryVector(list, set);
        HashMap<Integer, Double> docScores = __getDocScores(set, queryVec);
//        HashMap<Integer, Double> docScores = __getDocScores(query, queryVec);

        // return k best results:
        HashSet<Integer> doc_idx = new HashSet<>(docScores.keySet());
        Integer[] doc_indices = doc_idx.toArray(new Integer[doc_idx.size()]);
        Arrays.sort(doc_indices);

        double[] scores = new double[docScores.size()];

        int idx = 0;
        for (Integer key :doc_indices) {
            scores[idx++] =  docScores.get(key);
        }

//        Collections.reverse(Arrays.asList(scores));
//        Collections.reverse(Arrays.asList(doc_indices));
        return this.__getKTopRes(scores, doc_indices, k);

    }

    private double[] __getQueryVector(ArrayList<String> list, Set<String> set) {
        double tf, df;
//        ArrayList<String> list = Collections.list(query);
//        Set<String> set = new TreeSet<String>(list);
        int numWords = set.size(), i = 0;
        int[] tf_query = new int[numWords];
        int[] df_query = new int[numWords];
        double[] tfXdf = new double[numWords];
        for (String s: set) {
            tf_query[i] = Collections.frequency(list, s);
            df_query[i++] = __ir.getTokenFrequency(s);
        }
        int N = __ir.getNumberOfReviews();
        for (i = 0; i < numWords; i++) {
            tf = 1 + Math.log10(tf_query[i]);
            df = Math.log10(N / df_query[i]);
            tfXdf[i] = tf * df;
        }
        double norm = __normalizeVector(tfXdf);
        return Arrays.stream(tfXdf).map(j -> j / norm).toArray();
    }

    private double __normalizeVector(double[] vec) {
        double sum = 0;
        for (double d : vec) {
            sum += d*d;
        }
        return Math.sqrt(sum);
    }

    private HashMap<Integer,Double> __getDocScores(Set<String> set, double[] queryVec) {
        HashMap<Integer,Double> reviewScores = new HashMap<Integer,Double>();
//        ArrayList<String> list = Collections.list(query);
//        Set<String> set = new TreeSet<String>(list);
        //initialize set for all reviewIds that hold tokens from the query
        Set<Integer> reviewIdSet = new HashSet<Integer>();
        int vecSize = set.size();
        int[] tf = new int[vecSize];
        double score = 0;
        ArrayList<ArrayList<Integer>> postingLists = new ArrayList<>();
        //get pl for each token, and get set with all reviewIds holding words from query
        for (String queryWord: set) {
            Enumeration<Integer> pl = __ir.getReviewsWithToken(queryWord);
            postingLists.add(__convertPLtoArray(pl, reviewIdSet));
        }
        ArrayList<Integer> ids = new ArrayList<Integer>(reviewIdSet.size());
        ArrayList<Double> scores = new ArrayList<Double>(reviewIdSet.size());
        for (int id : reviewIdSet) {
            tf = __getTf(id, postingLists, vecSize);
            //calculate score
            for (int i = 0; i < vecSize; i++) {
                score += queryVec[i] * __calcLog(tf[i]);
            }
            reviewScores.put(id, score);
            ids.add(id);
            scores.add(score);
            score = 0;
        }
//        double obj = Collections.max(scores);
        int index = ids.indexOf(83);
//        double obj = scores.get(index);

        return reviewScores;
    }

    private ArrayList<Integer> __convertPLtoArray(Enumeration<Integer> pl, Set<Integer> set) {
        int id;
        ArrayList<Integer> newPl = new ArrayList<Integer>();
        while(pl.hasMoreElements()) {
            //reading reviewId - add also to set
            id = pl.nextElement();
            set.add(id);
            newPl.add(id);
            //reading freq, add only to pl array
            newPl.add(pl.nextElement());
        }
        return newPl;
    }

    private int[] __getTf(int id, ArrayList<ArrayList<Integer>> postingLists, int size) {
        int i, j = 0;
        int[] tf = new int[size];
        //calculate tf vector for each review
        for (ArrayList<Integer> pl : postingLists) {
            i = __findIndex(id, pl);
            if (i == -1) {
                tf[j++] = 0;
            } else {
                tf[j++] = pl.get(i + 1);
            }
        }
        return tf;
    }

    private double __calcLog(int n) {
        if (n == 0)
            return 0;
        return 1 + Math.log10(n);
    }

    private int __findIndex(int id, ArrayList<Integer> pl) {
        for (int i = 0; i < pl.size(); i++) {
            if (pl.get(i) == id) {
                if (i % 2 == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    //######################################## Language Model #################################

    /**
     * Returns a list of the id-s of the k most highly ranked reviews for the
     * given query, using the language model ranking function, smoothed using a
     * mixture model with the given value of lambda
     * The list should be sorted by the ranking
     */

    public Enumeration<Integer> languageModelSearch(Enumeration<String> query, double lambda, int k) {
        ArrayList<String> query_list =  normalizeQuery(query);

        // get all tokens related data, and keep it in these data structures:
        List<Integer>[] postings = new List[query_list.size()];
        int[] query_frequencies_in_corpus = new int[query_list.size()];
        Set<Integer> docs_in_pls = new HashSet();

        String term;
        ArrayList<Integer> term_pl;
        int doc_id;
        for (int term_id = 0; term_id < query_list.size(); term_id++) {
            term = query_list.get(term_id);
            query_frequencies_in_corpus[term_id] = this.__ir.getTokenCollectionFrequency(term);
            term_pl = Collections.list(this.__ir.getReviewsWithToken(term));
            postings[term_id] = term_pl;
            for (int j = 0 ; j < term_pl.size(); j += 2) {
                doc_id = term_pl.get(j);
                docs_in_pls.add(doc_id);
            }
        }

        // compute scores for every document in the union of all term's pls  docs:
        Integer[] doc_indices = docs_in_pls.toArray(new Integer[docs_in_pls.size()]);
        Arrays.sort(doc_indices);

        double[] scores = new double[doc_indices.length];
        Arrays.fill(scores, 1);

        this.__fillLanguageModelScores(lambda, scores, doc_indices, query_list, postings, query_frequencies_in_corpus);

        // return k best results:
        return this.__getKTopRes(scores, doc_indices, k);
    }

    private void __fillLanguageModelScores(double lambda, double[] scores, final Integer[] doc_indices,
                                           final List<String> query_list,
                                           final List<Integer>[] postings,
                                           final int[] query_frequencies_in_corpus
    ){
        int corpus_size = this.__ir.getTokenSizeOfReviews();

        int doc_size, doc_id;
        double term_freq_in_doc, term_freq_in_corpus;
        List<Integer> term_pl;
        int[] pl_pointers = new int[postings.length]; // filled with 0

        for (int i = 0; i < doc_indices.length ; i++) {
            doc_id = doc_indices[i];
            doc_size = this.__ir.getReviewLength(doc_id);
            for (int term_id = 0; term_id < query_list.size(); term_id++) {
                term_freq_in_corpus = query_frequencies_in_corpus[term_id];
                term_pl = postings[term_id];

                term_freq_in_doc = 0;
                while (pl_pointers[term_id] < term_pl.size()) {
                    if (term_pl.get(pl_pointers[term_id]) == doc_id) {
                        pl_pointers[term_id] += 1;
                        term_freq_in_doc = term_pl.get(pl_pointers[term_id]);
                        pl_pointers[term_id] += 1;
                        break;
                    }
                    if (term_pl.get(pl_pointers[term_id]) > doc_id) {
                        break;
                    }
                }
                scores[i] *= (lambda * (term_freq_in_doc / doc_size)
                    + (1 - lambda) * (term_freq_in_corpus / corpus_size));
            }
        }
    }

//############################ Product Search #######################################
    /**
     * Returns a list of the id-s of the k most highly ranked productIds for the
     * given query using a function of your choice
     * The list should be sorted by the ranking
     */
    public Collection<String> productSearch(Enumeration<String> query, int k) {

        ArrayList<String> query_list = Collections.list(query);
        Enumeration<String> query_copyVS = Collections.enumeration(query_list);
        Enumeration<String> query_copyLM = Collections.enumeration(query_list);

        double lam = 0.2;
        Enumeration<String> aa = this.__topKProdsVS(query_copyVS, k);

//        System.out.println("VS top k prods:");
//        while (aa.hasMoreElements()) {
//            System.out.println(aa.nextElement());
//        }
//        System.out.println();


        Enumeration<String> bb = this.__topKProdsLM(query_copyLM, lam, k);

//        System.out.println("LM top k prods:");
//        while (bb.hasMoreElements()) {
//            System.out.println(bb.nextElement());
//        }
//        System.out.println();

        // make k the number of prodIds we can actually return (up to the original value of k)
        k = Math.min(Math.min(k, Collections.list(bb).size()), Collections.list(aa).size());

        double[]  scoresArr = new double[2*k];
        Arrays.fill(scoresArr, -1 * Double.POSITIVE_INFINITY);
        String[] product_namesArr = new String[2*k];

        //merge to these arrays:
        int i;
        for (i = 0; i < k; i ++){
            product_namesArr[i] = this._product_namesArrVS[this._product_namesArrVS.length -1 -i];
            scoresArr[i] = this._scoresArrVS[this._scoresArrVS.length -1 -i];
            for (int j = 0; j < k; j ++){
                if (this._product_namesArrLM[this._product_namesArrLM.length -1 -j].equals(product_namesArr[i])){
                    scoresArr[i] += this._scoresArrLM[this._scoresArrLM.length -1 -j];
                }
            }
        }
        for (int j = 0; j < k; j ++){
            if (!Arrays.asList(product_namesArr).contains(this._product_namesArrLM[this._product_namesArrLM.length -1 -j])){
                product_namesArr[i] = this._product_namesArrLM[this._product_namesArrLM.length -1 -j];
                scoresArr[i] = this._scoresArrLM[this._scoresArrLM.length -1 -j];
                i++;
            }
        }
//        System.out.println(Arrays.toString(product_namesArr));

        return(Collections.list(this.__getKTopRes(scoresArr, product_namesArr, k)));
    }


    private Enumeration<String> __topKProdsVS(Enumeration<String> query, int k) {
        //get the most relevant reviews
        ArrayList<Integer> relevantReviews = Collections.list(this.vectorSpaceSearch(query, k * 2));
        ArrayList<String> product_names = new ArrayList<>();
        ArrayList<Double> scores = new ArrayList<>();

        String prodId;
        int revId, currIdx, numProducts;
        double newScore, currScore = 0;

        //find the most common product id's within the reviews
        double weight;
        for (int i = 0; i < relevantReviews.size(); i++) {
            revId = relevantReviews.get(i);
            weight = 1 - ((double)(i) / relevantReviews.size());
            prodId = __ir.getProductId(revId);
            if (!product_names.contains(prodId)) {
                product_names.add(prodId);
                currScore = 0;
            }
            currIdx = product_names.indexOf(prodId);
            newScore = this.__getScore(revId);
            if (!(currScore == 0)) {
                currScore = scores.get(currIdx);
                scores.set(currIdx, currScore + newScore);
                currScore = 1;
                continue;
            }
            scores.add(currIdx, currScore + weight * newScore);
            currScore = 1;
        }

        // return k best results:
        numProducts = product_names.size();
        this._product_namesArrVS = new String[numProducts];
        this._scoresArrVS = new double[numProducts];
        for (int i = 0; i < numProducts; i++) {
            this._product_namesArrVS[i] = product_names.get(i);
            this._scoresArrVS[i] = scores.get(i);
        }
        return (this.__getKTopRes( this._scoresArrVS, this._product_namesArrVS, k));
    }

    private Enumeration<String> __topKProdsLM(Enumeration<String> query, double lam, int k) {
        //get the most relevant reviews
        ArrayList<Integer> relevantReviews = Collections.list(this.languageModelSearch(query, lam,k * 2));
        ArrayList<String> product_names = new ArrayList<>();
        ArrayList<Double> scores = new ArrayList<>();

        String prodId;
        int revId, currIdx, numProducts;
        double newScore, currScore = 0;

        //find the most common product id's within the reviews
        for (int i = 0; i < relevantReviews.size(); i++) {
            revId = relevantReviews.get(i);

            prodId = __ir.getProductId(revId);
            if (!product_names.contains(prodId)) {
                product_names.add(prodId);
                currScore = 0;
            }
            currIdx = product_names.indexOf(prodId);
            newScore = this.__getScore(revId);
            if (!(currScore == 0)) {
                currScore = scores.get(currIdx);
                scores.set(currIdx, currScore + newScore);
                currScore = 1;
                continue;
            }
            scores.add(currIdx, currScore + newScore);
            currScore = 1;
        }

        // return k best results:
        numProducts = product_names.size();
        this._product_namesArrLM = new String[numProducts];
        this._scoresArrLM = new double[numProducts];
        for (int i = 0; i < numProducts; i++) {
            this._product_namesArrLM[i] = product_names.get(i);
            this._scoresArrLM[i] = scores.get(i);
        }
        return(this.__getKTopRes( this._scoresArrLM, this._product_namesArrLM, k));
    }

//    /**
//     * Returns a list of the id-s of the k most highly ranked productIds for the
//     * given query using a function of your choice
//     * The list should be sorted by the ranking
//     */
//    public Collection<String> productSearch(Enumeration<String> query, int k) {
//        //get the most relevant reviews
//        ArrayList<Integer> relevantReviews = Collections.list(this.vectorSpaceSearch(query, k * 2));
//        ArrayList<String> product_names = new ArrayList<String>();
//        ArrayList<Double> scores = new ArrayList<Double>();
//
//        String prodId;
//        int revId= 0; double newScore, currScore =0;
//        int currIdx, numProducts;
//
//        //find the most common product id's within the reviews
//        for(int i = 0; i < relevantReviews.size(); i++) {
//            revId = relevantReviews.get(i);
//
//            prodId = __ir.getProductId(revId);
//            if (!product_names.contains(prodId)) {
//                product_names.add(prodId);
//                currScore = 0;
//            }
//            currIdx = product_names.indexOf(prodId);
//            newScore = this.__getScore(revId);
//            if (!(currScore == 0)) {
//                currScore = scores.get(currIdx);
//                scores.set(currIdx, currScore + newScore);
//                currScore = 1;
//                continue;
//            }
//            scores.add(currIdx, currScore + newScore);
//            currScore = 1;
//        }
//
//        // return k best results:
//        numProducts = product_names.size();
//        String[] product_namesArr = new String[numProducts];
//        double[] scoresArr = new double[numProducts];
//        for (int i = 0; i < numProducts; i++) {
//            product_namesArr[i] = product_names.get(i);
//            scoresArr[i] = scores.get(i);
//        }
//        return Collections.list(this.__getKTopRes(scoresArr, product_namesArr, k));
//    }


    private double __getScore(int revId) {
        double helpfulnessNum = __ir.getReviewHelpfulnessNumerator(revId);
        int helpfulnessDen = __ir.getReviewHelpfulnessDenominator(revId);
        int score = __ir.getReviewScore(revId);
        double bonus = 0;
        if (helpfulnessNum / helpfulnessDen > 0.33) {
            if (score >= scoreThreshold) {
                bonus = helpfulnessNum / helpfulnessDen;
            } else {
                bonus = 1 - (helpfulnessNum / helpfulnessDen);
            }
            return score + (score * bonus);
        }
        return score;
    }

    //##################### General Use ##################################

    private <T> Enumeration<T> __getKTopRes (double[] arr, T[] arr_indices, int k) {
        int n = arr.length;
        k = Math.min(k, n); // takes at most k elements (only if possible)
        this.__bubbleSortToK(arr, arr_indices, k);

        ArrayList<T> e = new ArrayList<>();
        for (int i = arr_indices.length - 1; i > arr_indices.length - k - 1; i--) {
            e.add(arr_indices[i]);
        }
        return Collections.enumeration(e);
    }

    private <T> void __bubbleSortToK(double[] arr, T[] arr_related_info, int k)
    {

        int n = arr.length;
        double temp_score;
        T temp_idx;
        for (int i = 0; i < k; i++)
            for (int j = 0; j < n-i-1; j++)
                if (arr[j] >= arr[j+1])
                {
                    // swap arr[j+1] and arr[i]
                    temp_score = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] = temp_score;
                    // swap their indices:
                    temp_idx = arr_related_info[j];
                    arr_related_info[j] = arr_related_info[j+1];
                    arr_related_info[j+1] = temp_idx;
                }
    }

    private ArrayList<String> normalizeQuery(Enumeration<String> query){
        ArrayList<String> query_list = new ArrayList();
        while (query.hasMoreElements()){
            query_list.add(query.nextElement().toLowerCase());
        }
        return query_list;
    }
    //################ DEBUGGING #################################
    public double[] checkNorm(double[] vec) {
        double n = this.__normalizeVector(vec);
        return Arrays.stream(vec).map(j -> j / n).toArray();
    }

    public HashMap<Integer, Double> getMap() {
        HashMap<Integer, Double> res = new HashMap<Integer,Double>();
        res.put(1, 3.0);
        res.put(2, 5.0);
        res.put(3, 3.0);
        res.put(4, 9.0);
        return res;
    }
}
