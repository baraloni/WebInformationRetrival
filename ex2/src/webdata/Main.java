package webdata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
public class Main {


    static final String INDICES_DIR_NAME = "indices";
    static final String SLOW_INDICES_DIR_NAME = "slow_indices";
    static final String FAST_INDICES_DIR_NAME = "fast_indices";
    static final String REVIEWS_FILE_NAME_10 = "10.txt";
    static final String REVIEWS_FILE_NAME_20 = "20.txt";
    static final String REVIEWS_FILE_NAME_100 = "100.txt";
    static final String REVIEWS_FILE_NAME_1000 = "1000.txt";
    static final String REVIEWS_FILE_NAME_2 = "2.txt";
    static final String REVIEWS_FILE_NAME_fam1 = "1fam.txt";
    static final String REVIEWS_FILE_NAME_MOVIES = "Movies_&_TV.txt";

    public static void main(String[] args) throws IOException {
        final String dir = System.getProperty("user.dir");
        final String indicesDir = dir + File.separatorChar + INDICES_DIR_NAME;
        final String slowIndicesDir = dir + File.separatorChar + SLOW_INDICES_DIR_NAME;
        final String fastIndicesDir = dir + File.separatorChar + FAST_INDICES_DIR_NAME;


        timeIRfunctions(REVIEWS_FILE_NAME_1000, slowIndicesDir, fastIndicesDir);
        //TODO: with limit
//        testMerged(indicesDir);
//        IndexReader ir = new IndexReader(slowIndicesDir);
//        for (int i = 0; i < ir.get__numOfTokens();i++) {
//            ir.__readPostingList(i);
//        }
    }

    public static void timeIRfunctions(String inputFile, String slow_indicesDir, String fast_indicesDir) {
        String[] tokens = initializeRandTokens(inputFile, slow_indicesDir);
        IndexWriter iw = new IndexWriter();
        iw.write(inputFile,fast_indicesDir);
        IndexReader ir = new IndexReader(fast_indicesDir);
        System.err.println("getRevWithToken:");
        timeGetReviewsWithToken(ir, tokens);
        System.err.println("getTokenFrequency:");
        timeGetTokenFrequency(ir, tokens);
        return;
    }

    public static void testMerged(String indicesDir)
    {
        long startTime = System.nanoTime();

        IndexWriter iw = new IndexWriter();
        iw.removeIndex(indicesDir);
        iw.write(REVIEWS_FILE_NAME_MOVIES, indicesDir);
//        iw.__mergeReviews(indicesDir);
//        iw.__mergeAllLexicons(indicesDir);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        System.err.println(duration / 1000000000);
    }

    public static void timeGetReviewsWithToken(IndexReader ir, String[] randTokens) {
        long startTime = System.nanoTime();
        for (String t : randTokens)
            ir.getReviewsWithToken(t);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        System.err.println(duration / 1000000);
    }

    public static void timeGetTokenFrequency(IndexReader ir, String[] randTokens) {
        long startTime = System.nanoTime();
        for (String t : randTokens)
            ir.getTokenFrequency(t);
        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        System.err.println(duration / 1000000);
    }

    public static String[] initializeRandTokens(String inputFile, String indicesDir) {
        SlowIndexWriter siw = new SlowIndexWriter();
        siw.slowWrite(inputFile,indicesDir);
        int max = siw.postingsDebug.keySet().size();
        String[] strings = new String[100];
        List<String> keysAsArray = new ArrayList<String>(siw.postingsDebug.keySet());
        for (int i = 0; i < 100; i++) {
            Random r = new Random();
            strings[i] = String.valueOf(siw.postingsDebug.get(keysAsArray.get(r.nextInt(keysAsArray.size()))));
        }
        return strings;
    }


    public static void mergePostingListTest(){
        SlowIndexWriter siw = new SlowIndexWriter();
        ArrayList<Integer> pl1 = new ArrayList<>(Arrays.asList(1, 1, 3, 1, 7, 1, 10, 1));
        ArrayList<Integer> pl2 = new ArrayList<>(Arrays.asList(2, 1, 3, 1, 6, 1));
        ArrayList<Integer> m_pl = new ArrayList<>();
//        siw.__mergePostingLists(pl1, pl2, m_pl);
        assert( m_pl.equals(new ArrayList<>(Arrays.asList(1,1,2,1,3,2,6,1,7,1,10,1))));

        //TODO: if assert failed- use this to debug
//        System.out.print("[");
//        for (int i : m_pl)
//        {
//            System.out.printf("%d ", i);
//        }
//        System.out.println("]");

    }

    public void createTestFiles() {
        String[] reviewFiles = new String[]{"HelpNum.txt", "HelpDenom.txt", "Scores.txt"
            , "ReviewLength.txt", "ProductId.txt", "ProductId.txt"};
    }

    private static void queryReviewMetaData(String indicesDir) {
        IndexReader indexReader = new IndexReader(indicesDir);
        int[] ridsTestCases = {1, 11, 12, 999, 1001, 0, -2, 10, 32, 522};
        for (int rid : ridsTestCases) {
            System.out.println("rid: " + rid + " " +
                indexReader.getProductId(rid) + " " +
                indexReader.getReviewScore(rid) + " " +
                indexReader.getReviewHelpfulnessNumerator(rid) + " " +
                indexReader.getReviewHelpfulnessDenominator(rid) + " " +
                indexReader.getReviewLength(rid));
        }
    }

    private static void queryMetaData(String indicesDir) {
        IndexReader indexReader = new IndexReader(indicesDir);
        System.out.println(indexReader.getNumberOfReviews());
        System.out.println(indexReader.getTokenSizeOfReviews());
    }


    private static void testGetReviewsWithToken(IndexReader indexReader,
                                                String[] wordTestCases) {
        System.out.println("Checking getReviewsWithToken...");
        for (String word : wordTestCases) {
            Enumeration<Integer> res = indexReader.getReviewsWithToken(word);
            System.out.print(word + ": " + System.lineSeparator());
            while (res.hasMoreElements()) {
                System.out.print(res.nextElement().toString() + " ");
            }
            System.out.println();
        }
    }

    private static void testGetTokenFrequency(IndexReader indexReader,
                                              String[] wordTestCases) {
        System.out.println("Checking getTokenFrequency...");
        for (String word : wordTestCases) {
            int numOfReviews = indexReader.getTokenFrequency(word);
            System.out.println(word + ": " + "numOfReviews: " + numOfReviews);
        }
    }

    private static void testGetTokenCollectionFrequency(IndexReader indexReader,
                                                        String[] wordTestCases) {
        System.out.println("Checking getTokenCollectionFrequency...");
        for (String word : wordTestCases) {
            int numOfMentions = indexReader.getTokenCollectionFrequency(word);
            System.out.println(word + ": " + "numOfMentions: " + numOfMentions);
        }
    }


    private static void queryWordIndex(String indicesDir) {
        IndexReader indexReader = new IndexReader(indicesDir);
        String[] wordTestCases = {"0", "bulba", "zzz", "1", "9oz", "a", "crunchy", "how", "laxative",
            "prefer", "storebought", "zucchini", "the"};
        testGetReviewsWithToken(indexReader, wordTestCases);
        testGetTokenFrequency(indexReader, wordTestCases);
        testGetTokenCollectionFrequency(indexReader, wordTestCases);

    }


    private static void queryProductIndex(String indicesDir) {
        IndexReader indexReader = new IndexReader(indicesDir);
        String[] productTestCases = {"A009ASDF5", "B099ASDF5", "B0001PB9FE", "B0002567IW", "B000ER6YO0",
            "B000G6RYNE", "B006F2NYI2", "B009HINRX8", "B001E4KFG0"};
        for (String pid : productTestCases) {
            Enumeration<Integer> res = indexReader.getProductReviews(pid);
            System.out.print(pid + ": " + System.lineSeparator());
            while (res.hasMoreElements()) {
                System.out.print(res.nextElement().toString() + " ");
            }
            System.out.println();
        }
    }


    private static void deleteIndex(String indicesDir) {
        SlowIndexWriter slowIndexWriter = new SlowIndexWriter();
        slowIndexWriter.removeIndex(indicesDir);
    }

    private static void buildIndex(String indicesDir) {
        long startTime = System.nanoTime();

        SlowIndexWriter slowIndexWriter = new SlowIndexWriter();
        slowIndexWriter.slowWrite(REVIEWS_FILE_NAME_1000, indicesDir);

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        System.err.println(duration);
    }
}