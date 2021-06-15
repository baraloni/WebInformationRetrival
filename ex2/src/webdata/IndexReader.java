package webdata;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class allows to query the index stored in <dir>.
 */
public class IndexReader {
    // general functionality:
    private static final String ENCODING = "ISO-8859-1"; // TODO move to compact, memory-wise it's the same
    private String __directory;
    private int __numOfReviews;
    private int __numOfTokens;

    // Lexicon:
    private int[] __frequencies;
    private long[] __postingsPtrs;
    private byte[] __tokensLengths;
    private int[] __termPtrs;
    private CompactCharSequence __tokens;

    // Reviews Table:
    private String[] __productIds;
    private byte[] __reviewsScores;
    private short[] __reviewsHelpfulnessNumerators;
    private short[] __reviewsHelpfulnessDenominators;
    private int[] __reviewsLengths;
    private int[] __productIdsFirstReview;

    /**
     * Creates an IndexReader which will read from the given directory
     */
    public IndexReader(String dir) {
        // read Lexicon and store it in Reader:
        File directory = new File(dir);
        directory.mkdir();
//        __ReadReviewsDir(dir);    //CHANGE

        Path freqFilePath = Paths.get(dir, "Lexicon", "Frequencies.txt");
        Path postingPtrFilePath = Paths.get(dir, "Lexicon", "PostingPtrs.txt");
        Path TokenLengthFilePath = Paths.get(dir, "Lexicon", "TokenLength.txt");
        Path TermPtrFilePath = Paths.get(dir, "Lexicon", "TermPtr.txt");
        Path LongStringPath = Paths.get(dir,"Lexicon", "long_string.txt");
        this.__frequencies = ReadWriteArraysUtils.readIntArray(freqFilePath);
        this.__postingsPtrs = ReadWriteArraysUtils.readLongArray(postingPtrFilePath);
        this.__tokensLengths = ReadWriteArraysUtils.readByteArray(TokenLengthFilePath);
        this.__termPtrs = ReadWriteArraysUtils.readIntArray(TermPtrFilePath);
        this.__tokens = ReadWriteArraysUtils.readCompactCharSequence(LongStringPath);

        // read Reviews Table and store it in Reader:
        Path helpfulnessNumFilePath = Paths.get(dir,"Reviews", "HelpNum.txt");
        Path helpfulnessDenFilePath = Paths.get(dir,"Reviews", "HelpDenom.txt");
        Path scoreFilePath = Paths.get(dir,"Reviews", "Scores.txt");
        Path reviewLenFilePath = Paths.get(dir,"Reviews","ReviewLength.txt");
        Path prodIdFilePath = Paths.get(dir,"Reviews", "ProductId.txt");
        Path firstReviewFilePath = Paths.get(dir,"Reviews", "PidFirstReview.txt");

        __ReadReviewsDir(dir);
//        this.__reviewsScores = ReadWriteArraysUtils.readByteArray(scoreFilePath);
//        this.__reviewsHelpfulnessNumerators = ReadWriteArraysUtils.readShortArray(helpfulnessNumFilePath);
//        this.__reviewsHelpfulnessDenominators = ReadWriteArraysUtils.readShortArray(helpfulnessDenFilePath);
//        this.__reviewsLengths = ReadWriteArraysUtils.readIntArray(reviewLenFilePath);

        //CHANGE
        this.__numOfTokens = __postingsPtrs.length;
//        this.__numOfTokens = IntStream.of(__reviewsLengths).sum();
        this.__productIds = ReadWriteArraysUtils.readProductId(prodIdFilePath);
        this.__productIdsFirstReview = ReadWriteArraysUtils.readIntArray(firstReviewFilePath);


        // general functionality:
        this.__directory = dir;
//        this.__numOfReviews = this.__reviewsScores.length;  //CHANGE
    }

    public void __ReadReviewsDir(String dir) {

        String reviewsDirFile = dir + File.separator + "Reviews" + File.separator + "ReviewsDir.txt";
        InputStream is = null;
        DataInputStream dis = null;
//        ArrayList<Short> shorts = new ArrayList<Short>();
        try {
            int i = 0;
            System.out.println(dir);
            is = new FileInputStream(reviewsDirFile);
            dis = new DataInputStream(new BufferedInputStream(is));
            this.__numOfReviews = dis.readInt();
            this.__reviewsHelpfulnessNumerators = new short[this.__numOfReviews];
            this.__reviewsHelpfulnessDenominators = new short[this.__numOfReviews];
            this.__reviewsScores = new byte[this.__numOfReviews];
            this.__reviewsLengths = new int[this.__numOfReviews];
            while(dis.available()>0) {
                __reviewsHelpfulnessNumerators[i] = dis.readShort();
                __reviewsHelpfulnessDenominators[i] = dis.readShort();
                __reviewsScores[i] = dis.readByte();
                __reviewsLengths[i++] = dis.readInt();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
                try {
                    if (dis != null)
                        dis.close();
                    if(is != null)
                        is.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }

//        System.out.println(Arrays.toString(__reviewsHelpfulnessNumerators));
//        System.out.println(Arrays.toString(__reviewsHelpfulnessDenominators));
//        System.out.println(Arrays.toString(__reviewsScores));
//        System.out.println(Arrays.toString(__reviewsLengths));
    }

    // ##################### review queries ##################################

    /**
     * Returns the product identifier for the given review
     * Returns null if there is no review with the given identifier
     */
    public String getProductId(int reviewId) {
        //normalize index to 0-based system
        reviewId -= 1;
        if (reviewId >= this.__numOfReviews | reviewId < 0)
        {
            return null;
        }
        int idx = Arrays.binarySearch(this.__productIdsFirstReview, reviewId);
        if (idx >=0)
        {
            return this.__productIds[idx];
        }
        return this.__productIds[-idx - 2];
    }

    /**
     * Returns the score for a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewScore(int reviewId)
    {
        //normalize index to 0-based system
        reviewId -= 1;
        if (reviewId >= this.__numOfReviews | reviewId < 0)
        {
            return -1;
        }
        return this.__reviewsScores[reviewId];
    }

    /**
     * Returns the numerator for the helpfulness of a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewHelpfulnessNumerator(int reviewId)
    {
        //normalize index to 0-based system
        reviewId -= 1;
        if (reviewId >= this.__numOfReviews | reviewId < 0)
        {
            return -1;
        }
        return this.__reviewsHelpfulnessNumerators[reviewId];
    }

    /**
     * Returns the denominator for the helpfulness of a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewHelpfulnessDenominator(int reviewId)
    {
        //normalize index to 0-based system
        reviewId -= 1;
        if (reviewId >= this.__numOfReviews | reviewId < 0)
        {
            return -1;
        }
        return this.__reviewsHelpfulnessDenominators[reviewId];
    }

    /**
     * Returns the number of tokens in a given review
     * Returns -1 if there is no review with the given identifier
     */
    public int getReviewLength(int reviewId)
    {
        //normalize index to 0-based system
        reviewId -= 1;
        if (reviewId >= this.__numOfReviews | reviewId < 0)
        {
            return -1;
        }
        return this.__reviewsLengths[reviewId];
    }


    /**
     * Return the number of product reviews available in the system
     */
    public int getNumberOfReviews()
    {
        return this.__numOfReviews;
    }


    // ##################### product queries ###############################3


    /**
     * Return the ids of the reviews for a given product identifier
     * Note that the integers returned should be sorted by id
     *
     * Returns an empty Enumeration if there are no reviews for this product
     */
    public Enumeration<Integer> getProductReviews(String productId)
    {
        int rev_id = 0, revIndx;
        int last_rev_id = -1;

        for (revIndx =0; revIndx < this.__productIds.length; revIndx++)
        {
            if(productId.equals(this.__productIds[revIndx]))
            {
                rev_id = this.__productIdsFirstReview[revIndx];
                if (revIndx == this.__productIds.length - 1)
                {
                    last_rev_id = this.__numOfReviews - 1;
                }
                else{
                    last_rev_id = this.__productIdsFirstReview[revIndx + 1] - 1;
                }
                break;
            }
        }
        ArrayList<Integer> e = new ArrayList<>();
        if (last_rev_id != -1) // we've found the requested pId
        {
            for (int i = rev_id; i <= last_rev_id; i++) {
                //normalized for 0-based indexing system
                e.add(i + 1);
            }
        }
        return Collections.enumeration(e);
    }


    // ##################### token queries ##################################

    /**
     * Return a series of integers of the form id-1, freq-1, id-2, freq-2, ... such
     * that id-n is the n-th review containing the given token and freq-n is the
     * number of times that the token appears in review id-n
     * Note that the integers should be sorted by id
     *
     * @param token a word
     * @return an empty Enumeration if there are no reviews containing this token
     */
    public Enumeration<Integer> getReviewsWithToken(String token){
        //normalize token
        token = token.toLowerCase();
        int idx_in_lex = this.__binarySearchForToken(token);
        if (idx_in_lex != -1)
        {
            try{
                return Collections.enumeration(this.__readPostingList(idx_in_lex));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        return Collections.enumeration(new ArrayList<>());
    }

    /**
     * @param idx the idx of the term whose posting list we want to get
     * @return the decompressed posting list of the term (indexed int the lexicon at idx)
     * @throws IOException
     */
    public ArrayList<Integer> __readPostingList(int idx) throws IOException     //CHANGE
    {
        // read from file the bytes holding the compressed posting list:
        Path invertedIndexPath = Paths.get(this.__directory, "Lexicon","invertedIndex.txt");
        File inputFile = invertedIndexPath.toFile();
        long p_start = this.__postingsPtrs[idx];
        long p_end = (idx < this.__numOfTokens - 1) ? this.__postingsPtrs[idx + 1] : inputFile.length();

        byte[] buffer = new byte[(int) (p_end - p_start)];
        InputStream in = new FileInputStream(inputFile);

        long skipped = in.skip(p_start);
        assert(skipped == p_start);

        int count = 0;
        while (count < buffer.length) {
            count += in.read(buffer, 0, buffer.length);
            assert(count != -1);
        }
        in.close();
//        System.out.println(Arrays.toString(buffer));   //CHANGE

        // decompress the differences posting list:
        ArrayList<Integer> decompressed_diff = GroupVarint.decompress(buffer);


        // convert the differences posting list into full posting
        for (int i = 2 ; i < decompressed_diff.size(); i += 2 )
        {
            decompressed_diff.set(i, decompressed_diff.get(i) + decompressed_diff.get(i - 2));
        }
        for (int i = 0 ; i < decompressed_diff.size(); i += 2 )
        {
            //normalize so that review ids begin with 1 (rather than 0)
            int normalized = decompressed_diff.get(i) + 1;
            decompressed_diff.set(i, normalized);
        }

        return decompressed_diff;
    }

    /**
     * Return the number of reviews containing a given token (i.e., word)
     * Returns 0 if there are no reviews containing this token
     */
    public int getTokenFrequency(String token) {
        //normalize input
        token = token.toLowerCase();
        int idx_in_lex = this.__binarySearchForToken(token);
        if (idx_in_lex == -1)
        {
            return 0;
        }
        // get the token's posting list:
        Enumeration<Integer> postings = this.getReviewsWithToken(token);

        //get posting len:
        int posting_len = 0;
        while (postings.hasMoreElements())
        {
            postings.nextElement();
            posting_len += 1;
        }
        posting_len /= 2;
        return posting_len;
    }


    /**
     * Return the number of times that a given token (i.e., word) appears in
     * the reviews indexed
     * Returns 0 if there are no reviews containing this token
     */
    public int getTokenCollectionFrequency(String token)
    {
        //normalize input
        token = token.toLowerCase();
        int idx_in_lex = this.__binarySearchForToken(token);
        if (idx_in_lex == -1)
        {
            return 0;
        }
        return this.__frequencies[idx_in_lex];
    }


    /**
     * Return the number of tokens in the system
     * (Tokens should be counted as many times as they appear)
     */
    public int getTokenSizeOfReviews()
    {
        return this.__numOfTokens;
    }


    /**
     * binary search for token in lexicon.
     * @param token: a token to search for
     * @return  -1 of token isn't present in lexicon, int > 0 representing it's entry id otherwise
     */
    public int __binarySearchForToken(final String token) //TODO private
    {
        int l = 0, r =  this.__termPtrs.length - 1;
        while (l < r) {
            //go to middle block:
            int m = l + (r - l) / 2;

            //read the first word in the block:
            int m_s = this.__termPtrs[m];
            int m_e = m_s + this.__tokensLengths[m * (IndexWriter.__k - 1)];
            String middle_token =  this.__tokens.subSequence(m_s, m_e).toString();
            int middle_token_is_greater = middle_token.compareTo(token);

            if (middle_token_is_greater == 0) // we've found the token
                return m * IndexWriter.__k;

            else if (middle_token_is_greater > 0) // middle_token is greater than token: prev blocks
            {
                r = m - 1;
            }

            else // this or next blocks
            {
                //read first word in next block:
                m_s = this.__termPtrs[m + 1];
                m_e = m_s + this.__tokensLengths[(m + 1) * (IndexWriter.__k - 1)];
                middle_token =  this.__tokens.subSequence(m_s, m_e).toString();
                int next_token_is_greater = middle_token.compareTo(token);

                if (next_token_is_greater > 0)
                {
                    l = m;
                    break;
                }
                else{
                    l = m + 1;

                }
            }
        }
        //we've found the block that supposedly holds the token we're looking for
        return search_token_in_block(l, token);
    }

    /**
     * Searches block to find and return the term's index in the dictionary.
     * @param m: number of block
     * @param token: a token to search for.
     * @return the term's index in the dictionary if term is indeed in dictionary, -1 otherwise.
     */
    private int search_token_in_block(final int m, String token) {
        final int idx = m * (IndexWriter.__k - 1); //idx at tokenLengths
        int base = this.__termPtrs[m];
        int i;

        for (i = 0; i < IndexWriter.__k - 1; i++) {
            if (m * IndexWriter.__k + i == this.__numOfTokens - 1) { //end of words, mid-block
                if (token.equals(this.__tokens.subSequence(base, this.__tokens.length()).toString())) { // read until end of file
                    break;
                }
                return -1;
            } else {
                int len = this.__tokensLengths[idx + i];
                if (token.equals(this.__tokens.subSequence(base, base + len).toString())) { // read until end of file
                    break;
                }
                base += len;
            }
        }
        if (i == 4) { //last word on block
            String last_word;
            if (m == this.__termPtrs.length - 1) {//last block
                last_word = this.__tokens.subSequence(base, this.__tokens.length()).toString();
            }
            else{
                last_word = this.__tokens.subSequence(base, this.__termPtrs[m + 1]).toString();
            }
            if (!token.equals(last_word)) { // last hope.. but didn't find in block
                return -1;
            }
        }
        return m * IndexWriter.__k + i; // found somewhere in block
    }

    public void print_words(){
        for (int i = 0; i < __termPtrs.length  ; i++)
        {
            int tptr = this.__termPtrs[i];
            int len = this.__tokensLengths[i * (IndexWriter.__k - 1)];
            System.out.println(this.__tokens.subSequence(tptr, tptr + len));
        }
    }

    // m is block number
    public void print_words_in_block(final int m){
        int idx = m * (IndexWriter.__k - 1);
        int base = this.__termPtrs[m];
        for (int i = 0; i < IndexWriter.__k - 1; i++) //count words
        {
            if (idx + i == this.__tokensLengths.length - 1) { //end of words, mid block
                System.out.println(this.__tokens.subSequence(base, this.__tokens.length())); // read until end of file
                return;
            } else {
                int len = this.__tokensLengths[idx + i];
                System.out.println(this.__tokens.subSequence(base, base + len));
                base += len;
            }
        }
        System.out.println(this.__tokens.subSequence(base, this.__termPtrs[m + 1] ));
    }

    //CHANGE
    public int get__numOfTokens(){
        return this.__numOfTokens;
    }

    public static class Main {


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


    //        timeIRfunctions(REVIEWS_FILE_NAME_1000, slowIndicesDir, fastIndicesDir);
            //TODO: with limit
            testMerged(indicesDir);
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
            System.err.println(duration / 1000000000);
        }

        public static void timeGetTokenFrequency(IndexReader ir, String[] randTokens) {
            long startTime = System.nanoTime();
            for (String t : randTokens)
                ir.getTokenFrequency(t);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime);
            System.err.println(duration / 1000000000);
        }

        public static String[] initializeRandTokens(String inputFile, String indicesDir) {
            webdata.SlowIndexWriter siw = new webdata.SlowIndexWriter();
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
            webdata.SlowIndexWriter siw = new webdata.SlowIndexWriter();
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
            webdata.SlowIndexWriter slowIndexWriter = new webdata.SlowIndexWriter();
            slowIndexWriter.removeIndex(indicesDir);
        }

        private static void buildIndex(String indicesDir) {
            long startTime = System.nanoTime();

            webdata.SlowIndexWriter slowIndexWriter = new webdata.SlowIndexWriter();
            slowIndexWriter.slowWrite(REVIEWS_FILE_NAME_1000, indicesDir);

            long endTime = System.nanoTime();
            long duration = (endTime - startTime);
            System.err.println(duration);
        }
    }
}
