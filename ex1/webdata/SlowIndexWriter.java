package webdata;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;


public class SlowIndexWriter {


    /**
     * blocking factor
     * //TODO - find best value
     */
    final static int __k = 5;

    private static final String ENCODING = "ISO-8859-1";


    /**
     * constructs a SlowIndexWriter object that reads reviews data from inputFile,
     * and constructs an index from it.
     */
    public SlowIndexWriter() { }

    /**
     * Given product review data, creates an on disk index
     * @param inputFile: is the path to the file containing the review data
     * @param dir: is the directory in which all index files will be created
     * if the directory does not exist, it should be created
     */
    public void slowWrite(String inputFile, String dir) {

        // holds [helpfulnessNumerator, helpfulnessDenominator, score, length]:
        ArrayList<Integer> reviewsDir = new ArrayList<>();
        ArrayList<String> productIds = new ArrayList<>();
        productIds.add("");
        ArrayList<Integer> productIdsFirstReview = new ArrayList<>();

        // hash table whose elements hold:
        // key: token, value: [reviewId, freqInReview, ....]
        HashMap<String, ArrayList<Integer>> postings = new HashMap<>();
        this.__readInput(inputFile, reviewsDir, productIds, productIdsFirstReview, postings);
        productIds.remove(0);
        this.__writeReviews(dir, reviewsDir, productIds, productIdsFirstReview);
        this.__writeLexicon(dir, postings);
    }

    /**
     * Delete all index files by removing the given directory
     * @param dir: the name of the directory holding the index.
     */
    public void removeIndex(String dir) {
        File directoryToBeDeleted = new File(dir);
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                removeIndex(file.toString());
            }
        }
        directoryToBeDeleted.delete();
    }


    // ########################### READING & PROCESSING ##########################

    /**
     *  Reads and pre-process the text in inputFile.
     *  @param inputFile:  the filename of the file that is holding the reviews.
     */
    private void __readInput(String inputFile, ArrayList<Integer> reviewsDir, ArrayList<String> productIds,
                             ArrayList<Integer> productIdsFirstReview,
                             HashMap<String, ArrayList<Integer>> postings){
        int reviewId = 0;
        String line, text;
        HashMap<String, Integer> processedText;

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));

            while ((line = br.readLine()) != null){
                // Harvests a new Review, and updates the data structure that holds the reviews:
                text = this.__collectReview(br, reviewId, line, reviewsDir, productIds, productIdsFirstReview);

                // Processes the review text, to get all tokens and their frequencies in the review:
                processedText = this.__processReviewText(text, reviewsDir);

                // Updates the data structure that holds the processed data of all reviews:
                this.__addProcessedText(reviewId, processedText, postings);
                reviewId += 1;
            }
        } catch (IOException ex){
            ex.printStackTrace();
        }
    }

    /**
     * Collects a new Review and adds it to the data structure that holds
     * the review's information, i.e:  __reviewsDir.
     *  a review hold this information:
     *  reviewId, ProductID, helpfulnessNumerator, helpfulnessDenominator, score, Textlength.
     * @param br: reader
     * @param reviewId: the id of the next review to add
     * @param line: the last line bf read
     * @return the text of the review.
     * @throws IOException when bf fails reading
     */
    private String __collectReview(final BufferedReader br, final int reviewId, String line,
                                   ArrayList<Integer> reviewsDir, ArrayList<String> productIds,
                                   ArrayList<Integer> productIdsFirstReview)
        throws IOException {
        String[] parts = new String[]{""};
        String[] text = new String[]{""};
        StringBuilder sb  = new StringBuilder();
        do {

            parts = line.split(": ");
            if (parts[0].contains("productId")) {
                if (!productIds.get(productIds.size() - 1).equals(parts[1]))
                {
                    productIdsFirstReview.add(reviewId);
                    productIds.add(parts[1]);
                }
            }
            if (parts[0].contains("helpfulness")) {
                String[] helpfulness = parts[1].split("/");
                reviewsDir.add(Integer.parseInt(helpfulness[0]));
                reviewsDir.add(Integer.parseInt(helpfulness[1]));
            }
            if (parts[0].contains("score")) {
                reviewsDir.add(Integer.parseInt(parts[1].substring(0, 1)));
            }
            if (parts[0].contains("text")) {
                //end case where there is no text
                if (parts.length > 1) {
                    for (String p : parts) {
                        if (p.contains("review/text"))
                            continue;
                        text = p.split(" ");
                        br.mark(5000);
                        for (String s : text)
                            sb.append(s + " ");
                    }
                }
                while ((line = br.readLine()) != null) {
                    if (line.contains("productId")) {
                        //reset buffer to beginning of next review
                        br.reset();
                        break;
                    }
                    if (line.equals(""))
                        continue;
                    text = line.split(" ");
                    for (String s : text)
                        sb.append(s + " ");
                    br.mark(5000);
                }
                break;
            }
        } while ((line = br.readLine()) != null);
        return sb.toString();
    }

    /**
     * processes the review text, to get all tokens and their frequencies in the review:
     * @param text: the review's text
     * @return the Mapping of the tokens in the text with their frequencies in this text.
     */
    private HashMap<String, Integer> __processReviewText(String text, ArrayList<Integer> reviewsDir)
    {
        HashMap<String, Integer> reviewLexicon = new HashMap<>();
        int freq;
        String[] tokens = text.split("[\\W]");
        long reviewLength = tokens.length;
        for (String token : tokens)
        {
            if ("".equals(token)){
                reviewLength -= 1;
                continue;
            }

            freq = 1;
            token = token.toLowerCase(); // normalizes tokens
            if (reviewLexicon.containsKey(token)){
                freq = reviewLexicon.get(token) + 1;
            }
            reviewLexicon.put(token, freq);
        }

        reviewsDir.add(Math.toIntExact(reviewLength)); //TODO: prevent overflow of too long reviews
        return reviewLexicon;
    }

    /**
     * Updates the data structure that holds the processed data of all reviews: __postings.
     * Adds to the __postings value of a token the values: reviewId, freqInReview.
     * @param reviewId: the review's id.
     * @param reviewLexicon: the processed data of the review whose id is reviewId.
     */
    private void __addProcessedText(final int reviewId, HashMap<String, Integer> reviewLexicon,
                                    HashMap<String, ArrayList<Integer>> postings){
        for (String token : reviewLexicon.keySet())
        {
            if (!postings.containsKey(token)) {
                postings.put(token, new ArrayList<>());
            }
            int freq_in_review = reviewLexicon.get(token);
//            __postings.get(token).set(0, __postings.get(token).get(0) + freq_in_review); //TODO
            postings.get(token).add(reviewId);
            postings.get(token).add(freq_in_review);
        }
    }

    /**
     * writes the lexicon files to folder
     * Lexicon folder will hold one file holding all token values for each:
     * token frequencies - Frequencies.txt, posting pointers - PostingPtrs.txt
     * token length - TokenLength.txt, term pointers- TermPtr.txt
     * In addition this folder holds a file containing all tokens concatenated together - long_string.txt
     * @param dir directory which will hold the Lexicon folder
     * @param posting_lists all posting lists
     */
    private void __writeLexicon(String dir, HashMap<String, ArrayList<Integer>> posting_lists) {
        //sort tokens lexicographically
        Object[] sorted =  posting_lists.keySet().toArray();
        Arrays.sort(sorted);
        //create 2d array lexicon
        int tokens = sorted.length, termPtr = 0;
        int i,l,tp;
        ArrayList<ArrayList<Integer>> postings = new ArrayList<ArrayList<Integer>>();
        i = l = tp = 0; //indices for freq, length and termPtr lists
        int[] freqs = new int[tokens];
        double d = (__k - 1.0) / __k;
        int lenSize = (int) Math.ceil(d * tokens);
        byte[] length = new byte[lenSize];
        d = 1.0/__k * tokens;
        int termPtrsSize = (int)Math.ceil(d);

        int[] termPtrs = new int[termPtrsSize];
        StringBuilder builder = new StringBuilder();    //append all tokens to one long string
        for (Object key : sorted) {
            String token = String.valueOf(key);
            builder.append(token);
            postings.add(posting_lists.get(token));
            freqs[i] = __findTotFreq(posting_lists.get(token));
            if (i % __k == 0) {
                length[l++] = (byte) token.length();
                termPtrs[tp++] = termPtr;
            }
            else if (i % __k != __k - 1){
                length[l++] = (byte) token.length();
            }
            termPtr += token.length();
            i++;
        }
        String allTokens = builder.toString();
        long[] postingPtrs = this.writeInvertedIndex(dir, postings);
        File directory = new File(dir);
        directory.mkdir();
        File file;
        Path freqFilePath = Paths.get(dir, "Lexicon", "Frequencies.txt");
        Path postingPtrFilePath = Paths.get(dir, "Lexicon", "PostingPtrs.txt");
        Path TokenLengthFilePath = Paths.get(dir, "Lexicon", "TokenLength.txt");
        Path TermPtrFilePath = Paths.get(dir, "Lexicon", "TermPtr.txt");
        Path LongStringFilePath = Paths.get(dir,"Lexicon", "long_string.txt");
        //write frequencies to file
        file = freqFilePath.toFile();
        file.getParentFile().mkdirs();
        this.__writeIntArray(file, freqs);
        //write postingPtrs to file
        file = postingPtrFilePath.toFile();
        this.__writeLongArray(file, postingPtrs);
        //write tokenLengths to file
        file = TokenLengthFilePath.toFile();
        this.__writeByteArray(file, length);
        //write termPtr to file
        file = TermPtrFilePath.toFile();
        this.__writeIntArray(file, termPtrs);
        //write long string to file
        file = LongStringFilePath.toFile();
        this.__writeAllTokens(file, allTokens);
    }


    /**
     * given posting list, find number of total occurrences of token in all reviews
     * @param post posting list of certain token
     * @return sum occurrences of token in all reviews
     */
    private short __findTotFreq(ArrayList<Integer> post) {
        short sum = 0;
        for (int i = 1 ; i < post.size(); i += 2 ) {
            sum += post.get(i);
        }
        return sum;
    }

    /**
     * writes inverted index to Lexicon folder
     * @param dir directory holding Lexicon folder
     * @param postings all posting lists
     * @return file position of beginning of posting list for all tokens
     */
    private long[] writeInvertedIndex(String dir, ArrayList<ArrayList<Integer>> postings) {
        File directory = new File(dir);
        directory.mkdir();
        Path p = Paths.get(dir,"Lexicon", "invertedIndex.txt");
        File iif = new File(p.toString());
        if (iif.exists()) {
            iif.delete();
        }
        int size = postings.size();
        int i = 1;
        long[] result = new long[size + 1];
        long pos;
        RandomAccessFile raf;
        result[0] = 0;
        try {
            iif.getParentFile().mkdirs();
            iif.createNewFile();
            raf = new RandomAccessFile(iif, "rw");
            for (ArrayList<Integer> postingList : postings) {
                //make indices sum of current and last
                for (int j = postingList.size() - 2; j > 0;j -= 2) {
                    postingList.set(j, postingList.get(j) - postingList.get(j - 2));
                }
                pos = this.__writePostingList(raf, postingList, result[i - 1]);
                result[i++] = pos;
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    /**
     * writes all information we need about the reviews in to folder called: Reviews.
     * The folder will hold files holding following data for all tokens:
     * helpfulness numerator - HelpNum.txt, helpfulness denominator - HelpDenom.txt,
     * score - Scores.txt, review length- ReviewLength.txt
     * in addition, we will hold a file holding all unique product id's and the corresponding
     * review id's for the first review containing each:
     * for the product id's-  ProductId.txt, the corresponding review id's- PidFirstReview.txt
     * @param dir directory holding Reviews folder
     * @param reviewsDir array holding all information for all reviews, by order
     * @param productIds holds all unique product id's, by order
     * @param productIdsFirstReview holds review id's corresponding with product id's
     */
    private void __writeReviews(String dir, ArrayList<Integer> reviewsDir, ArrayList<String> productIds,
                                ArrayList<Integer> productIdsFirstReview) {
        int length = reviewsDir.size(),i,j = 0;
        short[] helpNums = new short[length / 4];
        short[] helpDenums = new short[length / 4];
        byte[] scores = new byte[length / 4];
        int[] lengths = new int[length / 4];
        for (i = 0; i < length; i++) {
            if (i % 4 == 0) {
                helpNums[j] = reviewsDir.get(i).shortValue();
            } else if (i % 4 == 1) {
                helpDenums[j] = reviewsDir.get(i).shortValue();
            } else if (i % 4 == 2) {
                scores[j] = reviewsDir.get(i).byteValue();
            } else {
                lengths[j++] = reviewsDir.get(i).intValue();
            }
        }

        File directory = new File(dir);
        directory.mkdir();
        File file;
        Path helpfulnessNumFilePath = Paths.get(dir,"Reviews", "HelpNum.txt");
        Path helpfulnessDenFilePath = Paths.get(dir,"Reviews", "HelpDenom.txt");
        Path scoreFilePath = Paths.get(dir,"Reviews", "Scores.txt");
        Path reviewLenFilePath = Paths.get(dir,"Reviews","ReviewLength.txt");
        Path prodIdFilePath = Paths.get(dir,"Reviews", "ProductId.txt");
        Path firstReviewFilePath = Paths.get(dir,"Reviews", "PidFirstReview.txt");
        //write helpfulnessNum to file
        file = helpfulnessNumFilePath.toFile();
        file.getParentFile().mkdirs();
        this.__writeShortArray(file, helpNums);
        //write helpfulnessDenom to file
        file = helpfulnessDenFilePath.toFile();
        this.__writeShortArray(file, helpDenums);
        //write scores to file
        file = scoreFilePath.toFile();
        this.__writeByteArray(file, scores);
        //write lengths to file
        file = reviewLenFilePath.toFile();
        this.__writeIntArray(file, lengths);
        //write prodIds to file
        String[] ProductIds = productIds.stream().toArray(String[]::new);
        file = prodIdFilePath.toFile();
        this.__writeStringArray(file, ProductIds);
        //write productIdsFirstReview to file
        int[] productIdsFirstReviewArray = productIdsFirstReview.stream().mapToInt(m->m).toArray();
        file = firstReviewFilePath.toFile();
        this.__writeIntArray(file, productIdsFirstReviewArray);
    }

    // ########################### WRITING TO FILE ##########################


    /**
     * writes an array of ints to file
     * @param file file to write to
     * @param array to write
     */
    private void __writeIntArray(File file, int[] array) {
        FileOutputStream fos;
        DataOutputStream dos;

        try {
            file.createNewFile();
            fos = new FileOutputStream(file);
            dos = new DataOutputStream(fos);
            for (int i : array) {
                dos.writeInt(i);
            }
            dos.close();
            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * writes an array of shorts to file
     * @param file file to write to
     * @param array to write
     */
    private void __writeShortArray(File file, short[] array) {
        FileOutputStream fos;
        DataOutputStream dos;
        try {

            file.createNewFile();
            fos = new FileOutputStream(file);
            dos = new DataOutputStream(fos);
            for (short s : array) {
                dos.writeShort(s);
            }
//            dos.flush();
            dos.close();
            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * writes an array of bytes to file
     * @param file file to write to
     * @param array to write
     */
    private void __writeByteArray(File file, byte[] array) {
        FileOutputStream fos;
        try {
            file.createNewFile();
            fos = new FileOutputStream(file);
            fos.write(array);
            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * writes an array of strings to file
     * @param file file to write to
     * @param array to write
     */
    private void __writeStringArray(File file, String[] array) {
        try {
            file.createNewFile();
            FileOutputStream fos;
            fos = new FileOutputStream(file);
            for (String s : array) {
                fos.write(s.getBytes(ENCODING));
            }
            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * writes an array of longs to file
     * @param file file to write to
     * @param array to write
     */
    private void __writeLongArray(File file, long[] array) {
        FileOutputStream fos;
        DataOutputStream dos;
        try {
            file.createNewFile();
            fos = new FileOutputStream(file);
            dos = new DataOutputStream(fos);
            for (long l : array) {
                dos.writeLong(l);
            }
            dos.flush();
            dos.close();
            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * writes very long string to file encoded so each character takes only one byte
     * @param file file to write to
     * @param longString to write
     */
    private void __writeAllTokens(File file, String longString) {
        FileOutputStream fos;
        try {
            file.createNewFile();
            fos = new FileOutputStream(file);
            fos.write(longString.getBytes(ENCODING));
            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * write posting list to file and return cursor position where next one will be written
     * @param raf randomAccessFile
     * @param postingList posting list
     * @param pos
     * @return cursor position where next posting list will be written
     */
    private long __writePostingList(RandomAccessFile raf, ArrayList<Integer> postingList, long pos) {
        ArrayList<byte[]> compressed = GroupVarint.compress(postingList);
        for (byte[] ba : compressed)
            try {
                raf.seek(pos);
                raf.write(ba);
                pos += ba.length;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        try {
            return raf.getFilePointer();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return 0;
    }



    // ########################### ENCODING ##########################

    /**
     * compresses an ArrayList<Integer> using groupVarInt encoding
     * @param list to compress
     * @return compressed list in bytes so it can be writen to file
     */
    public ArrayList<byte[]> __groupVarintEncoding(ArrayList<Integer> list)
    {
        return GroupVarint.compress(list);
    }

    // ########################### DEBUGGING ##########################

//    /**
//     * print the __reviewsDir.
//     */
//    public void __printReviews(ArrayList<int[]> reviewsDir){
//        reviewsDir.forEach((idx, val) ->
//            System.out.printf("{%s: [%d, %d, %d, %d]}\n ",
//                idx.getKey(),
//                val[0], val[1], val[2], val[3]));
//        System.out.println();
//    }

    /**
     * print the __postings.
     */
    public void __printPostings(HashMap<String, ArrayList<Integer>> postings){
        postings.forEach((idx, val) ->
            System.out.printf("{%s: [%s]}\n",
                idx, __getPostingList(postings.get(idx))));
        System.out.println();
    }

    private String __getPostingList(ArrayList<Integer> posting_list)
    {
        String s = "";
        for (int i = 0; i < posting_list.size(); i += 2){
            s +=  posting_list.get(i) + " " + posting_list.get(i + 1) + " " ;
        }
        return s;
    }

}

//  ######################## OLD VERSTIONS- KEPT IN CASE IT'S MORE EFFICIENT ##################

///**
// * writes the __reviewsDir to file [helpfulnessNumerator, helpfulnessDenominator, score, reviewLength]
// *@param dir: directory to write to.
// */
//private void __writeReviews(String dir, ArrayList<Integer> reviewsDir, ArrayList<String> productIds,
//                            ArrayList<Integer> productIdsFirstReview)
//{
//    // initialize a file to write to:
//    File directory = new File(dir);
//    directory.mkdir();
//    File file;
//    Path helpfulnessNumFilePath = Paths.get(dir,"Reviews", "HelpNum.txt");
//    Path helpfulnessDenFilePath = Paths.get(dir,"Reviews", "HelpDenom.txt");
//    Path scoreFilePath = Paths.get(dir,"Reviews", "Scores.txt");
//    Path reviewLenFilePath = Paths.get(dir,"Reviews","ReviewLength.txt");
//    Path prodIdFilePath = Paths.get(dir,"Reviews", "ProductId.txt");
//    Path firstReviewFilePath = Paths.get(dir,"Reviews", "PidFirstReview.txt");
//    FileOutputStream fos;
//    DataOutputStream dos;
//    int length = reviewsDir.size(),i;
//    int[] vals = new int[4];
//    short[] helpNums = new short[length];
//    short[] helpDenums = new short[length];
//    byte[] scores = new byte[length];
//    int[] lengths = new int[length];
//    ArrayList<String> prodIds = new ArrayList<String>();
//    ArrayList<Integer> firstReviews = new ArrayList<Integer>();
//    String prodId,newPid = "";
//    int rid;
//    for (i = 0; i < length; i++) {
//        helpNums[i] = reviewsDir.get(i)[0].shortValue();
//        System.out.println(helpNums[i]);
//        helpDenums[i] = (short) reviewsDir[1];
//        scores[i] = (byte) vals[2];
//        lengths[i++] = (short) vals[3];
//
//    }
//    try {
//        file = helpfulnessNumFilePath.toFile();
//        file.getParentFile().mkdirs();
//        file.createNewFile();
//        fos = new FileOutputStream(file, true);
//        dos = new DataOutputStream(fos);
//        for (short s : helpNums) {
//            System.out.println(s);
//            dos.writeShort(s);
//        }
//        dos.flush();
//        dos.close();
//        fos.close();
//
//        file = helpfulnessDenFilePath.toFile();
//        file.createNewFile();
//        fos = new FileOutputStream(file, true);
//        dos = new DataOutputStream(fos);
//        for (short s : helpDenums)
//            dos.writeShort(s);
//        dos.flush();
//        dos.close();
//        fos.close();
//
//        file = scoreFilePath.toFile();
//        file.createNewFile();
//        fos = new FileOutputStream(file, true);
//        dos = new DataOutputStream(fos);
//        for (byte b : scores)
//            dos.writeByte(b);
//        dos.flush();
//        dos.close();
//        fos.close();
//
//        file = reviewLenFilePath.toFile();
//        file.createNewFile();
//        fos = new FileOutputStream(file, true);
//        dos = new DataOutputStream(fos);
//        for (short s : lengths)
//            dos.writeShort(s);
//        dos.flush();
//        dos.close();
//        fos.close();
//
//        file = prodIdFilePath.toFile();
//        file.createNewFile();
//        fos = new FileOutputStream(file, true);
//        for (String s : prodIds) {
//            fos.write(s.getBytes(ENCODING));
//        }
//        fos.close();
//
//        file = firstReviewFilePath.toFile();
//        file.createNewFile();
//        fos = new FileOutputStream(file, true);
//        dos = new DataOutputStream(fos);
//        for (int s : firstReviews) {
//            dos.writeInt(s);
//        }
//        dos.flush();
//        dos.close();
//        fos.close();
//    } catch (FileNotFoundException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//    } catch (IOException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//    }
//}

//private void __writeAllTokens(String dir) {
//    Path filePath = Paths.get(dir,"Lexicon", "long_string.txt");
//    File file = new File(filePath.toString());
//    FileOutputStream fos;
//    try {
//        file.getParentFile().mkdirs();
//        file.createNewFile();
//        fos = new FileOutputStream(file, true);
//        fos.write(__allTokens.getBytes(ENCODING));
//        fos.close();
//    } catch (IOException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//    }
//}

///**
//* writes lexicon in compressed way to 4 files
//* @param dir directory in which to store the files
//* first file: frequencies
//* second file: posting pointers
//* third file: length of each token
//* fourth file: term pointers
//*/
//private void __writeLexicon(String dir, HashMap<String, ArrayList<Integer>> postings) {
// this.__processLexicon(dir, postings);
// Path freqFilePath = Paths.get(dir, "Lexicon", "Frequencies.txt");
// Path postingPtrFilePath = Paths.get(dir, "Lexicon", "PostingPtrs.txt");
// Path TokenLengthFilePath = Paths.get(dir, "Lexicon", "TokenLength.txt");
// Path TermPtrFilePath = Paths.get(dir, "Lexicon", "TermPtr.txt");
// File file = new File(freqFilePath.toString());
// FileOutputStream fos;
// DataOutputStream dos;
// try {
//     //write frequencies to file
//     file.getParentFile().mkdirs();
//     file.createNewFile();
//     fos = new FileOutputStream(file, true);
//     dos = new DataOutputStream(fos);
//     for (int s :__lexicon.getFreqs()) {
//         dos.writeInt(s);
//     }
//     dos.flush();
//     dos.close();
//     fos.close();
//
//     file = postingPtrFilePath.toFile();
//     file.createNewFile();
//     fos = new FileOutputStream(file, true);
//     dos = new DataOutputStream(fos);
//     for (long l : __lexicon.getPostingPtrs()) {
//         dos.writeLong(l);
//     }
//     dos.flush();
//     dos.close();
//     fos.close();
//
//     file = TokenLengthFilePath.toFile();
//     file.createNewFile();
//     fos = new FileOutputStream(file, true);
//     dos = new DataOutputStream(fos);
//     for (byte b : __lexicon.getLens()) {
//         dos.writeByte(b);
//     }
//     dos.flush();
//     dos.close();
//     fos.close();
//
//     file = TermPtrFilePath.toFile();
//     file.createNewFile();
//     fos = new FileOutputStream(file, true);
//     dos = new DataOutputStream(fos);
//     for (int i : __lexicon.getTermPtrs()) {
//         dos.writeInt(i);
//     }
//     dos.flush();
//     dos.close();
//     fos.close();
//
// } catch (IOException e) {
//     // TODO Auto-generated catch block
//     e.printStackTrace();
// }
//}

//################### TEST ##################
//File test = new File("test.txt");
//try {
//  test.createNewFile();
//  FileWriter myWriter = new FileWriter(test);
//  myWriter.write(Arrays.deepToString(postings));
//  myWriter.close();
//} catch (IOException e) {
//  // TODO Auto-generated catch block
//  e.printStackTrace();
//}
//#################################################