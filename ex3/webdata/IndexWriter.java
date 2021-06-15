package webdata;

import static java.lang.Math.max;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;


public class IndexWriter {
        static int MAX_REVS = 10000;
    Runtime __gfg = Runtime.getRuntime();
    static final int __MEM_SCALAR = 1000;
    final static int __THRESHOLD = 360876;

    /**
     * blocking factor
     * //TODO - find best value
     */
    final static int __k = 5;

    private static final String ENCODING = "ISO-8859-1";
    String __lastPid = "";



    /**
     * Given product review data, creates an on disk index
     * inputFile is the path to the file containing the review data
     * dir is the directory in which all index files will be created
     * if the directory does not exist, it should be created
     */
    public void write(String inputFile, String dir) {
        this.removeIndex(dir);

        // Creates the Sub-Indices:
        this.__createSubIndices(dir, inputFile);

        // Merges the sub-Indices into one final Index:
        __mergeReviews(dir);
        __mergeAllLexicons(dir);
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

    private long __reviewsMem(ArrayList<Integer> reviewData , ArrayList<String> productIds) //TODO: BAR
    {
        // computation of array of ints rounded to upper mult of 8: num of elements /4 * 4 + 32
        // computation of array of strings of 10 chars rounded to upper mult of 8: (64 * num of elements) + 20
        return __MEM_SCALAR * max((reviewData.size() + 47) & (-8) , ((64 * productIds.size() ) + 27) & (-8));
    }
    // ########################### READING & PROCESSING ##########################

    /**
     * Reads and pre-processes the text in inputFile. Saves the data in as indices
     * in dir directory.
     * @param dir: the directory to which we will save the sub-indices.
     * @param inputFile:  the filename of the file that is holding the reviews.
     */
    private void __createSubIndices(String dir, String inputFile){

        // initialize data structures to hold the relevant (final) inputFile's data:
        HashMap<String, ArrayList<Integer>> postings = new HashMap<>(); // key: token, value: [reviewId, freqInReview, ....]
        ArrayList<Integer> reviewsDir = new ArrayList<>(); // [helpfulnessNumerator, helpfulnessDenominator, score, length]
        ArrayList<Integer> productIdsFirstReview = new ArrayList<>();
        ArrayList<String> productIds = new ArrayList<>();
        productIds.add("");

        // initialize data structures to hold pre-processed inputFile's data:
        HashMap<String, Integer> reviewLexicon = new HashMap<>();
        // loop variables:
        long reviewsCount = 0;
        int reviewId = 0;
        String line;

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));

            do {
//                while ((reviewId < __THRESHOLD) & ((line = br.readLine()) != null)){ // not cautious
                while ((reviewId < __THRESHOLD) & ((line = br.readLine()) != null) & (reviewsCount + reviewId < MAX_REVS)){ // not cautious
                    // Parses the review data & processes its text, to get all tokens and their frequencies in the review:
                    this.__processReviewText(this.__collectReview(br, line, reviewId, reviewsDir, productIds, productIdsFirstReview),
                            reviewsDir, reviewLexicon);

                    // Updates the data structure that holds the processed data of all reviews:
                    this.__addProcessedText(reviewId, reviewLexicon, postings);

                    // Frees memory, to allow next iteration to run less restricted:
                    reviewLexicon.clear();
//                    this.__gfg.gc();

                    reviewId += 1;
                }
//                while ((this.__gfg.freeMemory() > this.__reviewsMem(reviewsDir, productIds))
//                        & ((line = br.readLine()) != null)){ //cautious
//                while ((this.__gfg.freeMemory() > this.__reviewsMem(reviewsDir, productIds))
//                        & ((line = br.readLine()) != null) & (reviewsCount + reviewId < MAX_REVS)) {{// cautious}
                while ((this.__gfg.freeMemory() > this.__reviewsMem(reviewsDir, productIds))
                                        & ((line = br.readLine()) != null) & (reviewsCount + reviewId < MAX_REVS)) {
                    // Parses the review data & processes its text, to get all tokens and their frequencies in the review:
                    this.__processReviewText(this.__collectReview(br, line, reviewId, reviewsDir, productIds, productIdsFirstReview),
                            reviewsDir, reviewLexicon);

                    // Updates the data structure that holds the processed data of all reviews:
                    this.__addProcessedText(reviewId, reviewLexicon, postings);

                    // Frees memory, to allow next iteration to run less restricted:
                    reviewLexicon.clear();
//                    this.__gfg.gc();

                    reviewId += 1;
                }

                productIds.remove(0);

                // Writes Sub_index to Files:
                String directory = dir + (File.separator + Long.toString(reviewsCount));
                this.__writeReviews(directory + (File.separator + "Reviews"), reviewsDir, productIds, productIdsFirstReview);
                this.__writeLexicon(directory + (File.separator + "Lexicon"), postings);

                reviewsCount += reviewId;

                // Clears Sub_Index data structures & indicators:
                postings.clear();
                reviewsDir.clear();
                productIds.clear();
                productIds.add("");
                productIdsFirstReview.clear();
                reviewId = 0;
                br.reset();

            } while (!(line == null | (reviewsCount + reviewId) == MAX_REVS));
//            } while (line != null);
        }
        catch (IOException ex){
            // TODO Auto-generated catch block
            ex.printStackTrace();
        }
    }

    /**
     * Collects the new Reviews' meta-data:
     *  reviewId, ProductID, helpfulnessNumerator, helpfulnessDenominator, score, Textlength.
     *  to the specified input vars.
     * @param br: reader
     * @param line: the last line bf read
     * @param lastPid:
     * @return the text of the review.
     * @throws IOException when bf fails reading
     */
    private String __collectReview(final BufferedReader br, String line, int reviewId, ArrayList<Integer> reviewsDir,
                                   ArrayList<String> productIds, ArrayList<Integer> productIdsFirstReview)
            throws IOException {
        String[] parts;
        StringBuilder text = new StringBuilder();

        do {
            parts = line.split(": ");
            if (parts[0].contains("productId")) {
                if (!this.__lastPid.equals(parts[1]))
                {
                    productIds.add(parts[1]);
                    productIdsFirstReview.add(reviewId);
                    this.__lastPid = parts[1];
                }
            }
            else if (parts[0].contains("helpfulness")) {
                String[] helpfulness = parts[1].split("/");
                reviewsDir.add(Integer.parseInt(helpfulness[0]));
                reviewsDir.add(Integer.parseInt(helpfulness[1]));
            }
            else if (parts[0].contains("score")) {
                reviewsDir.add(Integer.parseInt(parts[1].substring(0, 1)));
            }
            else if (parts[0].contains("text")) {
                text = new StringBuilder("");

                for (int i = 1; i < parts.length ; i++)
                {
                    text.append(" ");
                    text.append(parts[i]);
                }
                while ((line = br.readLine()) != null) {
                    if (line.contains("product/productId: ")) {
                        //reset buffer to beginning of next review
                        br.reset();
                        break;
                    }
                    text.append(line);
                    br.mark(5000);
                }
                break;
            }
        } while ((line = br.readLine()) != null);
        return text.toString();
    }

    /**
     * processes the review text, to get all tokens and their frequencies in the review:
     * @param text: the review's text
     * @return the Mapping of the tokens in the text with their frequencies in this text.
     */
    private void __processReviewText(String text, ArrayList<Integer> reviewsDir,
                                     HashMap<String, Integer> reviewLexicon)
    {
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

    //    #################### Writing the index ###################################################

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
//        __printPostings(posting_lists);

        //sort tokens lexicographically
        Object[] sorted =  posting_lists.keySet().toArray();
        Arrays.sort(sorted);


        File file = new File(dir);
        file.mkdirs();

        //Iterate over tokens lexicographically, remove
        Iterator<HashMap.Entry<String, ArrayList<Integer>>> itr = posting_lists.entrySet().iterator();


        //create 2d array lexicon
        int tokens = posting_lists.size(), tokenLength;
        int termPtr = 0;
        int i = 0, l = 0, tp = 0; //indices for freq, length and termPtr lists

        ArrayList<Integer>[] postings = new ArrayList[tokens];
        int[] freqs = new int[tokens];
        byte[] length = new byte[(int) Math.ceil((__k - 1.0) / __k * tokens)];
        int[] termPtrs = new int[(int)Math.ceil(1.0/__k * tokens)];
        StringBuilder builder = new StringBuilder();    //append all tokens to one long string

        for (Object key : sorted) {
            String token = String.valueOf(key);
            postings[i] = posting_lists.get(token);
            freqs[i] = __findTotFreq(postings[i]);
            //crop word if too long
            if (token.length() > 127)
                token = token.substring(0,126);
            builder.append(token);
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

        //write frequencies to file
        file = new File(dir, "Frequencies.txt");
        this.__writeIntArray(file, freqs);

        //write postingPtrs to file
        file = new File(dir, "PostingPtrs.txt");
        this.__writeLongArray(file, postingPtrs);

        //write tokenLengths to file
        file = new File(dir, "TokenLength.txt");
        this.__writeByteArray(file, length);

        //write termPtr to file
        file = new File(dir, "TermPtr.txt");
        this.__writeIntArray(file, termPtrs);

        //write long string to file
        file = new File(dir, "long_string.txt");
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
    private long[] writeInvertedIndex(String dir, ArrayList<Integer>[] postings) {
        File iif = new File(dir);
        iif.mkdir();
        iif = new File(dir, "invertedIndex.txt");
        if (iif.exists()) {
            iif.delete();
        }
        int size = postings.length;
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
            raf.close();
            //            if (Files.deleteIfExists(iif.toPath())) {
            //                System.out.println("be");
            //            }
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
        int numOfElements = reviewsDir.size() / 4;
        int i;
        new File(dir).mkdirs();
        //change writing all reviewsDir to one file
        File file = new File(dir, "ReviewsDir.txt");
        try {
            file.createNewFile();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        //        short[] helpNums = new short[numOfElements / 4];
        //        for (i = 0; i < numOfElements; i++) {
        //            helpNums[i/4] = reviewsDir.get(i).shortValue();
        //        }

        int j = 0;
        short[] reviewsHelpfulnessNumerators = new short[numOfElements];
        short[] reviewsHelpfulnessDenominators = new short[numOfElements];
        byte[] reviewsScores = new byte[numOfElements];
        int[] reviewsLengths = new int[numOfElements];

        ByteBuffer buffer = ByteBuffer.allocate((9 * numOfElements) + 4);
        buffer.putInt(numOfElements);
        for (i = 0; i < 4 * numOfElements; i+=4) {
            buffer.putShort(reviewsDir.get(i).shortValue()); //Num
            buffer.putShort(reviewsDir.get(i + 1).shortValue()); //Denum
            buffer.put((reviewsDir.get(i + 2).byteValue())); //Score
            buffer.putInt(reviewsDir.get(i + 3).intValue()); //Length

            reviewsHelpfulnessNumerators[j] = reviewsDir.get(i).shortValue();
            reviewsHelpfulnessDenominators[j] = reviewsDir.get(i + 1).shortValue();
            reviewsScores[j] = reviewsDir.get(i + 2).byteValue();
            reviewsLengths[j++] = reviewsDir.get(i + 3).intValue();
        }
//        System.out.println(Arrays.toString(reviewsHelpfulnessNumerators));
//        System.out.println(Arrays.toString(reviewsHelpfulnessDenominators));
//        System.out.println(Arrays.toString(reviewsScores));
//        System.out.println(Arrays.toString(reviewsLengths));
//
//        System.out.println("////////////////////////");


        //        for (i = 1; i < numOfElements; i+=4) {
        //            buffer.putShort(reviewsDir.get(i).shortValue()); //Denum
        //       }
        //        for (i = 2; i < numOfElements; i+=4) {
        //            buffer.put((reviewsDir.get(i).byteValue())); //Score
        //       }
        //        for (i = 3; i < numOfElements; i+=4) {
        //            buffer.putInt(reviewsDir.get(i).intValue()); //Length
        //       }

        buffer.flip();
        FileChannel out;
        try {
            out = FileChannel.open(file.toPath(), StandardOpenOption.APPEND);
            out.write(buffer);
            out.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        //write prodIds to file
        String[] ProductIds = productIds.stream().toArray(String[]::new);
        file = new File(dir, "ProductId.txt");
        this.__writeStringArray(file, ProductIds);

        //write productIdsFirstReview to file
        int[] productIdsFirstReviewArray = productIdsFirstReview.stream().mapToInt(m->m).toArray();

        file = new File(dir, "PidFirstReview.txt");
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
//        __printCompressed(compressed);
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

    //################## merge reviews ####################################

    //not sure about the openOptions
    public void __mergeReviews(String dir) {
        //        String[] reviewFiles = new String[]{"HelpNum.txt", "HelpDenom.txt", "Scores.txt"
        //            ,"ReviewLength.txt", "ProductId.txt"};
        String[] reviewFiles = new String[]{"ReviewsDir.txt", "ProductId.txt"};
        File file = new File(dir);
        String[] directories = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });
        Path outFile;
        Path dst;
        Path reviewsPath = Paths.get(dir, "Reviews");
        reviewsPath.toFile().mkdir();
        int[] subDirs = Arrays.stream(directories).mapToInt(Integer::parseInt).toArray();
//        System.out.println(Arrays.toString(subDirs)); //printshere
        Arrays.sort(subDirs);
        for (String s : reviewFiles) {
            outFile=Paths.get(dir, Integer.toString(subDirs[0]),"Reviews",s);
            this.__appendFiles(dir, outFile,"Reviews", s, subDirs);
            dst = Paths.get(reviewsPath.toString(), s);
            //create final directory for Reviews with all merged files
            try {
                Files.copy(outFile, dst, StandardCopyOption.REPLACE_EXISTING);
                outFile.toFile().delete();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            //            if (s.equals("Scores.txt"))
            //                System.out.println("Scores: " + Arrays.toString(this.__readByteArray(outFile)));
            //            else if (s.equals("HelpNum.txt"))
            //                System.out.println("HelpNum: " + Arrays.toString(this.__readShortArray(outFile)));
            //            else if (s.equals("HelpDenom.txt"))
            //                System.out.println("HelpDenom: " + Arrays.toString(this.__readShortArray(outFile)));
            //            else if (s.equals("ReviewLength.txt"))
            //                System.out.println("ReviewLength: " + Arrays.toString(this.__readIntArray(outFile)));
            //            else if (s.equals("ProductId.txt"))
            //                System.out.println("ProductId: " + Arrays.toString(this.__readProductId(outFile)));
        }
        //merge firstReviewFiles
        outFile=Paths.get(dir, Integer.toString(subDirs[0]),"Reviews","PidFirstReview.txt");
        this.__mergeFirstIdsFile(dir, outFile, subDirs);
        //        System.out.println("First reviews: " + Arrays.toString(this.__readIntArray(outFile)));
        try {
            dst = Paths.get(reviewsPath.toString(), "PidFirstReview.txt");
            Files.copy(outFile, dst, StandardCopyOption.REPLACE_EXISTING);
            outFile.toFile().delete();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //delete all subdirectories
        this.__deleteSubDirs(dir, subDirs);
    }

    /**
     * merge a certain file (readFrom) from all directories (subDirs) by appending
     * them all to the end of output file (outFile)
     * @param dir directory where all data is
     * @param outFile final merged file
     * @param readFromDir direct folder containing readFrom file
     * @param readFrom file name to read from
     * @param subDirs all subdirectories to be appended (merged) to outFile
     */
    private void __appendFiles(String dir, Path outFile, String readFromDir, String readFrom, int[] subDirs) {
        int len = subDirs.length;
        int initial = 0;

        ByteBuffer num_elements_buff = ByteBuffer.allocate(4);
        try(FileChannel out=FileChannel.open(outFile, StandardOpenOption.READ)) {
            out.read(num_elements_buff, 0);
            num_elements_buff.rewind();
            initial = num_elements_buff.getInt();
            out.close();
        } catch (IOException e2) {
            // TODO Auto-generated catch block
            e2.printStackTrace();
        }
        try(FileChannel out=FileChannel.open(outFile, StandardOpenOption.WRITE, StandardOpenOption.SYNC)) {
            for(int ix=1; ix<len; ix++) {
                Path inFile=Paths.get(dir, Integer.toString(subDirs[ix]),readFromDir,readFrom);
                try(FileChannel in=FileChannel.open(inFile, StandardOpenOption.READ)) {
                    num_elements_buff.clear();
                    in.read(num_elements_buff);
                    num_elements_buff.rewind();
                    int offset = num_elements_buff.getInt();
                    //
                    initial += offset;

                    //
                    out.position(out.size());
                    for(long p=4, l=in.size(); p<l; )
                        p+=in.transferTo(p, l-p, out);
                    inFile.toFile().delete();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            //write updated number of reviews to the beginning of file
            num_elements_buff.clear();
            num_elements_buff.putInt(initial);
            num_elements_buff.flip();
            out.write(num_elements_buff, 0);

        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }

    private void __mergeFirstIdsFile(String dir, Path outFile, int[] subDirs) {
        int len = subDirs.length;
        int[] ids;
        int offset;

        for(int ix=1; ix<len; ix++) {
            try(FileChannel out=FileChannel.open(outFile, StandardOpenOption.APPEND, StandardOpenOption.SYNC)) {
                offset = subDirs[ix];
                Path inFile = Paths.get(dir,Integer.toString(subDirs[ix]), "Reviews","PidFirstReview.txt");
                ids = ReadWriteArraysUtils.readIntArray(inFile);
                int idsLen = ids.length;
                for (int j = 0;j < idsLen; j++)
                    ids[j] += offset;
                ByteBuffer buf = ByteBuffer.allocate(4 * ids.length);
                for (int i : ids) {
                    buf.putInt(i);
                }
                buf.flip();
                out.write(buf);
                if (out != null)
                    out.close();
                inFile.toFile().delete();

            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
    }

    /**
     * delete all partial subdirectories after merging to first one
     * @param dir outer directory
     * @param subDirs all subdirectories to erase (except the first which is the merged one)
     */
    private void __deleteSubDirs(String dir, int[] subDirs) {
        for (int i = 0; i< subDirs.length;i++) {
            Path p = Paths.get(dir,Integer.toString(subDirs[i]),"Reviews");
            if (!p.toFile().delete()) {
                System.out.println(p.toString());
            }
            p = Paths.get(dir,Integer.toString(subDirs[i]));
            p.toFile().delete();
        }
    }

    //################## merge Lexicon ####################################

    /**
     * recursively merges all lexicon files
     * @param dir directory containing subdirectories holing lexicon chunks
     * @return path to completely merged Lexicon files
     */
    public void __mergeAllLexicons(String dir) {
        File file = new File(dir);
        String[] directories = file.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                if (name.compareTo("Reviews") == 0)
                    return false;
                return new File(current, name).isDirectory();
            }
        });

        int[] subDirs = Arrays.stream(directories).mapToInt(Integer::parseInt).toArray();
        ArrayList<Integer> newFiles = new ArrayList<Integer>();
        //merge pairs until all files are merged to one
        while (subDirs.length > 1) {
            int len = subDirs.length;
            Arrays.sort(subDirs);
            for (int i = 0; i <= len; i+= 2) {
                if (i == len - 1 || i == len) {
                    //if odd number of files - add the last file to next iteration, otherwise, just begin new iteration
                    if (i == len - 1) {
                        newFiles.add(subDirs[len - 1]);
                    }
                    //finished merging files in current round
                    //                    firstIteration = false;
                    subDirs = Arrays.stream(newFiles.toArray()).mapToInt(o -> (int)o).toArray();
                    newFiles.clear();
                    len = subDirs.length;
                    //if only last element is left - finish
                    if (len == 1)
                        break;
                    i = 0;
                }
                String file1 = dir + File.separator + Integer.toString(subDirs[i]);
                String file2 = dir + File.separator + Integer.toString(subDirs[i + 1]);
                //store result in folder named by the value of file1 + 1
                this.__compareLexicons(file1, file2, subDirs[i + 1] - subDirs[i]);
                newFiles.add(subDirs[i]);
            }
        }
        //move all to external Lexicon folder and erase the rest

        String currentMerged = dir + File.separator + Integer.toString(subDirs[0]) + File.separator + "Lexicon";
        this.__placeMergedLexicon(dir, currentMerged);
        //delete old lexicon
        Paths.get(dir, Integer.toString(subDirs[0])).toFile().delete();

    }

    private void __placeMergedLexicon(String dir, String currentLexiconPath) {
        Path ps, pd;
        String[] filesToMove = {"Frequencies.txt","PostingPtrs.txt","TokenLength.txt","TermPtr.txt","long_string.txt","invertedIndex.txt"};
        Path dst = Paths.get(dir, "Lexicon");
        dst.toFile().mkdir();
        for (String s: filesToMove){
            ps = Paths.get(currentLexiconPath, s);
            pd = Paths.get(dst.toString(), s);
            try {
                Files.copy(ps, pd, StandardCopyOption.REPLACE_EXISTING);
                //delete old lexicon
                ps.toFile().delete();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        Paths.get(currentLexiconPath).toFile().delete();
    }

    /**
     * compares to lexicons and merges them to another file
     * @param dir1 directory of first lexicon
     * @param dir2 directory of second lexicon
     * @param results file to write merged lexicon to
     * @param firstIteration if currently on first iteration of merge - posting lists must be decompressed and offset added accordingly
     */
    private void __compareLexicons(String dir1, String dir2, int offsetsDiff) {

        String file1 = dir1 + File.separator + "Lexicon";
        String file2 = dir2 + File.separator + "Lexicon";

        //upload file1 lexicon
        Path freqFilePath = Paths.get(file1, "Frequencies.txt");
        Path postingPtrFilePath = Paths.get(file1, "PostingPtrs.txt");
        Path TokenLengthFilePath = Paths.get(file1, "TokenLength.txt");
        Path TermPtrFilePath = Paths.get(file1, "TermPtr.txt");
        Path LongStringPath = Paths.get(file1, "long_string.txt");
        Path invertedIndexPath1 = Paths.get(file1, "invertedIndex.txt");
        int[] frequencies1 = ReadWriteArraysUtils.readIntArray(freqFilePath);
        long[] postingsPtrs1 = ReadWriteArraysUtils.readLongArray(postingPtrFilePath);
        byte[] tokensLengths1 = ReadWriteArraysUtils.readByteArray(TokenLengthFilePath);
        int[] termPtrs1 = ReadWriteArraysUtils.readIntArray(TermPtrFilePath);
        CompactCharSequence tokens1 = ReadWriteArraysUtils.readCompactCharSequence(LongStringPath);
        Path pd;
        pd = Paths.get(dir1 + File.separator, "invertedIndexReadFrom.txt");
        try {
            Files.copy(invertedIndexPath1, pd, StandardCopyOption.REPLACE_EXISTING);
            //delete old lexicon
            Files.deleteIfExists(invertedIndexPath1);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        invertedIndexPath1 = pd;


        //upload file2 lexicon
        freqFilePath = Paths.get(file2, "Frequencies.txt");
        postingPtrFilePath = Paths.get(file2, "PostingPtrs.txt");
        TokenLengthFilePath = Paths.get(file2, "TokenLength.txt");
        TermPtrFilePath = Paths.get(file2, "TermPtr.txt");
        LongStringPath = Paths.get(file2, "long_string.txt");
        Path invertedIndexPath2 = Paths.get(file2, "invertedIndex.txt");
        int[] frequencies2 = ReadWriteArraysUtils.readIntArray(freqFilePath);
        long[] postingsPtrs2 = ReadWriteArraysUtils.readLongArray(postingPtrFilePath);
        byte[] tokensLengths2 = ReadWriteArraysUtils.readByteArray(TokenLengthFilePath);
        int[] termPtrs2 = ReadWriteArraysUtils.readIntArray(TermPtrFilePath);
        CompactCharSequence tokens2 = ReadWriteArraysUtils.readCompactCharSequence(LongStringPath);

        pd = Paths.get(dir2 + File.separator, "invertedIndexReadFrom.txt");
        try {
            Files.copy(invertedIndexPath2, pd, StandardCopyOption.REPLACE_EXISTING);
            //delete old lexicon
            invertedIndexPath2.toFile().delete();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        invertedIndexPath2 = pd;

        //erase both files
        __deleteLexicon(file1);
        __deleteLexicon(file2);

        String resultFile = file1;
        Paths.get(resultFile).toFile().mkdir();

        //initialize result
        //#######################

        ByteBuffer mergedFrequencies = ByteBuffer.allocate(4 * (frequencies1.length + frequencies2.length));
        ByteBuffer mergedPostingsPtrs = ByteBuffer.allocate(8 * (postingsPtrs1.length + postingsPtrs2.length));
        ByteBuffer mergedTokensLengths = ByteBuffer.allocate(tokensLengths1.length + tokensLengths2.length);
        ByteBuffer mergedTermPtrs = ByteBuffer.allocate( 4 * (termPtrs1.length + termPtrs2.length));

        //######################

        StringBuilder sb = new StringBuilder();
        //        String resultFile = results.toString();
        int t = 0,i, termPtr = 0, ind1, ind2;
        int numTokens1 = frequencies1.length, numTokens2 = frequencies2.length;
        boolean file1Done = false, file2Done = false;
        String token1 = "", token2 = "";
        long pptr = 0;
        int start1 = 0, l1 = 0, b1 = 0, i1 = 0;
        int start2= 0, l2 = 0, b2 = 0, i2 = 0;
        int[] file1Params = new int[] {start1,l1, b1, i1};
        int[] file2Params = new int[] {start2, l2, b2, i2};
        //initialize merged invertedIndex buffer according to how much space is left in memory
        ByteBuffer mergedInvertedIndex = ByteBuffer.allocate((int) (0.5 * this.__gfg.freeMemory()));
        while (!(file1Done && file2Done)) {
            if (!file1Done)
                token1 = getToken(file1Params, termPtrs1, tokensLengths1, tokens1);
            if (!file2Done)
                token2 = getToken(file2Params, termPtrs2, tokensLengths2, tokens2);
            if (file1Done || token1.compareTo("") == 0){
                if (file1Done != true) {
                    file1Done = true;
                    token1 = "~~~";
                }
            }
            if (file2Done || token2.compareTo("") == 0){
                if (file2Done != true) {
                    file2Done = true;
                    token2 = "~~~~~~";
                }
            }

            if (file1Done && file2Done)
                break;
//            System.out.println("before merge");
//            System.out.println(token1);
//            System.out.println(token2);
            if (token1.compareTo(token2) == 0) {
                sb.append(token1);
                ind1 = file1Params[3];
                ind2 = file2Params[3];
//                System.out.println("in merge");
//                System.out.println("took same: " + token1);
//                System.out.println(token2);
                mergedPostingsPtrs.putLong(pptr);
                pptr += this.__writeMergedPlToBuffer(invertedIndexPath1, numTokens1, ind1, postingsPtrs1,
                        invertedIndexPath2, numTokens2, ind2, postingsPtrs2, resultFile, mergedInvertedIndex, offsetsDiff);
                mergedFrequencies.putInt(frequencies1[ind1] + frequencies2[ind2]);
                if (t % __k == 0) {
                    mergedTokensLengths.put((byte)token1.length());
                    mergedTermPtrs.putInt(termPtr);
                }
                else if (t % __k != __k - 1){
                    mergedTokensLengths.put((byte)token1.length());
                }
                termPtr += token1.length();
//                file1Params = this.__advanceToken(file1Params, termPtrs1, tokensLengths1, token1.length());
                this.__advanceToken(file1Params, termPtrs1, tokensLengths1, token1.length());
//                file2Params = this.__advanceToken(file2Params, termPtrs2, tokensLengths2, token2.length());                file1Params = this.__advanceToken(file1Params, termPtrs1, tokensLengths1, token1.length());
                this.__advanceToken(file2Params, termPtrs2, tokensLengths2, token2.length());
            } else if (token1.compareTo(token2) < 0 || file2Done) {
                sb.append(token1);
//                System.out.println("took 1: " + token1);
                i = file1Params[3];
                mergedPostingsPtrs.putLong(pptr);
                pptr += this.__writeFirstPlToBuffer(invertedIndexPath1, numTokens1, i, postingsPtrs1, resultFile, mergedInvertedIndex);
                mergedFrequencies.putInt(frequencies1[i]);
                if (t % __k == 0) {
                    mergedTokensLengths.put((byte)token1.length());
                    mergedTermPtrs.putInt(termPtr);
                }
                else if (t % __k != __k - 1){
                    mergedTokensLengths.put((byte)token1.length());
                }
                termPtr += token1.length();
//                file1Params = this.__advanceToken(file1Params, termPtrs1, tokensLengths1, token1.length());
                this.__advanceToken(file1Params, termPtrs1, tokensLengths1, token1.length());
                t++;
                continue;
            } else if (token2.compareTo(token1) < 0 || file1Done){
                sb.append(token2);
//                System.out.println("took 2:" + token2);
                i = file2Params[3];
                //merge posting pointers
                mergedPostingsPtrs.putLong(pptr);
                pptr += this.__writeSecondPlToBuffer(invertedIndexPath2, numTokens2, i, postingsPtrs2, resultFile, mergedInvertedIndex, offsetsDiff);
                mergedFrequencies.putInt(frequencies2[i]);
                if (t % __k == 0) {
                    mergedTokensLengths.put((byte)token2.length());
                    mergedTermPtrs.putInt(termPtr);
                }
                else if (t % __k != __k - 1){
                    mergedTokensLengths.put((byte)token2.length());
                }
                termPtr += token2.length();
//                file2Params = this.__advanceToken(file2Params, termPtrs2, tokensLengths2, token2.length());
                this.__advanceToken(file2Params, termPtrs2, tokensLengths2, token2.length());

            }
            t++;
        }
        //both files are fully merged
        this.__writeMergedToFile(resultFile, sb, mergedFrequencies, mergedTermPtrs,
                mergedTokensLengths, mergedPostingsPtrs, mergedInvertedIndex);

        //delete inverted index
        invertedIndexPath1.toFile().delete();
        invertedIndexPath2.toFile().delete();

        //        Paths.get(dir1).toFile().delete();
        Paths.get(dir2).toFile().delete();
    }



    /**
     * checks if finished all tokens in lexicon
     * @param params file parameters
     * @return true if finished reading all tokens from lexicon, false otherwise
     */
    private boolean __fileFinished(int[] params) {
        if (params[0] == -1)
            return true;
        return false;
    }



    private int __writeFirstPlToBuffer(Path invertedIndexPath, int numTokens, int i, long[] postingsPtrs, String resultFile,
                                       ByteBuffer mergedInvertedIndex) {

        int bytesWritten = 0;
        //no need to decompress pl and add offset
        byte[] pl;
        try {
            pl = ReadWriteArraysUtils.readCompressedPostingList(invertedIndexPath, numTokens, i, postingsPtrs);
//                        __printCompressed1(pl);
//                        System.out.println("singleA");
            try {
                mergedInvertedIndex.put(pl);
            } catch (java.nio.BufferOverflowException ex) {
                //if no memory left, write invertedIndex to file and refilling the buffer
                this.__writeInvertedIndexToFile(resultFile, mergedInvertedIndex);
                mergedInvertedIndex.put(pl);
            }
            bytesWritten += pl.length;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return bytesWritten;
    }

    private int __writeSecondPlToBuffer(Path invertedIndexPath, int numTokens, int i, long[] postingsPtrs, String resultFile,
                                        ByteBuffer mergedInvertedIndex, int offset) {
        int bytesWritten = 0;
        try {
            ArrayList<Integer> pl = ReadWriteArraysUtils.readPlWithOffset(invertedIndexPath, numTokens, i, postingsPtrs, offset);
            ArrayList<byte[]> compressedPl = this.__groupVarintEncoding(pl);
//                        __printCompressed(compressedPl);
//                        System.out.println("singleB");

            for (byte[] b : compressedPl) {
                try {
                    mergedInvertedIndex.put(b);
                } catch (java.nio.BufferOverflowException ex) {
                    //if no memory left, write invertedIndex to file and refilling the buffer
                    this.__writeInvertedIndexToFile(resultFile, mergedInvertedIndex);
                    mergedInvertedIndex.put(b);
                }
                bytesWritten += b.length;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return bytesWritten;
    }


    /**
     * writes extracts single posting list and writes to buffer. According to iteration will or will not decompress and add offset
     * @param invertedIndexPath path to invertedIndex
     * @param numTokens number of all tokens
     * @param i index of current token
     * @param postingsPtrs posting pointers
     * @param resultFile file to which to write buffer if it overfills
     * @param mergedInvertedIndex buffer with invertedIndex of currently merged tokens
     * @param firstIteration According to iteration number will or will not decompress and add offset (if true- it will)
     * @return number of bytes written to buffer (in order to update pptr)
     */


    /**
     * merges to posting lists and writes to buffer
     * @param invertedIndexPath1 path to invertedIndex for first lexicon
     * @param numTokens1 number of all tokens of first lexicon
     * @param ind1 current index of first lexicon
     * @param postingsPtrs1 posting pointers of first lexicon
     * @param invertedIndexPath2 path to invertedIndex for secong lexicon
     * @param numTokens2 number of all tokens of second lexicon
     * @param ind2 current index of second lexicon
     * @param postingsPtrs2 posting pointers of second lexicon
     * @param resultFile file to which to write buffer if it overfills
     * @param mergedInvertedIndex buffer with invertedIndex of currently merged tokens
     * @param firstIteration According to iteration number will or will not decompress and add offset (if true- it will)
     * @return number of bytes written to buffer (in order to update pptr)
     */
    private int __writeMergedPlToBuffer(Path invertedIndexPath1, int numTokens1,int ind1,long[] postingsPtrs1,
                                        Path invertedIndexPath2, int numTokens2,int ind2,long[] postingsPtrs2, String resultFile,
                                        ByteBuffer mergedInvertedIndex, int offsetDiff) {
        int bytesWritten = 0;
        ArrayList<Integer> pl1,pl2;
        try {
            pl1 = ReadWriteArraysUtils.readPlWithOffset(invertedIndexPath1, numTokens1, ind1, postingsPtrs1, 0);
            pl2 = ReadWriteArraysUtils.readPlWithOffset(invertedIndexPath2, numTokens2, ind2, postingsPtrs2, offsetDiff);

            // compute largest reviewId in pl1, and continue pl2 from that id:
            int largestRevId = 0;
            for (int i = 0; i < pl1.size(); i += 2)
            {
                largestRevId += pl1.get(i);
            }
            pl2.set(0, pl2.get(0)- largestRevId);

            // merge:
            ArrayList<Integer> mergedPl = new ArrayList<>();
            mergedPl.addAll(pl1);
            mergedPl.addAll(pl2);

            ArrayList<byte[]> compressedPl = this.__groupVarintEncoding(mergedPl);
//            __printCompressed(compressedPl);
//            System.out.println("mergedBoth");
            for (byte[] b : compressedPl) {
                try {
                    mergedInvertedIndex.put(b);
                } catch (java.nio.BufferOverflowException ex) {
                    //if no memory left, write invertedIndex to file and refilling the buffer
                    this.__writeInvertedIndexToFile(resultFile, mergedInvertedIndex);
                    mergedInvertedIndex.put(b);
                }
                bytesWritten += b.length;
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return bytesWritten;
    }


    /**
     * advances lexicon parameters to point to next token
     * @param params lexicon parameters (current starting point,current token length, recent term ptr, current token)
     * @param termPtrs array of termPtrs of lexicon
     * @param tokenLengths all token lengths of lexicon
     * @param tokenLength length of last token read
     * @return updated lexicon parameters to point to next token
     */
    private void __advanceToken(int[] params, int[] termPtrs, byte[] tokensLengths, int tokenLength) { //TODO: change params in array instead of creating a new one
//        int start = params[0], l = params[1], b = params[2], i = params[3];
        //calculate starting point for next token and update parameters
        params[3] += 1;
        if (params[3] % __k != 0 || params[3] < __k ) {
            params[0] = params[0] + tokenLength;
            if (params[3] % __k != (__k - 1)) {
                params[1] += 1;
            } else {
                params[2]++;
            }
        } else {
            if (params[2] == termPtrs.length)
                params[0] = params[0] + tokenLength;
            else{
                params[0] = termPtrs[params[2]];
            }
            params[1] += 1;
        }
    }

//    private int[] __advanceToken(int[] params, int[] termPtrs, byte[] tokensLengths, int tokenLength) { //TODO: change params in array instead of creating a new one
//        int start = params[0], l = params[1], b = params[2], i = params[3];
//        //calculate starting point for next token and update parameters
//        i += 1;
//        if (i % __k != 0 || i < __k ) {
//            start = start + tokenLength;
//            if (i % __k != (__k - 1)) {
//                l += 1;
//            } else {
//                b++;
//            }
//        } else {
//            if (b == termPtrs.length)
//                start = start + tokenLength;
//            else{
//                start = termPtrs[b];
//            }
//            l += 1;
//        }
//        return new int[]{start, l, b, i};
//    }

    /**
     * read the token from the lexicon
     * @param params lexicon parameters (current starting point,current token length, recent term ptr, current token)
     * @param termPtrs array of termPtrs of lexicon
     * @param tokenLengths all token lengths of lexicon
     * @param allTokens all tokens in lexicon
     * @return the current token
     */
    private String getToken(int[] params, int[] termPtrs,byte[] tokenLengths, CompactCharSequence allTokens) {
        int start = params[0], l = params[1], b = params[2],i = params[3];
        int end;
        if (i % __k != (__k - 1)) {
            if (l == tokenLengths.length)
                end = allTokens.length();
            else{
                end = start + tokenLengths[l];
            }
        } else {
            if (b == termPtrs.length) {
                //end case- the last word of the block is at the end of the file
                end = allTokens.length();
            } else {
                end = termPtrs[b];
            }
        }
        return allTokens.subSequence(start, end).toString();
    }

    /**
     * writes buffer of currently merged posting lists
     * @param resultFile file to write merged pl to
     * @param mergedInvertedIndex bytebuffer holding currently merged postingLists (already compressed)
     */
    private void __writeInvertedIndexToFile(String resultFile, ByteBuffer mergedInvertedIndex) {
        mergedInvertedIndex.flip();
        FileChannel out;
        try {
            Paths.get(resultFile, "invertedIndexnew.txt").toFile().createNewFile();
            out = FileChannel.open(Paths.get(resultFile, "invertedIndexnew.txt"),
                    StandardOpenOption.APPEND);
            out.write(mergedInvertedIndex);
            mergedInvertedIndex.rewind();
            int g = mergedInvertedIndex.position();
            out.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    /**
     * writes all buffers containing data from both lexicons, merged, to file
     * @param resultFile file to write merged lexicon to
     * @param sb string builder holding all tokens in merged lexicon
     * @param mergedFrequencies bytebuffer holding all frequencies in merged lexicon
     * @param mergedTermPtrs bytebuffer holding all termPtrs in merged lexicon
     * @param mergedTokensLengths bytebuffer holding all tokenLengths in merged lexicon
     * @param mergedPostingsPtrs bytebuffer holding all postingPtrs in merged lexicon
     * @param mergedInvertedIndex bytebuffer holding InvertedIndex of merged lexicon
     */
    private void __writeMergedToFile(String resultFile, StringBuilder sb, ByteBuffer mergedFrequencies,
                                     ByteBuffer mergedTermPtrs,ByteBuffer mergedTokensLengths,ByteBuffer mergedPostingsPtrs,ByteBuffer mergedInvertedIndex) {
        mergedFrequencies.flip();
        mergedTermPtrs.flip();
        mergedTokensLengths.flip();
        mergedPostingsPtrs.flip();
        mergedInvertedIndex.flip();
        sb.toString();
        FileChannel out;
        try {
            Paths.get(resultFile, "Frequencies.txt").toFile().createNewFile();
            out = FileChannel.open((Paths.get(resultFile, "Frequencies.txt")),
                    StandardOpenOption.APPEND);
            out.write(mergedFrequencies);
            out.close();

            Paths.get(resultFile, "TokenLength.txt").toFile().createNewFile();
            out = FileChannel.open(Paths.get(resultFile, "TokenLength.txt"),
                    StandardOpenOption.APPEND);
            out.write(mergedTokensLengths);
            out.close();

            Paths.get(resultFile, "TermPtr.txt").toFile().createNewFile();
            out = FileChannel.open(Paths.get(resultFile, "TermPtr.txt"),
                    StandardOpenOption.APPEND);
            out.write(mergedTermPtrs);
            out.close();

            Paths.get(resultFile, "long_string.txt").toFile().createNewFile();
            File longString = Paths.get(resultFile, "long_string.txt").toFile();
            ReadWriteArraysUtils.writeAllTokens(longString, sb.toString());

            Paths.get(resultFile, "PostingPtrs.txt").toFile().createNewFile();
            out = FileChannel.open(Paths.get(resultFile, "PostingPtrs.txt"),
                    StandardOpenOption.APPEND);
            out.write(mergedPostingsPtrs);
            out.close();

            Paths.get(resultFile, "invertedIndex.txt").toFile().createNewFile();
            out = FileChannel.open(Paths.get(resultFile, "invertedIndex.txt"),
                    StandardOpenOption.APPEND);
            out.write(mergedInvertedIndex);
            out.close();


        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * deletes a lexicon folder
     * @param file lexicon folder to delete
     */
    private void __deleteLexicon(String file) {
        Path freqFilePath = Paths.get(file, "Frequencies.txt");
        freqFilePath.toFile().delete();
        Path postingPtrFilePath = Paths.get(file, "PostingPtrs.txt");
        postingPtrFilePath.toFile().delete();
        Path TokenLengthFilePath = Paths.get(file, "TokenLength.txt");
        TokenLengthFilePath.toFile().delete();
        Path TermPtrFilePath = Paths.get(file, "TermPtr.txt");
        TermPtrFilePath.toFile().delete();
        Path LongStringPath = Paths.get(file, "long_string.txt");
        LongStringPath.toFile().delete();
        Paths.get(file).toFile().delete();

    }

    /**
     * print the __postings.
     */
    public void __printProcessedText(HashMap<String, Integer> pt){
        pt.forEach((idx, val) ->
                System.out.printf("{%s: %s},",
                        idx, pt.get(idx)));
        System.out.println();
    }

    public void __printCompressed(ArrayList<byte[]> compressed) {
        for (byte[] b_l : compressed) {
            for (byte b : b_l) {
                System.out.print(b);
            }
        }
        System.out.println();
    }

    public void __printCompressed1(byte[] pl){
        for (byte b: pl) {
            System.out.print(b);
        }
        System.out.println();
    }

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