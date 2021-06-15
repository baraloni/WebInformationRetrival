package webdata;

import webdata.GroupVarint;
import webdata.IndexWriter;
import webdata.ReadWriteArraysUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

    public HashMap<String, ArrayList<Integer>> postingsDebug = new HashMap<>();


    /**
     * constructs a webdata.SlowIndexWriter object that reads reviews data from inputFile,
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

//        __printPostings(postings);
        postingsDebug = postings;

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

            while ((line = br.readLine()) != null && reviewId < IndexWriter.MAX_REVS){
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
        ReadWriteArraysUtils.writeIntArray(file, freqs);
        //write postingPtrs to file
        file = postingPtrFilePath.toFile();
        ReadWriteArraysUtils.writeLongArray(file, postingPtrs);
        //write tokenLengths to file
        file = TokenLengthFilePath.toFile();
        ReadWriteArraysUtils.writeByteArray(file, length);
        //write termPtr to file
        file = TermPtrFilePath.toFile();
        ReadWriteArraysUtils.writeIntArray(file, termPtrs);
        //write long string to file
        file = LongStringFilePath.toFile();
        ReadWriteArraysUtils.writeAllTokens(file, allTokens);
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
                pos = ReadWriteArraysUtils.writePostingList(raf, postingList, result[i - 1]);
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
        ReadWriteArraysUtils.writeShortArray(file, helpNums);
        //write helpfulnessDenom to file
        file = helpfulnessDenFilePath.toFile();
        ReadWriteArraysUtils.writeShortArray(file, helpDenums);
        //write scores to file
        file = scoreFilePath.toFile();
        ReadWriteArraysUtils.writeByteArray(file, scores);
        //write lengths to file
        file = reviewLenFilePath.toFile();
        ReadWriteArraysUtils.writeIntArray(file, lengths);
        //write prodIds to file
        String[] ProductIds = productIds.stream().toArray(String[]::new);
        file = prodIdFilePath.toFile();
        ReadWriteArraysUtils.writeStringArray(file, ProductIds);
        //write productIdsFirstReview to file
        int[] productIdsFirstReviewArray = productIdsFirstReview.stream().mapToInt(m->m).toArray();
        file = firstReviewFilePath.toFile();
        ReadWriteArraysUtils.writeIntArray(file, productIdsFirstReviewArray);
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


//    //################## merge reviews ####################################
//
//    //not sure about the openOptions
//    public void __mergeReviews(String dir) {
//        String[] reviewFiles = new String[]{"HelpNum.txt", "HelpDenom.txt", "Scores.txt"
//                ,"ReviewLength.txt", "ProductId.txt"};
//        File file = new File(dir);
//        String[] directories = file.list(new FilenameFilter() {
//            @Override
//            public boolean accept(File current, String name) {
//                return new File(current, name).isDirectory();
//            }
//        });
//        Path outFile;
//        Path dst;
//        Path reviewsPath = Paths.get(dir, "Reviews");
//        reviewsPath.toFile().mkdir();
//        int[] subDirs = Arrays.stream(directories).mapToInt(Integer::parseInt).toArray();
//        Arrays.sort(subDirs);
//        for (String s : reviewFiles) {
//            outFile=Paths.get(dir, Integer.toString(subDirs[0]),"Reviews",s);
//            this.__appendFiles(dir, outFile,"Reviews", s, subDirs);
//            dst = Paths.get(reviewsPath.toString(), s);
//            //create final directory for Reviews with all merged files
//            try {
//                Files.copy(outFile, dst, StandardCopyOption.REPLACE_EXISTING);
//                outFile.toFile().delete();
//            } catch (IOException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//            //            if (s.equals("Scores.txt"))
//            //                System.out.println("Scores: " + Arrays.toString(this.__readByteArray(outFile)));
//            //            else if (s.equals("HelpNum.txt"))
//            //                System.out.println("HelpNum: " + Arrays.toString(this.__readShortArray(outFile)));
//            //            else if (s.equals("HelpDenom.txt"))
//            //                System.out.println("HelpDenom: " + Arrays.toString(this.__readShortArray(outFile)));
//            //            else if (s.equals("ReviewLength.txt"))
//            //                System.out.println("ReviewLength: " + Arrays.toString(this.__readIntArray(outFile)));
//            //            else if (s.equals("ProductId.txt"))
//            //                System.out.println("ProductId: " + Arrays.toString(this.__readProductId(outFile)));
//        }
//        //merge firstReviewFiles
//        outFile=Paths.get(dir, Integer.toString(subDirs[0]),"Reviews","PidFirstReview.txt");
//        this.__mergeFirstIdsFile(dir, outFile, subDirs);
//        //        System.out.println("First reviews: " + Arrays.toString(this.__readIntArray(outFile)));
//        try {
//            dst = Paths.get(reviewsPath.toString(), "PidFirstReview.txt");
//            Files.copy(outFile, dst, StandardCopyOption.REPLACE_EXISTING);
//            outFile.toFile().delete();
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        //delete all subdirectories
////        this.__deleteSubDirs(dir, subDirs);
//    }
//
//    /**
//     * merge a certain file (readFrom) from all directories (subDirs) by appending
//     * them all to the end of output file (outFile)
//     * @param dir directory where all data is
//     * @param outFile final merged file
//     * @param readFromDir direct folder containing readFrom file
//     * @param readFrom file name to read from
//     * @param subDirs all subdirectories to be appended (merged) to outFile
//     */
//    private void __appendFiles(String dir, Path outFile, String readFromDir, String readFrom, int[] subDirs) {
//        int len = subDirs.length;
//        try(FileChannel out=FileChannel.open(outFile, StandardOpenOption.APPEND, StandardOpenOption.SYNC)) {
//            for(int ix=1; ix<len; ix++) {
//                Path inFile=Paths.get(dir, Integer.toString(subDirs[ix]),readFromDir,readFrom);
//                try(FileChannel in=FileChannel.open(inFile, StandardOpenOption.READ)) {
//                    for(long p=0, l=in.size(); p<l; )
//                        p+=in.transferTo(p, l-p, out);
//                    inFile.toFile().delete();
//                } catch (IOException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//        } catch (IOException e1) {
//            // TODO Auto-generated catch block
//            e1.printStackTrace();
//        }
//    }
//
//    private void __mergeFirstIdsFile(String dir, Path outFile, int[] subDirs) {
//        int len = subDirs.length;
//        int[] ids;
//        int offset;
//
//        for(int ix=1; ix<len; ix++) {
//            try(FileChannel out=FileChannel.open(outFile, StandardOpenOption.APPEND, StandardOpenOption.SYNC)) {
//                offset = subDirs[ix];
//                Path inFile = Paths.get(dir,Integer.toString(subDirs[ix]), "Reviews","PidFirstReview.txt");
//                ids = ReadWriteArraysUtils.readIntArray(inFile);
//                int idsLen = ids.length;
//                for (int j = 0;j < idsLen; j++)
//                    ids[j] += offset;
//                ByteBuffer buf = ByteBuffer.allocate(4 * ids.length);
//                for (int i : ids) {
//                    buf.putInt(i);
//                }
//                buf.flip();
//                out.write(buf);
//                if (out != null)
//                    out.close();
//                inFile.toFile().delete();
//
//            } catch (IOException e1) {
//                // TODO Auto-generated catch block
//                e1.printStackTrace();
//            }
//        }
//    }
//
//    /**
//     * delete all partial subdirectories after merging to first one
//     * @param dir outer directory
//     * @param subDirs all subdirectories to erase (except the first which is the merged one)
//     */
//    private void __deleteSubDirs(String dir, int[] subDirs) {
//        for (int i = 0; i< subDirs.length;i++) {
//            Path p = Paths.get(dir,Integer.toString(subDirs[i]),"Reviews");
//            if (!p.toFile().delete()) {
//                System.out.println(p.toString());
//            }
//            p = Paths.get(dir,Integer.toString(subDirs[i]));
//            p.toFile().delete();
//        }
//    }
//
//    //################## merge Lexicon ####################################
//    /**
//     * Updates merged_pl to hold the 2 merged posting lists given.
//     * Returns the actual number of elements in it.
//     * @param pl1 long arraylist holding a posting list
//     * @param pl2 long arraylist holding a posting list
//     * @param merged_pl: initialized arraylist of size (pl1.size()+pl2.size())
//     */
//    public void __mergePostingLists(final ArrayList<Integer> pl1, final ArrayList<Integer> pl2,  ArrayList<Integer> merged_pl)
//    {
//        ListIterator<Integer> pl1_iter = pl1.listIterator();
//        ListIterator<Integer> pl2_iter = pl2.listIterator();
//
//        Integer i1 = pl1_iter.next();
//        Integer i2 = pl2_iter.next();
//
//        //merge until one of the posting lists ends:
//        while (pl1_iter.hasNext() &  pl2_iter.hasNext()) {
//
//            if (i1 < i2) {
//                merged_pl.add(i1);
//                merged_pl.add(pl1_iter.next());
//                if (pl1_iter.hasNext())
//                {
//                    i1 = pl1_iter.next();
//                    continue;
//                }
//                break;
//            } else if (i2 < i1) {
//                merged_pl.add(i2);
//                merged_pl.add(pl2_iter.next());
//                if (pl2_iter.hasNext())
//                {
//                    i2 = pl2_iter.next();
//                    continue;
//                }
//                break;
//            } else {
//                merged_pl.add(i1);
//                merged_pl.add(pl1_iter.next() + pl2_iter.next());
//                if (pl1_iter.hasNext() & pl2_iter.hasNext())
//                {
//                    i1 = pl1_iter.next();
//                    i2 = pl2_iter.next();
//                    continue;
//                }
//                break;
//            }
//        }
//
//        if (pl1_iter.hasNext() | pl2_iter.hasNext()) // one of the lists weren't fully merged
//        {
//            // copy the remaining posting list that wasn't fully merged:
//            ListIterator<Integer> pl_iter = (pl1_iter.hasNext())? pl1_iter: pl2_iter;
//            Integer i = (pl1_iter.hasNext())? i1: i2;
//            merged_pl.add(i);
//            while (pl_iter.hasNext())
//            {
//                merged_pl.add(pl_iter.next());
//            }
//        }
//    }
//
//    /**
//     * recursively merges all lexicon files
//     * @param dir directory containing subdirectories holing lexicon chunks
//     * @return path to completely merged Lexicon files
//     */
//    public void __mergeAllLexicons(String dir) {
//        File file = new File(dir);
//        String[] directories = file.list(new FilenameFilter() {
//            @Override
//            public boolean accept(File current, String name) {
//                return new File(current, name).isDirectory();
//            }
//        });
//        Path mergedLexicon = Paths.get(dir, "Lexicon");
//        mergedLexicon.toFile().mkdir();
//        int[] subDirs = Arrays.stream(directories).mapToInt(Integer::parseInt).toArray();
//        ArrayList<Integer> newFiles = new ArrayList<Integer>();
//        //merge pairs until all files are merged to one
//        while (subDirs.length > 1) {
//            int len = subDirs.length;
//            Arrays.sort(subDirs);
//            for (int i = 0; i < len; i+= 2) {
//                if (i == len - 1) {
//                    //finished merging files in current round
//                    subDirs = Arrays.stream(newFiles.toArray()).mapToInt(o -> (int)o).toArray();
//                    newFiles.clear();
//                    continue;
//                }
//                Path file1 = Paths.get(dir, Integer.toString(subDirs[i]),"Lexicon");
//                Path file2 = Paths.get(dir, Integer.toString(subDirs[i + 1]),"Lexicon");
//                //store result in folder named by the value of file1 + 1
//                Path result = Paths.get(dir, Integer.toString(subDirs[i] + 1), "Lexicon");
//                newFiles.add(subDirs[i] + 1);
//                result.toFile().mkdir();
//                this.__compareLexicons(file1, file2, result);
//            }
//        }
//
//    }
//
//    private void __compareLexicons(Path file1, Path file2, Path results) {
//
//        //upload file1 lexicon
//        Path freqFilePath = Paths.get(file1.toString(), "Frequencies.txt");
//        Path postingPtrFilePath = Paths.get(file1.toString(), "PostingPtrs.txt");
//        Path TokenLengthFilePath = Paths.get(file1.toString(), "TokenLength.txt");
//        Path TermPtrFilePath = Paths.get(file1.toString(), "TermPtr.txt");
//        Path LongStringPath = Paths.get(file1.toString(), "long_string.txt");
//        Path invertedIndexPath1 = Paths.get(file1.toString(), "invertedIndex.txt");
//        int[] frequencies1 = ReadWriteArraysUtils.readIntArray(freqFilePath);
//        long[] postingsPtrs1 = ReadWriteArraysUtils.readLongArray(postingPtrFilePath);
//        byte[] tokensLengths1 = ReadWriteArraysUtils.readByteArray(TokenLengthFilePath);
//        int[] termPtrs1 = ReadWriteArraysUtils.readIntArray(TermPtrFilePath);
//        CompactCharSequence tokens1 = ReadWriteArraysUtils.readCompactCharSequence(LongStringPath);
//
//        //upload file2 lexicon
//        freqFilePath = Paths.get(file2.toString(), "Frequencies.txt");
//        postingPtrFilePath = Paths.get(file2.toString(), "PostingPtrs.txt");
//        TokenLengthFilePath = Paths.get(file2.toString(), "TokenLength.txt");
//        TermPtrFilePath = Paths.get(file2.toString(), "TermPtr.txt");
//        LongStringPath = Paths.get(file2.toString(), "long_string.txt");
//        Path invertedIndexPath2 = Paths.get(file2.toString(), "invertedIndex.txt");
//        int[] frequencies2 = ReadWriteArraysUtils.readIntArray(freqFilePath);
//        long[] postingsPtrs2 = ReadWriteArraysUtils.readLongArray(postingPtrFilePath);
//        byte[] tokensLengths2 = ReadWriteArraysUtils.readByteArray(TokenLengthFilePath);
//        int[] termPtrs2 = ReadWriteArraysUtils.readIntArray(TermPtrFilePath);
//        CompactCharSequence tokens2 = ReadWriteArraysUtils.readCompactCharSequence(LongStringPath);
//
//        //initialize result
//        //#######################
//
//        ByteBuffer mergedFrequencies = ByteBuffer.allocate(4 * (frequencies1.length + frequencies2.length));
//        ByteBuffer mergedPostingsPtrs = ByteBuffer.allocate(8 * (postingsPtrs1.length + postingsPtrs2.length));
//        ByteBuffer mergedTokensLengths = ByteBuffer.allocate(tokensLengths1.length + tokensLengths2.length);
//        ByteBuffer mergedTermPtrs = ByteBuffer.allocate( 4 * (termPtrs1.length + termPtrs2.length));
//        ByteBuffer mergedInvertedIndex = ByteBuffer.allocate( (int) (invertedIndexPath1.toFile().length()
//                + invertedIndexPath2.toFile().length()));
//
//        //###########################
//        StringBuilder sb = new StringBuilder();
//        String resultFile = results.toString();
//        int t = 0,i, termPtr = 0, ind1, ind2;
//        int numTokens1 = frequencies1.length, numTokens2 = frequencies2.length;
//        boolean file1Done = false, file2Done = false;
//        String token1 = "", token2 = "";
//        int[] mergedPlArr;
//        long pptr = 0;
//        ArrayList<byte[]> compressedPl;
//        ArrayList<Integer> mergedPl,pl1, pl2;
//        int start1 = 0, offset1 = tokensLengths1[0], l1 = 0, b1 = 0, i1 = 0;
//        int start2= 0, offset2 = tokensLengths2[0], l2 = 0, b2 = 0, i2 = 0;
//        int[] file1Params = new int[] {start1, offset1,l1, b1, i1};
//        int[] file2Params = new int[] {start2, offset2,l2, b2, i2};
//        while (!(file1Done && file2Done)) {
//            if (this.__memoryFinished()) {
//                this.__writeMergedToFile(resultFile, sb, mergedFrequencies, mergedTermPtrs,
//                        mergedTokensLengths);
//            }
//            if (this.__fileFinished(file1Params)) {
//                file1Done = true;
//                token1 = "zzz";
//                __deleteLexicon(file1);
//            }
//            if (this.__fileFinished(file2Params)) {
//                file2Done = true;
//                token2 = "zzzz";
//                __deleteLexicon(file2);
//            }
//            if (!file1Done && !file2Done) {
//                token1 = getToken(file1Params, termPtrs1, tokensLengths1, tokens1);
//                token2 = getToken(file2Params, termPtrs2, tokensLengths2, tokens2);
//            }
//            if (token1.compareTo(token2) == 0) {
//                ind1 = file1Params[4];
//                ind2 = file2Params[4];
//                try {
//                    mergedPostingsPtrs.putLong(pptr);
//                    pl1 = ReadWriteArraysUtils.readFullPostingList(invertedIndexPath1, numTokens1, ind1, postingsPtrs1);
//                    pl2 = ReadWriteArraysUtils.readFullPostingList(invertedIndexPath2, numTokens2, ind2, postingsPtrs2);
//
//                    mergedPl = new ArrayList<>();
//                    __mergePostingLists(pl1, pl2, mergedPl);
//
//                    compressedPl = this.__groupVarintEncoding(mergedPl);
//                    for (byte[] b : compressedPl) {
//                        mergedInvertedIndex.put(b);
//                        pptr += b.length;
//                    }
//                } catch (IOException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//
//                mergedFrequencies.putInt(frequencies1[ind1] + frequencies2[ind2]);
//                if (t % __k == 0) {
//                    mergedTokensLengths.put((byte)token1.length());
//                    mergedTermPtrs.putInt(termPtr);
//                }
//                else if (t % __k != __k - 1){
//                    mergedTokensLengths.put((byte)token1.length());
//                }
//                termPtr += token1.length();
//                file1Params = this.__advanceToken(file1Params, termPtrs1, tokensLengths1);
//                file2Params = this.__advanceToken(file2Params, termPtrs2, tokensLengths2);
//            } else if (token1.compareTo(token2) < 0 || file2Done) {
//                sb.append(token1);
//                i = file1Params[4];
//                mergedPostingsPtrs.putLong(pptr);
//                try {
//                    pl1 = ReadWriteArraysUtils.readPostingList(invertedIndexPath1, numTokens1, i, postingsPtrs1, false);
//                    compressedPl = this.__groupVarintEncoding(pl1);
//                    for (byte[] b : compressedPl) {
//                        mergedInvertedIndex.put(b);
//                        pptr += b.length;
//                    }
//                } catch (IOException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//                mergedFrequencies.putInt(frequencies1[i]);
//                if (t % __k == 0) {
//                    mergedTokensLengths.put((byte)token1.length());
//                    mergedTermPtrs.putInt(termPtr);
//                }
//                else if (t % __k != __k - 1){
//                    mergedTokensLengths.put((byte)token1.length());
//                }
//                termPtr += token1.length();
//                file1Params = this.__advanceToken(file1Params, termPtrs1, tokensLengths1);
//            } else if (token2.compareTo(token1) < 0 || file1Done){
//                sb.append(token2);
//                i = file2Params[4];
//                //merge posting pointers
//                mergedPostingsPtrs.putLong(pptr);
//                try {
//                    pl2 = ReadWriteArraysUtils.readPostingList(invertedIndexPath2, numTokens2, i, postingsPtrs2, false);
//                    compressedPl = this.__groupVarintEncoding(pl2);
//                    for (byte[] b : compressedPl) {
//                        mergedInvertedIndex.put(b);
//                        pptr += b.length;
//                    }
//                } catch (IOException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//                mergedFrequencies.putInt(frequencies2[i]);
//                if (t % __k == 0) {
//                    mergedTokensLengths.put((byte)token2.length());
//                    mergedTermPtrs.putInt(termPtr);
//                }
//                else if (t % __k != __k - 1){
//                    mergedTokensLengths.put((byte)token2.length());
//                }
//                termPtr += token2.length();
//                file2Params = this.__advanceToken(file2Params, termPtrs2, tokensLengths2);
//            }
//            t++;
//        }
//        //both files are fully merged
//        this.__writeMergedToFile(resultFile, sb, mergedFrequencies, mergedTermPtrs, mergedTokensLengths);
//        __deleteLexicon(file1);
//        __deleteLexicon(file2);
//        return;
//    }
//
//    private boolean __memoryFinished() {
//        //TODO
//        return true;
//    }
//
//    private boolean __fileFinished(int[] params) {
//        if (params[0] == -1)
//            return true;
//        return false;
//    }
//
//    private String getToken(int[] params, int[] termPtrs,byte[] tokenLengths, CompactCharSequence allTokens) {
//        int start = params[0], l = params[2], b = params[3],i = params[4];
//        int end;
//        if (i % __k != (__k - 1)) {
//            end = start + tokenLengths[l];
//        } else {
//            if (b == termPtrs.length) {
//                //end case- the last word of the block is at the end of the file
//            }
//            end = termPtrs[b];
//        }
//        return allTokens.subSequence(start, end).toString();
//    }
//
//    public long[] getPostingList(Path file, int index, long[] postingPtrs) {
//
//
//
//
//        return null;
//    }
//
//    private int[] __advanceToken(int[] params, int[] termPtrs, byte[] tokensLengths) {
//        int start = params[0], offset = params[1], l = params[2], b = params[3], i = params[4];
//        //calculate starting point for next token and update parameters
//        i += 1;
//        if (b == termPtrs.length) {
//            //reached end of lexicon
//            return new int[]{-1};
//
//        }
//        if (i % __k == 0) {
//            start = termPtrs[b];
//            offset = tokensLengths[l];
//            l += 1;
//        } else {
//            start = termPtrs[b] + offset;
//            if (i % __k != (__k - 1)) {
//                offset += tokensLengths[l];
//                l += 1;
//            } else {
//                b++;
//            }
//        }
//        return new int[] {start, offset, l, b, i};
//    }
//
//
//    private void __writeMergedToFile(String resultFile, StringBuilder sb,ByteBuffer mergedFrequencies,ByteBuffer mergedTermPtrs,ByteBuffer mergedTokensLengths) {
//        mergedFrequencies.flip();
//        mergedTermPtrs.flip();
//        mergedTokensLengths.flip();
//        sb.toString();
//        FileChannel out;
//        try {
//            out = FileChannel.open(Paths.get(resultFile, "Frequencies.txt"),
//                    StandardOpenOption.APPEND);
//            out.write(mergedFrequencies);
//            mergedFrequencies.rewind();
//
//            out = FileChannel.open(Paths.get(resultFile, "TokenLength.txt"),
//                    StandardOpenOption.APPEND);
//            out.write(mergedTokensLengths);
//            mergedTokensLengths.rewind();
//
//            out = FileChannel.open(Paths.get(resultFile, "TermPtr.txt"),
//                    StandardOpenOption.APPEND);
//            out.write(mergedTermPtrs);
//            mergedTermPtrs.rewind();
//
//            File longString = Paths.get(resultFile, "long_string.txt").toFile();
//            ReadWriteArraysUtils.writeAllTokens(longString, sb.toString());
//            sb = new StringBuilder();
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//
//    }
//
//    private void __deleteLexicon(Path file) {
//        Path freqFilePath = Paths.get(file.toString(), "Frequencies.txt");
//        freqFilePath.toFile().delete();
//        Path postingPtrFilePath = Paths.get(file.toString(), "PostingPtrs.txt");
//        postingPtrFilePath.toFile().delete();
//        Path TokenLengthFilePath = Paths.get(file.toString(), "TokenLength.txt");
//        TokenLengthFilePath.toFile().delete();
//        Path TermPtrFilePath = Paths.get(file.toString(), "TermPtr.txt");
//        TermPtrFilePath.toFile().delete();
//        Path LongStringPath = Paths.get(file.toString(), "long_string.txt");
//        LongStringPath.toFile().delete();
//        Path inveredIndexPath1 = Paths.get(file.toString(), "invertedIndex.txt");
//        inveredIndexPath1.toFile().delete();
//        file.toFile().delete();
//    }


// ########################### DEBUGGING ##########################

public void createTestFiles() {

    short[] nums = new short[]{3,3,3};
    short[] denoms = new short[]{4,4,4};
    byte[] scores = new byte[]{5,5,5};
    int[] lengths = new int[]{6,6,6};
    String[] ids = new String[]{"aaaaaaaaaa", "bbbbbbbbbb","cccccccccc"};
    int[] firstRev = new int[]{7,7,7};
    //        this.__writeShortArray(Paths.get("HelpNum.txt").toFile(), nums);
    //        this.__writeShortArray(Paths.get("HelpDenom.txt").toFile(), denoms);
    //        this.__writeByteArray(Paths.get("Scores.txt").toFile(), scores);
    //        this.__writeIntArray(Paths.get("ReviewLength.txt").toFile(), lengths);
    //        this.__writeIntArray(Paths.get("PidFirstReview.txt").toFile(), firstRev);
    ReadWriteArraysUtils.writeStringArray(Paths.get("ProductId.txt").toFile(), ids);
}

public void createLexiconFiles() {

}
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

