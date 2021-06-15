package webdata;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;

public final class ReadWriteArraysUtils {


    private static final String ENCODING = "ISO-8859-1";

    private ReadWriteArraysUtils(){}

    // ########################### WRITING TO FILE ##########################


    /**
     * change: finally blocks, flush in comment
     * writes an array of ints to file
     * @param file file to write to
     * @param array to write
     */
    public static void writeIntArray(File file, int[] array) {
        FileOutputStream fos = null;
        DataOutputStream dos = null;

        try {
            file.createNewFile();
            fos = new FileOutputStream(file);
            dos = new DataOutputStream(new BufferedOutputStream(fos));
            for (int i : array) {
                dos.writeInt(i);
            }
//            dos.close();
//            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                dos.close();
                fos.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * writes an array of shorts to file
     * @param file file to write to
     * @param array to write
     */
    public static void writeShortArray(File file, short[] array) {
        FileOutputStream fos = null;
        DataOutputStream dos = null;
        try {

            file.createNewFile();
            fos = new FileOutputStream(file);
            dos = new DataOutputStream(new BufferedOutputStream(fos));
            for (short s : array) {
                dos.writeShort(s);
            }
//            dos.flush();
//            dos.close();
//            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                dos.close();
                fos.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * writes an array of bytes to file
     * @param file file to write to
     * @param array to write
     */
    public static void writeByteArray(File file, byte[] array) {
        FileOutputStream fos = null;
        try {
            file.createNewFile();
            fos = new FileOutputStream(file);
            fos.write(array);
//            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * writes an array of strings to file
     * @param file file to write to
     * @param array to write
     */
    public static void writeStringArray(File file, String[] array) {
        FileOutputStream fos = null;
        try {
            file.createNewFile();
            fos = new FileOutputStream(file);
            for (String s : array) {
                fos.write(s.getBytes(ENCODING));
            }
//            fos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            try {
                fos.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * writes an array of longs to file
     * @param file file to write to
     * @param array to write
     */
    public static void writeLongArray(File file, long[] array) {
        FileOutputStream fos = null;
        DataOutputStream dos = null;
        try {
            file.createNewFile();
            fos = new FileOutputStream(file);
            dos = new DataOutputStream(new BufferedOutputStream(fos));
            for (long l : array) {
                dos.writeLong(l);
            }
//            dos.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                dos.close();
                fos.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * writes very long string to file encoded so each character takes only one byte
     * @param file file to write to
     * @param longString to write
     */
    public static void writeAllTokens(File file, String longString) {
        FileOutputStream fos = null;
        try {
            file.createNewFile();
            fos = new FileOutputStream(file);
            fos.write(longString.getBytes(ENCODING));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * write posting list to file and return cursor position where next one will be written
     * @param raf randomAccessFile
     * @param postingList posting list
     * @param pos
     * @return cursor position where next posting list will be written
     */
    public static long writePostingList(RandomAccessFile raf, ArrayList<Integer> postingList, long pos) {
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

    //################## reading index #####################################

    public static String[] readProductId(Path path)
    {
        ArrayList<String> strings = new ArrayList<String>();
        byte[] id = new byte[10];
        int i = 0;
        try {
            byte[] bytes = java.nio.file.Files.readAllBytes(path);
            for (byte b : bytes) {
                id[i++] = b;
                if (i == 10) {
                    strings.add(new String(id, ENCODING));
                    i = 0;
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        String[] ret = new String[strings.size()];
        for (i =0; i < strings.size(); i++)
            ret[i] = strings.get(i);
        return ret;
    }

    public static CompactCharSequence readCompactCharSequence(Path filepath) {
        try {
            byte[] bytes = java.nio.file.Files.readAllBytes(filepath);
            String str = new String(bytes, ENCODING);
            return new CompactCharSequence(str);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return new CompactCharSequence("");
    }

    public static byte[] readByteArray(Path filepath) {
        try {
            return java.nio.file.Files.readAllBytes(filepath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public static short[] readShortArray(Path filepath) {
        InputStream is = null;
        DataInputStream dis = null;
        short k;
        ArrayList<Short> shorts = new ArrayList<Short>();
        try {
            is = new FileInputStream(filepath.toString());
            dis = new DataInputStream(is);
            while(dis.available()>0) {
                k = dis.readShort();
                shorts.add(k);
            }
            // releases all system resources from the streams
            if(is!=null)
                is.close();
            if(dis!=null)
                dis.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        short[] ret = new short[shorts.size()];
        for (int i =0; i < shorts.size(); i++)
            ret[i] = shorts.get(i);
        return ret;
    }

    public static int[] readIntArray(Path filepath) {

        InputStream is = null;
        DataInputStream dis = null;
        int k;
        ArrayList<Integer> ints = new ArrayList<Integer>();
        try {
            is = new FileInputStream(filepath.toString());
            dis = new DataInputStream(is);
            while(dis.available()>0) {
                k = dis.readInt();
                ints.add(k);
            }
            // releases all system resources from the streams
            if(is!=null)
                is.close();
            if(dis!=null)
                dis.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        int[] ret = new int[ints.size()];
        for (int i =0; i < ints.size(); i++)
            ret[i] = ints.get(i);
        return ret;
    }

    public static long[] readLongArray(Path filepath) {
        InputStream is = null;
        DataInputStream dis = null;
        Long k;
        ArrayList<Long> longs = new ArrayList<Long>();
        try {
            is = new FileInputStream(filepath.toString());
            dis = new DataInputStream(is);
            while(dis.available()>0) {
                k = dis.readLong();
                longs.add(k);
            }
            dis.close();
            is.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        long[] ret = new long[longs.size()];
        for (int i =0; i < longs.size(); i++)
            ret[i] = longs.get(i);
        return ret;
    }

    public static ArrayList<Integer> readFullPostingList(Path invertedIndexPath,int numOfTokens, int idx, long[] postingsPtrs) throws IOException {
        byte[] buffer = ReadWriteArraysUtils.readCompressedPostingList(invertedIndexPath, numOfTokens, idx, postingsPtrs);
        // decompress the differences posting list:
        ArrayList<Integer> decompressed_diff = GroupVarint.decompress(buffer);
        // convert the differences posting list into full posting
        for (int i = 2 ; i < decompressed_diff.size(); i += 2 )
        {
            decompressed_diff.set(i, decompressed_diff.get(i) +
                decompressed_diff.get(i - 2));
        }
        return decompressed_diff;

    }

    public static ArrayList<Integer> readPlWithOffset(Path invertedIndexPath,int numOfTokens, int idx,
            long[] postingsPtrs, int offset) throws IOException {
        byte[] buffer = ReadWriteArraysUtils.readCompressedPostingList(invertedIndexPath, numOfTokens, idx, postingsPtrs);
//        __printCompressed1(buffer);

        // decompress the differences posting list:
//        System.out.println("index: " + idx);

        ArrayList<Integer> decompressed_diff = GroupVarint.decompress(buffer);

//        if (decompressed_diff.size() == 0)
//            System.out.println("index: " + idx);
        decompressed_diff.set(0, decompressed_diff.get(0) + offset);
        return decompressed_diff;
    }


    /**
     * @param idx the idx of the term whose posting list we want to get
     * @return the compressed posting list of the term (indexed int the lexicon at idx)
     * @throws IOException
     */
    public static byte[] readCompressedPostingList(Path invertedIndexPath,int numOfTokens, int idx, long[] postingsPtrs) throws IOException
    {

        // read from file the bytes holding the compressed posting list:
        File inputFile = invertedIndexPath.toFile();
        long p_start = postingsPtrs[idx];
        long p_end = (idx < numOfTokens - 1) ? postingsPtrs[idx + 1] : inputFile.length();
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
//        System.out.println(Arrays.toString(buffer));
        return buffer;
    }

    public static void __printCompressed(ArrayList<byte[]> compressed) {
        for (byte[] b_l : compressed) {
            for (byte b : b_l) {
                System.out.print(b);
            }
        }
        System.out.println();
    }
    public static void __printCompressed1(byte[] pl){
        for (byte b: pl) {
            System.out.print(b);
        }
        System.out.println();
    }
}

