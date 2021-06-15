package webdata;
import java.math.BigInteger;
import java.util.ArrayList;

/**
 * This class is in charge of compressing integers to be stored efficiently
 */
public class GroupVarint {

    static byte[] num_of_bytes = new byte[]{4, 4, 4, 4, 4, 4, 4, 4,
                                     3, 3, 3, 3, 3, 3, 3, 3,
                                     2, 2, 2, 2, 2, 2, 2 ,2,
                                     1, 1, 1, 1, 1, 1, 1, 1, 1};
    private final static int[] masks = {0xFF, 0x0F};

    /**
     * compresses a list of integers to bytes, using GroupVarInt encoding
     * @param toCompress list of integers to compress
     * @return input encoded and compressed into byte array
     */
    public static ArrayList<byte[]> compress(final ArrayList<Integer> toCompress)
    {
        ArrayList<byte[]> encoding = new ArrayList<>();
        int j;

        int num_full_iters = toCompress.size() / 4;
        for (j = 0; j < 4 * num_full_iters; j += 4)
        {
            int num_bytes_a = num_of_bytes[Integer.numberOfLeadingZeros(toCompress.get(j))];
            int num_bytes_b = num_of_bytes[Integer.numberOfLeadingZeros(toCompress.get(j + 1))];
            int num_bytes_c = num_of_bytes[Integer.numberOfLeadingZeros(toCompress.get(j + 2))];
            int num_bytes_d = num_of_bytes[Integer.numberOfLeadingZeros(toCompress.get(j + 3))];

            byte len_coding = (byte) (((num_bytes_a - 1) << 6) | ((num_bytes_b - 1) << 4) |
                    ((num_bytes_c - 1) << 2) | (num_bytes_d - 1));

            byte[] four_nums = new byte[num_bytes_a + num_bytes_b + num_bytes_c + num_bytes_d + 1];

            int i = 0;

            BigInteger bigInt = BigInteger.valueOf(len_coding);
            four_nums[i] = bigInt.toByteArray()[0];
            i += 1;

            i = a(toCompress.get(j), i, four_nums);
            i = a(toCompress.get(j + 1), i, four_nums);
            i = a(toCompress.get(j + 2), i, four_nums);
            a(toCompress.get(j + 3), i, four_nums);
            encoding.add(four_nums);
        }

        if (toCompress.size() - (num_full_iters * 4) != 0)
        {
            int num_bytes_a = num_of_bytes[Integer.numberOfLeadingZeros(toCompress.get(num_full_iters * 4))];
            int num_bytes_b = num_of_bytes[Integer.numberOfLeadingZeros(toCompress.get(num_full_iters * 4 + 1))];
            byte len_coding = (byte)(((num_bytes_a - 1) << 2) | (num_bytes_b - 1));
            byte[] two_nums = new byte[num_bytes_a + num_bytes_b + 1];

            int i = 0;

            BigInteger bigInt = BigInteger.valueOf(len_coding);
            two_nums[i] = bigInt.toByteArray()[0];
            i += 1;

            i = a(toCompress.get(j), i, two_nums);
            a(toCompress.get(j + 1), i, two_nums);

            encoding.add(two_nums);
        }
    return encoding;
    }

    private static int a(int num, int i, byte[] four_nums)
    {
        BigInteger bigInt = BigInteger.valueOf(num);
        byte[] nums_bytes = bigInt.toByteArray();
        int l = 0;
        if (nums_bytes[0] == 0 & nums_bytes.length > 1) {l = 1;}
        for (int k = l; k < nums_bytes.length ; ++k, i += 1)
        {
            four_nums[i] = nums_bytes[k];
        }
        return i;
    }

//    /**
//     * same functionality, but accepts and returns primitive array types
//     * @param intArray array of integers to compress
//     * @return array of bytes encoding the input list
//     */
//    public static byte[][] compress(int[] intArray)
//    {
//        //convert input to ArrayList
//        ArrayList<Integer> intList = new ArrayList<Integer>(intArray.length);
//        for (int i : intArray)
//        {
//            intList.add(i);
//        }
//        ArrayList<byte[]> compressed = GroupVarint.compress(intList);
//        //convert output to byte array
//        byte[][] ret = new byte[compressed.size()][];
//        for (int i=0; i < ret.length; i++)
//        {
//            ret[i] = compressed.get(i);
//        }
//        return ret;
//    }

    /**
     * decompresses and decodes byte array to integer list
     * @param toDecompress byte array to decompress
     * @return list of integers
     */
    public static ArrayList<Integer> decompress(final byte[] toDecompress)
    {
        ArrayList<Integer> decoding = new ArrayList<Integer>();

        for (int j = 0; j < toDecompress.length; ) {

            int len_coding = Byte.toUnsignedInt(toDecompress[j]);
            j += 1;

            // splits byte prefix to the int lengths they represent:
            int len_coding_a = (len_coding >>> 6) + 1;
            int len_coding_b = 0x3 & (len_coding >>> 4) + 1;
            int len_coding_c = 0x3 & (len_coding >>> 2) + 1;
            int len_coding_d = 0x3 & len_coding + 1;

            // read the ints, and add them to decoding:
            // notice: the condition handles the last bytes- we don't know whether the prefix encodes 4 int or 2
            if (j + len_coding_a + len_coding_b + len_coding_c + len_coding_d <= toDecompress.length) {
                j = b(toDecompress, j, len_coding_a, decoding);
                j = b(toDecompress, j, len_coding_b, decoding);
            }
            j = b(toDecompress, j, len_coding_c, decoding);
            j = b(toDecompress, j, len_coding_d, decoding);
        }
        return decoding;
    }


    private static int b(byte[] byte_array, int j, int len_coding_num, ArrayList<Integer> decoding)
    {
        int num = 0 ;
        for (int k= len_coding_num - 1; k > 0; k--, j += 1)
        {
            num |= ((byte_array[j] & masks[k - 1]) << (int) Math.pow(2, 2 + k));
        }
        num |= (byte_array[j] & 0xFF);
        j += 1;

        decoding.add(num);
        return j;
    }

//    public static ArrayList<Integer> decompress(byte[] toDecompress) {
//        ArrayList<Integer> decoding = new ArrayList();
//
//        for (int i = 0; i < toDecompress.length - 1; i++) {
//            byte[] byte_array = toDecompress[i];
//            int j = 0;
//
//            int len_coding = Byte.toUnsignedInt(byte_array[j]);
//            j += 1;
//
//            int len_coding_a = (len_coding >>> 6) + 1;
//            int len_coding_b = 0x3 & (len_coding >>> 4) + 1;
//            int len_coding_c = 0x3 & (len_coding >>> 2) + 1;
//            int len_coding_d = 0x3 & len_coding + 1;
//
//            j = b(byte_array, j, len_coding_a, decoding);
//            j = b(byte_array, j, len_coding_b, decoding);
//            j = b(byte_array, j, len_coding_c, decoding);
//            b(byte_array, j, len_coding_d, decoding);
//        }
//        byte[] byte_array = toDecompress.get(toDecompress.size() -1);
//        int j = 0;
//
//        int len_coding = Byte.toUnsignedInt(byte_array[j]);
//        j += 1;
//
//        int len_coding_a = (len_coding >>> 6) + 1;
//        int len_coding_b = 0x3 & (len_coding >>> 4) + 1;
//        int len_coding_c = 0x3 & (len_coding >>> 2) + 1;
//        int len_coding_d = 0x3 & len_coding + 1;
//
//        if (len_coding_a + len_coding_b + len_coding_c + len_coding_d == byte_array.length - 1)
//        {
//            j = b(byte_array, j, len_coding_a, decoding);
//            j = b(byte_array, j, len_coding_b, decoding);
//        }
//        j = b(byte_array, j, len_coding_c, decoding);
//        b(byte_array, j, len_coding_d, decoding);
//        return decoding;
//    }



}

