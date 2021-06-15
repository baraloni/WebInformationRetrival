package webdata;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

/**
 * This class implements charSequence in a compact way- 1 byte per char
 */
public class CompactCharSequence implements CharSequence, Serializable {
    static final long serialVersionUID = 1L;

    private static final String ENCODING = "ISO-8859-1";
    private final int offset;
    private final int end;
    private final byte[] data;

    public CompactCharSequence(String str) {
      try {
        data = str.getBytes(ENCODING);
        offset = 0;
        end = data.length;
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Unexpected: " + ENCODING + " not supported!");
      }
    }

    @Override
    public char charAt(int index) {
      int ix = index+offset;
      if (ix >= end) {
        throw new StringIndexOutOfBoundsException("Invalid index " +
          index + " length " + length());
      }
      return (char) (data[ix] & 0xff);
    }

    @Override
    public int length() {
      return end - offset;
    }

    private CompactCharSequence(byte[] data, int offset, int end) {
        this.data = data;
        this.offset = offset;
        this.end = end;
      }

    @Override
    public CharSequence subSequence(int start, int end) {
        if (start < 0 || end > (this.end-offset)) {
          throw new IllegalArgumentException("Illegal range " +
            start + "-" + end + " for sequence of length " + length());
        }
        return new CompactCharSequence(data, start + offset, end + offset);
      }

    @Override
    public String toString() {
        try {
          return new String(data, offset, end-offset, ENCODING);
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException("Unexpected: " + ENCODING + " not supported");
        }
      }
  }


