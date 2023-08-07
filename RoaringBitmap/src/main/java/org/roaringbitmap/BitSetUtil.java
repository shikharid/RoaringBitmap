package org.roaringbitmap;


import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;


/***
 *
 * This class provides convenience functions to manipulate BitSet and RoaringBitmap objects.
 *
 */
public class BitSetUtil {
  // todo: add a method to convert a RoaringBitmap to a BitSet using BitSet.valueOf

  // a block consists has a maximum of 1024 words, each representing 64 bits,
  // thus representing at maximum 65536 bits
  static final private int BLOCK_LENGTH = BitmapContainer.MAX_CAPACITY / Long.SIZE; //
  // 64-bit
  // word



  /**
   * Generate a RoaringBitmap out of a BitSet
   *
   * @param bitSet original bitset (will not be modified)
   * @return roaring bitmap equivalent to BitSet
   */
  public static RoaringBitmap bitmapOf(final BitSet bitSet) {
    return bitmapOf(bitSet.toLongArray());
  }

  /**
   * Generate a RoaringBitmap out of a long[], each long using little-endian representation of its
   * bits
   *
   * @see BitSet#toLongArray() for an equivalent
   * @param words array of longs (will not be modified)
   * @return roaring bitmap
   */
  public static RoaringBitmap bitmapOf(final long[] words) {
    // split long[] into blocks.
    // each block becomes a single container, if any bit is set
    final RoaringBitmap ans = new RoaringBitmap();
    int containerIndex = 0;
    for (int from = 0; from < words.length; from += BLOCK_LENGTH) {
      final int to = Math.min(from + BLOCK_LENGTH, words.length);
      final int blockCardinality = cardinality(from, to, words);
      if (blockCardinality > 0) {
        ans.highLowContainer.insertNewKeyValueAt(containerIndex++, Util.highbits(from * Long.SIZE),
            BitSetUtil.containerOf(from, to, blockCardinality, words));
      }
    }
    return ans;
  }

  private static ArrayContainer arrayContainerOf(final int from, final int to,
                                                 final int cardinality, final long[] words) {
    // precondition: cardinality is max 4096
    final char[] content = new char[cardinality];
    int index = 0;

    for (int i = from, socket = 0; i < to; ++i, socket += Long.SIZE) {
      long word = words[i];
      while (word != 0) {
        long t = word & -word;
        content[index++] = (char) (socket + Long.bitCount(t - 1));
        word ^= t;
      }
    }
    return new ArrayContainer(content);
  }

  public static RoaringBitmap bitOf(ByteBuffer bb, boolean fastRank) {
    bb = bb.slice().order(ByteOrder.LITTLE_ENDIAN);
    final RoaringBitmap ans = fastRank ? new FastRankRoaringBitmap() : new RoaringBitmap();

    // split buffer into blocks of long[], reuse a ThreadLocal array for blocks
    final long[] words = WORD_BLOCK.get();
    int containerIndex = 0;
    int blockLength = 0, blockCardinality = 0, offset = 0;
    long word;
    boolean allDone = false;
    while (!allDone) {
      if (bb.remaining() >= 8) {
        word = bb.getLong();
      } else {
        // Read remaining (less than 8) bytes
        word = 0;
        for (int remaining = bb.remaining(), j = 0; j < remaining; j++) {
          word |= (bb.get() & 0xffL) << (8 * j);
        }
        allDone = true;
      }

      // Add read long to block
      words[blockLength++] = word;
      blockCardinality += Long.bitCount(word);

      // When block is full, add block to bitmap
      if (blockLength == BLOCK_LENGTH) {
        // Each block becomes a single container, if any bit is set
        addBlock(ans, words, containerIndex, blockLength,
            blockCardinality, offset);
        offset += (blockLength * Long.SIZE);
        blockLength = blockCardinality = 0;
        ++containerIndex;
      }
    }

    // Add block to map, if any bit is set
    addBlock(ans, words, containerIndex, blockLength, blockCardinality, offset);
    return ans;
  }

  private static final ThreadLocal<char[]> WOR_BLOCK = ThreadLocal.withInitial(() ->
      new char[ArrayContainer.DEFAULT_MAX_SIZE + 64]);
  public static RoaringBitmap bitmapOfInPlace(ByteBuffer bb, boolean fastRank) {

    bb = bb.slice().order(ByteOrder.LITTLE_ENDIAN);

    final RoaringBitmap ans = fastRank ? new FastRankRoaringBitmap() : new RoaringBitmap();

    BitmapContainer bitmapContainer = null;
    long[] bitmapWords = null;
    final char[] array = WOR_BLOCK.get();
    Arrays.fill(array, (char) 0);
    boolean allDone = false; // Start with an ArrayContainer always
    int arrayOffset = 0, blockLength = 0, containerIdx = 0, offset = 0, cardinality = 0;
    long word = 0;
    while (!allDone) {

      // Read a full long, if possible
      if (bb.remaining() >= 8) {
        word = bb.getLong();
      } else {
        // Or Read remaining (less than 8) bytes
        word = 0;
        for (int remaining = bb.remaining(), j = 0; j < remaining; j++) {
          word |= (bb.get() & 0xffL) << (8 * j);
        }
        allDone = true;
      }

      if (bitmapContainer != null) {
        // If we are one a bitmap container now, directly add word
        bitmapWords[blockLength] = word;
        cardinality += Long.bitCount(word);
      } else {
        while (word != 0) {
          long t = word & -word;
          array[cardinality++] = (char) (arrayOffset + Long.bitCount(t -  1));
          word ^= t;
        }
        arrayOffset += Long.SIZE;
        // See if we need to move to a BitmapContainer
        if (cardinality >= ArrayContainer.DEFAULT_MAX_SIZE) {
          bitmapContainer = new BitmapContainer();
          for (int i = 0;i < cardinality; ++i) {
            bitmapContainer.add(Util.lowbits(array[i]));
          }
          // Get backing long[] array
          bitmapWords = bitmapContainer.bitmap;
          cardinality = bitmapContainer.cardinality;
          // Reset array container related things
          Arrays.fill(array, (char) 0);
          arrayOffset = 0;
        }
      }

      blockLength++;
      if (blockLength == BLOCK_LENGTH) { // Add container if full
        if (bitmapContainer != null) {
          if (cardinality > 0) {
            bitmapContainer.cardinality = cardinality;
            ans.highLowContainer.insertNewKeyValueAt(containerIdx++, Util.highbits(offset), bitmapContainer);
          }
          // Assume next container to be similar to whatever we used last
          bitmapContainer = null;
        } else {
          if (cardinality > 0) {
            ans.highLowContainer.insertNewKeyValueAt(containerIdx++, Util.highbits(offset), new ArrayContainer(Arrays.copyOf(array, cardinality)));
          }
          Arrays.fill(array, (char) 0);
        }
        offset += (blockLength * Long.SIZE);
        blockLength = 0;
        arrayOffset = 0;
      }
    }
    // Write last block
    if (blockLength > 0) {
      if (bitmapContainer != null && bitmapContainer.cardinality > 0) {
        ans.highLowContainer.insertNewKeyValueAt(containerIdx, Util.highbits(offset), bitmapContainer);
      } else if (cardinality > 0) {
        ans.highLowContainer.insertNewKeyValueAt(containerIdx, Util.highbits(offset), new ArrayContainer(Arrays.copyOf(array, cardinality)));
      }
    }
    // Add block to map, if any bit is set
    return ans;
  }

  // To avoid memory allocation, reuse ThreadLocal buffers
  private static final ThreadLocal<long[]> WORD_BLOCK = ThreadLocal.withInitial(() ->
      new long[BLOCK_LENGTH]);

  /**
   * Efficiently generate a RoaringBitmap from an uncompressed byte array or ByteBuffer
   * This method tries to minimise all kinds of memory allocation
   *
   * @param bb the uncompressed bitmap
   * @param fastRank if set, returned bitmap is of type
   *                 {@link org.roaringbitmap.FastRankRoaringBitmap}
   * @return roaring bitmap
   */
  public static RoaringBitmap bitmapOf(ByteBuffer bb, boolean fastRank) {

    bb = bb.slice().order(ByteOrder.LITTLE_ENDIAN);
    final RoaringBitmap ans = fastRank ? new FastRankRoaringBitmap() : new RoaringBitmap();

    // split buffer into blocks of long[], reuse a ThreadLocal array for blocks
    final long[] words = WORD_BLOCK.get();
    int containerIndex = 0;
    int blockLength = 0, blockCardinality = 0, offset = 0;
    long word;
    while (bb.remaining() >= 8) {
      word = bb.getLong();

      // Add read long to block
      words[blockLength++] = word;
      blockCardinality += Long.bitCount(word);

      // When block is full, add block to bitmap
      if (blockLength == BLOCK_LENGTH) {
        // Each block becomes a single container, if any bit is set
        containerIndex = addBlock(ans, words, containerIndex, blockLength,
            blockCardinality, offset);
        offset += (blockLength * Long.SIZE);
        blockLength = blockCardinality = 0;
      }
    }

    if (bb.remaining() > 0) {
      // Read remaining (less than 8) bytes
      word = 0;
      for (int remaining = bb.remaining(), j = 0; j < remaining; j++) {
        word |= (bb.get() & 0xffL) << (8 * j);
      }

      // Add last word to block, only if any bit is set
      if (word != 0) {
        words[blockLength++] = word;
        blockCardinality += Long.bitCount(word);
      }
    }

    // Add block to map, if any bit is set
    addBlock(ans, words, containerIndex, blockLength, blockCardinality, offset);
    return ans;
  }

  private static void addBlock(RoaringBitmap ans, long[] words, int containerIndex, int blockLength,
    int blockCardinality, int offset) {
    if (blockCardinality > 0) {
      ans.highLowContainer.insertNewKeyValueAt(containerIndex, Util.highbits(offset),
          BitSetUtil.containerOf(0, blockLength, blockCardinality, words));
    }
  }

  private static int cardinality(final int from, final int to, final long[] words) {
    int sum = 0;
    for (int i = from; i < to; i++) {
      sum += Long.bitCount(words[i]);
    }
    return sum;
  }




  private static Container containerOf(final int from, final int to, final int blockCardinality,
      final long[] words) {
    // find the best container available
    if (blockCardinality <= ArrayContainer.DEFAULT_MAX_SIZE) {
      // containers with DEFAULT_MAX_SIZE or less integers should be
      // ArrayContainers
      return arrayContainerOf(from, to, blockCardinality, words);
    } else {
      // otherwise use bitmap container
      long[] container = new long[BLOCK_LENGTH];
      System.arraycopy(words, from, container, 0, to - from);
      return new BitmapContainer(container, blockCardinality);
    }
  }


  /**
   * Compares a RoaringBitmap and a BitSet. They are equal if and only if they contain the same set
   * of integers.
   *
   * @param bitset first object to be compared
   * @param bitmap second object to be compared
   * @return whether they are equals
   */
  public static boolean equals(final BitSet bitset, final RoaringBitmap bitmap) {
    if (bitset.cardinality() != bitmap.getCardinality()) {
      return false;
    }
    final IntIterator it = bitmap.getIntIterator();
    while (it.hasNext()) {
      int val = it.next();
      if (!bitset.get(val)) {
        return false;
      }
    }
    return true;
  }
}
