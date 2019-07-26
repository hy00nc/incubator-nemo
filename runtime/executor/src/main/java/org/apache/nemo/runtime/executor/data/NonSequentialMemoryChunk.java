package org.apache.nemo.runtime.executor.data;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NonSequentialMemoryChunk implements MemoryChunk {

  // UNSAFE is used for random access and manipulation of the ByteBuffer.
  @SuppressWarnings("restriction") // to suppress warnings that are invoked whenever we use UNSAFE.
  protected static final sun.misc.Unsafe UNSAFE = getUnsafe();
  @SuppressWarnings("restriction")
  protected static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
  private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);
  private static final int SHORT_SIZE = 2;
  private static final int CHAR_SIZE = 2;
  private static final int INT_SIZE = 4;
  private static final int LONG_SIZE = 8;

  // Since using UNSAFE does not automatically track the address and limit, it should be accessed
  // through address for data write and get, and addressLimit for sanity checks on the buffer use.
  private final ByteBuffer buffer;
  private final int size;
  private final long address;
  private final long addressLimit;
  private boolean released;

  NonSequentialMemoryChunk(final long offHeapAddress, final ByteBuffer buffer) {
    if (offHeapAddress <= 0) {
      throw new IllegalArgumentException("negative pointer or size");
    }
    if (offHeapAddress >= Long.MAX_VALUE - Integer.MAX_VALUE) {
      throw new IllegalArgumentException("MemoryChunk initialized with too large address");
    }
    this.buffer = buffer;
    this.size = buffer.capacity();
    this.released = false;
    this.address = offHeapAddress;
    this.addressLimit = this.address + size;
  }

  NonSequentialMemoryChunk(final ByteBuffer buffer) {
    this(getAddress(buffer), buffer);
  }

  /**
   * Reads the byte at the given index.
   *
   * @param index from which the byte will be read
   * @return the byte at the given position
   */
  @SuppressWarnings("restriction")
  public final byte get(final int index) {
    final long pos = address + index;
    if (released) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      checkIndex(index, pos, 0);
      return UNSAFE.getByte(pos);
    }
  }

  /**
   * Writes the given byte into this buffer at the given index.
   *
   * @param index The position at which the byte will be written.
   * @param b     The byte value to be written.
   */
  @SuppressWarnings("restriction")
  public final void put(final int index, final byte b) {
    final long pos = address + index;
    if (released) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      checkIndex(index, pos, 0);
      UNSAFE.putByte(pos, b);
    }
  }

  /**
   * Copies the data of the MemoryChunk from the specified position to target byte array.
   *
   * @param index The position at which the first byte will be read.
   * @param dst The memory into which the memory will be copied.
   */
  public final void get(final int index, final byte[] dst) {
    get(index, dst, 0, dst.length);
  }

  /**
   * Copies all the data from the source byte array into the MemoryChunk
   * beginning at the specified position.
   *
   * @param index the position in MemoryChunk to start copying the data.
   * @param src the source byte array that holds the data to copy.
   */
  public final void put(final int index, final byte[] src) {
    put(index, src, 0, src.length);
  }

  /**
   * Bulk get method using nk.the specified index in the MemoryChunk.
   *
   * @param index the index in the MemoryChunk to start copying the data.
   * @param dst the target byte array to copy the data from MemoryChunk.
   * @param offset the offset in the destination byte array.
   * @param length the number of bytes to be copied.
   */
  @SuppressWarnings("restriction")
  public final void get(final int index, final byte[] dst, final int offset, final int length) {
    if ((offset | length | (offset + length) | (dst.length - (offset + length))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (released) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      final long pos = address + index;
      checkIndex(index, pos, length);
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      UNSAFE.copyMemory(null, pos, dst, arrayAddress, length);
    }
  }

  /**
   * Bulk put method using the specified index in the MemoryChunk.
   *
   * @param index the index in the MemoryChunk to start copying the data.
   * @param src the source byte array that holds the data to be copied to MemoryChunk.
   * @param offset the offset in the source byte array.
   * @param length the number of bytes to be copied.
   */
  @SuppressWarnings("restriction")
  public final void put(final int index, final byte[] src, final int offset, final int length) {
    if ((offset | length | (offset + length) | (src.length - (offset + length))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (released) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      final long pos = address + index;
      checkIndex(index, pos, length);
      final long arrayAddress = BYTE_ARRAY_BASE_OFFSET + offset;
      UNSAFE.copyMemory(src, arrayAddress, null, pos, length);
    }
  }

  /**
   * Reads a char value from the given position.
   *
   * @param index The position from which the memory will be read.
   * @return The char value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus CHAR_SIZE.
   */
  @SuppressWarnings("restriction")
  public final char getChar(final int index) {
    final long pos = address + index;
    if (released) {
      throw new IllegalStateException("This MemoryChunk has been freed.");
    }
    checkIndex(index, pos, CHAR_SIZE);
    if (LITTLE_ENDIAN) {
      return UNSAFE.getChar(pos);
    } else {
      return Character.reverseBytes(UNSAFE.getChar(pos));
    }
  }

  /**
   * Writes a char value to the given position.
   *
   * @param index The position at which the memory will be written.
   * @param value The char value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus CHAR_SIZE.
   */
  @SuppressWarnings("restriction")
  public final void putChar(final int index, final char value) {
    final long pos = address + index;
    if (released) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      checkIndex(index, pos, CHAR_SIZE);
      if (LITTLE_ENDIAN) {
        UNSAFE.putChar(pos, value);
      } else {
        UNSAFE.putChar(pos, Character.reverseBytes(value));
      }
    }
  }

  /**
   * Reads a short integer value from the given position, composing them into a short value
   * according to the current byte order.
   *
   * @param index The position from which the memory will be read.
   * @return The short value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus SHORT_SIZE.
   */
  @SuppressWarnings("restriction")
  public final short getShort(final int index) {
    final long pos = address + index;
    if (released) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      checkIndex(index, pos, SHORT_SIZE);
      if (LITTLE_ENDIAN) {
        return UNSAFE.getShort(pos);
      } else {
        return Short.reverseBytes(UNSAFE.getShort(pos));
      }
    }
  }

  /**
   * Writes the given short value into this buffer at the given position, using
   * the native byte order of the system.
   *
   * @param index The position at which the value will be written.
   * @param value The short value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus SHORT_SIZE.
   */
  @SuppressWarnings("restriction")
  public final void putShort(final int index, final short value) {
    final long pos = address + index;
    if (released) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      checkIndex(index, pos, SHORT_SIZE);
      if (LITTLE_ENDIAN) {
        UNSAFE.putShort(pos, value);
      } else {
        UNSAFE.putShort(pos, Short.reverseBytes(value));
      }
    }
  }

  /**
   * Reads an int value from the given position, in the system's native byte order.
   *
   * @param index The position from which the value will be read.
   * @return The int value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus INT_SIZE.
   */
  @SuppressWarnings("restriction")
  public final int getInt(final int index) {
    final long pos = address + index;
    if (released) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      checkIndex(index, pos, INT_SIZE);
      if (LITTLE_ENDIAN) {
        return UNSAFE.getInt(pos);
      } else {
        return Integer.reverseBytes(UNSAFE.getInt(pos));
      }
    }
  }

  /**
   * Writes the given int value to the given position in the system's native byte order.
   *
   * @param index The position at which the value will be written.
   * @param value The int value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus INT_SIZE.
   */
  @SuppressWarnings("restriction")
  public final void putInt(final int index, final int value) {
    final long pos = address + index;
    if (released) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      checkIndex(index, pos, INT_SIZE);
      if (LITTLE_ENDIAN) {
        UNSAFE.putInt(pos, value);
      } else {
        UNSAFE.putInt(pos, Integer.reverseBytes(value));
      }
    }
  }

  /**
   * Reads a long value from the given position.
   *
   * @param index The position from which the value will be read.
   * @return The long value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus LONG_SIZE.
   */
  @SuppressWarnings("restriction")
  public final long getLong(final int index) {
    final long pos = address + index;
    if (released) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      checkIndex(index, pos, LONG_SIZE);
      if (LITTLE_ENDIAN) {
        return UNSAFE.getLong(pos);
      } else {
        return Long.reverseBytes(UNSAFE.getLong(pos));
      }
    }
  }

  /**
   * Writes the given long value to the given position in the system's native byte order.
   *
   * @param index The position at which the value will be written.
   * @param value The long value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus LONG_SIZE.
   */
  @SuppressWarnings("restriction")
  public final void putLong(final int index, final long value) {
    final long pos = address + index;
    if (released) {
      throw new IllegalStateException("MemoryChunk has been freed");
    } else {
      checkIndex(index, pos, LONG_SIZE);
      if (LITTLE_ENDIAN) {
        UNSAFE.putLong(pos, value);
      } else {
        UNSAFE.putLong(pos, Long.reverseBytes(value));
      }
    }
  }


  /**
   * Reads a float value from the given position, in the system's native byte order.
   *
   * @param index The position from which the value will be read.
   * @return The float value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus size of float.
   */
  public final float getFloat(final int index) {
    return Float.intBitsToFloat(getInt(index));
  }

  /**
   * Writes the given float value to the given position in the system's native byte order.
   *
   * @param index The position at which the value will be written.
   * @param value The float value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus size of float.
   */
  public final void putFloat(final int index, final float value) {
    putInt(index, Float.floatToRawIntBits(value));
  }

  /**
   * Reads a double value from the given position, in the system's native byte order.
   *
   * @param index The position from which the value will be read.
   * @return The double value at the given position.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus size of double.
   */
  public final double getDouble(final int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  /**
   * Writes the given double value to the given position in the system's native byte order.
   *
   * @param index The position at which the memory will be written.
   * @param value The double value to be written.
   *
   * @throws IndexOutOfBoundsException If the index is negative, or larger then the chunk size minus size of double.
   */
  public final void putDouble(final int index, final double value) {
    putLong(index, Double.doubleToRawLongBits(value));
  }

  @SuppressWarnings("restriction")
  private static sun.misc.Unsafe getUnsafe() {
    try {
      Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      return (sun.misc.Unsafe) unsafeField.get(null);
    } catch (Exception e) {
      throw new RuntimeException("Error while trying to access the sun.misc.Unsafe handle.");
    }
  }

  private void checkIndex(final int index, final long pos, final int typeSize) throws IndexOutOfBoundsException {
    if (!(index >= 0 && pos <= addressLimit - typeSize)) {
      throw new IndexOutOfBoundsException();
    }
  }

  private static final Field ADDRESS_FIELD;

  static {
    try {
      ADDRESS_FIELD = java.nio.Buffer.class.getDeclaredField("address");
      ADDRESS_FIELD.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Cannot initialize MemoryChunk: off-heap memory is incompatible with this JVM.");
    }
  }

  private static long getAddress(final ByteBuffer buffer) {
    if (buffer == null) {
      throw new NullPointerException("Buffer null");
    }
    if (!buffer.isDirect()) {
      throw new IllegalArgumentException("Cannot initialize from non-direct ByteBuffer.");
    }
    try {
      return (Long) ADDRESS_FIELD.get(buffer);
    } catch (Exception e) {
      throw new RuntimeException("Could not access ByteBuffer address.");
    }
  }

  /**
   * Releases this MemoryChunk....
   */
  public void release() {
    released = true;
  }

  /**
   *
   * @return
   */
  public ByteBuffer getBuffer() {
    return buffer;
  }
}
