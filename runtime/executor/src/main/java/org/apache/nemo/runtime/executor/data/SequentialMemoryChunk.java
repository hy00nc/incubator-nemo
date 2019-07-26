/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.data;

import java.nio.ByteBuffer;


/**
 * This class represents chunk of memory residing in off-heap region
 * managed by {@link org.apache.nemo.runtime.executor.data.MemoryPoolAssigner}, which is backed by {@link ByteBuffer}.
 */
public class SequentialMemoryChunk implements MemoryChunk {
  private final ByteBuffer buffer;
  private boolean released;

  /**
   * Creates a new memory chunk that represents the off-heap memory at the absolute address.
   * This class can be created in two modes: sequential access mode or random access mode.
   * Sequential access mode supports convenient sequential access of {@link ByteBuffer}.
   * Random access mode supports random access and manipulation of the data in the {@code ByteBuffer} using UNSAFE.
   * No automatic tracking of position, limit, capacity, etc. of {@code ByteBuffer} for random access mode.
   *
   * @param buffer         the off-heap memory of this SequentialMemoryChunk
   */
  SequentialMemoryChunk(final ByteBuffer buffer) {
    this.buffer = buffer;
    this.released = false;
  }

  /**
   * Gets the {@link ByteBuffer} from this SequentialMemoryChunk.
   *
   * @return  {@link ByteBuffer}
   */
  public ByteBuffer getBuffer() {
    return buffer;
  }

  /**
   * Gets the remaining number of bytes in the {@link ByteBuffer} of this MemoryChunk.
   *
   * @return  the number of remaining bytes
   */
  public int remaining() {
    return buffer.remaining();
  }

  /**
   * Gets the current position of the {@link ByteBuffer} of this MemoryChunk.
   *
   * @return the position
   */
  public int position() {
    return buffer.position();
  }

  /**
   * Makes the duplicated instance of this MemoryChunk.
   *
   * @return the SequentialMemoryChunk with the same content of the caller instance
   */
  public SequentialMemoryChunk duplicate() {
    return new SequentialMemoryChunk(buffer.duplicate());
  }

  /**
   * Reads the byte at the current position of the {@link ByteBuffer}.
   *
   * @return the byte value
   */
  public final byte get() {
    if (released) {
      throw new IllegalStateException("SequentialMemoryChunk has been freed");
    }
    return buffer.get();
  }

  /**
   * Writes the given byte into the current position of the {@link ByteBuffer}.
   *
   * @param b the byte value to be written.
   */
  public final void put(final byte b) {
    if (released) {
      throw new IllegalStateException("SequentialMemoryChunk has been freed");
    }
    buffer.put(b);
  }

  /**
   * Copies the data of the SequentialMemoryChunk from the current position of the {@link ByteBuffer}
   * to target byte array.
   *
   * @param dst the target byte array to copy the data from SequentialMemoryChunk.
   */
  public final void get(final byte[] dst) {
    if (released) {
      throw new IllegalStateException("SequentialMemoryChunk has been freed");
    }
    buffer.get(dst);
  }

  /**
   * Copies all the data from the source byte array into the SequentialMemoryChunk
   * beginning at the current position of the {@link ByteBuffer}.
   *
   * @param src the source byte array that holds the data to copy.
   */
  public final void put(final byte[] src) {
    if (released) {
      throw new IllegalStateException("SequentialMemoryChunk has been freed");
    }
    buffer.put(src);
  }

  /**
   * Bulk get method using the current position of the {@link ByteBuffer}.
   *
   * @param dst the target byte array to copy the data from SequentialMemoryChunk.
   * @param offset the offset in the destination byte array.
   * @param length the number of bytes to be copied.
   */
  public final void get(final byte[] dst, final int offset, final int length) {
    if (released) {
      throw new IllegalStateException("SequentialMemoryChunk has been freed");
    }
    buffer.get(dst, offset, length);
  }

  /**
   * Bulk put method using the current position of the {@link ByteBuffer}.
   *
   * @param src the source byte array that holds the data to be copied to MemoryChunk.
   * @param offset  the offset in the source byte array.
   * @param length  the number of bytes to be copied.
   */
  public final void put(final byte[] src, final int offset, final int length) {
    if (released) {
      throw new IllegalStateException("SequentialMemoryChunk has been freed");
    }
    buffer.put(src, offset, length);
  }

  /**
   * Reads a char value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   */
  public final char getChar() {
    if (released) {
      throw new IllegalStateException("This SequentialMemoryChunk has been freed");
    }
    return buffer.getChar();
  }

  /**
   * Writes a char value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the SequentialMemoryChunk.
   */
  public final void putChar(final char value) {
    if (released) {
      throw new IllegalStateException("This SequentialMemoryChunk has been freed");
    }
    buffer.putChar(value);
  }

  /**
   * Reads a short integer value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   */
  public final short getShort() {
    if (released) {
      throw new IllegalStateException("This SequentialMemoryChunk has been freed");
    }
    return buffer.getShort();
  }

  /**
   * Writes a char value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the SequentialMemoryChunk.
   */
  public final void putShort(final short value) {
    if (released) {
      throw new IllegalStateException("This SequentialMemoryChunk has been freed");
    }
    buffer.putShort(value);
  }

  /**
   * Reads an int value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   */
  public final int getInt() {
    if (released) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    return buffer.getInt();
  }

  /**
   * Writes an int value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the MemoryChunk.
   */
  public final void putInt(final int value) {
    if (released) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    buffer.putInt(value);
  }

  /**
   * Reads a long value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   */
  public final long getLong() {
    if (released) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    return buffer.getLong();
  }

  /**
   * Writes a long value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the MemoryChunk.
   */
  public final void putLong(final long value) {
    if (released) {
      throw new IllegalStateException("This MemoryChunk has been freed");
    }
    buffer.putLong(value);
  }

  /**
   * Reads a float value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   */
  public final float getFloat() {
    if (released) {
      throw new IllegalStateException("This SequentialMemoryChunk has been freed");
    }
    return buffer.getFloat();
  }

  /**
   * Writes a float value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the SequentialMemoryChunk.
   */
  public final void putFloat(final float value) {
    if (released) {
      throw new IllegalStateException("This SequentialMemoryChunk has been freed");
    }
    buffer.putFloat(value);
  }

  /**
   * Reads a double value from the current position of the {@link ByteBuffer}.
   *
   * @return The char value at the current position.
   */
  public final double getDouble() {
    if (released) {
      throw new IllegalStateException("This SequentialMemoryChunk has been freed");
    }
    return buffer.getDouble();
  }

  /**
   * Writes a double value to the current position of the {@link ByteBuffer}.
   *
   * @param value to be copied to the SequentialMemoryChunk.
   */
  public final void putDouble(final double value) {
    if (released) {
      throw new IllegalStateException("This SequentialMemoryChunk has been freed");
    }
    buffer.putDouble(value);
  }

  /**
   * Releases this MemoryChunk....
   */
  public void release() {
    released = true;
  }
}
