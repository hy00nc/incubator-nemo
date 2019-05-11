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
package org.apache.nemo.common;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class is a customized output stream implementation backed by
 * {@link ByteBuffer}, which utilizes off heap memory when writing the data.
 * Memory is allocated when needed by the specified {@code pageSize}.
 */
public final class DirectByteBufferOutputStream extends OutputStream {

  private LinkedList<ByteBuffer> dataList = new LinkedList<>();
  private static final int DEFAULT_PAGE_SIZE = 32768; //32KB
  private final int pageSize;
  private ByteBuffer currentBuf;
  static ConcurrentLinkedQueue<ByteBuffer> queue = new ConcurrentLinkedQueue<>();
  static {
    int cnt = 10000;
    while(cnt > 0) {
      queue.add(ByteBuffer.allocateDirect(DEFAULT_PAGE_SIZE));
      cnt -= 1;
    }
  }

  /**
   * Default constructor.
   * Sets the {@code pageSize} as default size of 4096 bytes.
   */
  public DirectByteBufferOutputStream() {
    this(DEFAULT_PAGE_SIZE);
  }

  /**
   * Constructor specifying the {@code size}.
   * Sets the {@code pageSize} as {@code size}.
   *
   * @param size should be a power of 2 and greater than or equal to 4096.
   */
  public DirectByteBufferOutputStream(final int size) {
    if (size < DEFAULT_PAGE_SIZE || (size & (size - 1)) != 0) {
      throw new IllegalArgumentException("Invalid pageSize");
    }
    this.pageSize = size;
    newLastBuffer();
    currentBuf = dataList.getLast();
  }

  public void finalize() {
    for(final ByteBuffer buf: dataList) {
      buf.position(0);
      queue.add(buf);
    }
  }
  /**
   * Allocates new {@link ByteBuffer} with the capacity equal to {@code pageSize}.
   */
  private void newLastBuffer() {
    //dataList.addLast(ByteBuffer.allocateDirect(pageSize));
    dataList.addLast(queue.poll());
  }

  /**
   * Writes the specified byte to this output stream.
   *
   * @param   b   the byte to be written.
   */
  @Override
  public void write(final int b) {
    if (currentBuf.remaining() <= 0) {
      newLastBuffer();
      currentBuf = dataList.getLast();
    }
    currentBuf.put((byte) b);
  }

  /**
   * Writes {@code b.length} bytes from the specified byte array to this output stream.
   *
   * @param b the byte to be written.
   */
  @Override
  public void write(final byte[] b) {
    write(b, 0, b.length);
  }

  /**
   * Writes {@code len} bytes from the specified byte array
   * starting at offset {@code off} to this output stream.
   *
   * @param   b     the data.
   * @param   off   the start offset in the data.
   * @param   len   the number of bytes to write.
   */
  @Override
  public void write(final byte[] b, final int off, final int len) {
    int byteToWrite = len;
    int offset = off;
    while (byteToWrite > 0) {
      if (currentBuf.remaining() <= 0) {
        newLastBuffer();
        currentBuf = dataList.getLast();
      }
      final int bufRemaining = currentBuf.remaining();
      if (bufRemaining < byteToWrite) {
        currentBuf.put(b, offset, bufRemaining);
        offset += bufRemaining;
        byteToWrite -= bufRemaining;
      } else {
        currentBuf.put(b, offset, byteToWrite);
        offset += byteToWrite;
        byteToWrite = 0;
      }
    }
  }

  /**
   * Creates a byte array that contains the whole content currently written in this output stream.
   * Note that this method causes array copy which could degrade performance.
   * TODO #384: For performance issue, implement an input stream so that we do not have to use this method.
   *
   * @return the current contents of this output stream, as byte array.
   */
  public byte[] toByteArray() {
    if (dataList.isEmpty()) {
      final byte[] byteArray = new byte[0];
      return byteArray;
    }

    ByteBuffer lastBuf = dataList.getLast();
    // pageSize equals the size of the data filled in the ByteBuffers
    // except for the last ByteBuffer. The size of the data in the
    // ByteBuffer can be obtained by calling ByteBuffer.position().
    final int arraySize = pageSize * (dataList.size() - 1) + lastBuf.position();
    final byte[] byteArray = new byte[arraySize];
    int start = 0;

    for (final ByteBuffer buffer : dataList) {
      // ByteBuffer has to be shifted to read mode by calling ByteBuffer.flip(),
      // which sets limit to the current position and sets the position to 0.
      // Note that capacity remains unchanged.
      final ByteBuffer dupBuffer = buffer.duplicate();
      dupBuffer.flip();
      final int byteToWrite = dupBuffer.remaining();
      dupBuffer.get(byteArray, start, byteToWrite);
      start += byteToWrite;
    }
    // The limit of the last buffer has to be set to the capacity for additional write.
    // lastBuf.limit(lastBuf.capacity());

    return byteArray;
  }

  /**
   * Returns the list of {@code ByteBuffer}s that contains the written data.
   * Note that by calling this method, the existing list of {@code ByteBuffer}s is cleared.
   *
   * @return the {@code LinkedList} of {@code ByteBuffer}s.
   */
  public List<ByteBuffer> getBufferList() {
    List<ByteBuffer> result = new ArrayList<>(dataList.size());
    //dataList = new LinkedList<>(); //disable clearing for now
    for (final ByteBuffer buffer : dataList) {
      final ByteBuffer dupBuffer = buffer.duplicate();
      dupBuffer.flip();
      result.add(dupBuffer);
    }
    return result;
  }

  /**
   * Returns the size of the data written in this output stream.
   *
   * @return the size of the data
   */
  public int size() {
    return pageSize * (dataList.size() - 1) + dataList.getLast().position();
  }

  /**
   * Closing this output stream has no effect.
   */
  public void close() {
  }
}
