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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

public class DirectByteBufferInputStream extends InputStream {
  private List<ByteBuffer> bufList;
  private int current = 0;

  public DirectByteBufferInputStream(final List<ByteBuffer> bufList) {
   this.bufList = bufList;
  }

  @Override
  public int read() throws IOException {
    return getBuffer().get() & 0xff;
  }

  /**
   * Return next non-empty @code{ByteBuffer}.
   * @return @code{ByteBuffer} to write the data
   * @throws IOException
   */
  public ByteBuffer getBuffer() throws IOException {
    while (current < bufList.size()) {
      ByteBuffer buffer = bufList.get(current);
      if (buffer.hasRemaining()) {
        return buffer;
      }
      current += 1;
    }
    throw new EOFException();
  }
}
