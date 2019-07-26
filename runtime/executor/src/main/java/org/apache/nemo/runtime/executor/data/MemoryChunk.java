package org.apache.nemo.runtime.executor.data;

import java.nio.ByteBuffer;

/**
 * This interface represents MemoryChunk that consumes off-heap memory.
 */
public interface MemoryChunk {

  /**
   * Release this MemoryChunk so that it cannot be used anymore.
   */
  void release();

  /**
   * Return pointer to the ByteBuffer that this MemoryChunk holds.
   * @return
   */
  ByteBuffer getBuffer();
}
