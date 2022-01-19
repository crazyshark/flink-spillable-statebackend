/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.core.memory.MemorySegment;

/**
 * Chunk is a logically contiguous space backed by one or multiple {@link MemorySegment}.
 * Chunk是由一个或多个 {@link MemorySegment} 支持的逻辑连续空间。
 * <p/>
 * The backing MemorySegment may wrap an on-heap byte array, an off-heap {@link java.nio.DirectByteBuffer},
 * or a {@link java.nio.MappedByteBuffer} from a memory-mapped file.
 * 后备 MemorySegment 可以包装一个堆上字节数组、一个堆外 {@link java.nio.DirectByteBuffer} 或一个来自内存映射文件的 {@link java.nio.MappedByteBuffer}。
 */
public interface Chunk {
	/**
	 * Try to allocate size bytes from the chunk.
	 *
	 * @param len size of bytes to allocate.
	 * @return the offset of the successful allocation, or -1 to indicate not-enough-space
	 */
	int allocate(int len);

	/**
	 * release the space addressed by interChunkOffset.
	 *
	 * @param interChunkOffset offset of the chunk
	 */
	void free(int interChunkOffset);

	/**
	 * @return Id of this Chunk
	 */
	int getChunkId();

	int getChunkCapacity();

	/**
	 * Returns the backed {@link MemorySegment} for the space with the offset.
	 * 返回offset对应的底层MemorySegment
	 *
	 * @param offsetInChunk offset of space in the chunk.
	 * @return memory segment backed the space.
	 */
	MemorySegment getMemorySegment(int offsetInChunk);

	/**
	 * Returns the offset of the space in the backed {@link MemorySegment}.
	 * 返回支持的 {@link MemorySegment} 中空间的偏移量。
	 *
	 * @param offsetInChunk offset of space in the chunk.
	 * @return offset of space in the memory segment.
	 */
	int getOffsetInSegment(int offsetInChunk);
}
