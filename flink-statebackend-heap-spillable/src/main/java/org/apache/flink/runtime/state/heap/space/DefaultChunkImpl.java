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

import static org.apache.flink.runtime.state.heap.space.SpaceConstants.BUCKET_SIZE;

/**
 * Default implementation of {@link Chunk}.
 * Chunk 表示一整块大的空间，它会被分为若干大小相同的 Bucket。在每个 Bucket 里，又可以分为大小相同的若干 Block.
 * 需要注意的是，每个 Bucket 所采用的 Block 大小并不一定相同，所以可能出现某个 Bucket 的 Block 大，
 * 其他 Bucket 的 Block 小的情况，以充分根据实际情况来适配存储空间，减少内外零头的出现。
 *
 * 根据设计文档和当前的代码 Merge Request，目前有 PowerTwoBucketAllocator 和 DirectBucketAllocator 两种类型的 Bucket Allocator 来从 Chunk 中切分出各种 Bucket.
 *
 * PowerTwoBucketAllocator 适用于小空间的分配，它的工作原理类似于 Linux 的 Buddy 内存分配算法，
 * 分配的 Block 大小必须是 2 的正整数次幂。它采用一个栈来维护当前可用的 Block，并通过位运算和对数运算来计算得到各个 Block 的内存地址。
 * 具体的算法参见设计文档及实现代码（目前还没有 Merge 到官方分支，仅供参考），这里不再重复描述。
 *
 * DirectBucketAllocator 适用于大块空间的分配，以避免 PowerTwoBucketAllocator 带来的碎片问题。
 * 它采用经典的首次适应法（First Fit），通过 SortedMap 有序结构来记录的空余空间、已用空间列表，
 * 在分配时按顺序查找到首个可用空间；如果没有的话，压缩整理现有空间，以腾出新的块来使用。
 */
public class DefaultChunkImpl extends AbstractChunk {

	/**
	 * The backed memory segment.
	 */
	private final MemorySegment memorySegment;

	/**
	 * Bucket allocator used for this chunk.
	 */
	private final BucketAllocator bucketAllocator;

	public DefaultChunkImpl(int chunkId, MemorySegment memorySegment, AllocateStrategy allocateStrategy) {
		super(chunkId, memorySegment.size());
		this.memorySegment = memorySegment;
		switch (allocateStrategy) {
			case SmallBucket:
				this.bucketAllocator = new PowerTwoBucketAllocator(capacity, BUCKET_SIZE);
				break;
			default:
				this.bucketAllocator = new DirectBucketAllocator(capacity);
		}
	}

	@Override
	public int allocate(int len) {
		return bucketAllocator.allocate(len);
	}

	@Override
	public void free(int interChunkOffset) {
		bucketAllocator.free(interChunkOffset);
	}

	@Override
	public int getOffsetInSegment(int offsetInChunk) {
		return offsetInChunk;
	}

	@Override
	public MemorySegment getMemorySegment(int chunkOffset) {
		return memorySegment;
	}
}
