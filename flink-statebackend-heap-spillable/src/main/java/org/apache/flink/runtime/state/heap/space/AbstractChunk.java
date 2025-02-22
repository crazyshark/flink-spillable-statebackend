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

import org.apache.flink.util.Preconditions;

/**
 * Base implementation of {@link Chunk}.
 * Chunk的基本实现
 */
public abstract class AbstractChunk implements Chunk {

	/** Id of the chunk. */
	private final int chunkId;

	/** Capacity of the chunk. */
	final int capacity;

	AbstractChunk(int chunkId, int capacity) {
		this.chunkId = chunkId;
		Preconditions.checkArgument((capacity & capacity - 1) == 0,
			"Capacity of chunk should be a power of 2, but the actual is " + capacity);
		this.capacity = capacity;
	}

	@Override
	public int getChunkId() {
		return chunkId;
	}

	@Override
	public int getChunkCapacity() {
		return capacity;
	}
}
