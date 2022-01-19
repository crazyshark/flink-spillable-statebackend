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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Base class for spillable state table.
 */
public abstract class SpillableStateTable<K, N, S> extends StateTable<K, N, S> implements Closeable {

	public SpillableStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer) {
		super(keyContext, metaInfo, keySerializer);
	}

	/**
	 * Update estimated memory size of state.
	 */
	public abstract void updateStateEstimate(N namespace, S state);

	/**
	 * Return the estimated size of state. When {@param force} is true,
	 * an estimation will be made if there hasn't been an estimation.
	 */
	public abstract long getStateEstimatedSize(boolean force);

	/**
	 * Spill state in the given key group.
	 */
	public abstract void spillState(int keyGroupIndex);

	/**
	 * Load state in the given key group.
	 */
	public abstract void loadState(int keyGroupIndex);

	/**
	 * Returns an iterator over all state map metas.
	 */
	public abstract Iterator<SpillableStateTable.StateMapMeta> stateMapIterator();

	/**
	 * Meta of a {@link StateMap}.
	 * 这个方法主要描述了在状态表中的一个 KeyGroup 所保存的所有状态的元数据信息，
	 * 称为一个 StateMap。 在 Flink 的状态系统中，分配的最小单元是 KeyGroup，
	 * 表示哈希值一致的一组 Key。若干个连续的 KeyGroup 组成一个 KeyGroupRange，分配给相关算子的某个并行实例中。
	 * StateMapMeta 对象包括了这个 KeyGroup 所属对象的多种典型属性，
	 * Flink Spillable Backend 就是根据这些属性来计算权重、决定 Spill 还是 Load 等动作的影响范围。
	 */
	public static class StateMapMeta {

		// KeyGroup 所属的状态表引用
		private final SpillableStateTable stateTable;
		// 该状态表中, 此 KeyGroup 的偏移量 (索引)
		private final int keyGroupIndex;
		// 目前是否在堆内存里
		private final boolean isOnHeap;
		// 该 KeyGroup 的状态数
		private final int size;
		// 该 KeyGroup 的总请求数
		private final long numRequests;
		/** Initialize lazily. -1 indicates uninitialized. */
		// 估计的状态总大小, 如果是 -1 表示未初始化
		private long estimatedMemorySize;

		public StateMapMeta(
			SpillableStateTable stateTable,
			int keyGroupIndex,
			boolean isOnHeap,
			int size,
			long numRequests) {
			this.stateTable = stateTable;
			this.keyGroupIndex = keyGroupIndex;
			this.isOnHeap = isOnHeap;
			this.size = size;
			this.numRequests = numRequests;
			this.estimatedMemorySize = -1;
		}

		public SpillableStateTable getStateTable() {
			return stateTable;
		}

		public boolean isOnHeap() {
			return isOnHeap;
		}

		public int getSize() {
			return size;
		}

		public int getKeyGroupIndex() {
			return keyGroupIndex;
		}

		public long getNumRequests() {
			return numRequests;
		}

		public long getEstimatedMemorySize() {
			return estimatedMemorySize;
		}

		public void setEstimatedMemorySize(long estimatedMemorySize) {
			this.estimatedMemorySize = estimatedMemorySize;
		}
	}
}
