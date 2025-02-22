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

/**
 * The strategy for space allocation.
 * 分配空间的策略
 */
public enum AllocateStrategy {

	/**
	 * This strategy implements a simple buddy-like allocator used for small space.
	 * 该策略实现了一个简单的类似伙伴的分配器，用于小空间。
	 */
	SmallBucket,

	/**
	 * This strategy is used to allocate large space, and reduce fragments.
	 * 该策略用于分配大空间，并减少碎片。
	 */
	HugeBucket
}
