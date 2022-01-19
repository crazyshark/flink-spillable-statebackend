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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of {@link SpillAndLoadManager}.
 */
public class SpillAndLoadManagerImpl implements SpillAndLoadManager {

	private static final Logger LOG = LoggerFactory.getLogger(SpillAndLoadManagerImpl.class);

	/** For spill, we prefer to spill bigger bucket with less requests first, and retained size has higher weight. */
	private static final double WEIGHT_SPILL_RETAINED_SIZE = 0.7;
	private static final double WEIGHT_SPILL_REQUEST_RATE = -0.3;
	private static final double WEIGHT_SPILL_SUM = WEIGHT_SPILL_RETAINED_SIZE + WEIGHT_SPILL_REQUEST_RATE;

	/** For load, we prefer to load smaller bucket with more requests first, and request rate has higher weight. */
	private static final double WEIGHT_LOAD_RETAINED_SIZE = -0.3;
	private static final double WEIGHT_LOAD_REQUEST_RATE = 0.7;
	private static final double WEIGHT_LOAD_SUM = WEIGHT_LOAD_RETAINED_SIZE + WEIGHT_LOAD_REQUEST_RATE;

	private final StateTableContainer stateTableContainer;
	private final HeapStatusMonitor heapStatusMonitor;
	private final CheckpointManager checkpointManager;

	private final boolean cancelCheckpoint;
	private final long gcTimeThreshold;
	private final float spillSizeRatio;
	private final float loadStartRatio;
	private final float loadEndRatio;
	private final long triggerInterval;
	private final long resourceCheckInterval;

	private final long maxMemory;
	private final long loadStartSize;
	private final long loadEndSize;

	private long lastResourceCheckTime;
	private long lastTriggerTime;
	private HeapStatusMonitor.MonitorResult lastMonitorResult;

	public SpillAndLoadManagerImpl(
		StateTableContainer stateTableContainer,
		HeapStatusMonitor heapStatusMonitor,
		CheckpointManager checkpointManager,
		Configuration configuration) {
		this.stateTableContainer = Preconditions.checkNotNull(stateTableContainer);
		this.heapStatusMonitor = Preconditions.checkNotNull(heapStatusMonitor);
		this.checkpointManager = Preconditions.checkNotNull(checkpointManager);

		this.cancelCheckpoint = configuration.get(SpillableOptions.CANCEL_CHECKPOINT);
		this.gcTimeThreshold = configuration.get(SpillableOptions.GC_TIME_THRESHOLD).toMillis();

		float localLoadStartRatio = configuration.get(SpillableOptions.LOAD_START_RATIO);
		float localLoadEndRatio = configuration.get(SpillableOptions.LOAD_END_RATIO);
		// Check and make sure loadStartSize < loadEndSize < spillThreshold after separate adjustment
		if (localLoadStartRatio >= localLoadEndRatio) {
			LOG.warn("Load start ratio {} >= end ratio {} even with adjustment, "
					+ "will use default (startRatio={}, endRatio={}) instead",
					localLoadStartRatio, localLoadEndRatio,
					SpillableOptions.LOAD_START_RATIO.defaultValue(),
					SpillableOptions.LOAD_END_RATIO.defaultValue());
			localLoadStartRatio = SpillableOptions.LOAD_START_RATIO.defaultValue();
			localLoadEndRatio = SpillableOptions.LOAD_END_RATIO.defaultValue();
		}
		this.loadStartRatio = localLoadStartRatio;
		this.loadEndRatio = localLoadEndRatio;
		this.spillSizeRatio = configuration.get(SpillableOptions.SPILL_SIZE_RATIO);

		this.triggerInterval = configuration.get(SpillableOptions.TRIGGER_INTERVAL).toMillis();
		this.resourceCheckInterval = configuration.get(SpillableOptions.RESOURCE_CHECK_INTERVAL).toMillis();

		this.maxMemory = heapStatusMonitor.getMaxMemory();
		this.loadStartSize = (long) (maxMemory * loadStartRatio);
		this.loadEndSize = (long) (maxMemory * loadEndRatio);

		this.lastResourceCheckTime = System.currentTimeMillis();
		this.lastTriggerTime = System.currentTimeMillis();
	}

	@Override
	public void checkResource() {
		// 获取当前 Unix 时间戳
		long currentTime = System.currentTimeMillis();
		// 如果距离上次资源检查的时间小于阈值, 就不做检查. 配置项为 state.backend.spillable.resource-check.interval, 默认值为 10s
		if (currentTime - lastResourceCheckTime < resourceCheckInterval) {
			return;
		}

		// 重置上次资源检查时间为当前值
		lastResourceCheckTime = currentTime;
		// getMonitorResult will access a volatile variable, so this is a heavy operation
		// getMonitorResult 访问的正是之前介绍的 heapStatusMonitor 里保存的资源负载。由于变量定义为了 volatile, 不能缓存, 因而访问开销较大
		HeapStatusMonitor.MonitorResult monitorResult = heapStatusMonitor.getMonitorResult();
		LOG.debug("Update monitor result {}", monitorResult);

		// monitor hasn't update result
		// 如果和上次获取的 monitorResult 相同, 则不做处理, 直接返回
		if (lastMonitorResult != null && lastMonitorResult.getId() == monitorResult.getId()) {
			return;
		}
		lastMonitorResult = monitorResult;

		// 根据 monitorResult 的值, 决定下一步的动作是 SPILL, LOAD 还是 NONE
		ActionResult checkResult = decideAction(monitorResult);
		LOG.debug("Decide action {}", checkResult);
		// 如果无需动作, 则直接返回
		if (checkResult.action == Action.NONE) {
			return;
		}

		// limit the frequence of spill/load so that monitor can update memory usage after spill/load
		// 如果走到这里, 说明需要进行 SPILL 或 LOAD 动作. 首先确保动作不能太频繁, 如果小于阈值则直接返回, 不做动作
		// triggerInterval 的配置项为 state.backend.spillable.trigger-interval, 默认值为 1 分钟
		if (monitorResult.getTimestamp() - lastTriggerTime < triggerInterval) {
			LOG.debug("Too frequent to spill/load, last time is {}", lastTriggerTime);
			return;
		}

		if (checkResult.action == Action.SPILL) {
			// 调用 doSpill 方法将状态从内存移动到磁盘
			doSpill(checkResult);
		} else {
			// 调用 doLoad 方法将状态从磁盘载入回内存
			doLoad(checkResult);
		}

		// because spill/load may cost much time, so update trigger time after the process is finished
		// spill 和 load 比较耗时, 所有事项做完后再更新 lastTriggerTime 时间
		lastTriggerTime = System.currentTimeMillis();
	}

	//这个方法根据 HeapStatusMonitor 返回的资源观测结果，做出实际的决策。
	//但是从下面的源码注释可以看到，目前决策的依据非常单一且不可靠，
	// 例如 Spill 只判断 GC 时间（这里计算的是近几次的平均时间，经常无法应对突发情况），
	// Load 只判断内存用量等，而且不可动态调整，局限性非常大，因此这里也是迫切需要改进的一个点。
	@VisibleForTesting
	ActionResult decideAction(HeapStatusMonitor.MonitorResult monitorResult) {
		// 获取近期 GC 平均时间
		long gcTime = monitorResult.getGarbageCollectionTime();
		// 获取堆内存总用量
		long usedMemory = monitorResult.getTotalUsedMemory();

		// 1. check whether to spill
		// 1. 检查是否需要触发 SPILL
		if (gcTime > gcTimeThreshold) {// TODO: 目前只有 GC 时间是否超过阈值, 非常简陋
			// TODO whether to calculate spill ration dynamically
			// TODO: 这个 ratio 需要改为动态的, 目前为配置项 state.backend.spillable.spill-size.ratio 设置, 默认值为 0.2, 表示每次有 20% 的状态被移动到磁盘上
			return ActionResult.ofSpill(spillSizeRatio);
		}

		// 2. check whether to load
		// 2. 检查是否需要触发 LOAD
		// TODO: 目前只有一个指标, loadStartSize = maxMemory * loadStartRatio, 而 loadStartRatio 由 state.backend.spillable.load-start.ratio 配置项决定, 默认是 0.1
		if (usedMemory < loadStartSize) {
			// loadEndSize = maxMemory * loadEndRatio, 而 loadEndRatio 由 state.backend.spillable.load-end.ratio 配置项决定, 默认为 0.3
			float loadRatio = (float) (loadEndSize - usedMemory) / usedMemory;
			//决定载入的内存比例
			return ActionResult.ofLoad(loadRatio);
		}
		// 都不需要, 那么无动作
		return ActionResult.ofNone();
	}

	//Spillable Backend 是如何将内存里的状态对象，Spill 到磁盘上的。这个 doSpill 方法由前述的 decideAction 方法调用，
	// 执行具体的 Spill 操作（优先选择访问不频繁、尺寸较大的 KeyGroup 进行 spill）。
	//它包括了筛选和大小估计、权重排序、执行 Spill 等多个步骤，最终达到阈值而完成整个流程。
	@VisibleForTesting
	void doSpill(ActionResult actionResult) {
		// 筛选当前 KeyGroup 并得到统计的元数据 (过滤条件是 onHeap 并且 size 大于 0)
		List<SpillableStateTable.StateMapMeta> onHeapStateMapMetas =
			getStateMapMetas((meta) -> meta.isOnHeap() && meta.getSize() > 0);
		if (onHeapStateMapMetas.isEmpty()) {
			// 如果没有筛选到, 说明不用 spill, 直接返回
			LOG.debug("There is no StateMap to spill.");
			return;
		}
		// 根据 KeyGroup 的状态大小和访问频次进行权重排序, 权重大的放在前面
		sortStateMapMeta(actionResult.action, onHeapStateMapMetas);
		// 获取所有堆内 KeyGroup 状态大小的总和
		long totalSize = onHeapStateMapMetas.stream()
			.map(SpillableStateTable.StateMapMeta::getEstimatedMemorySize)
			.reduce(0L, (a, b) -> a + b); // 可以用 Long::sum 代替
		// 计算需要 spill 的比例
		long spillSize = (long) (totalSize * actionResult.spillOrLoadRatio);

		if (spillSize == 0) {
			return;
		}
		// 配置项 state.backend.spillable.cancel.checkpoint 可以控制是否在 spill 时取消当前的所有进行中的快照（默认为 true）。取消快照可以加快 GC 和 Spill 过程
		if (cancelCheckpoint) {
			checkpointManager.cancelAllCheckpoints();
		}
		// 根据排序得到的 KeyGroup 列表, 从权重最大（访问频次最小、大小最大）的开始, 逐个进行 Spill 操作, 直到达到阈值
		for (SpillableStateTable.StateMapMeta meta : onHeapStateMapMetas) {
			meta.getStateTable().spillState(meta.getKeyGroupIndex());
			LOG.debug("Spill state in key group {} successfully", meta.getKeyGroupIndex());
			spillSize -= meta.getEstimatedMemorySize();
			// 如果 Spill 的大小已经达到了阈值, 就不再继续, 本次 Spill 操作结束
			if (spillSize <= 0) {
				break;
			}
		}
	}

	//doLoad 方法同样由 decideAction 方法调用，是 doSpill 方法的“对偶函数”，
	// 即将状态从磁盘等外存，载入到堆内存中（优先选择访问频繁、尺寸较小的 KeyGroup 进行 load）。
	//整体的函数逻辑与 doSpill 相同，只是更新阈值的代码放在了操作之前，以避免多载入对象到内存中，造成较大压力。
	// 将状态从磁盘载入回堆内存
	@VisibleForTesting
	void doLoad(ActionResult actionResult) {
		// 筛选出所有不在堆内存且状态不为 0 的 KeyGroup 状态列表
		List<SpillableStateTable.StateMapMeta> onDiskStateMapMetas =
			getStateMapMetas((meta) -> !meta.isOnHeap() && meta.getSize() > 0);
		if (onDiskStateMapMetas.isEmpty()) {
			LOG.debug("There is no StateMap to load.");
			return;
		}
		// 对所有的 KeyGroup 状态列表进行权重排序, 最大的 (访问次数最多、状态最小的）放在前面, 优先进行 Load
		sortStateMapMeta(actionResult.action, onDiskStateMapMetas);
		// 计算符合条件的状态总大小
		long totalSize = onDiskStateMapMetas.stream()
			.map(SpillableStateTable.StateMapMeta::getEstimatedMemorySize)
			.reduce(0L, (a, b) -> a + b);
		// 计算出需要载入的最大比例
		long loadSize = (long) (totalSize * actionResult.spillOrLoadRatio);

		if (loadSize == 0) {
			return;
		}
		// 开始载入到内存, 直到满足阈值
		for (SpillableStateTable.StateMapMeta meta : onDiskStateMapMetas) {
			loadSize -= meta.getEstimatedMemorySize();
			// if before do load so that not load more data than the expected
			// Load 时先减去状态大小, 避免多载入了一些状态, 导致内存压力比预期的更大
			if (loadSize < 0) {
				break;
			}
			// 按照 KeyGroup 元数据里面记录的 KeyGroupIndex 来载入状态到当前状态表
			meta.getStateTable().loadState(meta.getKeyGroupIndex());
			LOG.debug("Load state in key group {} successfully", meta.getKeyGroupIndex());
		}
	}

	//特别需要注意的是，它的作用不仅仅在于筛选，而且还在筛选之后，对本 StateMap 里所有状态的大小进行估计，
	// 并保存在前面所述的 StateMapMeta 对象中。这样，后面的权重计算和排序，才有了数据支持。
	private List<SpillableStateTable.StateMapMeta> getStateMapMetas(
			// 传入筛选策略函数, 按条件筛选状态表中符合条件的 KeyGroup 列表
		Function<SpillableStateTable.StateMapMeta, Boolean> stateMapFilter) {
		// 创建一个列表, 以作为本函数返回值
		List<SpillableStateTable.StateMapMeta> stateMapMetas = new ArrayList<>();
		// stateTableContainer 是即当前实例中, 所有已注册的状态名和状态表的 Tuple2 映射
		for (Tuple2<String, SpillableStateTable> tuple : stateTableContainer) {
			int len = stateMapMetas.size();
			// tuple.f1 是某个状态的状态表, 而 tuple.f0 是状态名, 这里用不到
			SpillableStateTable spillableStateTable = tuple.f1;
			// 准备按状态表中 KeyGroup 的顺序, 依次遍历检查该状态表里的所有的状态
			Iterator<SpillableStateTable.StateMapMeta> iterator = spillableStateTable.stateMapIterator();
			// 对该状态表中的每个 KeyGroup 下的状态映射进行遍历
			while (iterator.hasNext()) {
				SpillableStateTable.StateMapMeta meta = iterator.next();
				if (stateMapFilter.apply(meta)) {
					// 如果这个状态表的 KeyGroup 的 Meta 信息符合传入函数的筛选条件, 就加入返回列表
					stateMapMetas.add(meta);
				}
			}
			// 如果发现上述循环中, 新增了符合条件的 KeyGroup, 需要估计表中每个 KeyGroup 的状态大小, 并写入元数据 (Meta) 中
			if (len < stateMapMetas.size()) {
				// 更新对象的平均大小, 注意是平均大小
				long estimatedSize = spillableStateTable.getStateEstimatedSize(true);
				Preconditions.checkState(estimatedSize >= 0,
					"state estimated size should be positive but is {}", estimatedSize);

				// update estimated state map memory on heap
				// 逐个更新每个新增的 KeyGroup 元数据中堆内存的估计大小, 计算公式是: 状态总数 * 估计的状态平均大小
				for (int i = len; i < stateMapMetas.size(); i++) {
					SpillableStateTable.StateMapMeta stateMapMeta = stateMapMetas.get(i);
					// 状态总数 * 估计的状态平均大小
					stateMapMeta.setEstimatedMemorySize(stateMapMeta.getSize() * estimatedSize);
				}
			}
		}
		// 返回含有最新状态估计大小的状态表元数据
		return stateMapMetas;
	}

	//会根据传入的 StateMapMeta 列表，将其归一化以后，按照既定的规则进行权重排序。
	//需要注意的是，权重计算的方法在 computeWeight 方法中，根据 Spill（优先选择访问频率低、尺寸大的状态进行 Spill）
	// 还是 Load（优先选择访问频率高、尺寸小的状态进行 Load）有完全相反的权重计算方法。
	//这个方法利用了 Java 自带的排序机制，通过自定义 Comparator 的方式，让权重最大的对象排在前面，
	// 这样构造了一个优先队列，前面介绍的doSpill 和 doLoad 方法都只需要从首部开始处理即可。
	private void sortStateMapMeta(Action action, List<SpillableStateTable.StateMapMeta> stateMapMetas) {
		if (stateMapMetas.isEmpty()) {
			return;
		}

		// We use formula (X - Xmin)/(Xmax - Xmin) for normalization, to make sure the normalized value range is [0,1]
		// 使用 (X - Xmin)/(Xmax - Xmin) 公式来进行归一化, 以确保归一化后的值域位于 [0,1]
		long sizeMax = 0L, sizeMin = Long.MAX_VALUE, requestMax = 0L, requestMin = Long.MAX_VALUE;
		// 统计最大、最小的的 KeyGroup 状态对象总大小
		for (SpillableStateTable.StateMapMeta meta : stateMapMetas) {
			long estimatedMemorySize = meta.getEstimatedMemorySize();
			sizeMax = Math.max(sizeMax, estimatedMemorySize);
			sizeMin = Math.min(sizeMin, estimatedMemorySize);
			// 获取该 KeyGroup 状态的总请求数
			long numRequests = meta.getNumRequests();
			requestMax = Math.max(requestMax, numRequests);
			requestMin = Math.min(requestMin, numRequests);
		}
		// 根据上述公式进行归一化
		final long sizeDenominator = sizeMax - sizeMin;
		final long requestDenominator = requestMax - requestMin;
		final long sizeMinForCompare = sizeMin;
		final long requestMinForCompare = requestMin;
		// 准备一个 Map 来表示各个 KeyGroup 的相对权重
		final Map<SpillableStateTable.StateMapMeta, Double> computedWeights = new IdentityHashMap<>();
		Comparator<SpillableStateTable.StateMapMeta> comparator = (o1, o2) -> {// 创建一个 KeyGroup 权重的 Comparator 用来排序
			if (o1 == o2) {
				return 0;
			}
			if (o1 == null) {
				return -1;
			}
			if (o2 == null) {
				return 1;
			}
			// 计算第一个 KeyGroup 状态的权重, 并放入 computedWeights Map 中准备排序
			double weight1 = computedWeights.computeIfAbsent(o1,
				k -> computeWeight(k, action, sizeMinForCompare, requestMinForCompare, sizeDenominator,
					requestDenominator)); // computeWeight 的公式是 (weightRetainedSize * normalizedSize + weightRequestRate * normalizedRequest) / weightSum
			// 计算第二个 KeyGroup 状态的权重, 并放入 computedWeights Map 中准备排序
			double weight2 = computedWeights.computeIfAbsent(o2,
				k -> computeWeight(k, action, sizeMinForCompare, requestMinForCompare, sizeDenominator,
					requestDenominator));
			// The StateMapMeta with higher weight should be spill/load first, and we will use priority queue
			// which is a minimum heap, so we return -1 here if weight is higher
			// 对比权重, 然后对大的一方优先返回 -1, 这样在排序时会排到最前面, 优先进行 SPILL 或者 LOAD 等动作
			return (weight1 > weight2) ? -1 : 1;
		};
		// 进行排序, 令大的 KeyGroup 可以出现在最前面
		stateMapMetas.sort(comparator);
	}

	/**
	 * Compute the weight of the given RowMapMeta.
	 * The formula is weighted average on the normalized retained-size and request-count
	 *  computeWeight 方法决定了排序时的先后顺序。可以看到，对于 SPILL 和 LOAD，计算的公式可以说是相反的
	 *  ，但是最终目的一致：根据概率原理，留在内存里的相对都是访问频繁及占空间较小的对象，有利于保持性能。
	 *
	 * @param meta               the StateMapMeta to compute weight against
	 * @param action             the type of action
	 * @param sizeMin            the minimum retained-size of all RowMapMeta instances
	 * @param requestMin         the minimum request-count of all RowMapMeta instances
	 * @param sizeDenominator    the Xmax minus Xmin result of retained-size
	 * @param requestDenominator the Xmax minus Xmin result of request-count
	 * @return the computed weight
	 */
	private double computeWeight(
		SpillableStateTable.StateMapMeta meta,
		Action action,
		long sizeMin, long requestMin,
		long sizeDenominator, long requestDenominator) {
		// 对本 KeyGroup 的状态大小进行归一化
		double normalizedSize = sizeDenominator == 0L ? 0.0 : (meta.getEstimatedMemorySize() - sizeMin) / (double) sizeDenominator;
		// 对本 KeyGroup 里状态的请求次数进行归一化
		double normalizedRequest =
			requestDenominator == 0L ? 0.0 : (meta.getNumRequests() - requestMin) / (double) requestDenominator;
		double weightRetainedSize, weightRequestRate, weightSum;
		switch (action) {
			// 如果是 SPILL 操作, 倾向于选择访问不频繁的、较大的 Bucket 来进行
			case SPILL:
				weightRetainedSize = WEIGHT_SPILL_RETAINED_SIZE;// 固定为 0.7, 正权重
				weightRequestRate = WEIGHT_SPILL_REQUEST_RATE;// 固定为 -0.3, 负权重
				weightSum = WEIGHT_SPILL_SUM;// 固定为前两者之和, 即 0.4
				break;
			// 如果是 LOAD 操作, 倾向于选择请求频繁的、较小的 Bucket 来进行
			case LOAD:
				weightRetainedSize = WEIGHT_LOAD_RETAINED_SIZE;// 固定为 -0.3, 正权重
				weightRequestRate = WEIGHT_LOAD_REQUEST_RATE;// 固定为 0.7, 负权重
				weightSum = WEIGHT_LOAD_SUM;// 固定为前两者之和, 即 0.4
				break;
			default:
				throw new RuntimeException("Unsupported action: " + action);
		}
		// 如果是 Spill, 公式目前为 (0.7*normalizedSize-0.3*normalizedRequest)/0.4; 如果是 Load, 公式目前是 (0.7*normalizedRequest-0.3*normalizedSize)/0.4
		return (weightRetainedSize * normalizedSize + weightRequestRate * normalizedRequest) / weightSum;
	}

	private float floatSum(float d1, float d2) {
		BigDecimal bd1 = new BigDecimal(Float.toString(d1));
		BigDecimal bd2 = new BigDecimal(Float.toString(d2));
		return bd1.add(bd2).floatValue();
	}

	private float floatSub(float d1, float d2) {
		BigDecimal bd1 = new BigDecimal(Float.toString(d1));
		BigDecimal bd2 = new BigDecimal(Float.toString(d2));
		return bd1.subtract(bd2).floatValue();
	}

	@VisibleForTesting
	long getGcTimeThreshold() {
		return gcTimeThreshold;
	}

	@VisibleForTesting
	long getTriggerInterval() {
		return triggerInterval;
	}

	@VisibleForTesting
	long getResourceCheckInterval() {
		return resourceCheckInterval;
	}

	@VisibleForTesting
	long getMaxMemory() {
		return maxMemory;
	}

	public float getSpillSizeRatio() {
		return spillSizeRatio;
	}

	@VisibleForTesting
	long getLoadStartSize() {
		return loadStartSize;
	}

	@VisibleForTesting
	long getLoadEndSize() {
		return loadEndSize;
	}

	/**
	 * Enumeration of action.
	 */
	//Action 是一个枚举类型，有 SPILL、LOAD、NONE 三种取值，
	// 分别表示三种动作（NONE 表示”无动作“这种动作），因此很容易猜到，它是本方法的决策信号，决定了相关状态的去向。
	enum Action {
		NONE, SPILL, LOAD
	}

	//ActionResult 则是对 Action 及比例参数（spillOrLoadRatio）的封装类，
	// 作为行动的指南：例如传入了 SPILL Action 和 0.2 的 spillOrLoadRatio 作为 ActionResult, 表明需要把 20% 的冷状态写入磁盘。
	static class ActionResult {
		Action action;
		float spillOrLoadRatio;

		ActionResult(Action action, float spillOrLoadRatio) {
			this.action = action;
			this.spillOrLoadRatio = spillOrLoadRatio;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}

			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}

			ActionResult other = (ActionResult) obj;
			return action == other.action &&
				spillOrLoadRatio == other.spillOrLoadRatio;
		}

		@Override
		public String toString() {
			return "ActionResult{" +
				"action=" + action +
				", spillOrLoadRatio=" + spillOrLoadRatio +
				'}';
		}

		static ActionResult ofNone() {
			return new ActionResult(Action.NONE, 0.0f);
		}

		static ActionResult ofSpill(float ratio) {
			return new ActionResult(Action.SPILL, ratio);
		}

		static ActionResult ofLoad(float ratio) {
			return new ActionResult(Action.LOAD, ratio);
		}
	}

	interface StateTableContainer extends Iterable<Tuple2<String, SpillableStateTable>> {
	}

	static class StateTableContainerImpl<K> implements StateTableContainer {

		private final Map<String, StateTable<K, ?, ?>> registeredKVStates;

		public StateTableContainerImpl(Map<String, StateTable<K, ?, ?>> registeredKVStates) {
			this.registeredKVStates = registeredKVStates;
		}

		@Override
		public Iterator<Tuple2<String, SpillableStateTable>> iterator() {
			return new Iterator<Tuple2<String, SpillableStateTable>>() {
				private final Iterator<Map.Entry<String, StateTable<K, ?, ?>>> iter =
					registeredKVStates.entrySet().iterator();

				@Override
				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public Tuple2<String, SpillableStateTable> next() {
					Map.Entry<String, StateTable<K, ?, ?>> entry = iter.next();
					return Tuple2.of(entry.getKey(), (SpillableStateTable) entry.getValue());
				}
			};
		}
	}
}
