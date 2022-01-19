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

import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitor memory usage and garbage collection.
 * 监控内存使用和垃圾收集。
 */
public class HeapStatusMonitor {

	private static final Logger LOG = LoggerFactory.getLogger(HeapStatusMonitor.class);

	/** Single instance in a JVM process. */
	private static HeapStatusMonitor statusMonitor;

	/** Interval to check memory usage. */
	//堆内存检测周期
	private final long checkIntervalInMs;
	//JVM 自带的 MemoryMXBean 对象，可以从中获取当前的堆内存用量、堆内存最大值等
	private final MemoryMXBean memoryMXBean;
	//从上述 Bean 中获取的堆内存最大值，用来作为一个常量。
	private final long maxMemory;
	//JVM 自带的描述 GC 统计信息的 MXBean 对象列表（不止一个）。
	private final List<GarbageCollectorMXBean> garbageCollectorMXBeans;

	/** Generate ascending id for each monitor result. */
	//为每次检测的结果生成一个自增的 ID。因为可能并发访问，这里需要使用 AtomicLong 对象。
	private final AtomicLong resultIdGenerator;

	/** Executor to check memory usage periodically. */
	//创建一个周期执行器，并设置相关的取消策略。
	private final ScheduledThreadPoolExecutor checkExecutor;
	//向上述周期执行器提交一个周期为 checkIntervalInMs，会定时运行本类的 runCheck()方法，来检查堆内存的情况，并生成对应的 monitorResult。
	private final ScheduledFuture checkFuture;

	/** The latest monitor result. */
	//描述单次的检测结果。MonitorResult 对象包含了当前时间戳、ID、堆内存用量、GC 时间等关键信息。当然，目前这些指标还是太少，生产环境还是需要更多决策项。
	private volatile MonitorResult monitorResult;

	/** Time for gc when last check. */
	//上次 GC 的时间值
	private long lastGcTime;

	/** Number of gc when last check. */
	//上次 GC 的统计数
	private long lastGcCount;

	/** Flag to signify that the monitor has been shut down already. */
	private final AtomicBoolean isShutdown = new AtomicBoolean();

	/** Shutdown hook to make sure that scheduler is closed. */
	//Flink 里常见的用法，注册一个 JVM 的 shutdown hook，这样在进程关闭时，可以打印相关的日志，并设置 isShutdown 环境变量。
	private final Thread shutdownHook;

	HeapStatusMonitor(long checkIntervalInMs) {
		Preconditions.checkArgument(checkIntervalInMs > 0, "Check interval should be positive.");
		this.checkIntervalInMs = checkIntervalInMs;
		this.memoryMXBean = ManagementFactory.getMemoryMXBean();
		this.maxMemory = memoryMXBean.getHeapMemoryUsage().getMax();
		this.garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
		this.resultIdGenerator = new AtomicLong(0L);
		this.monitorResult = new MonitorResult(System.currentTimeMillis(), resultIdGenerator.getAndIncrement(),
			memoryMXBean.getHeapMemoryUsage(), 0);
		this.lastGcTime = 0L;
		this.lastGcCount = 0L;

		this.shutdownHook = ShutdownHookUtil.addShutdownHook(this::shutDown, getClass().getSimpleName(), LOG);

		this.checkExecutor = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("memory-status-monitor"));
		this.checkExecutor.setRemoveOnCancelPolicy(true);
		this.checkExecutor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		this.checkExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

		this.checkFuture = this.checkExecutor.scheduleWithFixedDelay(this::runCheck, 10, checkIntervalInMs, TimeUnit.MILLISECONDS);

		LOG.info("Max memory {}, Check interval {}", maxMemory, checkIntervalInMs);
	}

	//定期将当前的堆内存用量，以及最近一次 GC 的平均时间保存在本实例的 monitorResult 对象中，
	// 以备决策者读取。堆内存用量的获取非常直白，即直接获取 memoryMXBean 的 getHeapMemoryUsage() 方法即可
	private void runCheck() {
		long timestamp = System.currentTimeMillis();
		long id = resultIdGenerator.getAndIncrement();
		this.monitorResult = new MonitorResult(timestamp, id, memoryMXBean.getHeapMemoryUsage(), getGarbageCollectionTime());
		if (LOG.isDebugEnabled()) {
			LOG.debug("Check memory status, {}", monitorResult.toString());
		}
	}

	//这个方法用来获取最近 GC 的平均时间,通过一个 for 循环，遍历 garbageCollectorMXBeans 列表里的所有 GC 的 MXBean，
	// 然后逐个读取当前的 GC 次数和时间，加到变量里。然后将得到的总次数和总时间，分别减去上次记录的值（lastGcCount 和 lastGcTime），
	// 然后进行相除，就可以得到本次检测时的 GC 平均时间了。
	private long getGarbageCollectionTime() {
		long count = 0;
		long timeMillis = 0;
		//通过一个 for 循环，遍历 garbageCollectorMXBeans 列表里的所有 GC 的 MXBean
		for (GarbageCollectorMXBean gcBean : garbageCollectorMXBeans) {
			//然后逐个读取当前的 GC 次数和时间，加到变量里。然后将得到的总次数和总时间
			long c = gcBean.getCollectionCount();
			long t = gcBean.getCollectionTime();
			count += c;
			timeMillis += t;
		}

		if (count == lastGcCount) {
			return 0;
		}
		//分别减去上次记录的值（lastGcCount 和 lastGcTime）,然后进行相除，就可以得到本次检测时的 GC 平均时间了。
		long gcCountIncrement = count - lastGcCount;
		long averageGcTime = (timeMillis - lastGcTime) / gcCountIncrement;

		lastGcCount = count;
		lastGcTime = timeMillis;

		return averageGcTime;
	}

	public MonitorResult getMonitorResult() {
		return monitorResult;
	}

	public long getMaxMemory() {
		return maxMemory;
	}

	void shutDown() {
		if (!isShutdown.compareAndSet(false, true)) {
			return;
		}

		ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

		if (checkFuture != null) {
			checkFuture.cancel(true);
		}

		if (checkExecutor != null) {
			checkExecutor.shutdownNow();
		}

		LOG.info("Memory monitor is shutdown.");
	}

	public static HeapStatusMonitor getStatusMonitor() {
		return statusMonitor;
	}

	public static void initStatusMonitor(long checkIntervalInMs) {
		synchronized (HeapStatusMonitor.class) {
			if (statusMonitor != null) {
				return;
			}

			statusMonitor = new HeapStatusMonitor(checkIntervalInMs);
		}
	}

	/**
	 * Monitor result.
	 */
	static class MonitorResult {

		/** Time of status. */
		private final long timestamp;

		/** Unique id of status. */
		private final long id;

		private final long totalMemory;

		private final long totalUsedMemory;

		private final long garbageCollectionTime;

		MonitorResult(long timestamp, long id, MemoryUsage memoryUsage, long garbageCollectionTime) {
			this(timestamp, id, memoryUsage.getMax(), memoryUsage.getUsed(), garbageCollectionTime);
		}

		MonitorResult(long timestamp, long id, long totalMemory, long totalUsedMemory, long garbageCollectionTime) {
			this.timestamp = timestamp;
			this.id = id;
			this.totalMemory = totalMemory;
			this.totalUsedMemory = totalUsedMemory;
			this.garbageCollectionTime = garbageCollectionTime;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public long getId() {
			return id;
		}

		public long getTotalMemory() {
			return totalMemory;
		}

		public long getTotalUsedMemory() {
			return totalUsedMemory;
		}

		public long getGarbageCollectionTime() {
			return garbageCollectionTime;
		}

		@Override
		public String toString() {
			return "MonitorResult{" +
				"timestamp=" + timestamp +
				", id=" + id +
				", totalMemory=" + totalMemory +
				", totalUsedMemory=" + totalUsedMemory +
				", garbageCollectionTime=" + garbageCollectionTime +
				'}';
		}
	}
}
