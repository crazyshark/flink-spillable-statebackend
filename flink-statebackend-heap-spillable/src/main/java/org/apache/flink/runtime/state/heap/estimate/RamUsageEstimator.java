/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap.estimate;

import org.apache.flink.api.common.state.State;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/**
 * Estimates the size (memory representation) of Java objects.
 * 估计 Java 对象的大小（内存表示）。
 *
 * <p>This class uses assumptions that were discovered for the Hotspot
 * virtual machine. If you use a non-OpenJDK/Oracle-based JVM,
 * the measurements may be slightly wrong.
 * 此类使用为 Hotspot 虚拟机发现的假设。 如果您使用非基于 OpenJDK/Oracle 的 JVM，则测量结果可能略有错误。
 * Refer to https://github.com/apache/lucene-solr/blob/master/lucene/core/src/java/org/apache/lucene/util/RamUsageEstimator.java.
 * 	//Shallow Size
 * 	//对象自身占用的内存大小，不包括它引用的对象。
 * 	//针对非数组类型的对象，它的大小就是对象与它所有的成员变量大小的总和。当然这里面还会包括一些java语言特性的数据存储单元。
 * 	//针对数组类型的对象，它的大小是数组元素对象的大小总和。
 * 	//
 * 	//Retained Size
 * 	//Retained Size=当前对象大小+当前对象可直接或间接引用到的对象的大小总和。(间接引用的含义：A->B->C, C就是间接引用)
 * 	//换句话说，Retained Size就是当前对象被GC后，从Heap上总共能释放掉的内存。
 * 	//不过，释放的时候还要排除被GC Roots直接或间接引用的对象。他们暂时不会被被当做Garbage。
 */
public final class RamUsageEstimator {

	/** One kilobyte bytes. */
	//1K
	public static final long ONE_KB = 1024;

	/** One megabyte bytes. */
	//1M
	public static final long ONE_MB = ONE_KB * ONE_KB;

	/** One gigabyte bytes.*/
	//1G
	public static final long ONE_GB = ONE_KB * ONE_MB;

	/** No instantiation. */
	private RamUsageEstimator() {}

	/**
	 * True, iff compressed references (oops) are enabled by this JVM.
	 * 布尔值，如果为true说明JVM开启了指针压缩（即启用了-XX:+UseCompressedOops）
	 */
	public static final boolean COMPRESSED_REFS_ENABLED;

	/**
	 * Number of bytes this JVM uses to represent an object reference.
	 * 引用对象需要占用的大小
	 */
	public static final  int NUM_BYTES_OBJECT_REF;

	/**
	 * Number of bytes to represent an object header (no fields, no alignments).
	 * 对象头占用的大小
	 */
	public static final int NUM_BYTES_OBJECT_HEADER;

	/**
	 * Number of bytes to represent an array header (no content, but with alignments).
	 * 数组头占用的大小
	 */
	public static final int NUM_BYTES_ARRAY_HEADER;

	/**
	 * A constant specifying the object alignment boundary inside the JVM. Objects will
	 * always take a full multiple of this constant, possibly wasting some space.
	 * 对齐值，一个对象通过填充值（padding value）使其对象大小为NUM_BYTES_OBJECT_ALIGNMENT的整数倍（最接近的）
	 */
	public static final int NUM_BYTES_OBJECT_ALIGNMENT;

	/**
	 * Approximate memory usage that we assign to all unknown objects -
	 * this maps roughly to a few primitive fields and a couple short String-s.
	 * 我们分配给所有未知对象的近似内存使用量 - 这大致映射到一些原始字段和几个短字符串。
	 */
	public static final int UNKNOWN_DEFAULT_RAM_BYTES_USED = 256;

	/**
	 * Sizes of primitive classes.
	 * 原始数据类型的大小
	 */
	public static final Map<Class<?>, Integer> PRIMITIVE_SIZES;

	static {
		//设置基本数据类型的大小，所占的字节数
		Map<Class<?>, Integer> primitiveSizesMap = new IdentityHashMap<>();
		primitiveSizesMap.put(boolean.class, 1);
		primitiveSizesMap.put(byte.class, 1);
		primitiveSizesMap.put(char.class, Integer.valueOf(Character.BYTES));
		primitiveSizesMap.put(short.class, Integer.valueOf(Short.BYTES));
		primitiveSizesMap.put(int.class, Integer.valueOf(Integer.BYTES));
		primitiveSizesMap.put(float.class, Integer.valueOf(Float.BYTES));
		primitiveSizesMap.put(double.class, Integer.valueOf(Double.BYTES));
		primitiveSizesMap.put(long.class, Integer.valueOf(Long.BYTES));

		PRIMITIVE_SIZES = Collections.unmodifiableMap(primitiveSizesMap);
	}

	/**
	 * JVMs typically cache small longs. This tries to find out what the range is.
	 */
	static final int LONG_SIZE, STRING_SIZE;

	/** For testing only. */
	static final boolean JVM_IS_HOTSPOT_64BIT;

	static final String MANAGEMENT_FACTORY_CLASS = "java.lang.management.ManagementFactory";
	static final String HOTSPOT_BEAN_CLASS = "com.sun.management.HotSpotDiagnosticMXBean";

	/**
	 * Initialize constants and try to collect information about the JVM internals.
	 * 初始化常量并尝试收集有关 JVM 内部的信息。
	 */
	static {
		if (Constants.JRE_IS_64BIT) {
			// Try to get compressed oops and object alignment (the default seems to be 8 on Hotspot);
			// (this only works on 64 bit, on 32 bits the alignment and reference size is fixed):
			boolean compressedOops = false;
			int objectAlignment = 8;
			boolean isHotspot = false;
			try {
				final Class<?> beanClazz = Class.forName(HOTSPOT_BEAN_CLASS);
				// we use reflection for this, because the management factory is not part
				// of Java 8's compact profile:
				final Object hotSpotBean = Class.forName(MANAGEMENT_FACTORY_CLASS)
					.getMethod("getPlatformMXBean", Class.class)
					.invoke(null, beanClazz);
				if (hotSpotBean != null) {
					isHotspot = true;
					final Method getVMOptionMethod = beanClazz.getMethod("getVMOption", String.class);
					try {
						final Object vmOption = getVMOptionMethod.invoke(hotSpotBean, "UseCompressedOops");
						compressedOops = Boolean.parseBoolean(
							vmOption.getClass().getMethod("getValue").invoke(vmOption).toString()
						);
					} catch (ReflectiveOperationException | RuntimeException e) {
						isHotspot = false;
					}
					try {
						final Object vmOption = getVMOptionMethod.invoke(hotSpotBean, "ObjectAlignmentInBytes");
						objectAlignment = Integer.parseInt(
							vmOption.getClass().getMethod("getValue").invoke(vmOption).toString()
						);
					} catch (ReflectiveOperationException | RuntimeException e) {
						isHotspot = false;
					}
				}
			} catch (ReflectiveOperationException | RuntimeException e) {
				isHotspot = false;
			}
			JVM_IS_HOTSPOT_64BIT = isHotspot;
			COMPRESSED_REFS_ENABLED = compressedOops;
			NUM_BYTES_OBJECT_ALIGNMENT = objectAlignment;
			// reference size is 4, if we have compressed oops:
			NUM_BYTES_OBJECT_REF = COMPRESSED_REFS_ENABLED ? 4 : 8;
			// "best guess" based on reference size:
			NUM_BYTES_OBJECT_HEADER = 8 + NUM_BYTES_OBJECT_REF;
			// array header is NUM_BYTES_OBJECT_HEADER + NUM_BYTES_INT, but aligned (object alignment):
			NUM_BYTES_ARRAY_HEADER = (int) alignObjectSize(NUM_BYTES_OBJECT_HEADER + Integer.BYTES);
		} else {
			JVM_IS_HOTSPOT_64BIT = false;
			COMPRESSED_REFS_ENABLED = false;
			NUM_BYTES_OBJECT_ALIGNMENT = 8;
			NUM_BYTES_OBJECT_REF = 4;
			NUM_BYTES_OBJECT_HEADER = 8;
			// For 32 bit JVMs, no extra alignment of array header:
			NUM_BYTES_ARRAY_HEADER = NUM_BYTES_OBJECT_HEADER + Integer.BYTES;
		}

		LONG_SIZE = (int) shallowSizeOfInstance(Long.class);
		STRING_SIZE = (int) shallowSizeOfInstance(String.class);
	}

	/** Approximate memory usage that we assign to a Hashtable / HashMap entry.
	 * 我们分配给 Hashtable / HashMap 条目的近似内存使用量。
	 * */
	public static final long HASHTABLE_RAM_BYTES_PER_ENTRY =
		2 * NUM_BYTES_OBJECT_REF // key + value
			* 2; // hash tables need to be oversized to avoid collisions, assume 2x capacity

	/** Approximate memory usage that we assign to a LinkedHashMap entry.
	 * 我们分配给 LinkedHashMap 条目的近似内存使用量。
	 * */
	public static final long LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY =
		HASHTABLE_RAM_BYTES_PER_ENTRY
			+ 2 * NUM_BYTES_OBJECT_REF; // previous & next references

	/**
	 * Aligns an object size to be the next multiple of {@link #NUM_BYTES_OBJECT_ALIGNMENT}.
	 * 将对象大小对齐为 {@link #NUM_BYTES_OBJECT_ALIGNMENT} 的下一个倍数。
	 */
	public static long alignObjectSize(long size) {
		size += (long) NUM_BYTES_OBJECT_ALIGNMENT - 1L;
		return size - (size % NUM_BYTES_OBJECT_ALIGNMENT);
	}

	/**
	 * Return the size of the provided {@link Long} object, returning 0 if it is
	 * cached by the JVM and its shallow size otherwise.
	 * 返回提供的 {@link Long} 对象的大小，如果它被 JVM 缓存，则返回 0，否则返回浅大小。
	 */
	public static long sizeOf(Long value) {
		return LONG_SIZE;
	}

	/** Returns the size in bytes of the byte[] object. */
	public static long sizeOf(byte[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + arr.length);
	}

	/** Returns the size in bytes of the boolean[] object. */
	public static long sizeOf(boolean[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + arr.length);
	}

	/** Returns the size in bytes of the char[] object. */
	public static long sizeOf(char[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Character.BYTES * arr.length);
	}

	/** Returns the size in bytes of the short[] object. */
	public static long sizeOf(short[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Short.BYTES * arr.length);
	}

	/** Returns the size in bytes of the int[] object. */
	public static long sizeOf(int[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * arr.length);
	}

	/** Returns the size in bytes of the float[] object. */
	public static long sizeOf(float[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Float.BYTES * arr.length);
	}

	/** Returns the size in bytes of the long[] object. */
	public static long sizeOf(long[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * arr.length);
	}

	/** Returns the size in bytes of the double[] object. */
	public static long sizeOf(double[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Double.BYTES * arr.length);
	}

	/** Returns the size in bytes of the String[] object. */
	public static long sizeOf(String[] arr) {
		long size = shallowSizeOf(arr);
		for (String s : arr) {
			if (s == null) {
				continue;
			}
			size += sizeOf(s);
		}
		return size;
	}

	/** Returns the size in bytes of the String object. */
	public static long sizeOf(String s) {
		if (s == null) {
			return 0;
		}
		// may not be true in Java 9+ and CompactStrings - but we have no way to determine this

		// char[] + hashCode
		long size = STRING_SIZE + (long) NUM_BYTES_ARRAY_HEADER + (long) Character.BYTES * s.length();
		return alignObjectSize(size);
	}

	/** Same as calling <code>sizeOf(obj, DEFAULT_FILTER)</code>. */
	public static long sizeOf(Object obj) {
		return sizeOf(obj, new Accumulator());
	}

	/**
	 * Estimates the RAM usage by the given object. It will
	 * walk the object tree and sum up all referenced objects.
	 * 估计给定对象的 RAM 使用情况。 它将遍历对象树并总结所有引用的对象。
	 * <p><b>Resource Usage:</b> This method internally uses a set of
	 * every object seen during traversals so it does allocate memory
	 * (it isn't side-effect free). After the method exits, this memory
	 * should be GCed.</p>
	 * <p><b>资源使用：</b>此方法在内部使用遍历期间看到的每个对象的集合，因此它确实分配了内存（它不是无副作用的）。 方法退出后，这块内存应该被GCed。</p>
	 */
	public static long sizeOf(Object obj, Accumulator accumulator) {
		return measureObjectSize(obj, accumulator);
	}

	//Shallow Size
	//对象自身占用的内存大小，不包括它引用的对象。
	//针对非数组类型的对象，它的大小就是对象与它所有的成员变量大小的总和。当然这里面还会包括一些java语言特性的数据存储单元。
	//针对数组类型的对象，它的大小是数组元素对象的大小总和。
	//
	//Retained Size
	//Retained Size=当前对象大小+当前对象可直接或间接引用到的对象的大小总和。(间接引用的含义：A->B->C, C就是间接引用)
	//换句话说，Retained Size就是当前对象被GC后，从Heap上总共能释放掉的内存。
	//不过，释放的时候还要排除被GC Roots直接或间接引用的对象。他们暂时不会被被当做Garbage。


	/** Returns the shallow size in bytes of the Object[] object. */
	// Use this method instead of #shallowSizeOf(Object) to avoid costly reflection
	public static long shallowSizeOf(Object[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_OBJECT_REF * arr.length);
	}

	/**
	 * Estimates a "shallow" memory usage of the given object. For arrays, this will be the
	 * memory taken by array storage (no subreferences will be followed). For objects, this
	 * will be the memory taken by the fields.
	 * JVM object alignments are also applied.
	 * 估计给定对象的“浅”内存使用情况。 对于数组，这将是数组存储占用的内存（不会跟随子引用）。 对于对象，这将是字段占用的内存。 JVM 对象对齐也适用。
	 */
	public static long shallowSizeOf(Object obj) {
		if (obj == null) {
			return 0;
		}
		final Class<?> clz = obj.getClass();
		//根据类是否是数组
		if (clz.isArray()) {
			//计算数组的Shallow Size大小
			return shallowSizeOfArray(obj);
		} else {
			//计算实例类的Shallow Size大小
			return shallowSizeOfInstance(clz);
		}
	}

	/**
	 * Returns the shallow instance size in bytes an instance of the given class would occupy.
	 * This works with all conventional classes and primitive types, but not with arrays
	 * (the size then depends on the number of elements and varies from object to object).
	 * 返回给定类的实例将占用的浅实例大小（以字节为单位）。 这适用于所有常规类和原始类型，但不适用于数组（大小取决于元素的数量，并且因对象而异）。
	 *
	 * @see #shallowSizeOf(Object)
	 * @throws IllegalArgumentException if {@code clazz} is an array class.
	 */
	public static long shallowSizeOfInstance(Class<?> clazz) {
		if (clazz.isArray()) {
			throw new IllegalArgumentException("This method does not work with array classes.");
		}
		//返回基本数据类型大小
		if (clazz.isPrimitive()) {
			return PRIMITIVE_SIZES.get(clazz);
		}
		//对象头大小
		long size = NUM_BYTES_OBJECT_HEADER;

		// Walk type hierarchy
		for (; clazz != null; clazz = clazz.getSuperclass()) {
			final Class<?> target = clazz;
			final Field[] fields = AccessController.doPrivileged(new PrivilegedAction<Field[]>() {
				@Override
				public Field[] run() {
					return target.getDeclaredFields();
				}
			});
			//循环叠加类中除了静态字段以外字段大小
			for (Field f : fields) {
				if (!Modifier.isStatic(f.getModifiers())) {
					size = adjustForField(size, f);
				}
			}
		}
		//返回对齐后的大小
		return alignObjectSize(size);
	}

	/**
	 * Return shallow size of any <code>array</code>.
	 */
	private static long shallowSizeOfArray(Object array) {
		long size = NUM_BYTES_ARRAY_HEADER;
		//获取数组的长度
		final int len = Array.getLength(array);
		if (len > 0) {
			//获取数组中数据类型
			Class<?> arrayElementClazz = array.getClass().getComponentType();
			//如果数据类型是基本类型使用长度 * 基本类型大小酸楚数组大小
			if (arrayElementClazz.isPrimitive()) {
				size += (long) len * PRIMITIVE_SIZES.get(arrayElementClazz);
			} else {
				//如果数据类型为引用类型，就使用长度 * 引用对象大小
				size += (long) NUM_BYTES_OBJECT_REF * len;
			}
		}
		//获取对齐后的大小
		return alignObjectSize(size);
	}

	/**
	 * This method returns the maximum representation size of an object. <code>sizeSoFar</code>
	 * is the object's size measured so far. <code>f</code> is the field being probed.
	 * 此方法返回对象的最大表示大小。 <code>sizeSoFar</code> 是到目前为止测量的对象大小。 <code>f</code> 是被探测的字段。
	 *
	 * <p>The returned offset will be the maximum of whatever was measured so far and
	 * <code>f</code> field's offset and representation size (unaligned).
	 * <p>返回的偏移量将是迄今为止测量的最大值和 <code>f</code> 字段的偏移量和表示大小（未对齐）。
	 */
	static long adjustForField(long sizeSoFar, final Field f) {
		final Class<?> type = f.getType();
		//如果字段是基本类型就使用基本类型的大小，否则就是用引用字段大小，这里的引用字段不是说引用指向的字段，而是引用本身大小
		final int fsize = type.isPrimitive() ? PRIMITIVE_SIZES.get(type) : NUM_BYTES_OBJECT_REF;
		// TODO: No alignments based on field type/ subclass fields alignments?
		return sizeSoFar + fsize;
	}

	/**
	 * Returns <code>size</code> in human-readable units (GB, MB, KB or bytes).
	 */
	public static String humanReadableUnits(long bytes) {
		return humanReadableUnits(bytes,
			new DecimalFormat("0.#", DecimalFormatSymbols.getInstance(Locale.ROOT)));
	}

	/**
	 * Returns <code>size</code> in human-readable units (GB, MB, KB or bytes).
	 */
	public static String humanReadableUnits(long bytes, DecimalFormat df) {
		if (bytes / ONE_GB > 0) {
			return df.format((float) bytes / ONE_GB) + " GB";
		} else if (bytes / ONE_MB > 0) {
			return df.format((float) bytes / ONE_MB) + " MB";
		} else if (bytes / ONE_KB > 0) {
			return df.format((float) bytes / ONE_KB) + " KB";
		} else {
			return bytes + " bytes";
		}
	}

	/**
	 * Return a human-readable size of a given object.
	 * @see #sizeOf(Object)
	 * @see RamUsageEstimator#humanReadableUnits(long)
	 */
	public static String humanSizeOf(Object object) {
		return RamUsageEstimator.humanReadableUnits(sizeOf(object));
	}

	/**
	 * An accumulator of object references. This class allows for customizing RAM usage estimation.
	 * 对象引用的累加器。 此类允许自定义 RAM 使用估计。
	 */
	public static class Accumulator {

		private static final int DEFAULT_MAX_DEEP = 10;

		private final int maxDeep;

		public Accumulator() {
			this(DEFAULT_MAX_DEEP);
		}

		public Accumulator(int maxDeep) {
			this.maxDeep = maxDeep;
		}

		/**
		 * Accumulate transitive references for the provided fields of the given
		 * object into <code>queue</code> and return the shallow size of this object.
		 * 将给定对象的提供字段的传递引用累积到 <code>queue</code> 并返回该对象的浅大小。
		 */
		public long accumulateObject(
			Object o,
			long shallowSize,
			Map<Field, Object> fieldValues,
			Collection<ObjectWithDeep> queue,
			int deep) {
			//如果遍历的深度小于最大深度
			if (deep < maxDeep) {
				//遍历当前类中每一个字段，将每一个字段的实例对象添加到队列里面用来继续遍历
				for (Object object : fieldValues.values()) {
					queue.add(new ObjectWithDeep(object, deep + 1));
				}
			}
			return shallowSize;
		}

		/**
		 * Accumulate transitive references for the provided values of the given
		 * array into <code>queue</code> and return the shallow size of this array.
		 * 将给定数组的提供值的传递引用累积到 <code>queue</code> 并返回此数组的浅大小。
		 */
		public long accumulateArray(
			Object array,
			long shallowSize,
			List<Object> values,
			Collection<ObjectWithDeep> queue,
			int deep) {
			//如果遍历的深度小于最大深度
			if (deep < maxDeep) {
				//遍历数组中的每一个对象，将每一个对象添加到队列中用来后续遍历
				for (Object object : values) {
					queue.add(new ObjectWithDeep(object, deep + 1));
				}
			}
			return shallowSize;
		}
	}

	//表示要遍历的对象以及遍历的深度
	private static class ObjectWithDeep {
		private Object object;
		private int deep;

		public ObjectWithDeep(Object ob, int deep) {
			this.object = ob;
			this.deep = deep;
		}

		public Object getObject() {
			return object;
		}

		public int getDeep() {
			return deep;
		}
	}

	/**
	 * Non-recursive version of object descend. This consumes more memory than recursive in-depth
	 * traversal but prevents stack overflows on long chains of objects
	 * or complex graphs (a max. recursion depth on my machine was ~5000 objects linked in a chain
	 * so not too much).
	 * 对象下降的非递归版本。 这比递归深入遍历消耗更多内存，但可以防止长链对象或复杂图上的堆栈溢出
	 * （我的机器上的最大递归深度约为 5000 个链接在链中的对象，所以不会太多）。
	 * 使用bfs算法进行遍历
	 */
	private static long measureObjectSize(Object root, Accumulator accumulator) {
		// Objects seen so far.
		//到目前为止看到的对象。
		final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
		// Class cache with reference Field and precalculated shallow size.
		//具有引用字段和预先计算的浅大小的类缓存。
		final IdentityHashMap<Class<?>, ClassCache> classCache = new IdentityHashMap<>();
		// Stack of objects pending traversal. Recursion caused stack overflows.
		//待遍历的对象堆栈。 递归导致堆栈溢出。
		final ArrayList<ObjectWithDeep> stack = new ArrayList<>();
		stack.add(new ObjectWithDeep(root, 0));

		long totalSize = 0;
		while (!stack.isEmpty()) {
			//获取要遍历的ObjectWithDeep（对象和对应的深度）
			final ObjectWithDeep objectWithDeep = stack.remove(stack.size() - 1);
			//获取要遍历的对象
			final Object ob = objectWithDeep.getObject();
			//获取对象的遍历深度
			final int currentDeep = objectWithDeep.getDeep();

			// skip the state object
			//跳过状态对象
			if (ob == null || ob instanceof State || seen.contains(ob)) {
				continue;
			}
			//添加对象已经遍历过
			seen.add(ob);

			final long obSize;
			//对象的class
			final Class<?> obClazz = ob.getClass();
			assert obClazz != null : "jvm bug detected (Object.getClass() == null). please report this to your vendor";
			//判断这个class对象是否表示数组类。
			if (obClazz.isArray()) {
				//如果是数组就走遍历处理数组
				obSize = handleArray(accumulator, stack, ob, obClazz, currentDeep);
			} else {
				//如果class对象不是数组，就走遍历对象
				obSize = handleOther(accumulator, classCache, stack, ob, obClazz, currentDeep);
			}
			//累加对象大小
			totalSize += obSize;
			// Dump size of each object for comparisons across JVMs and flags.
			// System.out.println("  += " + obClazz + " | " + obSize);
		}

		// Help the GC (?).
		seen.clear();
		stack.clear();
		classCache.clear();

		return totalSize;
	}

	private static long handleOther(
		Accumulator accumulator,
		IdentityHashMap<Class<?>,
		ClassCache> classCache,
		ArrayList<ObjectWithDeep> stack,
		Object ob,
		Class<?> obClazz,
		int deep) {
		/*
		 * Consider an object. Push any references it has to the processing stack
		 * and accumulate this object's shallow size.
		 * 考虑一个对象。 将它拥有的任何引用推送到处理堆栈并累积此对象的浅层大小。
		 */
		try {
			//如果说jre版本 >= 9
			if (Constants.JRE_IS_MINIMUM_JAVA9) {
				long alignedShallowInstanceSize = RamUsageEstimator.shallowSizeOf(ob);

				Predicate<Class<?>> isJavaModule = (clazz) -> clazz.getName().startsWith("java.");

				// Java 9: Best guess for some known types, as we cannot precisely look into runtime classes
				final ToLongFunction<Object> func = SIMPLE_TYPES.get(obClazz);
				if (func != null) {
					// some simple type like String where the size is easy to get from public properties
					return accumulator.accumulateObject(
						ob, alignedShallowInstanceSize + func.applyAsLong(ob), Collections.emptyMap(), stack, deep);
				} else if (ob instanceof Enum) {
					return alignedShallowInstanceSize;
				} else if (ob instanceof ByteBuffer) {
					// Approximate ByteBuffers with their underlying storage (ignores field overhead).
					return byteArraySize(((ByteBuffer) ob).capacity());
				} else {
					// Fallback to reflective access.
				}
			}
			//获取缓存的class
			ClassCache cachedInfo = classCache.get(obClazz);
			//如果没有就缓存
			if (cachedInfo == null) {
				classCache.put(obClazz, cachedInfo = createCacheEntry(obClazz));
			}

			final Map<Field, Object> fieldValues = new HashMap<>();
			//循环class里面的字段并获取对应的实例对象
			for (Field f : cachedInfo.referenceFields) {
				fieldValues.put(f, f.get(ob));
			}
			//累计遍历实例对象
			return accumulator.accumulateObject(ob, cachedInfo.alignedShallowInstanceSize, fieldValues, stack, deep);
		} catch (IllegalAccessException e) {
			// this should never happen as we enabled setAccessible().
			throw new RuntimeException("Reflective field access failed?", e);
		}
	}

	private static long handleArray(
		Accumulator accumulator,
		ArrayList<ObjectWithDeep> stack,
		Object ob,
		Class<?> obClazz,
		int deep) {
		/*
		 * Consider an array, possibly of primitive types. Push any of its references to
		 * the processing stack and accumulate this array's shallow size.
		 * 考虑一个数组，可能是原始类型。 将其任何引用推送到处理堆栈并累积此数组的浅大小。
		 */
		//计算数组对象的shallow Size
		final long shallowSize = RamUsageEstimator.shallowSizeOf(ob);
		//数组的长度
		final int len = Array.getLength(ob);
		final List<Object> values;
		//获取元素的数据类型
		Class<?> componentClazz = obClazz.getComponentType();
		//如果是基本类型
		if (componentClazz.isPrimitive()) {
			values = Collections.emptyList();
		} else {
			values = new AbstractList<Object>() {

				@Override
				public Object get(int index) {
					return Array.get(ob, index);
				}

				@Override
				public int size() {
					return len;
				}
			};
		}
		//遍历累加数组对象
		return accumulator.accumulateArray(ob, shallowSize, values, stack, deep);
	}

	/**
	 * This map contains a function to calculate sizes of some "simple types" like String just from their public properties.
	 * This is needed for Java 9, which does not allow to look into runtime class fields.
	 * 这个映射包含一个函数来计算一些“简单类型”的大小，比如 String 只是从它们的公共属性。 这对于 Java 9 是必需的，它不允许查看运行时类字段。
	 */
	@SuppressWarnings("serial")
	private static final Map<Class<?>, ToLongFunction<Object>> SIMPLE_TYPES =
		Collections.unmodifiableMap(new IdentityHashMap<Class<?>, ToLongFunction<Object>>() {
			{ init(); }

			private void init() {
				// String types:
				a(String.class, v -> charArraySize(v.length())); // may not be correct with Java 9's compact strings!
				a(StringBuilder.class, v -> charArraySize(v.capacity()));
				a(StringBuffer.class, v -> charArraySize(v.capacity()));
				// Types with large buffers:
				a(ByteArrayOutputStream.class, v -> byteArraySize(v.size()));
				// For File and Path, we just take the length of String representation as approximation:
				a(File.class, v -> charArraySize(v.toString().length()));
				a(Path.class, v -> charArraySize(v.toString().length()));
				a(ByteOrder.class, v -> 0); // Instances of ByteOrder are constants
			}

			@SuppressWarnings("unchecked")
			private <T> void a(Class<T> clazz, ToLongFunction<T> func) {
				put(clazz, (ToLongFunction<Object>) func);
			}

			private long charArraySize(int len) {
				return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Character.BYTES * len);
			}
		});

	/**
	 * Cached information about a given class.
	 * 关于给定类的缓存信息。
	 */
	private static final class ClassCache {
		//对齐的Shallow Instance Size
		public final long alignedShallowInstanceSize;
		//类中定义的所有字段
		public final Field[] referenceFields;

		public ClassCache(long alignedShallowInstanceSize, Field[] referenceFields) {
			this.alignedShallowInstanceSize = alignedShallowInstanceSize;
			this.referenceFields = referenceFields;
		}
	}

	/**
	 * Create a cached information about shallow size and reference fields for a given class.
	 * 为一个给定的类创建一个关于浅层尺寸和参考字段的缓存信息。
	 */
	private static ClassCache createCacheEntry(final Class<?> clazz) {
		return AccessController.doPrivileged((PrivilegedAction<ClassCache>) () -> {
			ClassCache cachedInfo;
			long shallowInstanceSize = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
			final ArrayList<Field> referenceFields = new ArrayList<>(32);
			for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
				if (c == Class.class) {
					// prevent inspection of Class' fields, throws SecurityException in Java 9!
					continue;
				}
				//获取类中定义的字段
				final Field[] fields = c.getDeclaredFields();
				//
				for (final Field f : fields) {
					if (!Modifier.isStatic(f.getModifiers())) {
						shallowInstanceSize = RamUsageEstimator.adjustForField(shallowInstanceSize, f);

						if (!f.getType().isPrimitive()) {
							try {
								f.setAccessible(true);
								referenceFields.add(f);
							} catch (RuntimeException re) {
								throw new RuntimeException(String.format(Locale.ROOT,
									"Can't access field '%s' of class '%s' for RAM estimation.",
									f.getName(),
									clazz.getName()), re);
							}
						}
					}
				}
			}

			cachedInfo = new ClassCache(RamUsageEstimator.alignObjectSize(shallowInstanceSize),
				referenceFields.toArray(new Field[referenceFields.size()]));
			return cachedInfo;
		});
	}

	private static long byteArraySize(int len) {
		return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + len);
	}
}
