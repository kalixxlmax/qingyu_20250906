package com.basic.im.comm.utils;

import java.util.*;
import java.util.stream.Collectors;

public class CollectionUtil {

	private CollectionUtil() {
		super();
	}

	// 判断一个集合是否为空
	public static <T> boolean isEmpty(Collection<T> col) {
		if (col == null || col.isEmpty()) {
			return true;
		}

		return false;
	}

	// 判断一个集合是否不为空
	public static <T> boolean isNotEmpty(Collection<T> col) {
		return !isEmpty(col);
	}

	// 判断Map是否为空
	public static <K, V> boolean isEmpty(Map<K, V> map) {
		if (map == null || map.isEmpty()) {
			return true;
		}

		return false;
	}

	// 判断Map是否不为空为空
	public static <K, V> boolean isNotEmpty(Map<K, V> map) {
		return !isEmpty(map);
	}

	// 去除list中的重复数据
	public static <T> List<T> removeRepeat(List<T> list) {
		if (isEmpty(list)) {
			return list;
		}

		List<T> result = new ArrayList<T>();
		for (T e : list) {
			if (!result.contains(e)) {
				result.add(e);
			}
		}

		return result;
	}

	// 将集合转换为String数组
	public static <T> String[] toArray(List<T> list) {
		if (isEmpty(list)) {
			return null;
		}

		String[] result = new String[list.size()];
		for (int i = 0; i < list.size(); i++) {
			result[i] = String.valueOf(list.get(i));
		}

		return result;
	}


	/**
	 * map 根据value 排序
	 */
	public static <K extends Comparable, V extends Comparable> Map<K, V> sortMapByValue(Map<K, V> aMap) {
		if (aMap == null || aMap.isEmpty()) {
			return null;
		}
		HashMap<K, V> finalOut = new LinkedHashMap<>();
		aMap.entrySet()
				.stream()
				.sorted((p1, p2) -> p2.getValue().compareTo(p1.getValue()))
				.collect(Collectors.toList()).forEach(ele -> finalOut.put(ele.getKey(), ele.getValue()));
		return finalOut;
	}

}
