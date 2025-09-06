package com.basic.mongodb.reflect;


import com.basic.mongodb.entity.ColumnCache;
import com.basic.mongodb.function.SFunction;
import com.basic.mongodb.utils.SerializedLambda;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Locale.ENGLISH;


/**
 * lambda 工具类
 * @author chat 
 * @date   2020-9-17
 */
public final class LambdaUtils {

    /**
     * 字段映射
     */
    private static final Map<String, Map<String, ColumnCache>> LAMBDA_MAP = new ConcurrentHashMap<>();

    /**
     * SerializedLambda 反序列化缓存
     */
    private static final Map<Class<?>, WeakReference<SerializedLambda>> FUNC_CACHE = new ConcurrentHashMap<>();

    /**
     * 解析 lambda 表达式, 该方法只是调用了 {@link SerializedLambda#resolve(SFunction)} 中的方法，在此基础上加了缓存。
     * 该缓存可能会在任意不定的时间被清除
     * @param func 需要解析的 lambda 对象
     * @param <T>  类型，被调用的 Function 对象的目标类型
     * @return     返回解析后的结果
     * @see SerializedLambda#resolve(SFunction)
     */
    public static <T> SerializedLambda resolve(SFunction<T, ?> func) {
        Class<?> clazz = func.getClass();
        return Optional.ofNullable(FUNC_CACHE.get(clazz))
            .map(WeakReference::get)
            .orElseGet(() -> {
                SerializedLambda lambda = SerializedLambda.resolve(func);
                FUNC_CACHE.put(clazz, new WeakReference<>(lambda));
                return lambda;
            });
    }

    /**
     * 格式化 key 将传入的 key 变更为大写格式
     */
    public static String formatKey(String key) {
        return key.toUpperCase(ENGLISH);
    }



    /**
     * 获取实体对应字段 MAP
     * @param clazz 实体类
     * @return 缓存 map
     */
    public static Map<String, ColumnCache> getColumnMap(Class<?> clazz) {
        return LAMBDA_MAP.getOrDefault(clazz.getName(), new ConcurrentHashMap<>());
    }

    /**
     * 保存实体对应的字段缓存
     */
    public static ColumnCache putColumnMap(Class<?> clazz,ColumnCache cache) {
        Map<String, ColumnCache> cacheMap = getColumnMap(clazz);
        cacheMap.put(cache.getColumn(),cache);
        LAMBDA_MAP.put(clazz.getName(),cacheMap);
        return cache;
    }
}