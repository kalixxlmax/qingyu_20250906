package com.basic.mongodb.reflect;


import com.basic.mongodb.exception.ClassReflectExpression;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

/**
 * JAVA 反射工具类
 * @author chat 
 */
public final class ClassUtils {

    private static final char PACKAGE_SEPARATOR = '.';

    /**
     * 代理 class 的名称
     */
    private static final List<String> PROXY_CLASS_NAMES = Arrays.asList("net.sf.cglib.proxy.Factory"
        // cglib
        , "org.springframework.cglib.proxy.Factory"
        , "javassist.util.proxy.ProxyObject"
        // javassist
        , "org.apache.ibatis.javassist.util.proxy.ProxyObject");

    private ClassUtils() { }

    /**
     * 判断是否为代理对象
     * @param clazz 传入 class 对象
     * @return 如果对象class是代理 class，返回 true
     */
    public static boolean isProxy(Class<?> clazz) {
        if (clazz != null) {
            for (Class<?> cls : clazz.getInterfaces()) {
                if (PROXY_CLASS_NAMES.contains(cls.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 获取当前对象的 class
     * @param clazz 传入
     * @return 如果是代理的class，返回父 class，否则返回自身
     */
    public static Class<?> getUserClass(Class<?> clazz) {
        return isProxy(clazz) ? clazz.getSuperclass() : clazz;
    }

    /**
     * 获取当前对象的class
     * @param object 对象
     * @return 返回对象的 user class
     */
    public static Class<?> getUserClass(Object object) {
        return getUserClass(object.getClass());
    }


    /**
     * 根据字段名称取得字段
     */
    public static Field getField(Class<?> clazz,String fieldName){
        try{
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new ClassReflectExpression("Class:" + clazz.getName() +" Field: "+fieldName+" does not exist");
        }
    }

    /**
     * 根据指定的 class ， 实例化一个对象，根据构造参数来实例化
     * 在 java9 及其之后的版本 Class.newInstance() 方法已被废弃
     * @param clazz 需要实例化的对象
     * @param <T>   类型，由输入类型决定
     * @return      返回新的实例
     */
    public static <T> T newInstance(Class<T> clazz) {
        try {
            Constructor<T> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new ClassReflectExpression("实例化对象时出现错误,请尝试给 "+ clazz +" 添加无参的构造方法");
        }
    }

    /**
     * 在确定类存在的情况下调用该方法
     * @param name 类名称
     * @return 返回转换后的 Class
     */
    public static Class<?> toClassConfident(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new ClassReflectExpression("找不到指定的class！请仅在明确确定会有 class 的时候，调用该方法");
        }
    }


    /**
     * @return 类的包名，或空字符串
     */
    public static String getPackageName(Class<?> clazz) {
        return getPackageName(clazz.getName());
    }

    /**
     * @return 类的包名，或空字符串
     */
    public static String getPackageName(String fqClassName) {
        int lastDotIndex = fqClassName.lastIndexOf(PACKAGE_SEPARATOR);
        return (lastDotIndex != -1 ? fqClassName.substring(0, lastDotIndex) : "");
    }
}