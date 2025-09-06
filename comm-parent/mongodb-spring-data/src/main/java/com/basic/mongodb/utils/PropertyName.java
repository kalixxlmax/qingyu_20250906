package com.basic.mongodb.utils;

import java.util.Locale;

/**
 * 根据 setter or getter 取得类名称
 * @author chat 
 * @data   2020-9-17
 */
public final class PropertyName {
    private PropertyName() {
    }

    public static String methodToProperty(String name) {
        if (name.startsWith("is")) {
            name = name.substring(2);
        } else {
            if (!name.startsWith("get") && !name.startsWith("set")) {
                throw new RuntimeException("Error parsing property name '" + name + "'.  Didn't start with 'is', 'get' or 'set'.");
            }
            name = name.substring(3);
        }
        if (name.length() == 1 || name.length() > 1 && !Character.isUpperCase(name.charAt(1))) {
            name = name.substring(0, 1).toLowerCase(Locale.ENGLISH) + name.substring(1);
        }
        return name;
    }

    public static boolean isProperty(String name) {
        return isGetter(name) || isSetter(name);
    }

    public static boolean isGetter(String name) {
        return name.startsWith("get") && name.length() > 3 || name.startsWith("is") && name.length() > 2;
    }

    public static boolean isSetter(String name) {
        return name.startsWith("set") && name.length() > 3;
    }
}