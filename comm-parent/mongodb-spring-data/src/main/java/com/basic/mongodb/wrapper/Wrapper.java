package com.basic.mongodb.wrapper;


import com.basic.mongodb.entity.ColumnCache;
import com.basic.mongodb.function.SFunction;
import com.basic.mongodb.reflect.ClassUtils;
import com.basic.mongodb.reflect.LambdaUtils;
import com.basic.mongodb.utils.PropertyName;
import com.basic.mongodb.utils.SerializedLambda;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;


/**
 * 数据库操作包装类
 * @author chat 
 * @data   2020-9-17
 */
public abstract class Wrapper {

    protected static String columnToString(SFunction<?, ?> column) {
        return getColumn(LambdaUtils.resolve(column));
    }

    /**
     * 取得字段名称
     */
    private static String getColumn(SerializedLambda lambda){
        Class aClass = lambda.getInstantiatedMethodType();
        String fieldName = PropertyName.methodToProperty(lambda.getImplMethodName());
        Map<String, ColumnCache> columnMap = LambdaUtils.getColumnMap(aClass);
        ColumnCache columnCache = Optional.ofNullable(columnMap.get(LambdaUtils.formatKey(fieldName))).orElseGet(() -> {
            Field field = ClassUtils.getField(aClass,fieldName);
            org.springframework.data.mongodb.core.mapping.Field aliasAnnotation = field.getAnnotation(org.springframework.data.mongodb.core.mapping.Field.class);
            if (aliasAnnotation==null || isBlank(aliasAnnotation.value())){
                return LambdaUtils.putColumnMap(aClass,ColumnCache.instance(fieldName,fieldName));
            }else{
                return LambdaUtils.putColumnMap(aClass,ColumnCache.instance(fieldName,aliasAnnotation.value()));
            }
        });
        return isBlank(columnCache.getColumnField()) ? columnCache.getColumn() : columnCache.getColumnField();
    }

    /**
     * 判断字符串是否为空
     */
    private static boolean isBlank(CharSequence str) {
        int length;
        if ((str == null) || ((length = str.length()) == 0)) {
            return true;
        }
        for (int i = 0; i < length; i++) {
            if (!isBlankChar(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isBlankChar(int c) {
        return Character.isWhitespace(c) || Character.isSpaceChar(c) || c == '\ufeff' || c == '\u202a';
    }

}

