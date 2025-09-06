package com.basic.mongodb.utils;


import org.springframework.beans.BeanUtils;

public class BeanCopyUtils {


    public static void copyProperties(Object srource,Object target){
         BeanUtils.copyProperties(srource,target);
    }
}
