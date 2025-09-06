package com.basic.commons.utils;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.util.StrUtil;
import org.apache.commons.io.FileUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 字符串有关的工具类
 * @author Administrator
 *
 */
public class StringUtils {
	
	public boolean isNumeric(String str){
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        if( !isNum.matches() ){
            return false;
        }
        return true;
   }

   public static List<Long> getLongList(String str,String sep){
	    if(StrUtil.isEmpty(str)){
	        return null;
        }
       String[] split = str.split(sep);
	    List<Long> list=new ArrayList<>();
       for (String s : split) {
           if(StrUtil.isEmpty(s)){
               continue;
           }
           list.add(Long.parseLong(s));
       }
       return list;
   }
	
}
