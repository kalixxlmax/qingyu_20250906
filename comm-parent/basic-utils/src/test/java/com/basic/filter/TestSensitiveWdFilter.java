package com.basic.filter;

import java.util.Arrays;

/**
 * TestSensitiveWdFilter <br>
 *
 * @author: chat  <br>
 * @date: 2021/5/7 0007  <br>
 */
public class TestSensitiveWdFilter {

    public static void main(String[] args) {
        WordFilter wordFilter=new WordFilter();
        wordFilter.init();

        String s="(1). import { Services } from '@/database'\n" +
                "(2). Services.Conversations.Messages.UnreadCounts.totalUnread";
        wordFilter.addSensitiveWord(Arrays.asList(s));
        //String s = "你好，小傑";
        System.out.println("解析问题： " + s);
        System.out.println("解析字数 : " + s.length());
        String re;
        long nano = System.nanoTime();
        re = wordFilter.doFilter(s,false);
        nano = (System.nanoTime() - nano);
        System.out.println("解析时间 : " + nano + "ns");
        System.out.println("解析时间 : " + nano / 1000000 + "ms");
        System.out.println(re);
        System.out.println();

        nano = System.nanoTime();
        System.out.println("是否包含敏感词： " + wordFilter.isContains(s));
        nano = (System.nanoTime() - nano);
        System.out.println("解析时间 : " + nano + "ns");
        System.out.println("解析时间 : " + nano / 1000000 + "ms");
    }


}
