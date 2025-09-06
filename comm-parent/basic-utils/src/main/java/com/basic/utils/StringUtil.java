package com.basic.utils;

import java.util.Random;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class StringUtil {

	public static String trim(String s) {
		StringBuilder sb = new StringBuilder();
		for (char ch : s.toCharArray())
			if (' ' != ch)
				sb.append(ch);
		s = sb.toString();

		return s.replaceAll("&nbsp;", "").replaceAll(" ", "").replaceAll("　", "").replaceAll("\t", "").replaceAll("\n", "");
	}

	private static final char[] charArray = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
			'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };

	public static String getExt(String filename) {
		return filename.substring(filename.lastIndexOf('.'));
	}
	public static boolean isNumeric(String str){
		   Pattern pattern = Pattern.compile("[0-9]*");
		   Matcher isNum = pattern.matcher(str);
		   if( !isNum.matches() ){
		       return false; 
		   } 
		   return true; 
	}
	public static boolean isEscapeChar(String str){
		Pattern pattern = Pattern.compile("^[?+*.](.*?)");
		Matcher isEscape = pattern.matcher(str);
		if(!isEscape.matches()){
			return false;
		}
		return true;
	}

	private static  Random random=new Random();

	public static int getRandom(int num) {
		if(null==random) {
			random=new Random();
		}
		return (random.nextInt(num));
	}
	public static boolean isEmpty(String s) {
		return isNullOrEmpty(s);
	}

	public static boolean isNullOrEmpty(String s) {
		return null == s || 0 == s.trim().length();
	}

	public static String randomCode() {
		return "" + (random.nextInt(899999) + 100000);
	}

	public static String randomPassword() {
		return randomString(6);
	}

	public static String randomString(int length) {
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < length; i++) {
			int index = new Random().nextInt(36);
			sb.append(charArray[index]);
		}

		return sb.toString();
	}
	public static String getOutTradeNo() {
		int r1 = (int) (Math.random() * (10));// 产生2个0-9的随机数
		int r2 = (int) (Math.random() * (10));
		long now = System.currentTimeMillis();// 一个13位的时间戳
		String id = String.valueOf(r1) + String.valueOf(r2)
				+ String.valueOf(now);// 订单ID
		return id;
	}

	public static String randomUUID() {
		UUID uuid = UUID.randomUUID();
		String uuidStr = uuid.toString().replace("-", "");

		return uuidStr;
	}

	public static String getFormatName(String fileName) {
		int index = fileName.lastIndexOf('.');
		return -1 == index ? "jpg" : fileName.substring(index + 1);
	}

}
