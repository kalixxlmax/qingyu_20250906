package com.basic.im.open.opensdk.until;

import com.basic.im.comm.utils.NumberUtil;
import com.basic.utils.DateUtil;
import com.basic.utils.Md5Util;

public class SkOpenUtil {
	
	public static String getAppId(){
		final String sk = "sk";
		return sk + NumberUtil.get16UUID();
	}
	
	// 暂定这样
	public static String getAppScrect(String appId){
		return Md5Util.md5Hex(appId+ DateUtil.currentTimeSeconds());
	}

}
