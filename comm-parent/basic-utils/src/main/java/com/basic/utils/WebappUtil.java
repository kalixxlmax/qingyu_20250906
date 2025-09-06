package com.basic.utils;

public class WebappUtil {

	public static String getWebAppPath() {
		String webAppPath = System.getProperty("webAppPath");
		if (null == webAppPath || "".equals(webAppPath.trim())) {
			String path = WebappUtil.class.getResource("").getPath();
			webAppPath = path.substring(0, path.indexOf("/WEB-INF/"));
		}
		return webAppPath;
	}

}
