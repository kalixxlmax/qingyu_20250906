package com.basic.translate.util;


import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class CommonUtils {

    private static final PoolingHttpClientConnectionManager connectionManager = initHttpClientPool();
    private static final RequestConfig requestConfig = initRequestConfig();
    private static final int CONTENT_LIMIT = 20;
    private static final int CONTENT_EDGE = 10;

    public static CloseableHttpClient createHttpClient() {
        return HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig).build();
    }

    private static PoolingHttpClientConnectionManager initHttpClientPool() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(20);
        connectionManager.setDefaultMaxPerRoute(connectionManager.getMaxTotal());
        return connectionManager;
    }

    private static RequestConfig initRequestConfig() {
        return RequestConfig.custom().setConnectTimeout(10 * 1000).setSocketTimeout(60 * 1000).build();
    }


    public static String truncateContent(String raw) {
        if (raw.length() <= CONTENT_LIMIT) {
            return raw;
        }
        return raw.substring(0, CONTENT_EDGE) + raw.length() + raw.substring(raw.length() - CONTENT_EDGE);
    }

    public static boolean isChineseCharExists(String content) {
        for (char item : content.toCharArray()) {
            if (item >= '\u4e00' && item <= '\u9fa5') {
                return true;
            }
        }
        return false;
    }
}
