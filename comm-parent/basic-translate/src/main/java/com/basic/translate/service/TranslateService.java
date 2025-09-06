package com.basic.translate.service;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.basic.common.response.JSONBaseMessage;
import com.basic.translate.util.CommonUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public interface TranslateService {
    Map<Integer, String> ERROR_MESSAGE = new HashMap<>();

    JSONBaseMessage getTranslatedContent(String content, String from, String to, String messageId);

    default JSONBaseMessage sendRequest(HttpRequestBase httpRequestBase, String statusKeyName, String resultPartName, String resultKeyName, String messageId) throws IOException {
        CloseableHttpClient client = CommonUtils.createHttpClient();
        CloseableHttpResponse response = client.execute(httpRequestBase);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_OK) {
            Map returnedContent = (Map) JSON.parse(EntityUtils.toString(response.getEntity()));
            int errorCode = returnedContent.containsKey(statusKeyName) ? Integer.parseInt(returnedContent.get(statusKeyName).toString()) : 0;
            if (errorCode == 0) {
                Map<String,String> resultMap=new HashMap<>();
                resultMap.put("messageId",messageId);
                if (!resultKeyName.isEmpty()) {
                    JSONArray transResult = (JSONArray) returnedContent.get(resultPartName);
                    String translation=transResult.isEmpty() ? "" : ((Map) transResult.get(0)).get(resultKeyName).toString();
                    resultMap.put("translation",translation);
                    return JSONBaseMessage.valueOf(resultMap);
                } else {
                    Object result = returnedContent.get(resultPartName);
                    String translation=(result instanceof String) ? result.toString() : ((JSONArray) (result)).getString(0);
                    resultMap.put("translation",translation);
                    return JSONBaseMessage.valueOf(resultMap);
                }
            }
            return JSONBaseMessage.failure(errorCode + ":" + ERROR_MESSAGE.getOrDefault(errorCode, "Unknown Error."));
        } else {
            return JSONBaseMessage.failure( "ErrorCode:" + statusCode + ", please contract the admin.");
        }
    }
}
