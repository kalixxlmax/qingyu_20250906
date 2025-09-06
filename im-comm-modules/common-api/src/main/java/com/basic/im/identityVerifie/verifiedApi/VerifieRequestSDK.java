package com.basic.im.identityVerifie.verifiedApi;

import com.basic.im.identityVerifie.Utils.HttpUtils;
import com.basic.im.identityVerifie.model.VerifieConfig;
import lombok.extern.slf4j.Slf4j;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author
 * @version V1.0
 * @Description: 请求认证用户信息
 * @date 2020/3/16 14:17
 */
@Slf4j
@Component
public class VerifieRequestSDK {

    @Autowired
    private VerifieConfig verifieConfig;

    /**
     * 身份校验
     * @param idcard 身份证号码
     * @param name 真实姓名
     * @return
     */
    public String identityVerifieFlag(String idcard,String name) {
        Map<String, String> params = new HashMap<>();
        params.put("idcard", idcard);
        params.put("name", name);
        try {
            OkHttpClient client = new OkHttpClient.Builder().build();
            FormBody.Builder formbuilder = new FormBody.Builder();

            for (Map.Entry<String, String> entry : params.entrySet()) {
                formbuilder.add(entry.getKey(), entry.getValue());
            }

            FormBody body = formbuilder.build();
            Request request = new Request.Builder().url(verifieConfig.getVerifieUrl()).addHeader("Authorization", "APPCODE " + verifieConfig.getVerifieAppCode()).post(body).build();
            Response response = client.newCall(request).execute();
            log.info("返回状态码 :{}, message:{}",response.code(),response.message());
            String result = response.body().string();
            return result;
           /* if(StringUtil.isEmpty(result))
                return false;
            JSONObject jsonObject = JSONObject.parseObject(result);
            return "0".equals(jsonObject.get("code"));*/
        } catch (Exception e) {
            log.info(" === 实名认证身份校验服务异常 === ");
            e.printStackTrace();
            return null;
        }

    }
    
    /**
     * 身份校验（弃用）
     * @param idCardNum 身份证号码
     * @param realName 真实姓名
     * @return 
     */
    public boolean identityVerifieFlagTest(String idCardNum,String realName){
        String host = verifieConfig.getVerifieUrl();
        String path = "/simple";
        String method = "POST";
        String appcode = verifieConfig.getVerifieAppCode();
        Map<String, String> headers = new HashMap<>(2);
        //最后在header中的格式(中间是英文空格)为Authorization:APPCODE 833xxxxxxxxxxxxxxxxx
        headers.put("Authorization", "APPCODE " + appcode);
        //根据API的要求，定义相对应的Content-Type
        headers.put("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
        Map<String, String> querys = new HashMap<>();
        Map<String, String> bodys = new HashMap<>();
        bodys.put("idCardNum", idCardNum);
        bodys.put("realName", realName);

        try {
            HttpResponse response = HttpUtils.doPost(host, path, method, headers, querys, bodys);
            System.out.println(response.toString());
            //获取response的body
            System.out.println("== "+EntityUtils.toString(response.getEntity()));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }


}
