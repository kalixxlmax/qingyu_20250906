package com.basic.im.businessLicense.businessApi;

import com.alibaba.fastjson.JSONObject;
import com.basic.im.businessLicense.businessModel.BusinessConfig;
import com.basic.im.identityVerifie.Utils.HttpUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2020/3/17 11:07
 */
@Slf4j
@Component
public class BusinessRequest {
    @Autowired
    private BusinessConfig businessConfig;

    /**
     * 获取营业执照信息
     * @param imgCoding 营业执照图片二进制数据的base64编码或者图片url
     * @return 
     */
    public String getBusinessLicense(String imgCoding){
        String host = businessConfig.getVerifieUrl();
        String path = "/rest/160601/ocr/ocr_business_license.json";
        String method = "POST";
        String appcode = businessConfig.getVerifieAppCode();
        Map<String, String> headers = new HashMap<String, String>();
        //最后在header中的格式(中间是英文空格)为Authorization:APPCODE 83359fd73fe94948385f570e3c139105
        headers.put("Authorization", "APPCODE " + appcode);
        //根据API的要求，定义相对应的Content-Type
        headers.put("Content-Type", "application/json; charset=UTF-8");
        Map<String, String> querys = new HashMap<String, String>();
        String bodys = "{\"image\":\""+imgCoding+"\"}";
        try {
            HttpResponse response = HttpUtils.doPost(host, path, method, headers, querys, bodys);
            //获取response的body
            String businessInfo = EntityUtils.toString(response.getEntity());
            log.info("营业执照详情：{}",businessInfo);
            return businessInfo;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /*public static void main(String[] args) {
//        String url = "https://test.basic.co/guanwangTest/timg.jpg";
        String url = "http://test.basic.co:8089/image/20200401/t/60fa5ccd47fa462fb32eb4b6679bcc92.jpg";
        String host = "https://dm-58.data.aliyun.com";
        String path = "/rest/160601/ocr/ocr_business_license.json";
        String method = "POST";
        String appcode = "e3422b11517049dd93a04186f0672a8e";
        Map<String, String> headers = new HashMap<String, String>();
        //最后在header中的格式(中间是英文空格)为Authorization:APPCODE 83359fd73fe94948385f570e3c139105
        headers.put("Authorization", "APPCODE " + appcode);
        //根据API的要求，定义相对应的Content-Type
        headers.put("Content-Type", "application/json; charset=UTF-8");
        Map<String, String> querys = new HashMap<String, String>();
//        String bodys = "{\"image\":\"图片二进制数据的base64编码或者图片url\"}";
        String bodys = "{\"image\":\""+url+"\"}";

        try {
            HttpResponse response = HttpUtils.doPost(host, path, method, headers, querys, bodys);
            response.getStatusLine().getStatusCode();
            //获取response的body
            String s = response.toString();
            System.out.println("s"+s);
            String str = EntityUtils.toString(response.getEntity());
            System.out.println(str);
//            JSONObject jsonObject = JSONObject.parseObject(str);
            *//*jsonObject.getString("name");
            jsonObject.getString("person");
            jsonObject.getString("name");*//*
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/
}
