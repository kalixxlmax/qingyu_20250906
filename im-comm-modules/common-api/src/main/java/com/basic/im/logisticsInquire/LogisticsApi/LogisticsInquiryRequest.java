package com.basic.im.logisticsInquire.LogisticsApi;

import com.alibaba.fastjson.JSONObject;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.identityVerifie.Utils.HttpUtils;
import com.basic.im.logisticsInquire.logisticsModel.LogisticsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author
 * @version V1.0
 * @Description: 物流查询
 * @date 2020/3/16 17:27
 */
@Slf4j
@Component
public class LogisticsInquiryRequest {
    @Autowired
    private LogisticsConfig logisticsConfig;
    
    /**
     * 物流查询
     * @param no 物流单号
     * @param type 物流公司字母简写 eg:zto
     * @return 
     */
    public String getLogisticsAddress(String no, String type){
        String path = "/kdi";
        String method = "GET";
        String appcode = logisticsConfig.getVerifieAppCode();
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "APPCODE " + appcode); //格式为:Authorization:APPCODE 83359fd73fe11248385f570e3c139xxx
        Map<String, String> querys = new HashMap<>();
        querys.put("no", no);
        if(!StringUtil.isEmpty(type)) {
            querys.put("type", "");
        }
        try {
            HttpResponse response = HttpUtils.doGet(logisticsConfig.getVerifieUrl(), path, method, headers, querys);
            //状态码: 200 正常；203快递公司不存在；400 URL无效；401 appCode错误； 403 次数用完； 500 API网管错误
            String logisticsInfo = EntityUtils.toString(response.getEntity());
            JSONObject jsonObject = JSONObject.parseObject(logisticsInfo);
            if(!jsonObject.get("status").equals("0")) {
                log.info("物流msg：{}",jsonObject.get("msg"));
            }
            log.info("物流信息 ： {}",logisticsInfo);
            return logisticsInfo;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    /*public static void main(String[] args) {
        String host = "https://wuliu.market.alicloudapi.com";
        String path = "/kdi";
        String method = "GET";
        String appcode = "e3422b11517049dd93a04186f0672a8e";  // !!!替换填写自己的AppCode 在买家中心查看
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Authorization", "APPCODE " + appcode); //格式为:Authorization:APPCODE 83359fd73fe11248385f570e3c139xxx
        Map<String, String> querys = new HashMap<String, String>();
        querys.put("no", "455666788999");// !!! 请求参数
        querys.put("type", "");// !!! 请求参数
        //JDK 1.8示例代码请在这里下载：  http://code.fegine.com/Tools.zip
        try {
            HttpResponse response = HttpUtils.doGet(host, path, method, headers, querys);
            //System.out.println(response.toString());如不输出json, 请打开这行代码，打印调试头部状态码。
            //状态码: 200 正常；400 URL无效；401 appCode错误； 403 次数用完； 500 API网管错误;
            //获取response的body
            String logisticsInfo = EntityUtils.toString(response.getEntity());
            System.out.println("455666788999 : "+logisticsInfo);
            JSONObject jsonObject = JSONObject.parseObject(logisticsInfo);
            String list = jsonObject.getString("result");
            JSONObject listInfo = JSONObject.parseObject(list);
            System.out.println(listInfo.getString("list"));
            System.out.println(jsonObject.toJSONString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/
}
