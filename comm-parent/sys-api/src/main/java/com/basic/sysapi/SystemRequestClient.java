package com.basic.sysapi;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.basic.sysapi.model.SyncUserInfoModel;
import com.basic.utils.encrypt.RSA;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * AuthRequestClient <br>
 *
 * @author: lidaye <br>
 * @date: 2021/11/9  <br>
 */
public class SystemRequestClient {



    private Logger logger= LoggerFactory.getLogger(SystemRequestClient.class);


    public SystemRequestClient(String apiUrl,String publicKey,String apiSecret) {
        if(StrUtil.isEmpty(apiUrl)){
            throw new RuntimeException("apiUrl is null");
        }else if(StrUtil.isEmpty(publicKey)){
            throw new RuntimeException("publicKey is null");
        }else if(StrUtil.isEmpty(apiSecret)){
            throw new RuntimeException("apiSecret is null");
        }
        this.apiUrl = apiUrl;
        this.publicKey=publicKey;
        this.apiSecret=apiSecret;
    }

    private String apiUrl;

    private String apiSecret;


    private String publicKey;


    private String access_token;



    public String getAccess_token() {
        return access_token;
    }
    public void setAccess_token(String access_token) {
        this.access_token = access_token;
    }

    /**
     * 请求刷新获取 接口请求令牌 access_token
     * @return
     */
    public String requestAccess_token(){
        Map<String, Object> paramMap=new HashMap<>();
        JSONObject resultObject = doRequest(SystemApiEnum.GET_ACCESS_TOKEN,paramMap);
        if(null==resultObject.get("data")){
            return null;
        }
        this.access_token=resultObject.get("data").toString();

        return this.access_token;


    }



    /**
     * 同步注册第三方系统用户
     * @param model
     * @return
     */
    public JSONObject syncRegisterUser(SyncUserInfoModel model){

        model.setExtension(cn.hutool.core.codec.Base64.encode(model.getExtension()));
        Map<String, Object> paramMap=JSON.parseObject(JSON.toJSONString(model),new TypeReference<HashMap<String,Object>>(){});

        JSONObject resultObject=doRequest(SystemApiEnum.SYNC_REGISTER_USER,paramMap);
        return resultObject;

    }
    /**
     * 同步修改第三方系统用户
     * @param model
     * @return
     */
    public JSONObject syncUserInfo(SyncUserInfoModel model){

        if(!StrUtil.isEmpty(model.getExtension())) {
            model.setExtension(cn.hutool.core.codec.Base64.encode(model.getExtension()));
        }
        Map<String, Object> paramMap=JSON.parseObject(JSON.toJSONString(model),new TypeReference<HashMap<String,Object>>(){});

        JSONObject resultObject=doRequest(SystemApiEnum.SYNC_USERINFO,paramMap);
        return resultObject;

    }
    /**
     * 同步修改第三方系统用户 thirdId 查询用户基本信息
     * @param model
     * @return
     */
    public JSONObject queryUserInfo(String thirdId){
        if(StrUtil.isEmpty(getAccess_token())){
            throw new RuntimeException("access_token is null");
        }
        Map<String, Object> paramMap=new HashMap<>();
        paramMap.put("thirdId",thirdId);
        JSONObject resultObject=doRequest(SystemApiEnum.QUERY_USERINFO,paramMap);
        return resultObject;

    }

    /**
     * 同步用户第三方系统登陆的 请求令牌token
     * @param userId  聊天系统用户Id 查询用户基本信息获得
     * @param deviceId  登陆设备:web,android,ios 默认android
     * @param loginToken 用户请求令牌，同步后用户令牌请求接口
     * @param language   用户语言,默认 zh_cn
     * @param expired  token有效期 秒单位:1分钟=60 默认值 86400
     * @return
     */
    public JSONObject syncLoginSession(int userId,String deviceId,String loginToken,String language,long expired){
        Map<String, Object> paramMap=new HashMap<>();
        paramMap.put("userId",userId);
        paramMap.put("deviceId",deviceId);
        paramMap.put("loginToken",loginToken);
        paramMap.put("language",language);
        paramMap.put("expired",expired);


        JSONObject resultObject=doRequest(SystemApiEnum.SYNC_LOGIN_SESSION,paramMap);
        return resultObject;

    }
    /**
     * 清除第三方系统登陆的 请求令牌token，清除后请求令牌失效
     * @param userId  聊天系统用户Id 查询用户基本信息获得
     * @param deviceId  登陆设备:web,android,ios 默认android
     * @return
     */
    public JSONObject clearLoginSession(int userId,String deviceId){

        Map<String, Object> paramMap=new HashMap<>();
        paramMap.put("userId",userId);
        paramMap.put("deviceId",deviceId);

        JSONObject resultObject=doRequest(SystemApiEnum.CLEAR_LOGIN_SESSION,paramMap);
        return resultObject;

    }



    private JSONObject doRequest(String requestApi,Map<String, Object> paramMap){
        System.out.println();
        if(!StrUtil.isEmpty(getAccess_token())){
            //已经获取token ,已经授权
            paramMap.put("access_token",this.access_token);
        }else {
            if(!SystemApiEnum.GET_ACCESS_TOKEN.equals(requestApi)){
                if(StrUtil.isEmpty(getAccess_token())){
                    throw new RuntimeException("access_token is null");
                }
            }
            byte[] encrypt = RSA.encrypt(this.apiSecret.getBytes(StandardCharsets.UTF_8),
                    Base64.decodeBase64(this.publicKey));
            String encodeBase64 = Base64.encodeBase64String(encrypt);
            paramMap.put("apiSecret",encodeBase64);
        }
        requestApi=this.apiUrl+requestApi;

        logger.info("request {} param {} ",requestApi,JSON.toJSONString(paramMap));
        String result = HttpUtil.post(requestApi, paramMap);
        if(StrUtil.isEmpty(result)){
            logger.error(" requestApi {} result is null  ",requestApi);
        }
        logger.info(" requestApi {} result is {}  ",requestApi,result);
        JSONObject resultObject = JSON.parseObject(result);

        return resultObject;
    }


}
