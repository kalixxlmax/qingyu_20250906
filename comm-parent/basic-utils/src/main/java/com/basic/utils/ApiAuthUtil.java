package com.basic.utils;

import com.basic.utils.encrypt.AES;
import com.basic.utils.encrypt.HEX;
import com.basic.utils.encrypt.MAC;
import com.basic.utils.encrypt.MD5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class ApiAuthUtil {


    public static final String APIKEY = "vtsvtalkshop";

    /**
     * 登录及登录前生成验证参数
     * 生成secret salt 用于接口鉴权参数的工具方法
     * @param params
     */
    public static void createSecertAndSaltParam(Map<String,String> params){
        String salt = params.remove("salt");
        if(StringUtil.isEmpty(salt)){
            salt = String.valueOf(System.currentTimeMillis());
        }
        String macContent = APIKEY + ParamsSign.joinValues(params)+salt;
        byte[] key = MD5.encrypt(APIKEY);
        String mac = MAC.encodeBase64(macContent.getBytes(),key);
        params.put("salt",salt);
        params.put("secret",mac);
    }


    /**
     * 登录后普通接口mac验参，添加参数，
     参数salt为时间，精确到毫秒的13位数字，
     参数secret验签，内容为apiKey+userId+accessToken+所有参数依次排列+salt，key为httpKey做base64解码后的16字节数据, HMACMD5算法结果取base64编码成字符串，
     *  userId 当前请求的人
     * 登录后的秘钥生成规则
     *
     *  params  为不包含token 的参数集
     */
    public static void createLoginAfterVerifyParam(Map<String,String> params,int userId, String access_token,String httpKey){

        String salt = params.remove("salt");
        if(StringUtil.isEmpty(salt)){
            salt = String.valueOf(System.currentTimeMillis());
        }
        String macContent = APIKEY + userId + access_token + ParamsSign.joinValues(params)+salt;
        byte[] key = Base64.decode(httpKey);
        String mac = MAC.encodeBase64(macContent.getBytes(),key);

        params.put("salt",salt);
        params.put("secret",mac);
        params.put("access_token",access_token);
    }



    public static void  registerDemo(){

        String enablePassword = "123456";
        Map<String,String> registerParams = new HashMap<>();
        registerParams.put("telephone","15678848276");
        registerParams.put("password",MD5.encryptHex(AES.encrypt(MD5.encrypt(enablePassword),MD5.encrypt(enablePassword))) );
        registerParams.put("nickname","哈哈哈");
        registerParams.put("areaCode","86");
        registerParams.put("sex","1");
        registerParams.put("birthday",String.valueOf(DateUtil.currentTimeSeconds()));
        registerParams.put("isSmsRegister","0");
        registerParams.put("smsCode","");

        createSecertAndSaltParam(registerParams);
        String result =  HttpUtil.sendPost("http://192.168.0.171:8092/user/register",registerParams);

        System.out.println("=======请求结果 ===>>>"+result);
    }


    public static void  loginDemo(){

        String enablePassword = "123456";
        Map<String,String> loginParams = new HashMap<>();
        //loginParams.put("telephone", MD5.encryptHex("8615678848206"));
        loginParams.put("telephone", "8615678848206");
        loginParams.put("password", MD5.encryptHex(AES.encrypt(MD5.encrypt(enablePassword),MD5.encrypt(enablePassword))) );
        //loginParams.put("areaCode","86");

        createSecertAndSaltParam(loginParams);
        //String result =  HttpUtil.sendPost("http://api.vtalkshop.com:8092/user/login",loginParams);
        String result =  HttpUtil.sendPost("http://192.168.0.27:8092/user/login",loginParams);

        System.out.println("======= loginDemo 请求结果 ===>>>"+result);
    }

    public static void  getUserDemo(){

        Map<String,String> params = new HashMap<>();
        params.put("userId", "10000290");
        int userId = 10000291;
        String access_token = "c40cfb364b654ea98bd3d5210ce534ef";
        String httpKey = "U8MSU0Y1//QoCpnDEp7OTA==";
        createLoginAfterVerifyParam(params,userId,access_token,httpKey);
        String result =  HttpUtil.sendPost("http://39.99.242.119:8094/user/get",params);

        System.out.println("=======getUserDemo 请求结果 ===>>>"+result);
    }


    public static void main(String... strings) {
        //registerDemo();
        loginDemo();
        //getUserDemo();
    }





}
