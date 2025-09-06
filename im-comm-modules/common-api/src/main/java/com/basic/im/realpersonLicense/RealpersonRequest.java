package com.basic.im.realpersonLicense;

import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.cloudauth.model.v20190307.DescribeVerifyResultRequest;
import com.aliyuncs.cloudauth.model.v20190307.DescribeVerifyResultResponse;
import com.aliyuncs.cloudauth.model.v20190307.DescribeVerifyTokenRequest;
import com.aliyuncs.cloudauth.model.v20190307.DescribeVerifyTokenResponse;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.profile.DefaultProfile;
import com.google.gson.Gson;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.entity.SmsConfig;
import com.basic.im.utils.SKBeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author zhm
 * @version V1.0
 * @Description: 实人认证
 * @date 2020/4/1 16:33
 */
@Slf4j
@Component
public class RealpersonRequest {

    /**
     * 发起认证请求，获取认证Token
     */
    public String getDescribeVerifyToken(String bizId){
        SmsConfig smsConfig = SKBeanUtils.getImCoreService().getSmsConfig();

        DefaultProfile profile = DefaultProfile.getProfile("cn-hangzhou", smsConfig.getAccesskeyid(), smsConfig.getAccesskeysecret());
        IAcsClient client = new DefaultAcsClient(profile);

        DescribeVerifyTokenRequest request = new DescribeVerifyTokenRequest();
        request.setRegionId("cn-hangzhou");
        request.setBizId(bizId);
        request.setBizType(smsConfig.getBizType());
        try {
            DescribeVerifyTokenResponse response = client.getAcsResponse(request);
            String verifyResult = new Gson().toJson(response);
            log.info(" getDescribeVerifyToken : {}",verifyResult);
            JSONObject verifyTokenObject = JSONObject.parseObject(verifyResult);
            verifyTokenObject.remove("requestId");
            String verifyToken = verifyTokenObject.getString("verifyToken");
            verifyTokenObject.remove("verifyToken");
            verifyTokenObject.remove("ossUploadToken");
            verifyTokenObject.put("token",verifyToken);
            return verifyTokenObject.toJSONString();
        } catch (ServerException e) {
            e.printStackTrace();
            throw new ServiceException(KConstants.ResultCode.RealpersonRequestError);
        } catch (ClientException e) {
            System.out.println("ErrCode:" + e.getErrCode());
            System.out.println("ErrMsg:" + e.getErrMsg());
            System.out.println("RequestId:" + e.getRequestId());
            throw new ServiceException(KConstants.ResultCode.RealpersonRequestError);
        }
    }
    
    /**
     * 获取查询认证结果
     */
    public String describeVerifyResult(String bizId){
        SmsConfig smsConfig = SKBeanUtils.getImCoreService().getSmsConfig();
        DefaultProfile profile = DefaultProfile.getProfile("cn-hangzhou", smsConfig.getAccesskeyid(), smsConfig.getAccesskeysecret());
        IAcsClient client = new DefaultAcsClient(profile);

        DescribeVerifyResultRequest request = new DescribeVerifyResultRequest();
        request.setRegionId("cn-hangzhou");
        request.setBizId(bizId);
        request.setBizType(smsConfig.getBizType());

        try {
            DescribeVerifyResultResponse response = client.getAcsResponse(request);
            String verifyResult = new Gson().toJson(response);
            log.info("describeVerifyResult :{}",verifyResult);
            return verifyResult;
        } catch (ServerException e) {
            e.printStackTrace();
            throw new ServiceException(KConstants.ResultCode.RealpersonResultError);
        } catch (ClientException e) {
            System.out.println("ErrCode:" + e.getErrCode());
            System.out.println("ErrMsg:" + e.getErrMsg());
            System.out.println("RequestId:" + e.getRequestId());
            throw new ServiceException(KConstants.ResultCode.RealpersonResultError);
        }

    }

}
