package com.basic.im.oneclickLogin;

import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.dypnsapi.model.v20170525.GetMobileRequest;
import com.aliyuncs.dypnsapi.model.v20170525.GetMobileResponse;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.profile.DefaultProfile;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.entity.SmsConfig;
import com.basic.im.utils.SKBeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2020/3/18 12:32
 */
@Slf4j
@Component
public class OneCickLoginApi {
    /**
     * 一键登录取号API
     * @param
     * @return 
     */
    private String getMobile(String accessToken,String outId){
        SmsConfig smsConfig = SKBeanUtils.getImCoreService().getSmsConfig();
        String accessKeyId = smsConfig.getAccesskeyid();
        String accessSecret = smsConfig.getAccesskeysecret();
//        DefaultProfile profile = DefaultProfile.getProfile("cn-hangzhou", "<accessKeyId>", "<accessSecret>");
        DefaultProfile profile = DefaultProfile.getProfile("cn-hangzhou", accessKeyId, accessSecret);
        IAcsClient client = new DefaultAcsClient(profile);
        GetMobileRequest request = new GetMobileRequest();
        request.setRegionId("cn-hangzhou");
        request.setAccessToken(accessToken);
        if(!StringUtil.isEmpty(outId)) {
            request.setOutId(outId);
        }
        try {
            GetMobileResponse response = client.getAcsResponse(request);
            log.info("GetMobileResponse response :{}", JSONObject.toJSONString(response));
            return JSONObject.toJSONString(response);
        } catch (ServerException e) {
            log.error(e.getMessage(),e);
            return null;
        } catch (ClientException e) {
            log.error("ErrCode:" + e.getErrCode());
            log.error("ErrMsg:" + e.getErrMsg());
            log.error("RequestId:" + e.getRequestId());
            return null;
        }
    }
    
    /**
     *  一键登录获取格式化的手机号
     * @param
     * @return 
     */
    public String getFormatMobile(String token){
        String mobile = null;
        try {
            mobile = getMobile(token, null);
        } catch (Exception e) {
            log.error("一键登录取号失败",e);
        }
        JSONObject jsonOneCickLogin = JSONObject.parseObject(mobile);
        if(!"OK".equals(jsonOneCickLogin.getString("code"))){
            log.info("getFormatMobile 一键登录response ：{}",jsonOneCickLogin.toJSONString());
            throw new ServiceException(KConstants.ResultCode.FailedToGetNumber);
        }
        String phone = JSONObject.parseObject(jsonOneCickLogin.getString("getMobileResultDTO")).getString("mobile");
        if(StringUtil.isEmpty(phone)){
            log.error("getFormatMobile phone is null");
            throw new ServiceException(KConstants.ResultCode.FailedToGetNumber);
        }
        return phone;
    }

}
