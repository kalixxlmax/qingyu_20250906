package com.basic.im.vo;

import com.basic.common.model.PageResult;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.constants.KConstants.*;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.utils.ConstantUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.basic.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JSONMessage extends JSONObject {
	private static final long serialVersionUID = 1L;
	public static final Object EMPTY_OBJECT = new Object();

	private static Logger logger= LoggerFactory.getLogger(JSONMessage.class);

	public static JSONMessage success(String resultMsg, Object data) {
		return new JSONMessage(KConstants.ResultCode.Success, resultMsg,data);
	}

	public static JSONMessage success() {
		return new JSONMessage(ResultCode.Success,null);
	}
	public static JSONMessage success(Object data) {
		return new JSONMessage(ResultCode.Success,null, data);
	}
	public static JSONMessage success(int resultCode, Object data) {
		String msg = ConstantUtil.getMsgByCode(String.valueOf(resultCode), ReqUtil.getRequestLanguage());
		return new JSONMessage(ResultCode.Success, msg, data);
	}

	public static JSONMessage failure(String resultMsg) {
		return new JSONMessage(ResultCode.Failure, resultMsg);
	}

	public static JSONMessage success(PageResult result) {
		JSONMessage success = success(null,result.getData());
		success.put("count", result.getCount());
		success.put("total", result.getTotal());
		success.put("totalVo", result.getTotalVo());
		return success;
	}
	
	public static JSONMessage error(Exception e) {
		return failureByException(e);
	}
	
	public static JSONMessage failureByErrCode(Integer errCode, Object data) {
		String msg=ConstantUtil.getMsgByCode(errCode.toString(), ReqUtil.getRequestLanguage());
		if(null!=msg){
			
			return new JSONMessage(errCode, msg,data);
		}
		return new JSONMessage(errCode, null,data);
	}

	public static JSONMessage failureByErrCode(Integer errCode) {
		return failureByErrCode(errCode, ReqUtil.getRequestLanguage());
	}

	public static JSONMessage failureByException(Exception e) {
		// 判断异常类型
		if(e instanceof ServiceException){
			ServiceException s = (ServiceException) e;
			if(0 == s.getResultCode() && !StringUtil.isEmpty(s.getMessage())) {
                return JSONMessage.failure(s.getMessage());
            }
			return failureByErrCode(s.getResultCode(),ReqUtil.getRequestLanguage());
		}else{
			logger.error(e.getMessage(),e);
			return failureByErrCode(ResultCode.InternalException, ReqUtil.getRequestLanguage());
		}

	}

	public static JSONMessage failureByErrCode(Integer errCode, String language) {
		if(StringUtil.isEmpty(language)){
			language = ConstantUtil.defLanguage;
		}
		String msg=ConstantUtil.getMsgByCode(String.valueOf(errCode), language);
		if(null!=msg){
			logger.info("===     {}     =======>",msg);
			return new JSONMessage(errCode, msg);
		}
		return new JSONMessage(errCode, null);
	}

	
	public JSONMessage() {
	}
	public JSONMessage(String errCode, String resultMsg, Object data) {
		setResultCode(errCode);
		setErrCode(errCode);
		setResultMsg(resultMsg);
		setDetailMsg(resultMsg);
		setData(data);
		setCurrentTime(DateUtil.currentTimeMilliSeconds()+"");
	}
	
	public JSONMessage(int resultCode, String resultMsg) {
		setResultCode(resultCode);
		if(0==resultCode&&StringUtil.isEmpty(resultMsg)) {
            setResultMsg(ConstantUtil.getMsgByCode(resultCode+"",ReqUtil.getRequestLanguage()));
        } else{
			setResultMsg(resultMsg);
		}
		setCurrentTime(DateUtil.currentTimeMilliSeconds());
	}

	public JSONMessage(int resultCode, String resultMsg, String detailMsg) {
		setResultCode(resultCode);
		setResultMsg(resultMsg);
		setDetailMsg(detailMsg);
		setCurrentTime(DateUtil.currentTimeMilliSeconds());
	}
	
	public static JSONMessage failureByErrCodeAndData(Integer errCode, Object data) {
		String msg=ConstantUtil.getMsgByCode(String.valueOf(errCode), ReqUtil.getRequestLanguage());
		if(null!=msg){
			return new JSONMessage(errCode,msg,data);
		}
		return new JSONMessage(errCode,"",data);
	}

	public JSONMessage(int resultCode, String resultMsg, Object data) {
		setResultCode(resultCode);
		setResultMsg(resultMsg);
		setData(data);
		setCurrentTime(DateUtil.currentTimeMilliSeconds());
	}

	public JSONMessage(String groupCode, String serviceCode, String nodeCode,
                       String resultMsg) {
		setResultCode(new StringBuffer().append(groupCode).append(serviceCode)
				.append(nodeCode).toString());
		setResultMsg(resultMsg);
	}
	public JSONMessage(String errCode, String resultMsg) {
		setResultCode(errCode);
		setErrCode(errCode);
		setResultMsg(resultMsg);
		setDetailMsg(resultMsg);
		setData(new Object());
		setCurrentTime(DateUtil.currentTimeMilliSeconds()+"");
	}
	
	public Object getCurrentTime() {
		return get("currentTime");
	}

	public void setCurrentTime(Object currentTime) {
		put("currentTime", currentTime);
	}
	public Object getErrCode() {
		return get("errCode");
	}

	public void setErrCode(Object errCode) {
		put("errCode", errCode);
	}
	public Object getResultCode() {
		return get("resultCode");
	}

	public void setResultCode(Object resultCode) {
		put("resultCode", resultCode);
	}

	public String getResultMsg() {
		return getString("resultMsg");
	}

	public void setResultMsg(String resultMsg) {
		put("resultMsg", resultMsg);
	}

	public String getDetailMsg() {
		return getString("detailMsg");
	}

	public JSONMessage setDetailMsg(String detailMsg) {
		put("detailMsg", detailMsg);
		return this;
	}

	public Object getData() {
		return get("data");
	}

	public void setData(Object data) {
		put("data", data);
	}
	
	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}


}