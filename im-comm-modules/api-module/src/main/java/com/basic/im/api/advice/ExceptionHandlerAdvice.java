package com.basic.im.api.advice;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.basic.im.api.ResponseUtil;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.utils.ConstantUtil;
import com.basic.im.vo.JSONMessage;
import org.apache.catalina.connector.ClientAbortException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.BindException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.EOFException;
import java.util.Map;


@ControllerAdvice
public class ExceptionHandlerAdvice {

	private Logger logger=LoggerFactory.getLogger(ExceptionHandlerAdvice.class);





	@ExceptionHandler(value ={MissingServletRequestParameterException.class})
	@ResponseBody
	public JSONMessage errorHandler(MissingServletRequestParameterException ex) throws Exception {
		logger.error("RequestParameterException info:{}",ex.getMessage());


		return new JSONMessage(KConstants.ResultCode.ParamsAuthFail,ex.getMessage());
	}

	@ExceptionHandler(value ={BindException.class})
	@ResponseBody
	public JSONMessage bindExceptionErrorHandler(BindException ex) throws Exception {
		logger.error("bindExceptionErrorHandler info:{}",ex.getMessage());


		return new JSONMessage(KConstants.ResultCode.ParamsAuthFail,ex.getFieldError().getDefaultMessage());

		/*if (e instanceof MissingServletRequestParameterException
				|| e instanceof BindException) {
			resultCode = ResultCode.ParamsAuthFail;
			resultMsg = getResultCode(resultCode);
		}*/
	}


	@ExceptionHandler(value = { Exception.class, ServiceException.class, RuntimeException.class })
	public void handleErrors(HttpServletRequest request,
			HttpServletResponse response, Exception e) throws Exception {
		int resultCode = KConstants.ResultCode.InternalException;
		String resultMsg =getResultCode(resultCode);
		String detailMsg = "";
		logger.info(request.getRequestURI() + "错误：");
		 if (e instanceof ServiceException) {
			ServiceException ex = ((ServiceException) e);
			resultCode = ex.getResultCode();
			resultMsg =(0==resultCode&&null!=ex.getErrMessage())?ex.getErrMessage(): getResultCode(ex.getResultCode());
		} else if (e instanceof ClientAbortException) {
			resultCode=-1;
		}else if(e instanceof EOFException){
			detailMsg = e.getMessage();
		}else {
			e.printStackTrace();
			detailMsg = e.getMessage();
		}
		logger.info("error msg is {},exception is {}" , resultMsg,e.getMessage());
		Map<String, Object> map = Maps.newHashMap();
		map.put("resultCode", resultCode);
		map.put("resultMsg", resultMsg);
		map.put("detailMsg", detailMsg);

		String text = JSONObject.toJSONString(map);

		ResponseUtil.output(response, text);
	}
	
	public String getResultCode(Integer resultCode){
		return ConstantUtil.getMsgByCode(resultCode.toString(), ReqUtil.getRequestLanguage());
	}
}
