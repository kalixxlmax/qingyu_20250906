package com.basic.im.comm.utils;

import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.mongodb.DBObject;
import com.basic.im.comm.ex.ServiceException;
import com.basic.utils.StringUtil;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.List;
import java.util.Objects;

public class ReqUtil {

	private static final String name = "LOGIN_USER_ID";
	public static  String DEFAULT_LANG="zh";
	private static final String LangName = "REQUEST_LANGEUAGE";
	private static final String REQUEST_IP = "REQUEST_IP";

	private final static Logger logger = LoggerFactory.getLogger(ReqUtil.class);


	public static void setLoginUserId(String userId){
		if (!StringUtil.isEmpty(userId)) {
			ReqUtil.setLoginUserId(Integer.parseInt(userId));
		}
	}

	public static void setLoginUserId(int userId) {
		try {
			Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).setAttribute(name, userId, RequestAttributes.SCOPE_REQUEST);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}


	public static Integer getUserId() {
		// 获取AuthorizationFilter通过查询令牌用户映射设置的userId
		try{
			Object obj = Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).getAttribute(name, RequestAttributes.SCOPE_REQUEST);
			return null == obj ? 0 : Integer.parseInt(obj.toString());
		}catch (NullPointerException e){
			return 0;
		}
		// if (null == obj) {
		// HttpServletRequest request = ((ServletRequestAttributes)
		// RequestContextHolder.getRequestAttributes()).getRequest();
		// obj = request.getParameter("userId");
		// obj = (null == obj || "".equals(obj)) ? null : obj;
		// }
	}

	public static void setRequestLanguage(String language){
		Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).setAttribute(LangName, language, RequestAttributes.SCOPE_REQUEST);
	}

	public static String getRequestLanguage(){
		Object obj = Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).getAttribute(LangName, RequestAttributes.SCOPE_REQUEST);
		if(null==obj) {
			return DEFAULT_LANG;
		}
		return obj.toString();
	}




	public static ObjectId parseId(String s) {
		try {
			return (null == s || "".equals(s.trim())) ? null : new ObjectId(s);
		} catch (Exception e) {
			throw new ServiceException("请求参数错误");
		}
	}

	public static DBObject parseDBObj(String s) {
		return (DBObject) com.mongodb.util.JSON.parse(s);
	}

	public static List<ObjectId> parseArray(String text) {
		try {
			return new ObjectMapper().readValue(text, TypeFactory.defaultInstance().constructCollectionType(List.class, ObjectId.class));
		} catch (Exception e) {
			throw new ServiceException("请求参数错误");
		}
	}

	public static void setRequestIp(String requestIp) {
		try {
			Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).setAttribute(REQUEST_IP, requestIp, RequestAttributes.SCOPE_REQUEST);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
	}

	public static String getRequestIp() {
		try{
			String obj = StrUtil.toString(Objects.requireNonNull(RequestContextHolder.getRequestAttributes()).getAttribute(REQUEST_IP, RequestAttributes.SCOPE_REQUEST));
			return null == obj ? StrUtil.EMPTY : obj;
		}catch (NullPointerException e){
			return StrUtil.EMPTY;
		}
	}
}
