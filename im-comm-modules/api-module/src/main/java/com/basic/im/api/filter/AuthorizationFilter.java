package com.basic.im.api.filter;

import com.google.common.collect.Maps;
import com.basic.im.admin.jedis.AdminRedisRepository;
import com.basic.im.api.ResponseUtil;
import com.basic.im.api.service.AuthServiceOldUtils;
import com.basic.im.api.service.AuthServiceUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.utils.HttpUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.user.service.UserRedisService;
import com.basic.im.user.utils.KSessionUtil;
import com.basic.im.utils.ConstantUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.utils.SpringBeansUtils;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;

import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import javax.servlet.annotation.WebInitParam;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
@WebFilter(filterName = "authorizationfilter", urlPatterns = { "/*" }, initParams = {
		@WebInitParam(name = "enable", value = "true") })
@Component
public class AuthorizationFilter implements Filter {
	
	private Map<String, String> requestUriMap;

	private AuthorizationFilterProperties properties;
	
	private final String defLanguage="zh";

	@Autowired
	private UserRedisService userRedisService;

	@Autowired
	private AuthServiceUtils authServiceUtils;

	@Autowired
	private AuthServiceOldUtils authServiceOldUtils;

	@Autowired
	private AdminRedisRepository adminRedisRepository;

	private Logger logger=LoggerFactory.getLogger(AuthorizationFilter.class);
	@Override
	public void init(FilterConfig arg0) throws ServletException {
		
	}
	
	@Override
	public void destroy() {

	}

	@Override
	public void doFilter(ServletRequest arg0, ServletResponse arg1, FilterChain arg2)
			throws IOException, ServletException {

		if (null == requestUriMap || null == properties) {
			requestUriMap = Maps.newHashMap();
			properties = SpringBeansUtils.getContext().getBean(AuthorizationFilterProperties.class);

			for (String requestUri : properties.getRequestUriList()) {
				requestUriMap.put(requestUri, requestUri);
			}
		}
		HttpServletRequest request = (HttpServletRequest) arg0;
		HttpServletResponse response = (HttpServletResponse) arg1;

		ReqUtil.setRequestIp(HttpUtil.getIpAddress(request));  // 保存客户端IP地址

		//过滤静态文件
		String path = request.getRequestURI();

		//拒绝访问的路径设置
		if (path.contains("/actuator/beans") ||
			path.contains("/actuator/configprops") ||
			path.contains("/actuator/scheduledtasks")){
			renderByErrorKey(response, KConstants.ResultCode.AUTH_FAILED);
			return;
		}

		if (path.endsWith(".html") ||
			path.endsWith(".css") ||
			path.endsWith(".js")||
			path.endsWith(".map") ||
			path.equals("/v2/api-docs") ||path.equals("/swagger-resources")||
			path.endsWith(".png") ||
			path.endsWith(".ico") ||
			path.contains(".gif")||
			path.endsWith("/")||
			path.contains("/actuator")
		) {
			arg2.doFilter(arg0, arg1);
			return;
		}

		try {
			request.setCharacterEncoding("utf-8");
			response.setCharacterEncoding("utf-8");
			response.setContentType("text/html;charset=utf-8");

			String accessToken = request.getParameter("access_token");
			long time = NumberUtils.toLong(request.getParameter("time"), 0);
			String secret =request.getParameter("secret");
			//是否检验接口   老版客户端没有参数
			boolean falg=true;//!StringUtil.isEmpty(secret);
			String requestUri = request.getRequestURI();
			if("/favicon.ico".equals(requestUri)) {
				return;
			}

			// DEBUG**************************************************DEBUG
			StringBuffer sb = new StringBuffer();
			sb.append(request.getMethod()).append(" 请求：" + request.getRequestURI());

			logger.info(sb.toString());

			/**
			 * 部分第三方调用接口 不验证
			 */
			if(KConstants.NO_CHECKAPI_SET.contains(requestUri)){
				arg2.doFilter(arg0, arg1);
				return;
			}

			// DEBUG**************************************************DEBUG
			// 如果访问的是控制台或资源目录
			if(requestUri.startsWith("/customerService/admin") || requestUri.startsWith("/console")||requestUri.startsWith("/manualAdmin")||requestUri.startsWith("/mp")||requestUri.startsWith("/open")||requestUri.startsWith("/pages")||requestUri.startsWith("/allowRequest")){
				if(requestUri.startsWith("/console/authInterface")||requestUri.startsWith("/console/oauth/authorize")||requestUri.startsWith("/console/login")||requestUri.startsWith("/mp/login")||requestUri.startsWith("/open/login")||
						requestUri.startsWith("/pages")||requestUri.startsWith("/console/randcode/sendSms")||requestUri.startsWith("/console/find/openAdminLoginCode") ||requestUri.startsWith("/console/check/sms")||requestUri.startsWith("/console/find/admin/phone")
					|| requestUri.startsWith("/console/saveSecretKey")
						|| requestUri.startsWith("/console/getQrcode")
						|| requestUri.startsWith("/console/checkCode")
						|| requestUri.startsWith("/console/find/googleVerification")
				){
					arg2.doFilter(arg0, arg1);
					return;
				}
				checkAdminRequest(request, falg, accessToken, response, time, secret, arg0, arg1, arg2, requestUri);;
			} else {
				if(requestUri.startsWith("/config")||requestUri.startsWith("/getCurrentTime")||requestUri.equals("/getImgCode")) {
					arg2.doFilter(arg0, arg1);
					return;
				}

				checkOtherRequest(request, falg, accessToken, response, time, secret, arg0, arg1, arg2, requestUri);
			}
		}catch (Exception e){
			logger.error(e.getMessage(),e);
		}finally {
			RequestContextHolder.resetRequestAttributes();
		}


	}

	private boolean isNeedLogin(String requestUri) {
		return !requestUriMap.containsKey(requestUri.trim());
	}

	private String getUserId(String accessToekn) {
		String userId = null;

		try {
			userId = KSessionUtil.getUserIdBytoken(accessToekn);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return userId;
	}
	
	private String getAdminUserId(String accessToekn){
		String userId = null;
		try {
			userId = KSessionUtil.getAdminUserIdByToken(accessToekn);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return userId;
	}

	private static final String template = "{\"resultCode\":%1$s,\"resultMsg\":\"%2$s\"}";

	private static void renderByErrorKey(ServletResponse response, int tipsKey) {
		if(KConstants.ResultCode.AUTH_FAILED==tipsKey){
			log.info("Auth Filter 权限验证失败---");
		}
		String tipsValue = ConstantUtil.getMsgByCode(tipsKey+"", ReqUtil.getRequestLanguage());
		String s = String.format(template, tipsKey, tipsValue);

		ResponseUtil.output(response, s);
	}
	private static void renderByError(ServletResponse response, String errMsg) {
		
		String s = String.format(template, 0, errMsg);

		ResponseUtil.output(response, s);
	}
	
	// 校验后台所有相关接口
	public void checkAdminRequest(HttpServletRequest request,boolean falg,String accessToken,HttpServletResponse response,
			long time,String secret,ServletRequest arg0, ServletResponse arg1, FilterChain arg2,String requestUri) throws IOException, ServletException{
		// 需要登录
		if (isNeedLogin(request.getRequestURI())) {
			falg=true;
			// 请求令牌是否包含
			if (StringUtil.isEmpty(accessToken)) {
				logger.info("不包含请求令牌");
				int tipsKey = 1030101;
				renderByErrorKey(response, tipsKey);
				return;
			} else {
				String userId = getAdminUserId(accessToken);
				if(StringUtil.isEmpty(userId)){
					if(requestUri.startsWith("/open/getHelperList")||requestUri.startsWith("/open/codeAuthorCheck")||requestUri.startsWith("/open/authInterface")
				||requestUri.startsWith("/open/sendMsgByGroupHelper")||requestUri.startsWith("/open/webAppCheck")){
						userId = getUserId(accessToken);
					}
				}
				// 请求令牌是否有效
				if (null == userId) {
					logger.info("请求令牌无效或已过期...,token is {}，reqUti is{}",accessToken,requestUri);
					int tipsKey = 1030102;
					renderByErrorKey(response, tipsKey);
					return;
				} else {
					if(falg) {
						 if(!authServiceOldUtils.authRequestApi(userId, time, accessToken, secret,requestUri)) {
							 renderByErrorKey(response, KConstants.ResultCode.AUTH_FAILED);
							 return;
						 }
					}
					ReqUtil.setLoginUserId(Integer.parseInt(userId));
					arg2.doFilter(arg0, arg1);
					return;
				}
			}
		}else{
			/**
			 * 校验没有登陆的接口
			 */
			if(null==accessToken) {
				if(falg) {
					if(!authServiceOldUtils.authOpenApiSecret(time, secret)) {
						renderByErrorKey(response, KConstants.ResultCode.AUTH_FAILED);
						return;
					}
				}
			}

			String userId = getUserId(accessToken);
			if (null != userId) {
				ReqUtil.setLoginUserId(Integer.parseInt(userId));
			}
			arg2.doFilter(arg0, arg1);
		}
	}
	
	public void checkOtherRequest(HttpServletRequest request,boolean falg,String accessToken,HttpServletResponse response,
			long time,String secret,ServletRequest arg0, ServletResponse arg1, FilterChain arg2,String requestUri) throws IOException, ServletException{
		//设置国际化语言
		//获取请求ip地址
		String language= "zh";

				//request.getParameter("language");
		if(StringUtils.isBlank(language)){
			language = defLanguage;
		}
//		else{
//			if("zh".equals(language)){
//				language = "en";
//			}
//		}
		ReqUtil.setRequestLanguage(language);

		if(requestUri.startsWith("/yopPay")){
			if(SKBeanUtils.getImCoreService().getPayConfig().getIsOpenCloudWallet()!=1){
				logger.info("云钱包功能未开放");
				renderByErrorKey(response,10002);
				return;
			}
		}
		if(requestUri.startsWith("/manual/pay")){
			if(SKBeanUtils.getImCoreService().getPayConfig().getIsOpenManualPay()!=1){
				logger.info("扫码支付功能未开放");
				renderByErrorKey(response,10002);
				return;
			}
		}

		// 需要登录
		if (isNeedLogin(request.getRequestURI())) {
			falg=true;
			// 请求令牌是否包含
			if (StringUtil.isEmpty(accessToken)) {
				logger.info("不包含请求令牌");
				int tipsKey =1030101;
				renderByErrorKey(response, tipsKey);
				return;
			} else {
				String userId =  (null!=getUserId(accessToken))?getUserId(accessToken):getAdminUserId(accessToken);
				// 请求令牌是否有效
				if (null == userId) {
					logger.info("请求令牌无效或已过期...,token is {}，reqUti is{}",accessToken,requestUri);
					renderByErrorKey(response, 1030102);
					return;
				} else {
					if(falg && !userId.equals("10000")) {
						if((request.getHeader("user-agent")!=null && request.getHeader("user-agent").contains("MicroMessenger"))){
							// 微信小程序的请求
							if(!authServiceOldUtils.authRequestApi(userId, time, accessToken, secret,requestUri)) {
								renderByErrorKey(response, KConstants.ResultCode.AUTH_FAILED);
								return;
							}
						}else{
							/*if (null != request.getParameter("secret") && null != request.getParameter("time")){
								renderByErrorKey(response, KConstants.ResultCode.PleaseUpgradeLatestVersion);
								return;
							}*/
							//if (null != request.getParameter("secret") && null != request.getParameter("salt")) {
								Map<String, String> paramMap =request.getParameterMap().entrySet().stream()
										.collect(Collectors.toMap(Map.Entry::getKey,obj -> obj.getValue()[0]));
								if(!authServiceUtils.authRequestApiByMac(paramMap,userRedisService.queryUserSession(accessToken),requestUri)) {
									renderByErrorKey(response, KConstants.ResultCode.AUTH_FAILED);
									return;
								}
							//}
						}
					}
					try{
						if(!StringUtil.isEmpty(userId)) {
							ReqUtil.setLoginUserId(Integer.parseInt(userId));
						}
					}catch (Exception e){
						logger.error(e.getMessage(),e);
					}
					try{
						arg2.doFilter(arg0, arg1);

						return;
					}catch (Exception e){
						logger.error(e.getMessage(),e);
						return;
					}

				}
			}
		} else {
			if(requestUri.startsWith("/config")) {
				arg2.doFilter(arg0, arg1);
				return;
			}
			/**
			 * 校验没有登陆的接口
			 */
				if(falg) {
					/*if (null != request.getParameter("secret") && null != request.getParameter("time")){
						renderByErrorKey(response, KConstants.ResultCode.PleaseUpgradeLatestVersion);
						return;
					}*/
					if (null != request.getParameter("secret") && null != request.getParameter("salt")) {
						 Map<String, String[]> parameterMap = request.getParameterMap();
						 if(parameterMap.isEmpty()) {
							 renderByErrorKey(response, KConstants.ResultCode.AUTH_FAILED);
						 }
						 Map<String, String> paramMap = parameterMap.entrySet().stream()
									.collect(Collectors.toMap(Map.Entry::getKey, obj -> obj.getValue()[0]));
						if (!authServiceUtils.authOpenApiByMac(paramMap)) {
							renderByErrorKey(response, KConstants.ResultCode.AUTH_FAILED);
							return;
						}
					} /*else {
						if(!authServiceOldUtils.authOpenApiSecret(time, secret)) {
							renderByErrorKey(response, KConstants.ResultCode.AUTH_FAILED);
							return;
						}
					}*/
				}
			}
			


			if(null!=accessToken){
			String userId = getUserId(accessToken);
				try{
					if(!StringUtil.isEmpty(userId)) {
						ReqUtil.setLoginUserId(Integer.parseInt(userId));
					}
				}catch (Exception e){
					logger.error(e.getMessage(),e);
			}
			}
			try{
				arg2.doFilter(arg0, arg1);
				return;
			}catch (Exception e){
				logger.error(e.getMessage(),e);
				return;
			}
		}
}


