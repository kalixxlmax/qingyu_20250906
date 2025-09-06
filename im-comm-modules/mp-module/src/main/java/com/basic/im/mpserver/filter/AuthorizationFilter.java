package com.basic.im.mpserver.filter;

import com.google.common.collect.Maps;
import com.basic.im.api.service.AuthServiceOldUtils;
import com.basic.im.api.service.AuthServiceUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.utils.HttpUtil;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.mpserver.utils.ResponseUtil;
import com.basic.im.user.service.UserRedisService;
import com.basic.im.user.utils.KSessionUtil;
import com.basic.im.utils.ConstantUtil;
import com.basic.im.utils.SpringBeansUtils;
import com.basic.utils.StringUtil;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import javax.servlet.annotation.WebInitParam;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;


@WebFilter(filterName = "authorizationfilter", urlPatterns = { "/*" }, initParams = {
		@WebInitParam(name = "enable", value = "true") })
@Component
public class AuthorizationFilter implements Filter {
	
	private Map<String, String> requestUriMap;
	
	private AuthorizationFilterProperties properties;

	private final String defLanguage="zh";

	private Logger logger=LoggerFactory.getLogger(AuthorizationFilter.class);

	@Autowired
	private UserRedisService userRedisService;

	@Autowired
	@Lazy
	private AuthServiceUtils authServiceUtils;
	@Autowired
	@Lazy
	private AuthServiceOldUtils authServiceOldUtils;

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

		logger.info("请求地址："+ getRequestPast(request));

		ReqUtil.setRequestIp(HttpUtil.getIpAddress(request));  // 保存客户端IP地址

		request.setCharacterEncoding("utf-8"); 
		response.setCharacterEncoding("utf-8"); 
		response.setContentType("text/html;charset=utf-8");
		String accessToken = request.getParameter("access_token");
		long time = NumberUtils.toLong(request.getParameter("time"), 0);
		
		String secret =request.getParameter("secret");
		//是否检验接口   老版客户端没有参数
		boolean falg=!StringUtil.isEmpty(secret);
		String requestUri = request.getRequestURI();
		if("/favicon.ico".equals(requestUri)) {
            return;
        }
		// DEBUG**************************************************DEBUG
		StringBuffer sb = new StringBuffer();
		sb.append(request.getMethod()).append(" 请求：" + request.getRequestURI());
		
		logger.info(sb.toString());
		
		// DEBUG**************************************************DEBUG
		
		//如果访问的是公众号后台或资源目录
		if(requestUri.startsWith("/mp/") || requestUri.startsWith("/public/") || requestUri.startsWith("/customerService/admin/")) {
			
			if(requestUri.endsWith(".html") ||  requestUri.endsWith(".png") || requestUri.endsWith(".css") || 
					 requestUri.endsWith(".gif") || requestUri.endsWith(".proto")||requestUri.endsWith(".js")||
					requestUri.endsWith(".jpg") || requestUri.endsWith(".ico") || requestUri.startsWith("/mp/common/")||
					requestUri.startsWith("/mp/login") ||requestUri.startsWith("/mp/js/") || requestUri.endsWith(".map") ||
					requestUri.startsWith("/mp/find/comment/count")) {
				arg2.doFilter(arg0, arg1);
				return;
			}
			checkMpRequest(request, falg, accessToken, response, time, secret, arg0, arg1, arg2, requestUri);
		}
	}

	private boolean isNeedLogin(String requestUri) {
		return !requestUriMap.containsKey(requestUri.trim());
	}

	private String getUserId(String _AccessToken) {
		String userId = null;

		try {
			userId = KSessionUtil.getUserIdBytoken(_AccessToken);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return userId;
	}
	
	private String getAdminUserId(String _AccessToken){
		String userId = null;
		try {
			userId = KSessionUtil.getAdminUserIdByToken(_AccessToken);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return userId;
	}

	private static final String template = "{\"resultCode\":%1$s,\"resultMsg\":\"%2$s\"}";

	private static void renderByErrorKey(ServletResponse response, int tipsKey) {
		String tipsValue = ConstantUtil.getMsgByCode(tipsKey+"", "zh");
		String s = String.format(template, tipsKey, tipsValue);

		ResponseUtil.output(response, s);
	}
	private static void renderByError(ServletResponse response, String errMsg) {
		
		String s = String.format(template, 0, errMsg);

		ResponseUtil.output(response, s);
	}
	 
	//校验公众号相关接口
	public void checkMpRequest(HttpServletRequest request,boolean falg,String accessToken,HttpServletResponse response,
			long time,String secret,ServletRequest arg0, ServletResponse arg1, FilterChain arg2,String requestUri) throws IOException, ServletException{
		//设置国际化语言
		//获取请求ip地址
		String language= request.getParameter("language");
		language=StringUtil.isEmpty(language)?defLanguage:language;
		ReqUtil.setRequestLanguage(language);
		// 需要登录
		if (isNeedLogin(request.getRequestURI())) {
			falg=true;
			// 请求令牌是否包含
			if (StringUtil.isEmpty(accessToken)) {
				logger.info("不包含请求令牌");
				int tipsKey =1030101;
				renderByErrorKey(response, tipsKey);
			} else {
				String userId = (requestUri.startsWith("/mp/")) ? getAdminUserId(accessToken) : getUserId(accessToken);

				if(StringUtil.isEmpty(userId)){
					if(requestUri.startsWith("/mp/sava/comment") || requestUri.startsWith("/mp/give/like")
					|| requestUri.startsWith("/mp/find/content") || requestUri.startsWith("/mp/find/comment")){
						userId = getUserId(accessToken);
					}
				}
				// 请求令牌是否有效
				if(StringUtil.isEmpty(userId)){
					logger.info("请求令牌无效或已过期...,token is {}，reqUti is{}",accessToken,requestUri);
					int tipsKey = 1030102;
					renderByErrorKey(response, tipsKey);
				} else {
					if(requestUri.startsWith("/public/pushToAll") || requestUri.startsWith("/public/manyToAll") ||
					requestUri.startsWith("/mp/sava/comment") || requestUri.startsWith("/mp/give/like") ||
					requestUri.startsWith("/mp/find/content") || requestUri.startsWith("/mp/find/comment")) {
                        falg = false;
                    }

					if(falg) {

                        if(null!=request.getParameter("secret") && null!=request.getParameter("salt")){
							Map<String, String> paramMap =request.getParameterMap().entrySet().stream()
									.collect(Collectors.toMap(Map.Entry::getKey,obj -> obj.getValue()[0]));
								if(!authServiceUtils.authRequestApiByMac(paramMap,userRedisService.queryUserSession(accessToken),requestUri)) {
									renderByErrorKey(response, KConstants.ResultCode.AUTH_FAILED);
									return;
								}
						}else {
							if (!authServiceOldUtils.authRequestApi(userId, time, accessToken, secret, requestUri)) {
								renderByErrorKey(response, KConstants.ResultCode.AUTH_FAILED);
								return;
							}
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
						renderByError(response, "授权认证失败");
						return;
					}
				}
			}
			
			String userId = getAdminUserId(accessToken);
			if (null != userId) {
				ReqUtil.setLoginUserId(Integer.parseInt(userId));
			}
			arg2.doFilter(arg0, arg1);
		}
	}


	/**
	 *  获取请求完整路径与参数
	 * @param request
	 * @return
	 */
	public String getRequestPast(HttpServletRequest request){
		String method = request.getMethod();
		if (method.equalsIgnoreCase("get")){
			return request.getRequestURL() + "?" + request.getQueryString();
		}

		Map<String, String[]> params = request.getParameterMap();
		String queryString = "";
		for (String key : params.keySet()) {
			String[] values = params.get(key);
			for (int i = 0; i < values.length; i++) {
				String value = values[i];
				queryString += key + "=" + value + "&";
			}
		}
		// 去掉最后一个空格
		queryString = queryString.substring(0, queryString.length() - 1);
		return request.getRequestURL() + "?" + queryString;
	}

}
