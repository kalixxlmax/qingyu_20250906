package com.basic.im.comm.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.SerializeWriter;
import com.google.common.collect.Maps;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.utils.FileUtil;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import javax.net.ssl.SSLHandshakeException;
import javax.servlet.http.HttpServletRequest;
import java.util.*;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.regex.Pattern.*;

public final class HttpUtil {



	public static class Request {
		private Map<String, Object> data = Maps.newHashMap();
		private RequestMethod method;
		private String spec;

		public Request() {
			super();
		}

		public Request(Map<String, Object> data, String spec) {
			super();
			this.data = data;
			this.spec = spec;
		}

		public Request(Map<String, Object> data, RequestMethod method, String spec) {
			super();
			this.data = data;
			this.method = method;
			this.spec = spec;
		}

		public Map<String, Object> getData() {
			return data;
		}

		public RequestMethod getMethod() {
			return method;
		}

		public String getSpec() {
			return spec;
		}

		public void setData(Map<String, Object> data) {
			this.data = data;
		}

		public void setMethod(RequestMethod method) {
			this.method = method;
		}

		public void setSpec(String spec) {
			this.spec = spec;
		}

	}

	public static enum RequestMethod {
		DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT, TRACE
	}

	private static byte[] getBytes(Map<String, Object> data) throws Exception {
		StringBuffer sb = new StringBuffer();
		for (Map.Entry<String, Object> entry : data.entrySet()) {
            sb.append(entry.getKey()).append('=').append(entry.getValue()).append('&');
        }
		return sb.substring(0, sb.length() - 1).getBytes("UTF-8");
	}

	public static String get(Request request) throws Exception {
		URL url = new URL(buildSpec(request));
		HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
		urlConn.setDoOutput(true);
		urlConn.setDoInput(true);
		urlConn.setRequestMethod("GET");

		return FileUtil.readAll(urlConn.getInputStream());
	}

	private static String buildSpec(Request request) {
		Map<String, Object> params = request.getData();
		StringBuffer sb = new StringBuffer();
		sb.append(request.getSpec());
		if (!params.isEmpty()) {
            sb.append("?");
        }
		for (Map.Entry<String, Object> entry : params.entrySet()) {
			sb.append(entry.getKey()).append('=').append(entry.getValue()).append('&');
		}
		String spec = sb.substring(0, sb.length() - 1);
		System.out.println(spec);
		return spec;
	}

	public static String asString(Request request) throws Exception {
		URL url = new URL(request.getSpec());
		HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
		urlConn.setDoOutput(true);
		urlConn.setDoInput(true);
		// urlConn.setRequestMethod("POST");
		// urlConn.setRequestMethod(method);
		urlConn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
		urlConn.setUseCaches(false);
		urlConn.setInstanceFollowRedirects(true);
		if (null != request.getData()) {
			OutputStream out = urlConn.getOutputStream();
			out.write(getBytes(request.getData()));
			out.flush();
			out.close();
		}

		return FileUtil.readAll(urlConn.getInputStream());
	}
	public static String  asBean(Request request) throws Exception {
		return asString(request);
	}
	@SuppressWarnings("unchecked")
	public static <T> T asBean(Request request, Class<?> clazz) throws Exception {
		String text = asString(request);

		return (T) JSON.parseObject(text, clazz);
	}

	public static <T> T asBean(Map<String, Object> data, String spec, Class<?> clazz) throws Exception {
		return asBean(new Request(data, spec), clazz);
	}

	public static byte[] getBytes(Object object) {
		SerializeWriter out = new SerializeWriter();
		JSONSerializer.write(out, object);
		return out.toBytes("UTF-8");
	}




    /*
     * 以下为向服务器发送 HTTP 请求
     *
     *  */


    private static Log log = LogFactory.getLog(HttpUtil.class);

	/**
	 *  UTF-8
	 */
	public static final String URL_PARAM_DECODECHARSET_UTF8 = "UTF-8";

	/**
	 *  GBK
	 */
	public static final String URL_PARAM_DECODECHARSET_GBK = "GBK";

	private static final String URL_PARAM_CONNECT_FLAG = "&";

	private static final String EMPTY = "";

	private static MultiThreadedHttpConnectionManager connectionManager = null;

	private static int connectionTimeOut = 25000;

	private static int socketTimeOut = 25000;

	private static int maxConnectionPerHost = 20;

	private static int maxTotalConnections = 20;

	private static HttpClient client;

	static{
		connectionManager = new MultiThreadedHttpConnectionManager();
		connectionManager.getParams().setConnectionTimeout(connectionTimeOut);
		connectionManager.getParams().setSoTimeout(socketTimeOut);
		connectionManager.getParams().setDefaultMaxConnectionsPerHost(maxConnectionPerHost);
		connectionManager.getParams().setMaxTotalConnections(maxTotalConnections);
		client = new HttpClient(connectionManager);
	}


	/**
	 * POST
	 * @param url
	 * 			URL
	 * @param params
	 *
	 * @param enc
	 *
	 * @return
	 *
	 * @throws IOException
	 *
	 */
	public static String URLPost(String url, Map<String, Object> params){
		String enc = URL_PARAM_DECODECHARSET_UTF8;
		String response = EMPTY;
		PostMethod postMethod = null;
		try {
			postMethod = new PostMethod(url);
			postMethod.setRequestHeader("Content-Type", "application/x-www-form-urlencoded;charset=" + enc); //设置请求头
			if(null!=params){
				for(Map.Entry<String, Object> entry : params.entrySet()){
					postMethod.addParameter(entry.getKey(), entry.getValue().toString());
				}
			}

			int statusCode = client.executeMethod(postMethod);
			if(statusCode == HttpStatus.SC_OK) {
				response = postMethod.getResponseBodyAsString();
				System.out.println("result:"+response);
			}else{
				log.error("请求失败 = " + postMethod.getStatusCode());
			}
		}catch(HttpException e){
			log.error("HttpException", e);
			e.printStackTrace();
		}catch(IOException e){
			log.error("IOException", e);
			e.printStackTrace();
		}finally{
			if(postMethod != null){
				postMethod.releaseConnection();
				postMethod = null;
			}
		}

		return response;
	}


	/**
	 * GET提交方式
	 * @param url
	 * 			URL
	 * @param params
	 *
	 * @param enc
	 *
	 * @return
	 *
	 * @throws IOException
	 *
	 */
	public static String URLGet(String url, Map<String, String> params ){
		String enc = URL_PARAM_DECODECHARSET_UTF8;
		String response = EMPTY;
		GetMethod getMethod = null;
		StringBuffer strtTotalURL = new StringBuffer(EMPTY);

	    if(strtTotalURL.indexOf("?") == -1) {
	      strtTotalURL.append(url).append("?").append(getUrl(params, enc));
	    } else {
	    	strtTotalURL.append(url).append("&").append(getUrl(params, enc));
	    }
	    log.debug("GETURL = \n" + strtTotalURL.toString());

		try {
			getMethod = new GetMethod(strtTotalURL.toString());
			getMethod.setRequestHeader("Content-Type", "application/x-www-form-urlencoded;charset=" + enc);

			int statusCode = client.executeMethod(getMethod);
			if(statusCode == HttpStatus.SC_OK) {
				response = getMethod.getResponseBodyAsString();
				System.out.println("result:"+response);
			}else{
				log.debug(" = " + getMethod.getStatusCode());
			}
		}catch(HttpException e){
			log.error("HttpException", e);
			e.printStackTrace();
		}catch(IOException e){
			log.error("IOException", e);
			e.printStackTrace();
		}finally{
			if(getMethod != null){
				getMethod.releaseConnection();
				getMethod = null;
			}
		}

		return response;
	}


	/**
	 *
	 * @param map
	 * 			Map
	 * @param valueEnc
	 * 			UR
	 * @return
	 * 			URL
	 */
	private static String getUrl(Map<String, String> map, String valueEnc) {

		if (null == map || map.keySet().size() == 0) {
			return (EMPTY);
		}
		StringBuffer url = new StringBuffer();
		Set<String> keys = map.keySet();
		for (Iterator<String> it = keys.iterator(); it.hasNext();) {
			String key = it.next();
			if (map.containsKey(key)) {
				String val = map.get(key);
				String str = val != null ? val : EMPTY;
				try {
					str = URLEncoder.encode(str, valueEnc);
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				url.append(key).append("=").append(str).append(URL_PARAM_CONNECT_FLAG);
			}
		}
		String strURL = EMPTY;
		strURL = url.toString();
		if (URL_PARAM_CONNECT_FLAG.equals(EMPTY + strURL.charAt(strURL.length() - 1))) {
			strURL = strURL.substring(0, strURL.length() - 1);
		}

		return (strURL);
	}

	/** @Description: 检测当前URL是否404（有效）
	 * @param address
	 * @param spareAddress
	 * @return
	 * @throws Exception
	 **/
    public static String testWsdlConnection(String address,String spareAddress) throws Exception {
		int status = 404;
		try {
			URL urlObj = new URL(address);
			HttpURLConnection oc = (HttpURLConnection) urlObj.openConnection();
			oc.setUseCaches(false);
			oc.setConnectTimeout(100); // 设置超时时间
			status = oc.getResponseCode();// 请求状态
			if (404 == status) {
				return spareAddress;
			}else{
				return address;
			}
		} catch (ServiceException e) {
			e.printStackTrace();
			return spareAddress;
		} catch (UnknownHostException e) {
			return spareAddress;
		} catch (SSLHandshakeException e) {
			return spareAddress;
		}
	}

	/**
	 * 获取请求主机IP地址,如果通过代理进来，则透过防火墙获取真实IP地址;
	 */
	public static String getIpAddress(HttpServletRequest request)  {
		// 获取请求主机IP地址,如果通过代理进来，则透过防火墙获取真实IP地址
		String ip = request.getHeader("X-Forwarded-For");
		try {
			if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
				if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
					ip = request.getHeader("Proxy-Client-IP");
				}
				if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
					ip = request.getHeader("WL-Proxy-Client-IP");
				}
				if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
					ip = request.getHeader("HTTP_CLIENT_IP");
				}
				if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
					ip = request.getHeader("HTTP_X_FORWARDED_FOR");
				}
				if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
					ip = request.getRemoteAddr();
				}
			} else if (ip.length() > 15) {
				String[] ips = ip.split(",");
				for (String str : ips) {
					if (!("unknown".equalsIgnoreCase(str))) {
						ip = str;
						break;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			ip = request.getRemoteAddr();
		}
		return ip;
	}

	/**
	 * 获取完整URL中的域名
	 * @param url 网址
	 * @return 域名
	 */
	public static List<String> getUrlDomain(String url){
		Pattern p;
		if (StringUtil.isEmpty(url)){
			return new ArrayList<>();
		}
		try {
			//截取域名部分字符串正则
			if (url.startsWith("https")){
				p = compile("(?<=http://|\\.)[^.]*?\\.("+ KConstants.ALLDOMAIN +")", CASE_INSENSITIVE);
			}else{
				p = compile("(?<=https://|\\.)[^.]*?\\.("+ KConstants.ALLDOMAIN +")", CASE_INSENSITIVE);
			}
			URL thisUrl = new URL(url);
			List<String> domainList = new ArrayList<String>(){{
				add(thisUrl.getHost());
			}};
			//截取域名字符，保留后半部分
			IntStream.range(0,domainList.size()).forEach(i->{
				String element = domainList.get(i);
				try {
					Matcher matcher = p.matcher(element);
					matcher.find();
					domainList.set(i,matcher.group());
				}catch (Exception e){
				}
			});
			return domainList;
		}catch (Exception e){
			e.printStackTrace();
			return new ArrayList<>();
		}
	}

	/**
	 * Get请求
	 * @param httpGet
	 * @return
	 */
	public static String doGet(HttpGet httpGet) {
		CloseableHttpClient httpClient = null;
		try {
			httpClient = HttpClients.createDefault();
			RequestConfig requestConfig = RequestConfig.custom()
					.setConnectTimeout(5000).setConnectionRequestTimeout(10000)
					.setSocketTimeout(5000).build();
			httpGet.setConfig(requestConfig);
			HttpResponse httpResponse = httpClient.execute(httpGet);
			if (httpResponse.getStatusLine().getStatusCode() == 200 ||
					httpResponse.getStatusLine().getStatusCode() == 302) {
				HttpEntity entity = httpResponse.getEntity();
				return EntityUtils.toString(entity, "utf-8");
			} else {
				log.error("Request StatusCode=" + httpResponse.getStatusLine().getStatusCode());
			}
		} catch (Exception e) {
			log.error("Request Exception={}:", e);
		} finally {
			if (httpClient != null) {
				try {
					httpClient.close();
				} catch (IOException e) {
					log.error("关闭httpClient失败", e);
				}
			}
		}
		return null;
	}

	/**
	 * IP解析域名
	 * 爬虫方式在 dns.aizhan.com 网站获取IP对应全部域名
	 * @param ip
	 * @return
	 */
	public static List<String> domainSpiderOfAizan(String ip) {
		String host = "dns.aizhan.com";
		String url = "https://" + host + "/" + ip + "/";

		HttpGet httpGet = new HttpGet(url);
		httpGet.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8");
		httpGet.setHeader("Accept-Encoding", "gzip, deflate, br");
		httpGet.setHeader("Accept-Language", "zh-CN,zh;q=0.9");
		httpGet.setHeader("Connection", "keep-alive");
		httpGet.setHeader("Cookie", "_csrf=64e78ae884b2f7c61eb69d507a619b0ba0c65778204dc1009b9ddfea24290480a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22wgL4L2-cZ0RvtpiqE-N4flrtSOpck07i%22%3B%7D");
		httpGet.setHeader("DNT", "1");
		httpGet.setHeader("Host", host);
		httpGet.setHeader("Referer", "https://" + host + "/");
		httpGet.setHeader("Upgrade-Insecure-Requests", "1");
		httpGet.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36");

		String result = doGet(httpGet);
		Document document = Jsoup.parse(result);
		if (document == null) {
			return new ArrayList<>();
		}
		List<String> list = document.getElementsByClass("domain").eachText();
		if (list.size() > KConstants.ZERO){
			list.remove(0);
			return list;
		}
		return new ArrayList<>();
	}

	/**
	 * IP解析域名
	 * 爬虫方式在 site.ip138.com 网站获取IP对应全部域名
	 * @param ip
	 * @return
	 */
	public static List<String> domainSpiderOfIp138(String ip) {
		String host = "site.ip138.com";
		String url = "http://" + host + "/" + ip + "/";

		HttpGet httpGet = new HttpGet(url);
		httpGet.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8");
		httpGet.setHeader("Accept-Encoding", "gzip, deflate");
		httpGet.setHeader("Accept-Language", "zh-CN,zh;q=0.9");
		httpGet.setHeader("Cache-Control", "max-age=0");
		httpGet.setHeader("Connection", "keep-alive");
		httpGet.setHeader("Cookie", "Hm_lvt_d39191a0b09bb1eb023933edaa468cd5=1553090128; BAIDU_SSP_lcr=https://www.baidu.com/link?url=FS0ccst469D77DpdXpcGyJhf7OSTLTyk6VcMEHxT_9_&wd=&eqid=fa0e26f70002e7dd000000065c924649; pgv_pvi=6200530944; pgv_si=s4712839168; Hm_lpvt_d39191a0b09bb1eb023933edaa468cd5=1553093270");
		httpGet.setHeader("DNT", "1");
		httpGet.setHeader("Host", host);
		httpGet.setHeader("Upgrade-Insecure-Requests", "1");
		httpGet.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36");

		String result = doGet(httpGet);
		Document document = Jsoup.parse(result);
		if (document == null) {
			return new ArrayList<>();
		}
		Element listEle = document.getElementById("list");
		if (document == null) {
			return new ArrayList<>();
		}
		try {
			return listEle.getElementsByAttributeValue("target", "_blank").eachText();
		}catch (Exception e){
			return new ArrayList<>();
		}
	}
	/**
	 * 是否为IP
	 * @param url 地址
	 * @return
	 */
	public static boolean isIPRight(String url) {
		Pattern ipPattern = compile(KConstants.PATTERN_IP);
		Matcher matcher = ipPattern.matcher(url);
		if (matcher.find()) {
			return true;
		}
		return false;
	}

	/**
	 * 根据Ip 获取域名
	 * @return
	 */
	public static List<String> IPConvertDomain(String ip){
		long startTime = System.currentTimeMillis();
		List<String> domains = HttpUtil.domainSpiderOfAizan(ip);
		if(domains == null || domains.size() == 0) {
			domains = HttpUtil.domainSpiderOfIp138(ip);
		}
		domains.forEach(System.out::println);
		long endTime = System.currentTimeMillis();
		log.info("完成爬虫总耗时："+(endTime - startTime) / 1000+"s.");
		return domains;
	}


	/*public static void main(String[] args) {
		String aaa = "http://www.baidu.co:8087/";
		System.out.println(aaa.startsWith("https"));
		Pattern p = Pattern.compile("(?<=http://|\\.)[^.]*?\\.("+ KConstants.ALLDOMAIN +")",Pattern.CASE_INSENSITIVE);
		Matcher matcher = p.matcher("https://www.baidu.co:8087/");
		matcher.find();
		System.out.println(matcher.group());


	*//*	System.out.println(HttpUtil.isIPRight("192.168.0.120"));
		List<String> urlDomain = HttpUtil.getUrlDomain("https://www.baidu.co");
		System.out.println("__________________");
		urlDomain.forEach(System.out::println);
		System.out.println("__________________");
		long startTime = System.currentTimeMillis();
		List<String> domains = HttpUtil.domainSpiderOfAizan("21.99.97.80");
		if(domains == null || domains.size() == 0) {
			domains = HttpUtil.domainSpiderOfAizan("21.97.80");
		}
		System.out.println("__________________");
		domains.forEach(System.out::println);
		System.out.println("__________________");
		long endTime = System.currentTimeMillis();

		log.info("完成爬虫任务总耗时：s"+(endTime - startTime) / 1000);*//*

	}*/
}
