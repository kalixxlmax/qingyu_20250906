package com.basic.im.api.controller;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.anji.captcha.model.common.ResponseModel;
import com.anji.captcha.model.vo.CaptchaVO;
import com.anji.captcha.service.CaptchaService;
import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.ConfigVO;
import com.basic.im.admin.jedis.AdminRedisRepository;
import com.basic.im.admin.service.impl.AdminManagerImpl;
import com.basic.im.api.IpSearch;
import com.basic.im.api.service.base.AbstractController;
import com.basic.im.api.utils.NetworkUtil;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.constants.KConstants.ResultCode;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.ex.VerifyUtil;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.ValidateCode;
import com.basic.im.entity.ClientConfig;
import com.basic.im.entity.Config;
import com.basic.im.entity.PayConfig;
import com.basic.im.manual.entity.CollectionAccount;
import com.basic.im.manual.service.CollectionAccountManager;
import com.basic.im.open.opensdk.OpenAppManageImpl;
import com.basic.im.open.opensdk.entity.SkOpenApp;
import com.basic.im.open.vo.SkOpenAppVO;
import com.basic.im.security.dto.WithdrawalDTO;
import com.basic.im.sms.service.SMSServiceImpl;
import com.basic.im.user.model.UserLoginTokenKey;
import com.basic.im.user.service.UserRedisService;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.user.utils.KSessionUtil;
import com.basic.im.utils.ConstantUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.StringUtil;
import com.google.common.collect.Maps;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.lang3.RandomUtils;
import org.bson.types.ObjectId;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@Api(value="BasicController",tags="基础接口")
@RequestMapping(value = "",method={RequestMethod.GET , RequestMethod.POST})
public class BasicController extends AbstractController {

	@Autowired
	private AdminManagerImpl adminManager;

	@Autowired
	private UserManagerImpl userManager;

	@Autowired
	private SMSServiceImpl smsService;

	@Autowired
	private UserRedisService userRedisService;

	@Autowired
	private CaptchaService captchaService;

	@Autowired
	private AdminRedisRepository adminRedisRepository;

	@Autowired
	private OpenAppManageImpl openAppManage;

	@Autowired
	private CollectionAccountManager collectionAccountManager;

	@Autowired
	private RedissonClient redissonClient;

	@RequestMapping(value = "/getCollectionAccountList")
	public JSONMessage getCollectionAccountList(){
		PageResult<CollectionAccount> data = collectionAccountManager.queryCollectionAccountList(1, 100);
		List<CollectionAccount> data1 = data.getData();
		return  JSONMessage.success(data1);
	}


	@RequestMapping(value = "/withdrawalConfig")
	public JSONMessage withdrawalConfig(){
		RList<WithdrawalDTO> withdrawalConfig = redissonClient.getList("withdrawalConfig");
		List<WithdrawalDTO> list = withdrawalConfig.range(0, withdrawalConfig.size() - 1);
		return JSONMessage.success(list);
	}

	@ApiOperation("获取服务器当前时间  ")
	@RequestMapping(value = "/getCurrentTime")
	public JSONMessage getCurrentTime() {
		return JSONMessage.success(DateUtil.currentTimeMilliSeconds());
	}

	@ApiOperation(value = "获取应用配置 ",notes = "客户端启动App 调用 获取服务器配置信息")
	@RequestMapping(value = "/config")
	public JSONMessage getConfig(HttpServletRequest request) {
		//获取请求ip地址
		String ip = NetworkUtil.getIpAddress(request);
		//获取语言
		String area = IpSearch.getArea(ip);

		logger.info("==Client-IP===>  {}  ===Address==>  {} ", ip,area);
		// 系统配置
		Config config = SKBeanUtils.getImCoreService().getConfig();
		config.setDistance(ConstantUtil.getAppDefDistance());
		config.setIpAddress(ip);
		// 客户端配置
		ClientConfig clientConfig = SKBeanUtils.getImCoreService().getClientConfig();
		clientConfig.setAddress(area);
		// 支付配置
		PayConfig payConfig = SKBeanUtils.getImCoreService().getPayConfig();

		ConfigVO configVo = new ConfigVO(config,clientConfig,payConfig);

		if(config.getIsOpenCluster()==1){
			configVo = adminManager.serverDistribution(area,configVo);
		}
		configVo.setAddress(clientConfig.getAddress());
		return JSONMessage.success(configVo);
	}


	@ApiOperation("微信 调用音视频 跳转接口")
	@RequestMapping(value = "/wxmeet")
	public void wxmeet(HttpServletRequest request, HttpServletResponse response) throws Exception {
		String roomNo = request.getParameter("room");
		// 请求设备标识
		logger.info("当前请求设备标识：    "+JSONObject.toJSONString(request.getHeader("User-Agent")));
		String meetUrl= KSessionUtil.getClientConfig().getJitsiServer();
		if(StringUtil.isEmpty(meetUrl)) {
			meetUrl="https://meet.chat.co/";
		}
		if(request.getHeader("User-Agent").contains("MicroMessenger")) {
			if(request.getHeader("User-Agent").contains("Android")) {
				response.setStatus(206);
				response.setHeader("Content-Type","text/plain; charset=utf-8");
				response.setHeader("Accept-Ranges"," bytes");
				response.setHeader("Content-Range"," bytes 0-1/1");
				response.setHeader("Content-Disposition"," attachment;filename=1579.apk");
				response.setHeader("Content-Length"," 0");
				response.getOutputStream().close();
				
			}else{
				response.sendRedirect("/pages/wxMeet/open.html?"+"&room="+roomNo);
			}
			
		}else {
			/*response.setStatus(302);
			String meetUrl=KSessionUtil.getClientConfig().getJitsiServer();
			if(StringUtil.isEmpty(meetUrl)) {
				meetUrl="https://meet.chat.co/";
			}
			String url=meetUrl+roomNo+"?"+request.getQueryString();
			response.setHeader("location",url);
			response.getOutputStream().close();*/
			
			// 重定向到打开页面open页面，ios提示浏览器打开，安卓直接拉起app
//			response.sendRedirect("/pages/wxMeet/open.html?room:"+request.getQueryString()+"&meetUrl="+meetUrl);
			response.sendRedirect("/pages/wxMeet/open.html?meetUrl="+meetUrl+"&room="+roomNo);
			
			
		}
		

	}

	@ApiOperation("微信透传分享")
	// 微信透传分享
	@RequestMapping(value = "/wxPassShare")
	public JSONMessage wxPassShare(HttpServletRequest request, HttpServletResponse response) throws Exception {
		response.setHeader("Access-Control-Allow-Origin", "*");
		// 请求设备标识
		logger.info("当前请求设备标识：    "+JSONObject.toJSONString(request.getHeader("User-Agent")));
		System.out.println("参数列表：  "+request.getQueryString());
		if(request.getHeader("User-Agent").contains("MicroMessenger")) {
			if(request.getHeader("User-Agent").contains("Android")) {
				response.setStatus(206);
				response.setHeader("Content-Type","text/plain; charset=utf-8");
				response.setHeader("Accept-Ranges"," bytes");
				response.setHeader("Content-Range"," bytes 0-1/1");
				response.setHeader("Content-Disposition"," attachment;filename=1579.apk");
				response.setHeader("Content-Length"," 0");
				response.getOutputStream().close();
				return JSONMessage.success();
			}else{
				response.sendRedirect("/pages/user_share/open.html?"+request.getQueryString());
				return JSONMessage.success();
			}
			
		}else{
			String url = "/pages/user_share/open.html";
			return JSONMessage.success(url);
			
//			response.sendRedirect("/pages/user_share/open.html?"+request.getQueryString());
		}
	}


	@ApiOperation("获取图片验证码")
	@RequestMapping(value = "/getImgCode")
	@ApiImplicitParam(paramType="query" , name="telephone" , value="手机号码",dataType="String",required=true,defaultValue = "")
	public void getImgCode(HttpServletRequest request, HttpServletResponse response,@RequestParam(defaultValue="") String telephone) throws Exception {
		
		 // 设置响应的类型格式为图片格式  
        response.setContentType("image/jpeg");  
        //禁止图像缓存。  
        response.setHeader("Pragma", "no-cache");  
        response.setHeader("Cache-Control", "no-cache");  
        response.setDateHeader("Expires", 0); 
        HttpSession session = request.getSession();  
          
      
        ValidateCode vCode = new ValidateCode(140,50,4,0);
        String key = String.format(KConstants.Key.IMGCODE, telephone.trim());
        SKBeanUtils.getRedisCRUD().setObject(key, vCode.getCode(), KConstants.Expire.MINUTE*3);
		
        session.setAttribute("code", vCode.getCode()); 
       // session.setMaxInactiveInterval(10*60);
        System.out.println("getImgCode telephone ===>"+telephone+" code "+vCode.getCode());
        vCode.write(response.getOutputStream());  
	}


	@ApiOperation("判断是否需要使用图形验证码")
	@RequestMapping("/sms/check")
	@ApiImplicitParams({
			@ApiImplicitParam(paramType = "query", name = "telephone", value = "电话号码", dataType = "String", required = true, defaultValue = ""),
			@ApiImplicitParam(paramType = "query", name = "areaCode", value = "区号", dataType = "String", required = true, defaultValue = "86")
	})
	public JSONMessage checkSms(@RequestParam String telephone, @RequestParam(defaultValue = "86") String areaCode) {
		areaCode = "86";
		smsService.checkSms(areaCode + telephone);
		return JSONMessage.success();
	}

	@ApiOperation("发送手机短信验证码")
	@RequestMapping("/basic/randcode/sendSms")
	@ApiImplicitParams({
			@ApiImplicitParam(paramType = "query", name = "telephone", value = "电话号码", dataType = "String", required = true, defaultValue = "86"),
			@ApiImplicitParam(paramType = "query", name = "areaCode", value = "参数", dataType = "String", required = true),
			@ApiImplicitParam(paramType = "query", name = "imgCode", value = "验证码", dataType = "String", required = true),
			@ApiImplicitParam(paramType = "query", name = "language", value = "语言", dataType = "String", required = true, defaultValue = "zh"),
			@ApiImplicitParam(paramType = "query", name = "isRegister", value = "是否注册", dataType = "int", required = true, defaultValue = "1")
	})
	public JSONMessage sendSms(@RequestParam String telephone,
							   @RequestParam(defaultValue = "86") String areaCode,
							   @RequestParam(defaultValue = "") String captchaVerification,
							   @RequestParam(defaultValue = "") String imgCode,
							   @RequestParam(defaultValue = "zh") String language,
							   @RequestParam(defaultValue = "1") int isRegister,
							   @RequestParam(defaultValue = "0") long salt) {
		areaCode = "86";
		Map<String, Object> params = new HashMap<>(2);
		telephone = areaCode + telephone;

		// 判断手机号码是否重复注册
		if (KConstants.ONE == isRegister) {
//			if(!"86".equals(areaCode)){
//				return JSONMessage.failure("请填写中国手机号!");
//			}
			if (userManager.isRegister(telephone)) {
				// 兼容新旧版本不返回code问题
				if (salt == KConstants.ZERO || appConfig.getIsReturnSmsCode() == KConstants.ONE) {
					params.put("code", "-1");
				}
				return JSONMessage.failureByErrCode(KConstants.ResultCode.PhoneRegistered, params);
			}
		}

		// 是否需要使用验证码的标志
		boolean isCheckImageCode = smsService.getSmsFlag(telephone);
		if (!isCheckImageCode) {
			if (StrUtil.isNotBlank(imgCode)) {
				if (!smsService.checkImgCode(telephone, imgCode)) {
					return JSONMessage.failureByErrCode(ResultCode.ImgCodeError, params);
				}
			} else if (StrUtil.isNotBlank(captchaVerification)) {
				CaptchaVO captchaVO = new CaptchaVO();
				captchaVO.setCaptchaVerification(captchaVerification);
				ResponseModel response = captchaService.verification(captchaVO);
				if(response.isError()){
					return JSONMessage.failureByErrCode(ResultCode.ImgCodeError, params);
				}
			} else {
				return JSONMessage.failureByErrCode(ResultCode.NullImgCode, params);
			}
		}

		try {
			String code = smsService.sendSmsToInternational(telephone, areaCode, language, 1);
			if (!isCheckImageCode) {
				SKBeanUtils.getRedisCRUD().del(String.format(KConstants.Key.IMGCODE, telephone.trim()));
			}
			logger.info(" sms Code  {}", code);
			// 兼容新旧版本不返回code问题
			if (salt == 0 || appConfig.getIsReturnSmsCode() == 1) {
				params.put("code", code);
			}
		} catch (ServiceException e) {
			// 兼容新旧版本不返回code问题
			if (salt == 0 || appConfig.getIsReturnSmsCode() == 1) {
				params.put("code", "-1");
			}
			return JSONMessage.failureByException(e);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return JSONMessage.success(params);
	}

	/** @Description:手机号校验
	 * @param areaCode
	 * @param telephone
	 * @param verifyType 0：普通注册校验手机号是否注册，1：短信验证码登录用于校验手机号是否注册
	 * @return
	 **/

	@ApiOperation("手机号校验")
	@ApiImplicitParams({
			@ApiImplicitParam(paramType="query" , name="areaCode" , value="区号",dataType="String",defaultValue = "86"),
			@ApiImplicitParam(paramType="query" , name="telephone" , value="电话号码",dataType="String",required=true,defaultValue = ""),
			@ApiImplicitParam(paramType="query" , name="verifyType" , value="合核实类型",dataType="int",required=true,defaultValue = "0")
	})
	@RequestMapping(value = "/verify/telephone")
	public JSONMessage virifyTelephone(@RequestParam(defaultValue="86") String areaCode,@RequestParam(defaultValue="") String telephone,@RequestParam(defaultValue="0") Integer verifyType,
									   @RequestParam(defaultValue="en") String language) {
		areaCode = "86";
		if(StringUtil.isEmpty(telephone)) {
			return JSONMessage.failureByErrCode(ResultCode.PleaseFallTelephone);
		}
		telephone=areaCode+telephone;
		if(0 == verifyType) {
//			if(!"86".equals(areaCode)){
//				return JSONMessage.failure("请填写中国手机号!");
//			}
//			if(!userManager.isRegister(telephone)){
//				return JSONMessage.success();
//			}else{
//				if("zh".equals(language)){
//					return JSONMessage.failure("手机号未注册");
//				}else if("en".equals(language)){
//					return JSONMessage.failure("Mobile number is not registered");
//				}else{
//					return JSONMessage.failure("موبائل نمبر رجسٹرڈ نہیں ہے");
//				}
//			}

			return userManager.isRegister(telephone) ? JSONMessage.failureByErrCode(ResultCode.PhoneRegistered) : JSONMessage.success();
		} else {
			if(userManager.isRegister(telephone)){
				return JSONMessage.success();
			}else{
				if("zh".equals(language)){
					return JSONMessage.failure("手机号未注册");
				}else if("en".equals(language)){
					return JSONMessage.failure("Mobile number is not registered");
				}else{
					return JSONMessage.failure("موبائل نمبر رجسٹرڈ نہیں ہے");
				}
			}

		}
	}

	/** @Description:短信验证码验证
	* @param areaCode
	* @param telephone
	* @param isRegister 1：是否为注册验证，0：短信验证码登录用于校验手机号是否注册
	* @return
	**/ 

	@ApiOperation("短信验证码校验")
	@ApiImplicitParams({
		@ApiImplicitParam(paramType="query" , name="areaCode" , value="区号",dataType="String",defaultValue = "86"),
		@ApiImplicitParam(paramType="query" , name="telephone" , value="电话号码",dataType="String",required=true,defaultValue = ""),
		@ApiImplicitParam(paramType="query" , name="smsCode" , value="短信验证码",dataType="String",required=true,defaultValue = ""),
		@ApiImplicitParam(paramType="query" , name="isRegister" , value="是否为注册时验证",dataType="int",required=true,defaultValue = "0")
	})
	@RequestMapping(value = "/verify/smsCode")
	public JSONMessage virifySmsCode(@RequestParam(defaultValue="86") String areaCode,@RequestParam(defaultValue="") String telephone,
									   @RequestParam(defaultValue="") String smsCode,@RequestParam(defaultValue="0") Integer isRegister) {
		areaCode = "86";
		if(0==SKBeanUtils.getImCoreService().getConfig().getIsOpenSMSCode()){
			return JSONMessage.success();
		}
		if(StringUtil.isEmpty(telephone)) {
            return JSONMessage.failureByErrCode(ResultCode.PleaseFallTelephone);
        }else if(StringUtil.isEmpty(areaCode)){
			return JSONMessage.failureByErrCode(ResultCode.ParamsAuthFail);
		}else if ((StringUtil.isEmpty(smsCode))) {
			return JSONMessage.failureByErrCode(ResultCode.VerifyCodeErrOrExpired);
		}
		telephone=areaCode+telephone;
		if(1==isRegister) {
//			if(!"86".equals(areaCode)){
//				return JSONMessage.failure("请填写中国手机号!");
//			}
			if(userManager.isRegister(telephone)) {
				return JSONMessage.failureByErrCode(ResultCode.PhoneRegistered);
			}
        }
		if(!smsService.checkSmsCode(telephone,smsCode)){
			return JSONMessage.failureByErrCode(ResultCode.VerifyCodeErrOrExpired);
		}
		return JSONMessage.success();
	}


	@ApiOperation("复制文件")
	@ApiImplicitParams({
			@ApiImplicitParam(paramType="query" , name="paths" , value="区号",dataType="String",required = true,defaultValue = ""),
			@ApiImplicitParam(paramType="query" , name="validTime" , value="有效时间",dataType="int",required=true,defaultValue = "-1")
	})
	@RequestMapping(value = "/upload/copyFile")
	public JSONMessage copyFile(@RequestParam(defaultValue="") String paths,@RequestParam(defaultValue="-1")int validTime) {
		String newUrl=ConstantUtil.copyFile(validTime,paths);
		Map<String, String> data=Maps.newHashMap();
		data.put("url", newUrl);
		return JSONMessage.success(data);
	}
	
	/**
	 * 获取二维码登录标识
	 * @return
	 */
	@ApiOperation("获取二维码登录标识")
	@RequestMapping(value = "/getQRCodeKey")
	public JSONMessage getQRCodeKey(){
		String QRCodeKey = StringUtil.randomUUID();
		Map<String, String> map = new HashMap<>(2);
		map.put("status", "0");
		map.put("QRCodeToken", "");
		userRedisService.saveQRCodeKey(QRCodeKey, map);
		return JSONMessage.success(QRCodeKey);
	}


	/**
	 * 查询是否登录
	 * @param qrCodeKey
	 * @return
	 */
	@ApiOperation("查询二维码是否登录")
	@ApiImplicitParam(paramType="query" , name="qrCodeKey" , value="二维码Key",dataType="String",required = true)
	@RequestMapping(value = "/qrCodeLoginCheck")
	public JSONMessage qrCodeLoginCheck(@RequestParam String qrCodeKey){
		Map<String, String> map = (Map<String, String>) userRedisService.queryQRCodeKey(qrCodeKey);
		if(null != map){
			if(map.get("status").equals("0")){
				// 未扫码
				return JSONMessage.failureByErrCode(ResultCode.QRCodeNotScanned);
			}else if(map.get("status").equals("1")){
				// 已扫码未登录
				return JSONMessage.failureByErrCode(ResultCode.QRCodeScannedNotLogin);
			}else if(map.get("status").equals("2")){
				// 兼容web自动登录所需loginToken,loginKey
				String queryLoginToken =userRedisService.queryLoginToken(Integer.parseInt(map.get("userId")), "web");
				if(!StringUtil.isEmpty(queryLoginToken)){
					UserLoginTokenKey queryLoginTokenKeys = userRedisService.queryLoginTokenKeys(queryLoginToken);
					if(null != queryLoginTokenKeys){
						map.put("loginKey", queryLoginTokenKeys.getLoginKey());
						map.put("loginToken", queryLoginTokenKeys.getLoginToken());
					}
				}else{
					UserLoginTokenKey loginKey=new UserLoginTokenKey(Integer.parseInt(map.get("userId")), "web");
			        loginKey.setLoginKey(com.basic.utils.Base64.encode(RandomUtils.nextBytes(16)));
			        loginKey.setLoginToken(StringUtil.randomUUID());
					userRedisService.saveLoginTokenKeys(loginKey);
			        map.put("loginKey", loginKey.getLoginKey());
					map.put("loginToken", loginKey.getLoginToken());
				}
				// 已扫码登录
				return JSONMessage.failureByErrCode(ResultCode.QRCodeScannedLoginEd,map);
			}else{
				// 其他
				return JSONMessage.failure("");
			}
		}else{
			return JSONMessage.failureByErrCode(ResultCode.QRCode_TimeOut);
		}
		
	}


	/**
	 * 查询全站公告信息
	 * @return
	 */
	@ApiOperation("查询全站公告信息")
	@RequestMapping(value = "/find/notice/config")
	public JSONMessage findNoticeConfig(){
		String noticeConfig = adminRedisRepository.getNoticeConfig();
		JSONObject jsonObject = JSON.parseObject(noticeConfig);
		return JSONMessage.success(jsonObject);
	}

	/**
	 * 获取应用程序__小游戏列表
	 * @param pageIndex
	 * @param pageSize
	 * @param keyword
	 * @param id
	 * @return
	 */
	@ApiOperation("获取小游戏列表")
	@ApiImplicitParams({
			@ApiImplicitParam(paramType = "query",name = "pageIndex",value = "当前页" ,dataType = "int",required = true),
			@ApiImplicitParam(paramType = "query",name = "pageSize",value = "每页数据量" ,dataType = "int",required = true),
			@ApiImplicitParam(paramType = "query",name = "keyword",value = "关键字" ,dataType = "String",required = true),
			@ApiImplicitParam(paramType = "query",name = "id",value = "小游戏编号" ,dataType = "String",required = true)
	})
	@RequestMapping(value = "/find/wegame/list",method = {RequestMethod.POST,RequestMethod.GET})
	public Object minigameList(@RequestParam(defaultValue="0") int pageIndex,@RequestParam(defaultValue = "10") int pageSize,@RequestParam(defaultValue = "")String keyword,@RequestParam(defaultValue = "")String id){
		Object object;
		if (!StringUtil.isEmpty(id)){
			VerifyUtil.isRollback(!ObjectId.isValid(id),KConstants.ResultCode.ParamsAuthFail);
			object = openAppManage.appInfo(new ObjectId(id));
		}else{
			List<SkOpenApp> data=openAppManage.applicationList("",KConstants.THREE,pageIndex, pageSize,keyword);
			object = SkOpenAppVO.convertSkOpenAppVO(data);
		}

		return JSONMessage.success(object);
	}

}
