package com.basic.im.mpserver.controller;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.mongodb.DBObject;
import com.basic.common.model.PageResult;
import com.basic.commons.thread.ThreadUtils;
import com.basic.im.admin.dao.UploadItemeDao;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.constants.MsgType;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.ReqUtil;
import com.basic.im.entity.ClientConfig;
import com.basic.im.friends.entity.Friends;
import com.basic.im.friends.service.impl.FriendsManagerImpl;
import com.basic.im.message.IMessageRepository;
import com.basic.im.message.MessageService;
import com.basic.im.mpserver.MpConfig;
import com.basic.im.mpserver.dao.MassCommentDao;
import com.basic.im.mpserver.dao.MassCommentPraiseDao;
import com.basic.im.mpserver.dao.MassContentDao;
import com.basic.im.mpserver.model.MenuVO;
import com.basic.im.mpserver.redis.MpRedisRepository;
import com.basic.im.mpserver.service.impl.MPServiceImpl;
import com.basic.im.mpserver.service.impl.MenuManagerImpl;
import com.basic.im.mpserver.utils.MobileValidateUtils;
import com.basic.im.mpserver.utils.ValidateIDCardUtils;
import com.basic.im.mpserver.vo.MassComment;
import com.basic.im.mpserver.vo.MassCommentPraise;
import com.basic.im.mpserver.vo.MassContent;
import com.basic.im.mpserver.vo.Menu;
import com.basic.im.open.entity.OfficialInfo;
import com.basic.im.open.opensdk.OfficialInfoCheckImpl;
import com.basic.im.security.entity.SecurityRole;
import com.basic.im.security.service.SecurityRoleManager;
import com.basic.im.sms.service.SMSServiceImpl;
import com.basic.im.support.Callback;
import com.basic.im.user.dao.RoleDao;
import com.basic.im.user.dao.UserDao;
import com.basic.im.user.entity.Role;
import com.basic.im.user.entity.User;
import com.basic.im.user.model.KSession;
import com.basic.im.user.service.UserRedisService;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.user.utils.KSessionUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import com.basic.utils.DateUtil;
import com.basic.utils.Md5Util;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.IOException;
import java.util.*;

/**
 * 即时通讯公众号功能
 * @author hsg
 *
 */
@RestController
@Slf4j
@RequestMapping("/mp")
public class AdminMpController extends AbstractController {

	@Autowired
	private FriendsManagerImpl friendsManager;

	@Autowired
	private MPServiceImpl mpManager;

	@Autowired
	private MenuManagerImpl menuManager;

	@Autowired
	private UserManagerImpl userManager;

	@Autowired
	private UserDao userDao;

	@Autowired
	private IMessageRepository messageRepository;

	@Autowired
	private MessageService messageService;

	@Autowired
	private SMSServiceImpl smsService;

	@Autowired
	private OfficialInfoCheckImpl officialInfoCheck;

	@Autowired
	private UserRedisService userRedisService;
	
	@Resource(name = "mpConfig")
	protected MpConfig mpConfig;

	@Autowired
	private RoleDao roleDao;

	@Autowired
	private SecurityRoleManager securityRoleManager;

	@Autowired
	private MpRedisRepository mpRedisRepository;

	@Autowired
	private UploadItemeDao uploadItemeDao;

	@Autowired
	private MassContentDao massContentDao;

	@Autowired
	private MassCommentDao massCommentDao;

	@Autowired
	private MassCommentPraiseDao massCommentPraiseDao;

	@RequestMapping("/fans/delete")
	public JSONMessage deleteFans(HttpServletResponse response, @RequestParam int toUserId){
		Integer userId = ReqUtil.getUserId();
		friendsManager.consoleDeleteFriends(userId,userId, toUserId+"");
		return  JSONMessage.success();
	}



	@RequestMapping(value = "/login", method = { RequestMethod.GET })
	public void openLogin(HttpServletRequest request, HttpServletResponse response) {
		try {
			String path = request.getContextPath() + "/mp/login.html";
			response.sendRedirect(path);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@RequestMapping(value = "/config", method = { RequestMethod.GET })
	public JSONMessage getConofig() {
		HashMap<String, Object> map = new HashMap<>();
		try {
			
			ClientConfig clientConfig = SKBeanUtils.getImCoreService().getClientConfig();
			
			map.put("imServerAddr", clientConfig.getXMPPHost());
			map.put("imDomian",clientConfig.getXMPPDomain());
			map.put("apiAddr", clientConfig.getApiUrl());
			map.put("fileAddr", clientConfig.getDownloadAvatarUrl());
			map.put("uploadAddr", clientConfig.getUploadUrl());
			map.put("isOpenWss", mpConfig.getIsOpenWss());
			return JSONMessage.success(map);
			
		} catch (Exception e) {
			e.printStackTrace();
			return JSONMessage.failure(e.getMessage());
		}
	}


	/**
	 * @Description: 2019-01-17 17:41 公众号平台只能公众号身份的人登陆
	* @param request
	* @return
	**/
	@RequestMapping(value = "/login", method = RequestMethod.POST)
	public JSONMessage login(HttpServletRequest request,String areaCode) {
		String account = request.getParameter("account");
		if(!StringUtil.isEmpty(areaCode)) {
			account = areaCode + account;
		}
		String password = request.getParameter("password");
		HashMap<String, Object> map = new HashMap<>();
		try {
			User user = userManager.mpLogin(account, password);
			//公众号登入判断
			out:if (user.getUserType() == 2 || user.getUserType() == 4 || user.getUserId() == 10000){
				if(user.getStatus() == -1){
					return JSONMessage.failure("账号已被锁！");
				}
				if (user.getUserId() == KConstants.systemAccount.CUSTOMER_ACCOUNT){
					break out;
				}
				OfficialInfo officialInfo = officialInfoCheck.getOfficialInfo(account);
				if (null == officialInfo){
					return JSONMessage.failure("公众号不存在！");
				}
				//审核 0--未审核  1--审核通过  2--审核不通过
				 if (0 == officialInfo.getVerify()){
					return JSONMessage.failure("公众号还未审核，请耐心等待！");
				}else if(2 == officialInfo.getVerify()){
					return JSONMessage.failureByErrCodeAndData(KConstants.ResultCode.NO_PASS,"公众号审核未通过！");
				}
			}else{
				throw new ServiceException(KConstants.ResultCode.NO_PERMISSION);
			}

			//检查该用户是否注册到 Tigase
			userManager.examineTigaseUser(user.getUserId(), user.getPassword());

			Map<String, Object> tokenMap = KSessionUtil.adminLoginSaveToken(user.getUserId(), null);

			map.put("access_Token", tokenMap.get("access_Token"));
			map.put("userId", user.getUserId());
			map.put("nickname", user.getNickname());
			map.put("apiKey", mpConfig.getApiKey());
			map.put("userType",user.getUserType());
			OfficialInfo officialInfo = officialInfoCheck.getOfficialInfo(account);
			if (officialInfo != null){
				map.put("officialHeadImg",officialInfo.getOfficialHeadImg());
			}
			KSession session = new KSession(user.getUserId(),"zh","");
			session.setAccessToken(tokenMap.get("access_Token").toString());
			session.setHttpKey(com.basic.utils.Base64.encode(RandomUtils.nextBytes(16)));
			//缓存seession
			userRedisService.saveUserSession(session);
			//缓存token
			KSessionUtil.adminLoginSaveToken(user.getUserId(),tokenMap.get("access_Token").toString());
			map.put("httpKey",session.getHttpKey());

			return JSONMessage.success(map);

		} catch (ServiceException e) {
			return JSONMessage.failureByException(e);
		}

	}


	/**
	 * 退出登录，清除缓存
	 * @throws ServletException
	 * @throws IOException
	 */
	@RequestMapping(value="logout")
	public JSONMessage logout() {
		KSessionUtil.removeAdminToken(ReqUtil.getUserId());
		return JSONMessage.success();
	}

	@RequestMapping("/menu/add")
	public JSONMessage addMenu(@Valid MenuVO menuVO ){
		menuVO.setUserId(ReqUtil.getUserId());
		menuManager.addMenu(menuVO);
		return JSONMessage.success();
	}

	@RequestMapping("/menu/delete")
	public JSONMessage deleteMenu(@RequestParam String menuId){

		try {
			menuManager.delMenu(menuId);
			return JSONMessage.success();
		} catch (Exception e) {
			return JSONMessage.failure(e.getMessage());
		}
	}


	/**
	 * 修改菜单
	 * @param entity
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value="/menu/saveupdate",method=RequestMethod.POST)
	@ResponseBody
	public JSONMessage saveupdate(@ModelAttribute Menu entity) throws IOException{
		menuManager.saveupdate(entity);
		return JSONMessage.success();
	}

	@RequestMapping("/fans")
	@ResponseBody
	public JSONMessage navFans(@RequestParam(defaultValue = "1") int page,
							   @RequestParam(defaultValue = "15") int limit,@RequestParam(defaultValue = "") String keyWord) {
		User user = userManager.getUserDao().get(ReqUtil.getUserId());
		PageResult data = null;
		if (null != user) {
			data = friendsManager.queryFriends(user.getUserId(),0,keyWord, (page-1), limit);
		}
		return JSONMessage.success(data);
	}



	@ResponseBody
	@RequestMapping("/getHomeCount")
	public JSONMessage getHomeCount() {
		User user = userManager.getUserDao().get(ReqUtil.getUserId());
		if(!ObjectUtil.isEmpty(user)) {
			JSONObject homeCount = menuManager.getHomeCount(user.getUserId());
			return JSONMessage.success(homeCount);
		}
		return JSONMessage.failure("Session ===> user is null");
	}


	@RequestMapping("/menuList")
	@ResponseBody
	@JsonSerialize(using = ToStringSerializer.class)
	public JSONMessage getMenuList(@RequestParam(defaultValue = "0") String parentId) {
		User user = userManager.getUserDao().get(ReqUtil.getUserId());
		if(!ObjectUtil.isEmpty(user)) {
			List<Menu> menus = menuManager.getMenuListByParentId(user.getUserId(),parentId);
			return JSONMessage.success(null, menus);
		}
		return JSONMessage.failure("Session ===> user is null");
	}

	/**
	 *  一段时间内最新的聊天历史记录
	 * @param startTime
	 * @param pageSize
	 * @return
	 */
	@RequestMapping("/getLastChatList")
	@ResponseBody
	public JSONMessage msg(@RequestParam(defaultValue="0")long startTime, @RequestParam(defaultValue = "20") int pageSize) {
		try {
			Object result = mpManager.queryLastChatList( ReqUtil.getUserId(),startTime, pageSize);
			return JSONMessage.success(result);
		} catch (Exception e) {
			return JSONMessage.failure(e.getMessage());
		}
	}



	@SuppressWarnings("unchecked")
	@RequestMapping("/msg/list")
	@ResponseBody
	public JSONMessage msgList(@RequestParam int toUserId, @RequestParam(defaultValue = "0") int pageIndex,
							   @RequestParam(defaultValue = "10") int pageSize) {
		User user = userManager.getUserDao().get(ReqUtil.getUserId());
		List<DBObject> msgList = (List<DBObject>) mpManager.getMsgList(toUserId, user.getUserId(), pageIndex, pageSize);
		ThreadUtils.executeInThread(new Callback() {

			@Override
			public void execute(Object obj) {
				for (DBObject dbObject : msgList) {
					messageRepository.updateMsgIsReadStatus(user.getUserId(),dbObject.get("messageId").toString());
				}
			}
		});
		return JSONMessage.success(null, msgList);
	}






	@RequestMapping(value = "/msg/send")
	@ResponseBody
	public JSONMessage msgSend(@RequestParam Integer toUserId, @RequestParam String body, @RequestParam(defaultValue="1") int type ) throws Exception {
		User user = userManager.getUserDao().get(ReqUtil.getUserId());

		MessageBean mb = new MessageBean();
		// = new String(body.getBytes("ISO-8859-1"), "utf-8")
		mb.setContent(body);
		// mb.setFileName(fileName);
		mb.setFromUserId(user.getUserId() + "");
		mb.setFromUserName(user.getNickname());
		// mb.setObjectId(objectId);
		mb.setTimeSend(DateUtil.currentTimeSeconds());
		mb.setToUserId(toUserId + "");
		mb.setMessageId(UUID.randomUUID().toString());
		// mb.setToUserName(toUserName);
		mb.setMsgType(0);// 单聊消息
		mb.setType(type);
		mb.setMessageId(StringUtil.randomUUID());
		try {
			messageService.send(mb);
		} catch (Exception e) {
			System.out.println(user.getUserId() + "：推送失败  "+e.getMessage());
			return JSONMessage.failure(e.getMessage());
		}
		return JSONMessage.success();
	}


	@RequestMapping(value="/textToAll")
	@ResponseBody
	public JSONMessage textToAll(@RequestParam String title){
		User user = userManager.getUserDao().get(ReqUtil.getUserId());

		MessageBean mb = new MessageBean();
		mb.setContent(title);
		mb.setFromUserId(user.getUserId() + "");
		mb.setFromUserName(user.getNickname());
		mb.setTimeSend(DateUtil.currentTimeSeconds());
		mb.setMsgType(2);// 广播消息
		mb.setType(1);
		mb.setMessageId(StringUtil.randomUUID());
		try {
			ThreadUtils.executeInThread((Callback) obj -> messageService.send(mb));
		} catch (Exception e) {
			System.out.println(user.getUserId() + "：推送失败");
			return JSONMessage.failure(e.getMessage());
		}
		return JSONMessage.success();
	}

	@RequestMapping(value = "/pushToAll")
	@ResponseBody
	public JSONMessage pushToAll(@RequestParam String title,@RequestParam String sub,@RequestParam String img,@RequestParam String url) throws Exception {
		User user = userManager.getUserDao().get(ReqUtil.getUserId());

		MessageBean mb = new MessageBean();
		JSONObject jsonObj=new JSONObject();
		jsonObj.put("title", title);
		jsonObj.put("sub", sub);
		jsonObj.put("img", img);
		jsonObj.put("url", url);
		mb.setContent(jsonObj.toString());
		// mb.setFileName(fileName);
		mb.setFromUserId(user.getUserId() + "");
		mb.setFromUserName(user.getNickname());
		// mb.setObjectId(objectId);
		mb.setTimeSend(DateUtil.currentTimeSeconds());
		// mb.setToUserId(fans.getToUserId() + "");
		// mb.setToUserName(toUserName);
		mb.setMsgType(2);// 广播消息
		mb.setType(80);
		mb.setMessageId(StringUtil.randomUUID());
		try {
			ThreadUtils.executeInThread(new Callback() {

				@Override
				public void execute(Object obj) {
					messageService.send(mb);

				}
			});

		} catch (Exception e) {
			System.out.println(user.getUserId() + "：推送失败");
			return JSONMessage.failure(e.getMessage());
		}
		return JSONMessage.success();
	}

	@RequestMapping(value="/manyToAll")
	@ResponseBody
	public JSONMessage many(@RequestParam(defaultValue="") String[] title,@RequestParam(defaultValue="") String[] url,@RequestParam(defaultValue="") String[] img) throws ServletException, IOException{
		User user = userManager.getUserDao().get(ReqUtil.getUserId());
		List<Friends> fansList = friendsManager.getFansList(user.getUserId());
		/*List<Integer> toUserIdList = Lists.newArrayList();
		for (Friends fans : fansList) {
			toUserIdList.add(fans.getToUserId());
		}*/
		List<Object> list=new ArrayList<Object>();
		JSONObject jsonObj=null;
		for(int i=0;i<title.length;i++){
			jsonObj=new JSONObject();
			jsonObj.put("title",title[i]);
			jsonObj.put("url", url[i]);
			jsonObj.put("img", img[i]);
			list.add(jsonObj);
		}
		MessageBean messageBean=new MessageBean();
		messageBean.setContent(list.toString());
		messageBean.setFromUserId(user.getUserId() + "");
		messageBean.setFromUserName(user.getNickname());
		messageBean.setTimeSend(DateUtil.currentTimeSeconds());
		messageBean.setType(81);
		messageBean.setMsgType(2);// 广播消息
		messageBean.setMessageId(StringUtil.randomUUID());
		try {
			ThreadUtils.executeInThread((Callback) obj ->
					messageService.send(messageBean));
		} catch (Exception e) {
			System.out.println(user.getUserId() + "：推送失败");
			return JSONMessage.failure(e.getMessage());
		}
		return JSONMessage.success();

	}


	/**
	 * 公众号信息注册
	 * @param info
	 * @param randcode
	 * @return
	 */
	@RequestMapping("/opffcialInfoRegister")
	public JSONMessage registerOfficllnfo(@ModelAttribute OfficialInfo info, @RequestParam(defaultValue = "") String randcode){

		String phone=info.getTelephone();

		if(StringUtil.isEmpty(info.getAreaCode())){
			info.setAreaCode("86");
		}

		info.setTelephone(info.getAreaCode()+info.getTelephone());

		if (isRegister(info.getAreaCode()+info.getTelephone())){
			return JSONMessage.failure("账户已存在！");
		}

		//校验营业执照号
		/*if (info.getCompanyBusinessLicense().length() == 15){
			boolean businessLicense15 = BusinessUtils.isBusinessLicense15(info.getCompanyBusinessLicense());
			if (!businessLicense15){
				return JSONMessage.failure("营业执照号格式错误！");
			}
		}else if(info.getCompanyBusinessLicense().length() == 18){
			boolean businessLicense18 = BusinessUtils.isBusinessLicense18(info.getCompanyBusinessLicense());
			if (!businessLicense18){
				return JSONMessage.failure("统一社会信用代码格式错误！");
			}
		}*/



		//验证验证码
		/*if(!smsService.isAvailable(info.getTelephone(),randcode)) {
			//return JSONMessage.failureByErrCode(KConstants.ResultCode.VerifyCodeErrOrExpired);
			return JSONMessage.failure("验证码错误或以过期！");
		}*/

		//校验身份证
		/*boolean flag = ValidateIDCardUtils.validateIdCard18(info.getAdminID());
		if (!flag){
			return JSONMessage.failure("管理员身份证格号码式错误！");
		}*/


		//设置创建时间
		info.setCreateTime(DateUtil.currentTimeSeconds());
		//设置areaCode
		if (ObjectUtil.isEmpty(info)){
			info.setAreaCode("86");
		}
		/*info.setUserId();*/
		User user=userDao.getUser(info.getTelephone());
		if (ObjectUtil.isEmpty(user)){
			//生成用户
			User newUser = userManager.createUser(info.getTelephone(),info.getPassword());

			if (StringUtil.isEmpty(info.getAreaCode())){
				newUser.setAreaCode("86");
			}else{
				newUser.setAreaCode(info.getAreaCode());
			}

			newUser.setUserType(2);
			newUser.setNickname("客服公众号");
			//newUser.setNickname("Customer service public account");
			newUser.setCreateTime(DateUtil.currentTimeSeconds());
			newUser.setModifyTime(DateUtil.currentTimeSeconds());
			newUser.setCityId(400300);
			newUser.setStatus(1);
			newUser.setLoc(new User.Loc(10.0,10.0));
			newUser.setPhone(phone);
			//默认初始化配置
			newUser.setSettings(new User.UserSettings());
			newUser.setTelephone(info.getTelephone());
			System.out.println(Md5Util.md5Hex("8610000"));
			newUser.setUserKey(Md5Util.md5Hex(info.getAreaCode()+phone));
			//保存到数据库
			User user1 = userManager.getUserDao().addUsers(newUser);

			info.setUserId(newUser.getUserId());
			SKBeanUtils.getDatastore().save(info);

			//创建角色用户
			Role role = new Role();
			role.setUserId(user1.getUserId());
			role.setStatus((byte) 1);
			role.setPhone(info.getAreaCode()+phone);
			role.setRole((byte)2);
			role.setCreateTime(DateUtil.currentTimeSeconds());
			SecurityRole securityRole = securityRoleManager.querySecurityRoleByRoleName("公众号");
			role.setRoleId(String.valueOf(securityRole.getRoleId()));
			roleDao.save(role);
		}else{
			return JSONMessage.failure("账户已存在！");
		}
		return  JSONMessage.success();
	}


	/**
	 * 发送验证码
	 * @param telephone
	 * @param areaCode
	 * @param language
	 * @return
	 */
	@RequestMapping(value = "/sendCode")
	public JSONMessage sendCode(@RequestParam String telephone,@RequestParam(defaultValue="86") String areaCode,@RequestParam(defaultValue="zh") String language){
		areaCode = "86";
		if (!MobileValidateUtils.checkPhoneNumber(telephone, Integer.valueOf(areaCode))){
			return JSONMessage.failure("请输入正确的手机号！");
		}
		if (isRegister(areaCode+telephone)){
			return JSONMessage.failure("手机号已注册！");
		}
		try {
			telephone=areaCode+telephone;
			String code =smsService.sendSmsToInternational(telephone,areaCode,language,1);
			logger.info("code："  + code);
		}catch (ServiceException e){
			e.printStackTrace();
			return JSONMessage.failure(e.getMessage());
		}
		return JSONMessage.success();
	}

	/**
	 * 判断是否有该账号
	 * @param telephone
	 * @return
	 */
	public static boolean isRegister(String telephone){
		Query query =new Query().addCriteria(Criteria.where("telephone").is(telephone));
		if (null!=SKBeanUtils.getDatastore().findOne(query,OfficialInfo.class)) {
			return true;
		}
		return false;
	}

	/**
	 * 根据电话号码获取审核详情
	 * @param telephone
	 * @return
	 */
	@RequestMapping(value = "/getOfficialInfoByTel")
	public JSONMessage getOfficialInfoByTel(String telephone){
		String areaCode = "86";
		OfficialInfo officialInfo = officialInfoCheck.getOfficialInfo(areaCode+telephone);
		User user = userManager.getUser(areaCode+telephone);
        Map<String, Object> data = new HashMap<>();
        data.put("officialInfo",officialInfo);
        data.put("user",user);
        return JSONMessage.success(data);
	}

	/**
	 * 重新请求审核
	 * @param info
	 * @return
	 */
	@RequestMapping(value = "/updateOfficialInfoByTel")
	public JSONMessage updateOfficialInfoByTel(@ModelAttribute OfficialInfo info){
		//校验营业执照号
		/*if (info.getCompanyBusinessLicense().length() == 15){
			boolean businessLicense15 = BusinessUtils.isBusinessLicense15(info.getCompanyBusinessLicense());
			if (!businessLicense15){
				return JSONMessage.failure("营业执照号格式错误！");
			}
		}else if(info.getCompanyBusinessLicense().length() == 18){
			boolean businessLicense18 = BusinessUtils.isBusinessLicense18(info.getCompanyBusinessLicense());
			if (!businessLicense18){
				return JSONMessage.failure("统一社会信用代码格式错误！");
			}
		}*/

		if (!MobileValidateUtils.checkMobileNumber(info.getAdminTelephone())){
			return JSONMessage.failure("管理员手机号格式错误！");
		}

		//校验身份证
		boolean flag = ValidateIDCardUtils.validateIdCard18(info.getAdminID());
		if (!flag){
			return JSONMessage.failure("管理员身份证号码格式错误！");
		}

		officialInfoCheck.updateOfficialInfoByTel(info);
		return JSONMessage.success();
	}


	/**
	 * 获取全局配置
	 **/
	@RequestMapping(value = "/queryMpConfig")
	public JSONMessage queryMpConfig() {
		return JSONMessage.success(mpRedisRepository.getMpConfig());
	}

	/**
	 * 设置全局配置
	 **/
	@RequestMapping(value = "/saveMpConfig")
	public JSONMessage saveMallConfig(String config){
		mpRedisRepository.setMpConfig(config);
		return JSONMessage.success();
	}


	@RequestMapping(value = "/find/user/info")
	public JSONMessage findUserInfo(int userId){
		User user = userManager.getUser(userId);
		return JSONMessage.success(user);
	}

	/**
	 * 富文本发送内容
	 **/
	@RequestMapping(value="/send/rich/test/message")
	@ResponseBody
	public JSONMessage sendTestMessage(@RequestParam(defaultValue="") String details,@RequestParam(defaultValue = "") String title,
									   @RequestParam(defaultValue = "") String img,@RequestParam(defaultValue = "") String sub,
									   @RequestParam(defaultValue = "") String contentFrom){
		
		int userId = ReqUtil.getUserId();
		logger.info("[管理端富文本消息发送入口] 接收到富文本消息发送请求 - 用户ID: {}, 标题: {}, 副标题: {}, 图片: {}, 来源: {}", 
				   userId, title, sub, img, contentFrom);
		logger.debug("[管理端富文本消息发送入口] 详细内容长度: {}", details != null ? details.length() : 0);
		
		User user = userManager.getUserDao().get(userId);
		if (user == null) {
			logger.error("[管理端富文本消息发送入口] 用户不存在 - 用户ID: {}", userId);
			return JSONMessage.failure("用户不存在");
		}

		Map<String,Object> map = new HashMap<>();
		ObjectId objectId = new ObjectId();
		map.put("contentId",objectId.toString());
		map.put("title",title);
		map.put("sub",sub);
		map.put("img",img);
		String content = JSONObject.toJSONString(map);
		
		logger.debug("[管理端富文本消息发送入口] 构建消息内容完成 - 内容ID: {}, JSON长度: {}", 
					objectId.toString(), content.length());

		MessageBean messageBean=new MessageBean();
		messageBean.setContent(content);
		messageBean.setFromUserId(user.getUserId() + "");
		messageBean.setFromUserName(user.getNickname());
		messageBean.setTimeSend(DateUtil.currentTimeSeconds());
		messageBean.setType(MsgType.TYPE_RICH_TEXT_MALL);
		// 广播消息
		messageBean.setMsgType(2);
		String messageId = StringUtil.randomUUID();
		messageBean.setMessageId(messageId);
		
		logger.info("[管理端富文本消息发送入口] 构建MessageBean完成 - 消息ID: {}, 发送者: {}, 消息类型: {}, 广播消息: {}", 
				   messageId, userId, MsgType.TYPE_RICH_TEXT_MALL, true);
		logger.debug("[管理端富文本消息发送入口] 完整MessageBean: {}", messageBean.toString());
		
		try {
			logger.info("[管理端富文本消息发送入口] 开始异步发送消息 - 消息ID: {}", messageId);
			ThreadUtils.executeInThread((Callback) obj -> {
				try {
					logger.debug("[管理端富文本消息发送入口] 调用messageService.send - 消息ID: {}", messageId);
					messageService.send(messageBean);
					logger.info("[管理端富文本消息发送入口] 消息发送成功 - 消息ID: {}", messageId);
				} catch (Exception e) {
					logger.error("[管理端富文本消息发送入口] 异步消息发送失败 - 消息ID: {}, 异常: {}", messageId, e.getMessage(), e);
				}
			});
			
			logger.debug("[管理端富文本消息发送入口] 保存群发内容到数据库 - 内容ID: {}", objectId.toString());
			massContentDao.sava(new MassContent(objectId,details,title,sub,contentFrom));
			logger.info("[管理端富文本消息发送入口] 群发内容保存成功 - 内容ID: {}", objectId.toString());
			
		} catch (Exception e) {
			logger.error("[管理端富文本消息发送入口] 富文本消息发送失败 - 用户ID: {}, 消息ID: {}, 异常: {}", 
					 userId, messageId, e.getMessage(), e);
			return JSONMessage.failure(e.getMessage());
		}
		
		logger.info("[管理端富文本消息发送入口] 富文本消息发送请求处理完成 - 用户ID: {}, 消息ID: {}", userId, messageId);
		return JSONMessage.success();
	}

	/**
	 * 删除资源文件
	 **/
	@RequestMapping(value = "/delete/resource")
	public JSONMessage findUserInfo(String url){
		uploadItemeDao.deleteResource(url);
		return JSONMessage.success();
	}


	/**
	 * 获取群发内容
	 **/
	@RequestMapping(value = "/find/content")
	public JSONMessage findContent(@RequestParam(defaultValue = "") String id) {
		if (StringUtil.isEmpty(id)){
			return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
		}
		MassContent massContent = massContentDao.find(new ObjectId(id));
		if (ObjectUtil.isEmpty(massContent)){
			return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
		}
		MassCommentPraise massCommentPraise = massCommentPraiseDao.findByUserId(ReqUtil.getUserId(), massContent.getId().toString());
		if (ObjectUtil.isEmpty(massCommentPraise)){
			massContent.setIsPraise(MassContent.Type.NOT_PRAISE);
		}else{
			massContent.setIsPraise(MassContent.Type.PRAISE);
		}
		try {
			Map<String,Object> map = new HashMap<>();
			map.put("look",massContent.getLook()+1);
			//阅读加一
			massContentDao.update(massContent.getId(),map);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return JSONMessage.success(massContent);
	}

	/**
	 * 获取评论
	 **/
	@RequestMapping(value = "/find/comment")
	public JSONMessage findComment(@RequestParam(defaultValue = "") String id,@RequestParam(defaultValue = "0") int pageIndex,@RequestParam(defaultValue = "10") int pageSize) {
		if (StringUtil.isEmpty(id) || 0 == ReqUtil.getUserId()){
			return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
		}
		List<MassComment> massComments = massCommentDao.find(id, pageIndex-1, pageSize);
		massComments.forEach(massComment -> {
			MassCommentPraise massCommentPraise = massCommentPraiseDao.findByUserId(ReqUtil.getUserId(), massComment.getId().toString());
			if (massCommentPraise != null){
				massComment.setIsPraise(MassComment.Type.PRAISE);
			}else{
				massComment.setIsPraise(MassComment.Type.NOT_PRAISE);
			}
		});
		return JSONMessage.success(massComments);
	}

	/**
	 * 添加评论
	 **/
	@RequestMapping(value = "/sava/comment")
	public JSONMessage findComment(@RequestParam(defaultValue = "") String massContentId,@RequestParam(defaultValue = "") String commentContent) {
		Integer userId = ReqUtil.getUserId();
		if (StringUtil.isEmpty(massContentId) || StringUtil.isEmpty(commentContent)){
			return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
		}
		String userName = userManager.getNickName(Integer.valueOf(userId));
		MassComment massComment = massCommentDao.sava(new MassComment(massContentId, Long.valueOf(userId), userName, commentContent));
		return JSONMessage.success(massComment);
	}

	/**
	 * 获取评论数量
	 **/
	@RequestMapping(value = "/find/comment/count")
	public JSONMessage findCommentCount(@RequestParam(defaultValue = "") String id) {
		if (StringUtil.isEmpty(id)){
			return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
		}
		return JSONMessage.success(massCommentDao.countMassComment(id));
	}

	/**
	 * 评论,文章点赞
	 * type = 1 = 点赞
	 * type = -1 = 取消点赞
	 *
	 * isComment = 1 = 评论
	 * isComment = 0 = 文章
	 **/
	@RequestMapping(value = "/give/like")
	public JSONMessage giveLike(@RequestParam(defaultValue = "") String commentId,@RequestParam(defaultValue = "1") int type,@RequestParam(defaultValue = "1")byte isComment) {
		Integer userId = ReqUtil.getUserId();
		if (StringUtil.isEmpty(commentId) || 0 == userId){
			return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsAuthFail);
		}
		if (1 == isComment){
			massCommentDao.updateGiveLike(commentId, type);
		}else if(0 == isComment){
			massContentDao.updateGiveLike(new ObjectId(commentId),type);
		}

		if (1 == type){
			massCommentPraiseDao.sava(new MassCommentPraise(userId,commentId));
		}if (-1 == type){
			massCommentPraiseDao.delete(userId,commentId);
		}
		return JSONMessage.success();
	}

}
