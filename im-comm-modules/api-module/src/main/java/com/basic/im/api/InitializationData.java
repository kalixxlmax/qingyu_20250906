package com.basic.im.api;

import com.alibaba.fastjson.JSONArray;
import com.mongodb.BasicDBObject;
import com.basic.im.admin.dao.ErrorMessageDao;
import com.basic.im.admin.dao.PayConfigDao;
import com.basic.im.admin.dao.PushConfigDao;
import com.basic.im.admin.service.AdminManager;
import com.basic.im.common.service.PaymentManager;
import com.basic.im.config.AppConfig;
import com.basic.im.entity.PayConfig;
import com.basic.im.entity.PushConfig;
import com.basic.im.entity.SmsConfig;
import com.basic.im.model.ErrorMessage;
import com.basic.im.security.entity.ResourceInfo;
import com.basic.im.security.entity.SecurityRole;
import com.basic.im.security.service.ResourceInfoManager;
import com.basic.im.security.service.SecurityRoleManager;
import com.basic.im.user.config.WXConfig;
import com.basic.im.user.config.WXPublicConfig;
import com.basic.im.user.dao.UserDao;
import com.basic.im.user.entity.Role;
import com.basic.im.user.entity.User;
import com.basic.im.user.service.UserHandler;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.user.utils.WXUserUtils;
import com.basic.im.utils.ConstantUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.Md5Util;
import com.basic.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

/** @version:（1.0） 
* @ClassName	InitializationData
* @Description: （初始化数据） 
* @author: wcl
* @date:2018年8月25日下午4:07:23  
*/
@Component
@Slf4j
public class InitializationData  implements CommandLineRunner {
	
	
	
	@Value("classpath:data/message.json")
	private Resource resource;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
    @Lazy
	private ErrorMessageDao errorMessageDao;

	@Autowired
	@Lazy
	private UserDao userDao;

	@Autowired
	@Lazy
	private UserManagerImpl userManager;

	@Autowired
	private AppConfig appConfig;


	@Autowired
	private WXConfig wxConfig;

	@Autowired
	private WXPublicConfig wxPublicConfig;

	@Autowired
	private UserHandler userHandler;

	@Autowired
	private PayConfigDao payConfigDao;
	@Autowired
	@Lazy
	private AdminManager adminManager;

	@Autowired(required = false)
	private PaymentManager paymentManager;


	@Value("classpath:data/authority.json")
	private Resource authority;


	@Value("classpath:data/securityRole.json")
	private Resource securityRole;

	@Autowired
	private ResourceInfoManager resourceInfoManager;

	@Autowired
	private SecurityRoleManager securityRoleManager;

	@Autowired
	private Environment environment;


	@Override
	public void run(String... args) throws Exception {


		ConstantUtil.setMongoTemplate(mongoTemplate);
		ConstantUtil.setAppConfig(appConfig);
		
		WXUserUtils.setConfig(wxConfig,wxPublicConfig);


		if(1==appConfig.getOpenClearAdminToken())
			//启动时清空 redis 里的
        {
            SKBeanUtils.getRedisCRUD().deleteKeysByPattern("adminToken:*");
        }

		createDBIndex();

		initSuperAdminData();

		initErrorMassageData();

		initPayConfig();

		initAuthorityData();

		initSecurityRoleData();



		initSmsConfig();


		/**
		 * 内部定制功能,严禁修改 lidaye
		 */

		initAppConfig();

	}

	private void initAppConfig(){
		try {
			if(com.basic.im.comm.utils.StringUtil.isEmpty(appConfig.getServicePhones())){
				return;
			}

			String[] split = appConfig.getServicePhones().split(",");
			User user;
			for (String phone : split) {
				user= userDao.getUser(phone);
				if(null!=user){
					appConfig.getServiceNoList().add(user.getUserId());
				}

			}
		}catch (Exception e){

		}

	}


	private void createDBIndex(){
		try {
			BasicDBObject keys = new BasicDBObject();
			keys.put("loc", "2d");
			keys.put("nickname", 1);
			keys.put("sex", 1);
			keys.put("birthday", 1);
			keys.put("active", 1);

			mongoTemplate.getCollection("user").createIndex(keys);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @Description 初始化资源信息
	 * @Date 9:47 2020/3/14
	 **/
	public void  initAuthorityData() throws Exception{
		if (null == authority){
			System.out.println("error initAuthorityData()  resource is null");
			return;
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(authority.getInputStream()));
		StringBuffer auth = new StringBuffer();
		String line = null;
		while ((line = br.readLine()) != null) {
			auth.append(line);
		}
		String defaultString = auth.toString();
		if(!StringUtil.isEmpty(defaultString)){
			List<ResourceInfo> resourceInfo = resourceInfoManager.getResourceInfoByJson(defaultString);
			resourceInfo.stream().filter(msg->!StringUtil.isEmpty(msg.getResourceName())).forEach(info ->{
		
				/*if (info.getResourceName().equals("展示数据")){*/
					//判断是否有同父级的资源
					boolean flag = false;
					List<ResourceInfo> resourceInfos = resourceInfoManager.queryResourceInfoByPid(info.getPid());
					if (resourceInfos.size() > 0){
						for (ResourceInfo resourceInfo1 : resourceInfos) {
							//判断该父级下已经有该资源
							if (resourceInfo1.getResourceName().equals(info.getResourceName())){
								flag = true;
								return;
							}
						}
					}
				/*}*/
				if (flag){
					flag = false;
					return;
				}
				ResourceInfo resourceInfo1 = resourceInfoManager.queryResourceInfoById(info.getId());
				if (resourceInfo1 == null){
				log.info("insert authority msg {}",info.toString());
					resourceInfoManager.sava(info);
				}
			});
		}

		log.info(">>>>>>>>>>>>>>> 权限资源信息数据初始化完成  <<<<<<<<<<<<<");
	}

	/**
	 * @Description 初始角色列表
	 * @Date 10:38 2020/3/18
	 **/
	public void  initSecurityRoleData() throws Exception{
		if (null == securityRole){
			System.out.println("error initSecurityRoleData()  resource is null");
			return;
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(securityRole.getInputStream()));
		StringBuffer auth = new StringBuffer();
		String line = null;
		while ((line = br.readLine()) != null) {
			auth.append(line);
		}
		String defaultString = auth.toString();
		if(!StringUtil.isEmpty(defaultString)){
			List<SecurityRole> securityRoles = securityRoleManager.securityRoleManager(defaultString);

			securityRoles.stream().filter(msg->!StringUtil.isEmpty(String.valueOf(msg.getRoleId()))).forEach(info ->{
				SecurityRole securityRole = securityRoleManager.querySecurityRoleById(info.getRoleId());
				if (securityRole == null){
					log.info("insert securityRole msg {}",info.toString());
					securityRoleManager.sava(info);
				}
			});
		}

		log.info(">>>>>>>>>>>>>>> 角色信息数据初始化完成  <<<<<<<<<<<<<");
	}

	/**
    * 初始化异常信息数据
	* @throws Exception
	*/
	private void initErrorMassageData() throws Exception{
		if(null==resource) {
			System.out.println("error initErrorMassageData  resource is null");
			return;
		}
		//DBCollection errMsgCollection = getDatastore().getCollection(ErrorMessage.class);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(resource.getInputStream()));
		StringBuffer message = new StringBuffer();
		String line = null;
		while ((line = br.readLine()) != null) {
			message.append(line);
		}
		String defaultString = message.toString();
		if(!StringUtil.isEmpty(defaultString)){
			List<ErrorMessage> errorMessages = JSONArray.parseArray(defaultString, ErrorMessage.class);
			errorMessages.stream().filter(msg->!StringUtil.isEmpty(msg.getCode())).forEach(errorMessage ->{
				ErrorMessage code = errorMessageDao.queryOne("code", errorMessage.getCode());
				if(null==code) {
					log.info("insert error msg {}",errorMessage.toString());
					errorMessageDao.save(errorMessage);
				}
			});
			
		}
		log.info(">>>>>>>>>>>>>>> 异常信息数据初始化完成  <<<<<<<<<<<<<");

		ConstantUtil.initMsgMap();
	}
	/**
     * 初始化默认超级管理员数据
	*/
	private void initSuperAdminData() {

		if (mongoTemplate.count(new Query(),Role.class) == 0) {
			try {
				// 初始化后台管理超级管理员
				userDao.addUser(1000, "861000" ,"1000");
				userHandler.registerToIM("1000", Md5Util.md5Hex("1000"));
				Role role = new Role(1000, "1000", (byte) 6, (byte) 1, 0);
				mongoTemplate.save(role);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// 初始化10000号
			try {
				userDao.addUser(10000,"8610000" ,"10000");
				userHandler.registerToIM("10000", Md5Util.md5Hex("10000"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			log.info("\n" + ">>>>>>>>>>>>>>> 默认管理员数据初始化完成  <<<<<<<<<<<<<");
		}
		if(userDao.getUser(1100)==null){
			// 初始化1100号 作为金钱相关通知系统号码
			try {
				userDao.addUser(1100, "861100" ,"1100");
				userHandler.registerToIM("1100", Md5Util.md5Hex("1100"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			log.info("\n" + ">>>>>>>>>>>>>>> 默认系统通知数据初始化完成  <<<<<<<<<<<<<");
		}
		
		
	}

	/**
	 * 初始化支付配置
	 **/
	private void initPayConfig(){
		PayConfig payConfig = payConfigDao.getPayConfig();
		if(null == payConfig){
			payConfig = adminManager.initPayConfig();
		}
//		ConstantUtil.setPayConfig(payConfig);
		SKBeanUtils.getImCoreService().setPayConfig(payConfig);
	}

	/**
	 * 初始化短信配置
	 **/
	private void initSmsConfig() {
		SmsConfig config = SKBeanUtils.getImCoreRepository().getSmsConfig();
		if (null != config){
			return;
		}
		SmsConfig smsConfig = new SmsConfig();
		String opensms = environment.getProperty("smsconfig.opensms");
		String port = environment.getProperty("smsConfig.port");
		if (!StringUtil.isEmpty(opensms)){
			smsConfig.setOpenSMS(Integer.valueOf(opensms));
		}
		if (!StringUtil.isEmpty(port)){
			smsConfig.setPort(Integer.valueOf(port));
		}
		smsConfig.setHost(environment.getProperty("smsConfig.host"));
		smsConfig.setApi(environment.getProperty("smsConfig.api"));
		smsConfig.setUsername(environment.getProperty("smsConfig.username"));
		smsConfig.setPassword(environment.getProperty("smsConfig.password"));
		smsConfig.setTemplateChineseSMS(environment.getProperty("smsConfig.templateChineseSMS"));
		smsConfig.setTemplateEnglishSMS(environment.getProperty("smsConfig.templateEnglishSMS"));
		smsConfig.setProduct(environment.getProperty("smsConfig.product"));
		smsConfig.setDomain(environment.getProperty("smsConfig.domain"));
		smsConfig.setAccesskeyid(environment.getProperty("smsConfig.accesskeyid"));
		smsConfig.setAccesskeysecret(environment.getProperty("smsConfig.accesskeysecret"));
		smsConfig.setSignname(environment.getProperty("smsConfig.signname"));
		smsConfig.setChinase_templetecode(environment.getProperty("smsConfig.chinase_templetecode"));
		smsConfig.setInternational_templetecode(environment.getProperty("smsConfig.international_templetecode"));
		smsConfig.setCloudWalletVerification(environment.getProperty("smsConfig.cloudWalletVerification"));
		smsConfig.setCloudWalletNotification(environment.getProperty("smsConfig.cloudWalletNotification"));
		smsConfig.setBizType(environment.getProperty("smsConfig.bizType"));
		adminManager.setSmsConfig(smsConfig);
		log.info(">>>>>>>>>>>>>>> 短信配置数据初始化完成  <<<<<<<<<<<<<");
	}




}
