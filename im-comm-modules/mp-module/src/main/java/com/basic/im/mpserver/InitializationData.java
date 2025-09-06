package com.basic.im.mpserver;

import com.mongodb.BasicDBObject;
import com.basic.im.config.AppConfig;
import com.basic.im.user.entity.Role;
import com.basic.im.user.service.UserHandler;
import com.basic.im.user.service.impl.UserManagerImpl;
import com.basic.im.utils.ConstantUtil;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.Md5Util;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

/** @version:（1.0） 
* @ClassName	InitializationData
* @Description: （初始化数据） 
* @author: wcl
* @date:2018年8月25日下午4:07:23  
*/
@Component
@Slf4j
public class InitializationData implements CommandLineRunner {
	
	
	
	/*@Value("classpath:data/message.json")
	private Resource resource;*/

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private UserManagerImpl userManager;

	@Autowired
	private AppConfig appConfig;
	@Autowired
	private  MpConfig mpConfig;

/*

	@Autowired
	private MessageService messageService;
*/

	@Autowired
	private UserHandler userHandler;


	

	@Override
	public void run(String... args) throws Exception {
		log.info("apiKey ==="+mpConfig.getApiKey());
		appConfig.setApiKey(mpConfig.getApiKey());
		ConstantUtil.setMongoTemplate(mongoTemplate);
		ConstantUtil.setAppConfig(appConfig);




		if(1==appConfig.getOpenClearAdminToken())
			//启动时清空 redis 里的
        {
            SKBeanUtils.getRedisCRUD().deleteKeysByPattern("adminToken:*");
        }

		createDBIndex();

		initSuperAdminData();

//		initErrorMassageData();
		
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
	 * 初始化默认超级管理员数据
	*/
	private void initSuperAdminData() {

		if (mongoTemplate.count(new Query(),Role.class) == 0) {
			try {
				// 初始化后台管理超级管理员
				userManager.addUser(1000,"861000", "1000");
				userHandler.registerToIM("1000", Md5Util.md5Hex("1000"));
				Role role = new Role(1000, "1000", (byte) 6, (byte) 1, 0);
				mongoTemplate.save(role);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// 初始化10000号
			try {
				userManager.addUser(10000, "8610000","10000");
				userHandler.registerToIM("10000", Md5Util.md5Hex("10000"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			log.info("\n" + ">>>>>>>>>>>>>>> 默认管理员数据初始化完成  <<<<<<<<<<<<<");
		}
		
		if(userManager.getUser(1100)==null){
			// 初始化1100号 作为金钱相关通知系统号码
			try {
				userManager.addUser(1100, "861100","1100");
				userHandler.registerToIM("1100", Md5Util.md5Hex("1100"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			log.info("\n" + ">>>>>>>>>>>>>>> 默认系统通知数据初始化完成  <<<<<<<<<<<<<");
		}
		
		
	}
}
