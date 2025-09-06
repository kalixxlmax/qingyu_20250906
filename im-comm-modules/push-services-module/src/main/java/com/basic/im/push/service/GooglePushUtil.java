package com.basic.im.push.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import com.basic.im.entity.PushConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
@Slf4j
public class GooglePushUtil extends PushServiceUtils{

	public InputStream getJson(String packageName) throws FileNotFoundException{
		PushConfig.AndroidPush pushConfig = getPushConfig(packageName);
		log.info("getJSON :{}",pushConfig.getFcmKeyJson().startsWith("classpath:"));
		try {
			if(pushConfig.getFcmKeyJson().startsWith("classpath:")) {
				ClassPathResource resource = new ClassPathResource(pushConfig.getFcmKeyJson());
				String path = resource.getClassLoader().getResource(pushConfig.getFcmKeyJson().replace("classpath:", "")).getPath();
				pushConfig.setFcmKeyJson(path);
				InputStream stream = this.getClass().getResourceAsStream(pushConfig.getFcmKeyJson());
				return this.getClass().getResourceAsStream(pushConfig.getFcmKeyJson());
			}
			FileInputStream fileInputStream = new FileInputStream(new File(pushConfig.getFcmKeyJson()));
			System.out.println("file"+fileInputStream);
			return new FileInputStream(new File(pushConfig.getFcmKeyJson()));
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e.getMessage(),e);
		}
		return new FileInputStream(new File(""));
	}
}
