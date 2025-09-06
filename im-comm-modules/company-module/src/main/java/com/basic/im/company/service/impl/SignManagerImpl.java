package com.basic.im.company.service.impl;

import com.basic.im.company.service.SignManager;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class SignManagerImpl implements SignManager {

	private static final String USER_SIGN_IP = "userSign:ip:%s";

	private static final String USER_SIGN_USERID = "userSign:userId:%s";

	@Autowired(required=false)
	private RedissonClient redissonClient;


	@Override
	public boolean saveUserSign(Integer userId, String signIp, String device, Long timeOut) {
//		String signIpKey = String.format(USER_SIGN_IP,signIp);
//		RBucket<String> signIpBucket = redissonClient.getBucket(signIpKey);
//		if (signIpBucket != null && signIpBucket.get() != null){
//			return false;
//		}
//		String signDeviceKey = String.format(USER_SIGN_DEVICE,device);
//		RBucket<String> signDeviceBucket = redissonClient.getBucket(signDeviceKey);
//		if (signDeviceBucket != null && signDeviceBucket.get() != null){
//			return false;
//		}
		String signUserIdKey = String.format(USER_SIGN_USERID,userId);
		RBucket<Integer> signUserIdBucket = redissonClient.getBucket(signUserIdKey);
		if (signUserIdBucket != null && signUserIdBucket.get() != null){
			return false;
		}
//		signIpBucket.set(signIp);
//		signIpBucket.expire(timeOut, TimeUnit.SECONDS);
//		signDeviceBucket.set(device);
//		signDeviceBucket.expire(timeOut, TimeUnit.SECONDS);
		if (userId != null){
			signUserIdBucket.set(userId);
		}
		if (timeOut != null){
			signUserIdBucket.expire(timeOut, TimeUnit.SECONDS);
		}
		return true;
	}

	@Override
	public void delUserSign(Integer userId, String signIp, String device) {
		String signUserIdKey = String.format(USER_SIGN_USERID,userId);
		redissonClient.getBucket(signUserIdKey).delete();
	}


}