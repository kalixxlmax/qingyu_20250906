package com.basic.im.user.service.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.common.service.PaymentManager;
import com.basic.im.friends.service.impl.FriendsManagerImpl;
import com.basic.im.user.dao.AuthKeysDao;
import com.basic.im.user.entity.AuthKeys;
import com.basic.im.user.event.KeyPairChageEvent;
import com.basic.im.user.model.KeyPairParam;
import com.basic.im.user.service.*;
import com.basic.im.user.utils.WXUserUtils;
import com.basic.im.utils.SKBeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lidaye
 *
 */
@Slf4j
@Service
public class AuthKeysServiceImpl implements AuthKeysService {

	@Autowired
	private AuthKeysDao authKeysDao;


	/*@Autowired
	@Qualifier(value = "userDao")
	private UserDaoImpl userDao;*/

	@Autowired
	private UserCoreService userManager;

	@Autowired
	private UserRedisService userRedisService;

	@Autowired
	private UserCoreRedisRepository userCoreRedisRepository;

	@Autowired(required = false)
	private PaymentManager paymentManager;


	@Autowired(required = false)
	private FriendsManagerImpl friendsManager;

	@Autowired(required = false)
	private UserHandler userHandler;

	@Override
	public List<AuthKeys> getYopNotNull() {
		return authKeysDao.getYopNotNull();
	}

	public AuthKeys getAuthKeys(int userId){
		AuthKeys authKeys = userRedisService.getAuthKeys(userId);
		if(null == authKeys){
			authKeys = authKeysDao.queryAuthKeys(userId);
			if(null!=authKeys) {
                userRedisService.saveAuthKeys(userId,authKeys);
            }
		}
		return authKeys;
	}

	public synchronized void updateLoginPassword(int userId,String password) {
		AuthKeys userKeys = authKeysDao.getAuthKeys(userId);
		if(null==userKeys) {
			userKeys=new AuthKeys(userId);
			userKeys.setPassword(password);
			authKeysDao.addAuthKeys(userKeys);
			return;
		}

		Map<String,Object> map = new HashMap<>();
		map.put("password", password);
		map.put("modifyTime", DateUtil.currentTimeSeconds());
		authKeysDao.updateAuthKeys(userId,map);
		userManager.updatePassowrd(userId,password);
		//给好友发送更新公钥的xmpp 消息 803 服务端关闭端到端后不推送
		if(1 == SKBeanUtils.getImCoreService().getClientConfig().getIsOpenSecureChat() && userKeys.getMsgDHKeyPair()!=null && userKeys.getMsgRsaKeyPair()!=null) {
			sendUpdatePublicKeyMsgToFriends(userKeys.getMsgDHKeyPair().getPublicKey(), userKeys.getMsgRsaKeyPair().getPublicKey(), userId);
		}
		//删除自己的

		userRedisService.deleteAuthKeys(userId);
		userCoreRedisRepository.deleteUserByUserId(userId);
		updateLoginPasswordCleanKeyPair(userId);
	}
	@Override
	public  String queryLoginPassword(int userId) {
		Object dbObj = authKeysDao.queryOneFieldById("password", userId);
		if(null==dbObj){
			/*String oldPwd = userManager.queryPassword(userId);
			String newPassword = LoginPassword.encodeFromOldPassword(oldPwd);
			updateLoginPassword(userId,newPassword);*/
			return null;
		}

		return dbObj.toString();
	}
	public String getPayPassword(Integer userId) {
		Object key = authKeysDao.queryOneFieldById("payPassword", userId);
		if(null==key) {
            return null;
        } else {
            return String.valueOf(key);
        }
	}

	public String getWalletUserNo(int userId){
		Object key = authKeysDao.queryOneFieldById("walletUserNo",userId);
		if(null == key) {
            return null;
        } else {
            return String.valueOf(key);
        }
	}
	public String getUpayWalletId(int userId){
		Object key = authKeysDao.queryOneFieldById("walletId",userId);
		if(null == key) {
			return null;
		} else {
			return String.valueOf(key);
		}
	}
	public void setUpayWalletId(int userId,String walletId){
		if(null==getUpayWalletId(userId)) {
			authKeysDao.updateAttribute(userId,"walletId", walletId);
			userRedisService.deleteAuthKeys(userId);
		}
	}

	public String queryHideChatPassword(int userId){
		Object key = authKeysDao.queryOneFieldById("hideChatPassword",userId);
		if(null == key) {
			return null;
		} else {
			return String.valueOf(key);
		}
	}
	public void setHideChatPassword(int userId,String password){
		authKeysDao.updateAttribute(userId,"hideChatPassword",password);
		userRedisService.deleteAuthKeys(userId);
	}

	public synchronized void updatePayPassword(int userId,String payPassword) {
		AuthKeys userKeys = authKeysDao.getAuthKeys(userId);
		if(null==userKeys) {
			userKeys=new AuthKeys(userId);

			userKeys.setPayPassword(payPassword);
			authKeysDao.addAuthKeys(userKeys);
			return;
		}
		Map<String,Object> map = new HashMap<>();
		map.put("payPassword", payPassword);
		map.put("modifyTime",DateUtil.currentTimeSeconds());
		authKeysDao.updateAuthKeys(userId,map);
		userRedisService.deleteAuthKeys(userId);

	}
	public synchronized void uploadPayKey(int userId,String publicKey,String privateKey) {
		AuthKeys userKeys = authKeysDao.getAuthKeys(userId);
		if(null==userKeys) {
			userKeys=new AuthKeys(userId);
			AuthKeys.KeyPair keyPair=new AuthKeys.KeyPair(publicKey, privateKey);
			keyPair.setCreateTime(userKeys.getCreateTime());
			userKeys.setPayKeyPair(keyPair);
			authKeysDao.addAuthKeys(userKeys);
			return;
		}
		long time=DateUtil.currentTimeSeconds();
		Map<String,Object> map = new HashMap<>();
		map.put("payKeyPair.publicKey", publicKey);
		map.put("payKeyPair.privateKey", privateKey);
		map.put("payKeyPair.modifyTime",time);
		map.put("modifyTime",time);
		authKeysDao.updateAuthKeys(userId,map);

	}
	public synchronized void uploadLoginKeyPair(int userId,String publicKey,String privateKey) {
		AuthKeys userKeys = authKeysDao.getAuthKeys(userId);
		if(null==userKeys) {
			userKeys=new AuthKeys(userId);
			AuthKeys.KeyPair keyPair=new AuthKeys.KeyPair(publicKey, privateKey);
			keyPair.setCreateTime(userKeys.getCreateTime());
			userKeys.setLoginKeyPair(keyPair);
			authKeysDao.addAuthKeys(userKeys);
			return;
		}
		if(null!=userKeys.getLoginKeyPair()&&!StringUtil.isEmpty(userKeys.getLoginKeyPair().getPrivateKey())) {
			log.error("{}  登陆公私钥 已经存在  不能更新  ",userId);
			return;
		}
		long time=DateUtil.currentTimeSeconds();
		Map<String,Object> map = new HashMap<>();
		map.put("loginKeyPair.publicKey", publicKey);
		map.put("loginKeyPair.privateKey", privateKey);
		map.put("loginKeyPair.modifyTime",time);
		map.put("modifyTime",time);
		authKeysDao.updateAuthKeys(userId,map);
	}
	public  void deleteLoginKeyPair(int userId) {
		AuthKeys userKeys = authKeysDao.getAuthKeys(userId);
		if(null==userKeys) {
			return;
		}
		if(null==userKeys.getLoginKeyPair()||StringUtil.isEmpty(userKeys.getLoginKeyPair().getPublicKey())) {
            return;
        }
		long time=DateUtil.currentTimeSeconds();
		Map<String,Object> map = new HashMap<>();
		map.put("loginKeyPair.publicKey", "");
		map.put("loginKeyPair.privateKey", "");
		map.put("loginKeyPair.modifyTime",time);
		map.put("modifyTime",time);
		authKeysDao.updateAuthKeys(userId,map);

	}

	public  void deletePayKey(int userId) {
		AuthKeys userKeys = authKeysDao.getAuthKeys(userId);
		if(null==userKeys) {
			return;
		}
		long time=DateUtil.currentTimeSeconds();
		Map<String,Object> map = new HashMap<>();
		map.put("payKeyPair.publicKey", "");
		map.put("payKeyPair.privateKey", "");
		map.put("payKeyPair.modifyTime",time);
		map.put("modifyTime",time);
		authKeysDao.updateAuthKeys(userId,map);
	}
	public  String getPayPublicKey(int userId) {
		Document payPublicKey =(Document) authKeysDao.queryOneFieldById("payKeyPair", userId);
		if(null==payPublicKey) {
            return null;
        } else {
            return payPublicKey.getString("publicKey");
        }
	}


	@Override
	public void cleanTransactionSignCode(int userId, String codeId) {
		if (paymentManager == null){
			return;
		}
		paymentManager.cleanTransactionSignCode(userId,codeId);
	}

	@Override
	public String queryTransactionSignCode(int userId, String codeId) {
		if (paymentManager == null){
			return "";
		}
		return paymentManager.queryTransactionSignCode(userId,codeId);
	}



	public String getPayPrivateKey(int userId) {
		Document payPublicKey = (Document) authKeysDao.queryOneFieldById("payKeyPair", userId);
		if(null==payPublicKey) {
            return null;
        } else {
            return payPublicKey.getString("privateKey");
        }
	}
	public  String getLoginPublicKey(int userId) {
		Document dbObject =(Document) authKeysDao.queryOneFieldById("loginKeyPair", userId);
		if(null==dbObject) {
            return null;
        } else {
            return dbObject.getString("publicKey");
        }
	}
	public String getLoginPrivateKey(int userId) {
		Document dbObject =(Document) authKeysDao.queryOneFieldById("loginKeyPair", userId);
		if(null==dbObject) {
            return null;
        } else {
            return dbObject.getString("privateKey");
        }
	}

	/**
	 * 修改密码  清除 需要更新的 公私钥
	 */
	public void updateLoginPasswordCleanKeyPair(int userId){
		deleteLoginKeyPair(userId);
		userRedisService.deleteAuthKeys(userId);

	}
	public synchronized boolean uploadMsgKey(int userId, KeyPairParam param) {
		AuthKeys userKeys = authKeysDao.getAuthKeys(userId);
		if(null==userKeys) {
			userKeys=new AuthKeys(userId);
			if(!StrUtil.isEmpty(param.getRsaPublicKey())&&!StrUtil.isEmpty(param.getRsaPrivateKey())){
				AuthKeys.KeyPair rsakeyPair=new AuthKeys.KeyPair(param.getRsaPublicKey(), param.getRsaPrivateKey());
				rsakeyPair.setCreateTime(userKeys.getCreateTime());
				userKeys.setMsgRsaKeyPair(rsakeyPair);
			}

			if(!StrUtil.isEmpty(param.getDhPublicKey())&&!StrUtil.isEmpty(param.getDhPrivateKey())){
				AuthKeys.KeyPair dhkeyPair=new AuthKeys.KeyPair(param.getDhPublicKey(), param.getDhPrivateKey());
				dhkeyPair.setCreateTime(userKeys.getCreateTime());
				userKeys.setMsgDHKeyPair(dhkeyPair);

				AuthKeys.PublicKey puKey=new AuthKeys.PublicKey();
				puKey.setKey(param.getDhPublicKey());
				puKey.setTime(userKeys.getCreateTime());
				userKeys.getDhMsgKeyList().add(puKey);
			}

			authKeysDao.addAuthKeys(userKeys);
			if(!StringUtil.isEmpty(param.getDhPublicKey())&&!StringUtil.isEmpty(param.getRsaPublicKey())) {
				userHandler.updateKeyPairHandler(new KeyPairChageEvent(userId,param.getDhPublicKey(),param.getRsaPublicKey()));

			}
			return true;
		}
		Map<String,Object> map = new HashMap<>();
		if(!StringUtil.isEmpty(param.getDhPublicKey())) {
			AuthKeys.PublicKey puKey=new AuthKeys.PublicKey();
			puKey.setKey(param.getDhPublicKey());
			puKey.setTime(DateUtil.currentTimeSeconds());
			userKeys.getDhMsgKeyList().add(puKey);
			map.put("msgDHKeyPair.publicKey", param.getDhPublicKey());
			map.put("dhMsgKeyList", userKeys.getDhMsgKeyList());
		}
		if(!StringUtil.isEmpty(param.getDhPrivateKey())) {
			map.put("msgDHKeyPair.privateKey", param.getDhPrivateKey());
		}
		if(!StringUtil.isEmpty(param.getRsaPublicKey())) {
			map.put("msgRsaKeyPair.publicKey", param.getRsaPublicKey());
		}
		if(!StringUtil.isEmpty(param.getRsaPrivateKey())) {

			map.put("msgRsaKeyPair.privateKey", param.getRsaPrivateKey());
		}
		map.put("modifyTime", DateUtil.currentTimeSeconds());
		//清除缓存
		userRedisService.deleteAuthKeys(userId);

		if(!StringUtil.isEmpty(param.getDhPublicKey())&&!StringUtil.isEmpty(param.getRsaPublicKey())) {
			userHandler.updateKeyPairHandler(new KeyPairChageEvent(userId,param.getDhPublicKey(),param.getRsaPublicKey()));

		}

		return authKeysDao.updateAuthKeys(userId,map);
	}

	@Override
	public Document queryMsgAndDHPublicKey(Integer userId) {
		return authKeysDao.queryMsgAndDHPublicKey(userId);
	}
	/**
	 * 上传 dh 消息公钥
	 * @param userId
	 * @param publicKey
	 * @param privateKey
	 */
	public synchronized void uploadDHMsgKey(int userId,String publicKey,String privateKey) {
		AuthKeys userKeys = authKeysDao.getAuthKeys(userId);
		if(null==userKeys) {
			userKeys=new AuthKeys(userId);
			AuthKeys.KeyPair keyPair=new AuthKeys.KeyPair(publicKey, privateKey);
			keyPair.setCreateTime(userKeys.getCreateTime());
			userKeys.setMsgDHKeyPair(keyPair);
			AuthKeys.PublicKey puKey=new AuthKeys.PublicKey();
			puKey.setKey(publicKey);
			puKey.setTime(keyPair.getCreateTime());
			userKeys.getDhMsgKeyList().add(puKey);
			authKeysDao.addAuthKeys(userKeys);
			return;
		}
		Map<String,Object> map = new HashMap<>();
		if(!StringUtil.isEmpty(publicKey)) {
			AuthKeys.PublicKey puKey=new AuthKeys.PublicKey();
			puKey.setKey(publicKey);
			puKey.setTime(DateUtil.currentTimeSeconds());
			userKeys.getDhMsgKeyList().add(puKey);
			map.put("msgDHKeyPair.publicKey", publicKey);
			map.put("dhMsgKeyList", userKeys.getDhMsgKeyList());
		}
		if(!StringUtil.isEmpty(privateKey)) {	
			map.put("msgDHKeyPair.privateKey", privateKey);
		}
		map.put("modifyTime", DateUtil.currentTimeSeconds());
		
		authKeysDao.updateAuthKeys(userId,map);
	}
	
	public String getMsgDHPublicKey(int userId) {
		Document dbObject = (Document) authKeysDao.queryOneFieldById("msgDHKeyPair", userId);
		if(null==dbObject) {
            return null;
        } else {
            return dbObject.getString("publicKey");
        }
	}

	public List<AuthKeys.PublicKey> queryMsgDHPublicKeyList(int userId) {
		Object payPublicKey = authKeysDao.queryOneFieldById("dhMsgKeyList", userId);
		if(null==payPublicKey) {
            return null;
        } else {
            return (List)payPublicKey;
        }
	}

	public Map<String,String> queryUseRSAPublicKeyList(List<Integer> userList) {
		return authKeysDao.queryUseRSAPublicKeyList(userList);
	}

	public synchronized void uploadMsgRSAKey(int userId,String publicKey,String privateKey) {
		AuthKeys userKeys = authKeysDao.getAuthKeys(userId);
		if(null==userKeys) {
			userKeys=new AuthKeys(userId);
			AuthKeys.KeyPair keyPair=new AuthKeys.KeyPair(publicKey, privateKey);
			keyPair.setCreateTime(userKeys.getCreateTime());
			userKeys.setMsgRsaKeyPair(keyPair);

			AuthKeys.PublicKey puKey=new AuthKeys.PublicKey();
			puKey.setKey(publicKey);
			puKey.setTime(keyPair.getCreateTime());
			authKeysDao.addAuthKeys(userKeys);
			return;
		}
		Map<String,Object> map = new HashMap<>();
		if(!StringUtil.isEmpty(publicKey)){
			AuthKeys.PublicKey puKey=new AuthKeys.PublicKey();
			puKey.setKey(publicKey);
			puKey.setTime(DateUtil.currentTimeSeconds());
			userKeys.getDhMsgKeyList().add(puKey);
			map.put("msgRsaKeyPair.publicKey", publicKey);
		}

		if(!StringUtil.isEmpty(privateKey)) {
			map.put("msgRsaKeyPair.privateKey", privateKey);
		}
		map.put("modifyTime", DateUtil.currentTimeSeconds());

		authKeysDao.updateAuthKeys(userId,map);
	}


	/**
	 * 用户 绑定微信 openId
	 * @param userId
	 * @param code
	 */
	public Object bindWxopenid(int userId,String code) {
		if(StringUtil.isEmpty(code)) {
			return null;
		}
		JSONObject jsonObject = WXUserUtils.getWxOpenId(code);
		String openid=jsonObject.getString("openid");
		if(StringUtil.isEmpty(openid)) {
			return null;
		}
		System.out.println(String.format("======> bindWxopenid  userId %s  openid  %s", userId,openid));
		Map<String,Object> map = new HashMap<>();
		map.put("wxOpenId", openid);
		authKeysDao.updateAuthKeys(userId,map);
		return jsonObject;
	}

	public String getWxopenid(int userId) {
		Object openId = authKeysDao.queryOneFieldById("wxOpenId", userId);
		if(null==openId) {
            return null;
        } else {
            return String.valueOf(openId);
        }
	}

	public void bindAliUserId(int userId,String aliUserId){
		if(StringUtil.isEmpty(aliUserId)){
			return ;
		}
		Map<String,Object> map = new HashMap<>();
		map.put("aliUserId", aliUserId);
		authKeysDao.updateAuthKeys(userId,map);
	}

	public String getAliUserId(int userId) {
		Object openId = authKeysDao.queryOneFieldById("aliUserId", userId);
		if(null==openId) {
            return null;
        } else {
            return String.valueOf(openId);
        }
	}

	@Override
	public void deleteAuthKeys(int userId){
		authKeysDao.deleteAuthKeys(userId);
	}
	
	public void sendUpdatePublicKeyMsgToFriends(String dhPublicKey,String rsaPublicKey, int userId){
		 friendsManager.sendUpdatePublicKeyMsgToFriends(dhPublicKey,rsaPublicKey,userId);
	}
	@Override
	public void save(AuthKeys authKeys) {
		authKeysDao.save(authKeys);
	}

	@Override
	public void update(int userId,Map<String,Object> map) {
		authKeysDao.updateAuthKeys(userId,map);
		userRedisService.deleteAuthKeys(userId);
	}

	@Override
	public List<Integer>  queryIsRsaAccountUserIdList() {
		return authKeysDao.queryIsRsaAccountUserIdList();
	}


}
