package com.basic.im.user.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.user.entity.User;
import com.basic.im.user.model.KSession;
import com.basic.im.utils.SKBeanUtils;
import com.basic.redisson.AbstractRedisson;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service(value = "userCoreRedisRepository")
public class UserCoreRedisRepository extends AbstractRedisson {

    @Autowired(required = false)
    private RedissonClient redissonClient;

    @Override
    public RedissonClient getRedissonClient() {
        return redissonClient;
    }


    /**
     * 用户名
     */
    public static final String STATIC_NICKNAME = "static:nickname:%s";

    public static final String GET_USER_BY_ACCOUNT = "user:account:%s";


    /**
     * 用户在线状态
     */
    public static final String USER_ONLINE_BITMAP = "user_online_bitmap";


    /**
     * 用户设备在线状态
     */
    public static final String USER_ONLINE_DEVICE_BITMAP = "user_online_device_bitmap:%s";

    /**
     * 用户的收藏列表
     */
    public static final String USER_COLLECT_COMMON = "user_collect:common:%s";

    /**
     * 用户的自定义表情列表
     */
    public static final String USER_COLLECT_EMOTICON = "user_collect:emoticon:%s";


    public static final String AUTH_KEY = "authKey:%s";

    /**
     * 用户随机码 key
     */
    public static final String USER_RANDOM_STR_KEY = "userRandomStr:%s";


    /**
     * 根据用户Id获取access_token
     */
    //public static final String GET_USERID_BYTOKEN = "at_%1$s";
    public static final String GET_USERID_BYTOKEN = "loginToken:userId:%s";

    //public static final String GET_ACCESS_TOKEN_BY_USER_ID ="uk_%1$s";
    public static final String GET_ACCESS_TOKEN_BY_USER_ID = "loginToken:token:%s";

    public static final String GET_ACCESS_TOKEN_BY_USERDEVICEID = "loginToken:token:%s:%s";


    public static final String GET_USER_BY_USERID = "user:%s:data";


    //apns推送
    public static final String GET_APNS_KEY = "apns:%s:token";

    /**
     * ios 推送
     */
    public static final String GET_PUSH_IOS_KEY = "impush:ios:%s";

    /**
     * android 推送
     */
    public static final String GET_PUSH_Android_KEY = "impush:andoird:%s";

    /**
     * ios 推送角标数量
     */
    public static final String GET_USER_MsgNum = "userMsg_iosNum:%s";


    /**
     * User
     *
     * @param userId
     * @return
     */
    public User getUserByUserId(Integer userId) {
        //字符串格式化
        String key = String.format(GET_USER_BY_USERID, userId);
        //去redis查是否有该数据
        RBucket<String> bucket = redissonClient.getBucket(key);
        if (bucket != null) {
            try {
                return JSONObject.parseObject(bucket.get(), User.class);
            } catch (JSONException e) {
                log.error(e.getMessage(), e);
                return null;
            }
        }
        return null;
    }

    public void saveUserByUserId(Integer userId, User user) {
        String key = String.format(GET_USER_BY_USERID, userId);
        setBucket(key, user.toString(), KConstants.Expire.DAY1);

    }

    public void deleteUserByUserId(Integer userId) {
        String key = String.format(GET_USER_BY_USERID, userId);
        deleteBucket(key);
    }


    /*
     * 缓存用户在线状态
     */
    public void saveUserNickName(String userId, String nickName) {
        String key = String.format(STATIC_NICKNAME, userId);
        RBucket<String> bucket = redissonClient.getBucket(key);
        if (!StringUtil.isEmpty(nickName)) {
            bucket.set(nickName, KConstants.Expire.DAY1, TimeUnit.SECONDS);
        }
        bucket.deleteAsync();
    }

    public String queryUserNickName(Integer userId) {
        String key = String.format(STATIC_NICKNAME, userId);
        RBucket<String> bucket = redissonClient.getBucket(key);
        return bucket.get();
    }

    /*
     * 通讯好 查询用户
     */
    public User queryUserByAccount(String account) {
        String key = String.format(GET_USER_BY_ACCOUNT, account);
        RBucket<User> bucket = redissonClient.getBucket(key);
        return bucket.get();

    }

    public void saveUserByAccount(String account, User user) {
        String key = String.format(GET_USER_BY_ACCOUNT, account);
        RBucket<User> bucket = redissonClient.getBucket(key);
        bucket.setAsync(user, KConstants.Expire.HOUR12, TimeUnit.SECONDS);

    }

    public void deleteUserByAccount(String account) {
        String key = String.format(GET_USER_BY_ACCOUNT, account);
        RBucket<User> bucket = redissonClient.getBucket(key);
        bucket.deleteAsync();
    }

    /*
     * 缓存用户在线状态
     */
    public void saveUserOnline(long userId, String resource, boolean status) {
        String key = String.format(USER_ONLINE_DEVICE_BITMAP,userId);
        RBitSet deviceSet = redissonClient.getBitSet(key);

        deviceSet.set(getDeviceIndex(resource),status);
        deviceSet.expireAsync(KConstants.Expire.DAY7,TimeUnit.SECONDS);
        
        userId = KConstants.offsetUserIdToBitMapIndex(userId);
        RBitSet bitSet = redissonClient.getBitSet(USER_ONLINE_BITMAP);
        bitSet.set(userId, status);



    }

    private int getDeviceIndex(String resource){
        switch (resource){
            case KConstants.DeviceKey.Android:
                return KConstants.ZERO;
            case KConstants.DeviceKey.IOS:
                return KConstants.ONE;
            default:
                return 3;
        }
    }

    public boolean queryUserOnline(long userId) {

        userId = KConstants.offsetUserIdToBitMapIndex(userId);
        RBitSet bitSet = redissonClient.getBitSet(USER_ONLINE_BITMAP);
        return bitSet.get(userId);
    }

    /**
     * 查询需要推送的设备是否离线
     * @param userId
     * @return
     */
    public boolean queryPushDeviceOffLine(long userId) {
        String key = String.format(USER_ONLINE_DEVICE_BITMAP,userId);
        RBitSet deviceSet = redissonClient.getBitSet(key);
        return !deviceSet.get(KConstants.ZERO)||!deviceSet.get(KConstants.ONE);
    }
    /**
     * 查询需要推送的设备都在线
     * @param userId
     * @return
     */
    public boolean queryPushDeviceAllOnline(long userId) {
        String key = String.format(USER_ONLINE_DEVICE_BITMAP,userId);
        RBitSet deviceSet = redissonClient.getBitSet(key);
        return deviceSet.get(KConstants.ZERO)&&deviceSet.get(KConstants.ONE);
    }

    /**
     * 查询指定设备是否在线
     * @param userId
     * @param resource
     * @return
     */
    public boolean queryUserOnline(long userId,String resource) {
        String key = String.format(USER_ONLINE_DEVICE_BITMAP,userId);
        RBitSet deviceSet = redissonClient.getBitSet(key);
        return deviceSet.get(getDeviceIndex(resource));
    }

    /**
     * 查询在线用户总数
     *
     * @return
     */
    public long queryAllOnlineCount() {
        String key = String.format(USER_ONLINE_BITMAP);
        RBitSet bitSet = redissonClient.getBitSet(key);
        return bitSet.cardinality();
    }


    /**
     * 设置所有用户为离线状态
     */
    public void clearOnlineUserBitMap() {
        RBitSet onlineUserBitMap = redissonClient.getBitSet(USER_ONLINE_BITMAP);
        onlineUserBitMap.clear();
    }

    /**
     * 保存用戶随机码
     */
    public void saveUserRandomStr(int userId, String userRandomStr) {
        RBucket<String> bucket = redissonClient.getBucket(String.format(USER_RANDOM_STR_KEY, userId));
        bucket.set(userRandomStr, KConstants.Expire.HOUR, TimeUnit.SECONDS); //有效期一小时
    }

    /**
     * 获取用户随机码
     */
    public String getUserRandomStr(int userId) {
        RBucket<String> bucket = redissonClient.getBucket(String.format(USER_RANDOM_STR_KEY, userId));
        return bucket.get();
    }

    /**
     * 删除用户随机码
     */
    public boolean deleteUserRandomStr(int userId) {
        RBucket<String> bucket = redissonClient.getBucket(String.format(USER_RANDOM_STR_KEY, userId));
        return bucket.delete();
    }


    public static final String AUTHKEYS_KEY = "authkeys:%s";


    public void savaAuthKey(String authKey, Map<String, Object> mapResultStatus) {
        String key = String.format(AUTH_KEY, authKey);
        RBucket<Object> rbBucket = redissonClient.getBucket(key);
        rbBucket.set(mapResultStatus, 5, TimeUnit.MINUTES);
    }


    public static final String LOGINCODES_KEY = "LOGINCODES:%s";

    /**
     * @param userId
     * @param code
     */
    public void saveLoginCode(int userId, String deviceId, String code) {
        String key = String.format(LOGINCODES_KEY, userId, deviceId);
        RBucket<Object> rbucket = redissonClient.getBucket(key);
        rbucket.set(code, KConstants.Expire.MINUTE, TimeUnit.SECONDS);
    }

    /**
     * @param userId
     * @param deviceId
     * @return
     */
    public String queryLoginSignCode(int userId, String deviceId) {
        String key = String.format(LOGINCODES_KEY, userId, deviceId);
        RBucket<String> bucket = redissonClient.getBucket(key);
        return bucket.get();
    }

    public boolean cleanLoginCode(int userId, String deviceId) {
        String key = String.format(LOGINCODES_KEY, userId, deviceId);
        RBucket<String> bucket = redissonClient.getBucket(key);
        return bucket.delete();
    }

    /**
     *
     */
    public static final String GET_LOGIN_TOKEN_KEY = "login:loginToken:%s:%s";

    public static final String LOGIN_TOKEN_KEY = "login:loginTokenKeys:%s";


    /**
     * 根据 userId 和设备号保存  登陆token
     *
     * @param userId
     * @param deviceId
     * @param loginToken 登陆token  用于自动登陆使用
     */
    private void saveLoginToken(int userId, String deviceId, String loginToken) {
        String key = String.format(GET_LOGIN_TOKEN_KEY, userId, deviceId);
        RBucket<String> bucket = redissonClient.getBucket(key);
        String oldToken = bucket.get();
        /**
         * 上次登陆的  信息 需要清空
         */
        if (!StringUtil.isEmpty(oldToken)) {
            cleanLoginTokenKeys(oldToken);
        }

		/*UserLoginTokenKey oldLoginToken = bucket.get();
		if(null!=oldLoginToken){
			cleanLoginTokenKeys(oldLoginToken.getLoginToken());
		}*/
        bucket.set(loginToken, KConstants.Expire.DAY7 * 7, TimeUnit.SECONDS);
    }

    public String queryLoginToken(int userId, String deviceId) {
        String key = String.format(GET_LOGIN_TOKEN_KEY, userId, deviceId);
        RBucket<String> bucket = redissonClient.getBucket(key);
        return bucket.get();
    }

    public boolean cleanLoginToken(int userId, String deviceId) {
        String key = String.format(GET_LOGIN_TOKEN_KEY, userId, deviceId);
        RBucket<String> bucket = redissonClient.getBucket(key);
        cleanLoginTokenKeys(bucket.get());
        return bucket.delete();
    }

    public boolean cleanLoginTokenKeys(String loginToken) {
        if (StringUtil.isEmpty(loginToken)) {
            return true;
        }
        String key = String.format(LOGIN_TOKEN_KEY, loginToken);
        RBucket bucket = redissonClient.getBucket(key);
        return bucket.delete();
    }

    public static final String GET_SESSON_KEY = "sesson:%s";

    public void saveUserSesson(KSession session) {
        String key = String.format(GET_SESSON_KEY, session.getAccessToken());
        RBucket<KSession> bucket = redissonClient.getBucket(key);
        bucket.set(session, KConstants.Expire.DAY1 * 30, TimeUnit.SECONDS);
        saveMessageKey(session.getUserId(), session.getDeviceId(), session.getMessageKey());
    }

    public KSession queryUserSesson(String accessToken) {
        String key = String.format(GET_SESSON_KEY, accessToken);
        try{
            RBucket<KSession> bucket = redissonClient.getBucket(key);
            return bucket.get();
        }catch (Exception e){
            log.error("error is {},key is {}",e.getMessage(),key);
            return null;
        }
    }

    public boolean cleanUserSesson(String accessToken) {
        KSession session = queryUserSesson(accessToken);
        if (null != session) {
            cleanMessageKey(session.getUserId(), session.getDeviceId());
            removeAccessTokenByDeviceId(session.getUserId(), session.getDeviceId());
        }
        return deleteBucket(GET_SESSON_KEY, accessToken);
    }

    public Map<String, Object> loginSaveAccessToken(Object userKey, Object userId, String accessToken) {
        HashMap<String, Object> data = new HashMap<String, Object>();
        try {

            int expire = KConstants.Expire.DAY7 * 5;
            String atKey = String.format(GET_ACCESS_TOKEN_BY_USER_ID, userKey);
            if (com.basic.utils.StringUtil.isEmpty(accessToken)) {
                accessToken = SKBeanUtils.getRedisCRUD().get(atKey);
            }
            if (com.basic.utils.StringUtil.isEmpty(accessToken)) {
                accessToken = com.basic.utils.StringUtil.randomUUID();
            }
            SKBeanUtils.getRedisCRUD().setWithExpireTime(atKey, accessToken, expire);

            String userIdKey = String.format(GET_USERID_BYTOKEN, accessToken);
            SKBeanUtils.getRedisCRUD().setWithExpireTime(userIdKey, String.valueOf(userId), expire);

            data.put("access_token", accessToken);
            data.put("expires_in", expire);


            return data;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return data;
        }
    }

    public void loginSaveAccessTokenByDeviceId(int userId, String deviceId, String accessToken) {
        try {

            int expire = KConstants.Expire.DAY1 * 30;
            String atKey = String.format(GET_ACCESS_TOKEN_BY_USERDEVICEID, userId, deviceId);

            if (com.basic.utils.StringUtil.isEmpty(accessToken)) {
                accessToken = com.basic.utils.StringUtil.randomUUID();
            }
            String oldToken = SKBeanUtils.getRedisCRUD().get(atKey);
            /**
             * 清除 旧的 token
             */
            if (!com.basic.utils.StringUtil.isEmpty(oldToken)) {
                SKBeanUtils.getRedisCRUD().del(String.format(GET_USERID_BYTOKEN, oldToken));
                cleanUserSesson(oldToken);
            }
            SKBeanUtils.getRedisCRUD().setWithExpireTime(atKey, accessToken, expire);
            String userIdKey = String.format(GET_USERID_BYTOKEN, accessToken);


            SKBeanUtils.getRedisCRUD().setWithExpireTime(userIdKey, String.valueOf(userId), expire);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 根据userId+deviceId获取token
     */
    public String getTokenByUserIdAndDeviceId(int userId, String deviceId) {
        String atKey = String.format(GET_ACCESS_TOKEN_BY_USERDEVICEID, userId, deviceId);
        RBucket<String> bucket = redissonClient.getBucket(atKey);
        return bucket.get();
    }


    public void removeAccessTokenByDeviceId(int userId, String deviceId) {
        // 根据userKey拿token
        String key = String.format(GET_ACCESS_TOKEN_BY_USERDEVICEID, userId, deviceId);
        String access_token = getBucket(String.class, key);


        if (!StringUtil.isEmpty(access_token)) {
            deleteBucket(key);
        }
        if (!StringUtil.isEmpty(access_token)) {
            String userIdKey = String.format(GET_USERID_BYTOKEN, access_token);
            deleteBucket(userIdKey);
        }
    }

    public static final String GET_MESSAGEKEY_KEY = "messageKey:%s:%s";

    public void saveMessageKey(int userId, String deviceId, String messageKey) {
        String key = buildRedisKey(GET_MESSAGEKEY_KEY, userId, deviceId);
        setBucket(key, messageKey, KConstants.Expire.DAY1 * 30);
    }

    public void cleanMessageKey(int userId, String deviceId) {
        deleteBucket(GET_MESSAGEKEY_KEY, userId, deviceId);
    }


    /**
     * 通过authKey获取数据
     *
     * @param authKey 钥匙
     * @return
     */
    public Object queryAuthKey(String authKey) {
        String key = String.format(AUTH_KEY, authKey);
        RBucket<Object> bucket = redissonClient.getBucket(key);
        return bucket.get();
    }


    /**
     * @param @param userId
     * @param @param info    参数
     * @Description: TODO(保存 ios 设备推送 的 信息)
     */
    public void saveIosPushToken(Integer userId, User.DeviceInfo info) {
        String key = String.format(GET_PUSH_IOS_KEY, userId);
        setBucket(key, JSONObject.toJSONString(info), KConstants.Expire.DAY7 * 5);
    }

    /**
     * @param @param  userId
     * @param @return 参数
     * @Description: TODO(获取 ios 设备推送 的 信息)
     */
    public User.DeviceInfo getIosPushToken(Integer userId) {
        String key = String.format(GET_PUSH_IOS_KEY, userId);
        String value = getBucket(String.class, key);
        return StringUtil.isEmpty(value) ? null : JSON.parseObject(value, User.DeviceInfo.class);

    }

    public void removeIosPushToken(Integer userId) {
        String key = String.format(GET_PUSH_IOS_KEY, userId);
        deleteBucket(key);
    }

    /**
     * @param @param userId
     * @param @param info    参数
     * @Description: TODO(保存 android 设备推送 的 信息)
     */
    public void saveAndroidPushToken(Integer userId, User.DeviceInfo info) {
        String key = String.format(GET_PUSH_Android_KEY, userId);
        setBucket(key, JSONObject.toJSONString(info), KConstants.Expire.DAY7);
    }

    /**
     * @param @param  userId
     * @param @return 参数
     * @Description: TODO(获取 android 设备推送 的 信息)
     */
    public User.DeviceInfo getAndroidPushToken(Integer userId) {
        String key = String.format(GET_PUSH_Android_KEY, userId);
        String value = getBucket(String.class, key);
        return StringUtil.isEmpty(value) ? null : JSON.parseObject(value, User.DeviceInfo.class);

    }

    public void removeAndroidPushToken(Integer userId) {
        String key = String.format(GET_PUSH_Android_KEY, userId);
        deleteBucket(key);
    }


    public void saveAPNSToken(String regId, Integer userId) {
        String key = String.format(GET_APNS_KEY, userId);
        setBucket(key, regId, KConstants.Expire.DAY7);
    }

    public String getAPNSToken(Integer userId) {
        String key = String.format(GET_APNS_KEY, userId);
        return getBucket(String.class, key);
    }


    public boolean existOtherPushToken(Integer userId, String source){
        if(KConstants.DeviceKey.IOS.equals(source)){
            return null!=getAndroidPushToken(userId);
        }
        if(KConstants.DeviceKey.IOS.equals(source)){
            return null!=getIosPushToken(userId);
        }
        return false;
    }

    public  void changeMsgNum(Integer userId, Integer num) {
        String key = String.format(GET_USER_MsgNum, userId);

        RAtomicLong msgNum = getAtomicLong(key);
        msgNum.set(num);
        msgNum.expireAsync(KConstants.Expire.DAY7 * 3,TimeUnit.SECONDS);
    }

    public  long getUserMsgNum(Integer userId) {
        String key = String.format(GET_USER_MsgNum, userId);
        RAtomicLong msgNum = getAtomicLong(key);
        //msgNum.expireAsync(KConstants.Expire.DAY7 * 3,TimeUnit.SECONDS);
        return msgNum.get();
    }

    public long incrementAddGetMsgNum(int userId, int num) {
        String key = String.format(GET_USER_MsgNum, userId);
        RAtomicLong msgNum = getAtomicLong(key);
        msgNum.expireAsync(KConstants.Expire.DAY7 * 3,TimeUnit.SECONDS);
        return msgNum.addAndGet(num);
    }

    public long decrementAndGet(int userId) {
        String key = String.format(GET_USER_MsgNum, userId);
        RAtomicLong msgNum = getAtomicLong(key);
        msgNum.expireAsync(KConstants.Expire.DAY7 * 3,TimeUnit.SECONDS);
        return msgNum.decrementAndGet();
    }






    /**
     * 除了系统号的userId列表
     */
    public static final String NOSYSTENNUM_USERIDS = "nosystemnum:userIds";


    /**
     * 除了系统号的用户Id列表
     *
     * @param userIds
     */
    public void saveNoSystemNumUserIds(List<Integer> userIds) {
        RList<Object> list = redissonClient.getList(NOSYSTENNUM_USERIDS);
        list.clear();
        list.addAll(userIds);
        list.expire(KConstants.Expire.DAY7, TimeUnit.SECONDS);
    }

    /**
     * 获取除了系统号的userIds
     *
     * @return
     */
    public List<Integer> getNoSystemNumUserIds() {
        RList<Integer> list = redissonClient.getList(NOSYSTENNUM_USERIDS);
        return list.readAll();
    }

    public void deleteNoSystemNumUserIds() {
        redissonClient.getBucket(NOSYSTENNUM_USERIDS).delete();
    }


    public String getUserIdBytoken(String token) {
        String key = String.format(GET_USERID_BYTOKEN, token);
        return getBucket(String.class, key);
    }

    final String ONLINE_USER_KEY = "imuser:online_user";

    public long getAllOnlineUserCount() {
        long allCount = 0;

        try {

            RMap<String, Long> map = redissonClient.getMap(ONLINE_USER_KEY);
            if (map.isEmpty()) {
                return allCount;
            }
            Collection<Long> values = map.readAllValues();
            for (Long i : values) {
                allCount = allCount + i;
            }


            return allCount;
        } catch (Exception e) {
            return allCount;
        }
    }


}
