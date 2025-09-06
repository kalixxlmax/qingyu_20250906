package com.basic.im.user.service.impl;

import com.basic.commons.thread.ThreadUtils;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.ex.ServiceException;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.config.AppConfig;
import com.basic.im.message.MessageService;
import com.basic.im.message.MessageType;
import com.basic.im.support.Call;
import com.basic.im.user.constants.MoneyLogConstants.MoenyAddEnum;
import com.basic.im.user.constants.MoneyLogConstants.MoneyLogEnum;
import com.basic.im.user.constants.MoneyLogConstants.MoneyLogTypeEnum;
import com.basic.im.user.dao.MoneyLogDao;
import com.basic.im.user.dao.OfflineOperationDao;
import com.basic.im.user.dao.ReportDao;
import com.basic.im.user.dao.UserCoreDao;
import com.basic.im.user.entity.User;
import com.basic.im.user.entity.UserMoneyLog;
import com.basic.im.user.service.UserCoreRedisRepository;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.user.utils.MoneyUtils;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.Md5Util;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.Binary;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service(UserCoreServiceImpl.BEAN_ID)
public class UserCoreServiceImpl implements UserCoreService {

    public static final String BEAN_ID = "UserCoreServiceImpl";

    @Autowired(required = false)
    private UserCoreDao userCoreDao;

    public UserCoreDao getUserDao() {
        return userCoreDao;
    }

    @Autowired(required = false)
    private ReportDao reportDao;

    @Autowired
    private UserCoreRedisRepository userCoreRedisRepository;

    @Autowired
    @Lazy
    private MessageService messageService;

    @Autowired(required = false)
    private OfflineOperationDao offlineOperationDao;

	/*@Autowired(required =false)
	private AuthKeysService authKeysService;*/

    @Autowired
    private AppConfig appConfig;

    @Autowired
    private MoneyLogDao moneyLogDao;

    /**
     * 获取userID的 分布式锁 Key
     */
    private static final String userIdLockKey = "userIdLock";

    /**
     * increaseUserId name
     */
    private static final String INCREASE_USERID = "increaseUserId";


    /**
     * 余额改变 分布式锁
     */
    private static final String LOCK_BALANCE_CHANGE = "lock:balance_change:%s";

    /**
     * 余额交易 分布式锁
     */
    private static final String LOCK_BALANCE_TRANSACTION = "lock:balance_trans:%s";

    private final Logger moneyLogger = LoggerFactory.getLogger("moneyLogger");

    @Override
    public User createUser(String telephone, String password) {
        User user = new User();
        user.setUserId(createUserId());
        user.setUserKey(Md5Util.md5Hex(telephone));
        user.setPassword(Md5Util.md5Hex(password));
        user.setTelephone(telephone);

        getUserDao().addUser(user);

        return user;
    }

    @Override
    public void createUser(User user) {
        getUserDao().addUser(user);

    }

    @Override
    public User.UserSettings getSettings(int userId) {
        User.UserSettings settings;
        User user;
        user = getUser(userId);
        if (null == user) {
            return new User.UserSettings();
        }
        settings = user.getSettings();
        return null != settings ? settings : new User.UserSettings();
    }

    //不经过Redis 直接从数据库获取数据
    public User getUserFromDB(int userId) {
        //先从 Redis 缓存中获取
        User user = getUserDao().getUser(userId);
        if (null == user) {
            System.out.println("id为" + userId + "的用户不存在");
            return null;
        }

        return user;
    }

    @Override
    public User getUser(int userId) {
        //先从 Redis 缓存中获取
        User user = userCoreRedisRepository.getUserByUserId(userId);
        if (null == user) {
            user = getUserDao().getUser(userId);
            if (null == user) {
                log.info("该用户不存在, userId: {}", userId);
                return null;
            }
            userCoreRedisRepository.saveUserByUserId(userId, user);
        }
        return user;
    }

    @Override
    public User getUserByDB(int userId) {
        return getUserDao().getUser(userId);
    }

    public boolean getUserByAccount(String account, Integer userId) {
        return null == getUserDao().getUserByAccount(account, userId);
    }

    @Override
    public String queryUserAccount(Integer userId) {
        return userCoreDao.queryUserAccount(userId);
    }

    public User getUserByAccount(String account) {
        //先从 Redis 缓存中获取
        User user = userCoreRedisRepository.queryUserByAccount(account);
        if (null == user) {
            user = getUserDao().queryOne("account", account);
            if (null == user) {
                log.info("该用户不存在, account: {}", account);
                return null;
            }
            userCoreRedisRepository.saveUserByAccount(account, user);
        }

        return user;
    }


    /* (non-Javadoc)
     * @see cn.xyz.mianshi.service.UserManager#getNickName(int)
     */
    @Override
    public String getNickName(int userId) {
        String nickName = userCoreRedisRepository.queryUserNickName(userId);
        if (!StringUtil.isEmpty(nickName)) {
            return nickName;
        }
        return (String) getUserDao().getOneFieldById("nickname", userId);
    }

    @Override
    public  int getMsgNum(int userId) {
        return (int) userCoreRedisRepository.getUserMsgNum(userId);
    }

    @Override
    public  void changeMsgNum(int userId, int num) {
        userCoreRedisRepository.changeMsgNum(userId, num);
    }

    @Override
    public  int incrementAddMsgNum(int userId, int num) {
       return (int) userCoreRedisRepository.incrementAddGetMsgNum(userId, num);
    }
    @Override
    public int decrementAndGet(int userId){
        return (int)userCoreRedisRepository.decrementAndGet(userId);
    }



    @Override
    public User getUser(int userId, int toUserId) {
        return getUser(toUserId);

    }


    @Override
    public User getUser(String telephone) {
        User user = getUserDao().getUser(telephone);
        if (null == user) {
            throw new ServiceException(KConstants.ResultCode.UserNotExist);
        }
        return user;
    }

    @Override
    public User getUserByDB(String telephone) {
        User user = getUserDao().getUser(telephone);
        return user;
    }

    @Override
    public void rechargeUserMoeny(UserMoneyLog userMoneyLog) {
        rechargeUserMoenyV1(userMoneyLog);
    }

    @Override
    public void updatePassowrd(int userId, String password) {
        getUserDao().updatePassowrd(userId, password);
    }

    @Override
    public void updateTalkTime(Integer userId, long muteEndTime) {
        getUserDao().updateTalkTime(userId, muteEndTime);
    }

    @Override
    public Double getUserMoenyV1(Integer userId) {
        try {
            Object field = getUserDao().getOneFieldById("balanceSafe", userId);
            byte[] balanceSafe = null;
            if (null != field) {
                balanceSafe = ((Binary) field).getData();
            } else {
                if (0 == appConfig.getBalanceVersion()) {
                    Object oldBalance = getUserDao().getOneFieldById("balance", userId);
                    if (null == oldBalance) {
                        return 0.0;
                    }
                    balanceSafe = MoneyUtils.encrypt(Double.valueOf(oldBalance.toString()), userId.toString());

                } else {
                    balanceSafe = MoneyUtils.encrypt(Double.valueOf(0.0), userId.toString());
                }
            }

            return MoneyUtils.decrypt(balanceSafe, userId.toString());
        } catch (Exception e) {
            throw new ServiceException(104004);
        }
    }

    @Override
    public Double rechargeUserMoenyV1(UserMoneyLog userMoneyLog) {

        RLock lock = userCoreRedisRepository.getLock(LOCK_BALANCE_CHANGE, userMoneyLog.getUserId() + "");
        boolean lockResult = false;
        try {
            lockResult = lock.tryLock(3, 30, TimeUnit.SECONDS);

            if (lockResult) {
                try {
                    return rechargeUserMoenyOnLock(userMoneyLog);
                } catch (Exception e) {
                    throw e;
                } finally {
                    lock.unlock();
                }

            } else {
                throw new ServiceException(KConstants.ResultCode.SystemIsBusy);
            }
        } catch (InterruptedException e) {
            throw new ServiceException(KConstants.ResultCode.SystemIsBusy);
        }

    }

    @Override
    public Double rechargeUserMoenyV1(UserMoneyLog userMoneyLog, Call<Double> callback) {
        try {
            double balance = rechargeUserMoenyV1(userMoneyLog);
            callback.execute(balance);
            return balance;
        } catch (Exception e) {
            throw e;
        }
    }

    private double rechargeUserMoenyOnLock(UserMoneyLog userMoneyLog) {
//        if (null != userMoneyLog && userMoneyLog.getMoeny() < 0) {
//            throw new ServiceException(KConstants.ResultCode.AUTH_FAILED);
//        }
        double balance = getUserMoenyV1(userMoneyLog.getUserId());
        if (moneyLogDao.isExistMoneyLogProcessed(userMoneyLog)) {
            /**
             * 通业务操作的余额记录已经操作过 重复操作
             */
            moneyLogger.info("{} [余额记录重复操作 取消操作] {}  {} {} 金额 {}",
                    userMoneyLog.getUserId(),
                    MoneyLogEnum.getMoneyLogDesc(userMoneyLog.getBusinessType()),
                    MoneyLogTypeEnum.getLogTypeDesc(userMoneyLog.getLogType()),
                    MoenyAddEnum.getAddTypeDesc(userMoneyLog.getChangeType()),
                    userMoneyLog.getMoeny()
            );
            throw new ServiceException(104009);
        }

        userMoneyLog.setBeforeMoeny(balance);
        if (MoenyAddEnum.MOENY_ADD.getType() == userMoneyLog.getChangeType()) {
            balance = StringUtil.addDouble(balance, userMoneyLog.getMoeny());
            //ops.set("totalRecharge", StringUtil.addDouble(user.getTotalRecharge(), money));
        } else if (MoenyAddEnum.MOENY_REDUCE.getType() == userMoneyLog.getChangeType()){

            balance = StringUtil.subDouble(balance, userMoneyLog.getMoeny());
            if (balance < 0) {
                throw new ServiceException(104001);
            }

            //ops.set("totalConsume", StringUtil.addDouble(user.getTotalConsume(), money));
        }
        getUserDao().updateUserBalanceSafe(userMoneyLog.getUserId(), balance);
        userMoneyLog.setEndMoeny(balance);

        userMoneyLog.createDescription();

        moneyLogDao.saveMoneyLog(userMoneyLog);

        moneyLogger.info("{} [余额记录成功]-> to {} {}  {} {}  {}  当前余额 {} 备注: {}",
                userMoneyLog.getUserId(),
                userMoneyLog.getToUserId(),
                MoneyLogEnum.getMoneyLogDesc(userMoneyLog.getBusinessType()),
                MoneyLogTypeEnum.getLogTypeDesc(userMoneyLog.getLogType()),
                MoenyAddEnum.getAddTypeDesc(userMoneyLog.getChangeType()),
                userMoneyLog.getMoeny(),
                userMoneyLog.getEndMoeny(),
                null == userMoneyLog.getExtra() ? "暂无" : userMoneyLog.getExtra()

        );
        return balance;
    }

    @Override
    public Object payMoenyBalanceOnLock(Integer userId, double money, Call callback) throws ServiceException {
        try {
            RLock lock = userCoreRedisRepository.getLock(LOCK_BALANCE_TRANSACTION, userId.toString());
            boolean lockResult = lock.tryLock(3, 30, TimeUnit.SECONDS);

            if (lockResult) {
                try {
                    if (getUserMoenyV1(userId) < money) {
                        //余额不足
                        throw new ServiceException(104001);
                    }
                    return callback.execute(userId);
                } finally {
                    lock.unlock();
                }

            } else {
                throw new ServiceException(KConstants.ResultCode.SystemIsBusy);
            }

        } catch (InterruptedException e) {
            throw new ServiceException(KConstants.ResultCode.SystemIsBusy);
        }
    }


    @Override
    public int getUserId(String accessToken) {
        return 0;
    }

    @Override
    public boolean isRegister(String telephone) {
        return 1 == getUserDao().getCount(telephone);
    }

    @Override
    public void updateSignStartDate(int userId, long currentTimeSeconds) {
        getUserDao().updateSignStartDate(userId, currentTimeSeconds);
    }


    private void updateLoc(double latitude, double longitude, Integer userId) {
        User.Loc loc = new User.Loc(longitude, latitude);
        getUserDao().updateLoc(userId, loc);
    }


    public void cleanPushToken(Integer userId, String devicekey) {

        try {
            if (KConstants.DeviceKey.Android.equals(devicekey)) {
                userCoreRedisRepository.removeAndroidPushToken(userId);
            } else if (KConstants.DeviceKey.IOS.equals(devicekey)) {
                userCoreRedisRepository.removeIosPushToken(userId);
            }
            getUserDao().updateDeviceMap(userId, devicekey);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public String queryPassword(int userId) {
        return (String) getUserDao().getOneFieldById("password", userId);
    }

    @Override
    public List<Document> findUser(int pageIndex, int pageSize) {
        return getUserDao().findUser(pageIndex, pageSize);
    }


    @Override
    public List<Integer> getAllUserId() {
        return getUserDao().getAllUserId();
    }

    @Override
    public int getOnlinestateByUserId(Integer userId) {
        boolean online = userCoreRedisRepository.queryUserOnline(userId);
        return online ? 1 : 0;
    }


    @Override
    public boolean  queryUserOnline(int userId){
        return userCoreRedisRepository.queryUserOnline(userId);
    }


    //获取用户Id
    @Override
    public Integer createUserId() {
        try {
            RLock lock = userCoreRedisRepository.getLock(userIdLockKey);
            boolean lockResult = lock.tryLock(3, 10, TimeUnit.SECONDS);
            if (lockResult) {
                RAtomicLong rAtomicLong = userCoreRedisRepository.getRedissonClient().getAtomicLong(INCREASE_USERID);
                Long userId = rAtomicLong.get();
                try {
                    if (userId == 0) {
                        userId = getUserDao().createUserId(null).longValue();
                        rAtomicLong.set(userId);
                    } else {
                        userId = rAtomicLong.incrementAndGet();
                        getUserDao().createUserId(userId.intValue());
                    }
                    return userId.intValue();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        throw new ServiceException(KConstants.ResultCode.SystemIsBusy);
    }

    //获取Call
    public synchronized Integer createCall() {
        return getUserDao().createCall();
    }

    //获取videoMeetingNo
    public synchronized Integer createvideoMeetingNo() {
        return getUserDao().createvideoMeetingNo();
    }





    @Override
    public User updateSettings(int userId, User.UserSettings userSettings) {
        User user = getUserDao().updateSettings(userId, userSettings);
        if (
                KConstants.LOSE != userSettings.getIsTyping()
                        || KConstants.LOSE != userSettings.getIsVibration()
                        || KConstants.LOSE != userSettings.getIsSkidRemoveHistoryMsg()
                        || KConstants.LOSE != userSettings.getIsShowMsgState()
                        || KConstants.LOSE != userSettings.getBeInvitedJoinRoom()
                        || KConstants.ZERO != userSettings.getChatSyncTimeLen()
                        || KConstants.LOSE != userSettings.getHiding()
        ) {
            multipointLoginDataSync(userId, user.getNickname(), KConstants.MultipointLogin.SYNC_PRIVATE_SETTINGS);
        }
        return user;
    }

    /**
     * 多点登录同步
     */
    @Override
    public void multipointLoginDataSync(Integer userId, String nickName, String operationType){
        ThreadUtils.executeInThread(obj -> {
            MessageBean messageBean=new MessageBean();
            messageBean.setType(MessageType.multipointLoginDataSync);
            String userName=nickName;
            if(StringUtil.isEmpty(userName)){
                userName=getNickName(userId);
            }
            messageBean.setFromUserId(String.valueOf(userId));
            messageBean.setFromUserName(userName);
            messageBean.setToUserId(String.valueOf(userId));
            messageBean.setToUserName(nickName);
            messageBean.setObjectId(operationType);
            messageBean.setMessageId(StringUtil.randomUUID());
            try {
                messageService.send(messageBean);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void delReport(Integer userId, String roomId) {
        reportDao.deleteReport(userId, roomId);
    }

    @Override
    public boolean isOpenMultipleDevices(int userId) {
        User user = getUser(userId);
        if (null != user) {
            if (null == user.getSettings()) {
                return false;
            }
            return 1 == user.getSettings().getMultipleDevices();
        }
        return false;
    }

    @Override
    public User.LoginLog getLogin(int userId) {
        return getUserDao().getLogin(userId);
    }

    @Override
    public Integer createInviteCodeNo(int createNum) {
        return getUserDao().createInviteCodeNo(createNum);
    }

    @Override
    public long queryTodayRegisterUserCount() {
        return getUserDao().queryTodayRegisterUserCount();

    }

    @Override
    public List<String> getDefFriendTelephoneList() {
        String telephones = SKBeanUtils.getSystemConfig().getDefaultTelephones();
        //log.info(" config defaultTelephones : " + telephones);

        if (StringUtil.isEmpty(telephones)) {
            return null;
        }
        String[] phones = StringUtil.getStringList(telephones);
        return Arrays.asList(phones);
    }

    @Override
    public List<User> queryDefFriendUserList() {
        List<String> phones = getDefFriendTelephoneList();

        if (null == phones || 0 == phones.size()) {
            return null;
        }
        List<User> userList = new ArrayList<>();
        for (int i = 0; i < phones.size(); i++) {
            User user = getUserDao().getUser(phones.get(i));
            if (null == user) {
                continue;
            }
            userList.add(user);

        }
        return userList;
    }

    @Override
    public List<Integer> queryDefFriendUserIdList() {
        List<String> telephoneList = getDefFriendTelephoneList();
        if (null == telephoneList || 0 == telephoneList.size()) {
            return null;
        }
        return getUserDao().queryDefFriendUserIdList(telephoneList);
    }

    @Override
    public List<Integer> queryFilterCircleUserIds(Integer userId) {
        return userCoreDao.queryFilterCircleUserIds(userId);
    }

    @Override
    public List<User> findUserByIds(List<Integer> userIds) {
        return getUserDao().findByIds(userIds);
    }

    @Override
    public List<User> findUserByIds(List<Integer> userIds, Integer pageIndex, Integer pageSize) {
        return getUserDao().findByIds(userIds,pageIndex,pageSize);
    }

    @Override
    public List<User> findUserByNotInIds(List<Integer> userIds, int pageIndex, int pageSize) {
        return getUserDao().findByNotInIds(userIds,pageIndex,pageSize);
    }

    @Override
    public void updateNickName(int userId, String serviceName) {
        userCoreDao.updateAttribute(userId,"nickname",serviceName);
        userCoreRedisRepository.deleteUserByUserId(userId);
    }

    @Override
    public void updateUserType(Integer userId, int type) {
        userCoreDao.updateUserType(userId,type);
        userCoreRedisRepository.deleteUserByUserId(userId);
    }

    @Override
    public User getUserByPhone(String telephone) {
        return userCoreDao.getUserByPhone(telephone);
    }

}
