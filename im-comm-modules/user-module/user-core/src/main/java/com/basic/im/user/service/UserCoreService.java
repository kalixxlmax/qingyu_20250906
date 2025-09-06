package com.basic.im.user.service;

import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.support.Call;
import com.basic.im.user.dao.UserCoreDao;
import com.basic.im.user.entity.User;
import com.basic.im.user.entity.UserMoneyLog;
import org.bson.Document;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public interface UserCoreService {

	UserCoreDao getUserDao();

	User createUser(String telephone, String password);

	void createUser(User user);

	User.UserSettings getSettings(int userId);

	User getUser(int userId);

	User getUserByDB(int userId);

	User getUser(int userId, int toUserId);

	User getUser(String telephone);

	User getUserByDB(String telephone);

	String queryUserAccount(Integer userId);

	void updateUserType(Integer userId, int type);

	User getUserByPhone(String telephone);

	String getNickName(int userId);

	Double getUserMoenyV1(Integer userId);

	Double rechargeUserMoenyV1(UserMoneyLog userMoneyLog);

	Double rechargeUserMoenyV1(UserMoneyLog userMoneyLog, Call<Double> callback);

	/**
	 * 余额交易支出金额 加分布式锁
	 * @param userId 用户ID
	 * @param money 支出金额
	 * @param callback 加锁成功执行逻辑
	 */
	Object payMoenyBalanceOnLock(Integer userId, double money, Call callback)throws Exception;

	int getUserId(String accessToken);

	boolean isRegister(String telephone);


	void updateSignStartDate(int userId, long currentTimeSeconds);



	List<Document> findUser(int pageIndex, int pageSize);

	List<Integer> getAllUserId();


	void multipointLoginDataSync(Integer userId, String nickName, String operationType);

	User.LoginLog getLogin(int userId);

	Integer createInviteCodeNo(int createNum);

	int getOnlinestateByUserId(Integer key);

	Integer createUserId();

	boolean isOpenMultipleDevices(int userId);

    User updateSettings(int userId, User.UserSettings userSettings);

	void updateNickName(int userId, String serviceName);

    void delReport(Integer userId, String roomId);

    void rechargeUserMoeny(UserMoneyLog userMoneyLog);

    void updatePassowrd(int userId, String password);

	void updateTalkTime(Integer userId, long muteEndTime);

    int getMsgNum(int toUserId);

	void changeMsgNum(int toUserId, int msgNum);

	int decrementAndGet(int userId);

	int incrementAddMsgNum(int userId, int num);

	/**
	 * 统计今天注册用户数量
	 * @return
	 */
	long queryTodayRegisterUserCount();

	List<String> getDefFriendTelephoneList();

	List<User> queryDefFriendUserList();

	List<Integer> queryDefFriendUserIdList();

	List<Integer> queryFilterCircleUserIds(Integer userId);

    List<User> findUserByIds(List<Integer> userIds);

    List<User> findUserByIds(List<Integer> userIds,Integer pageIndex,Integer pageSize);

	List<User> findUserByNotInIds(List<Integer> userIds, int pageIndex, int pageSize);

	boolean  queryUserOnline(int userId);


	/**
	 * 包装用户信息，去除不需要的内容
	 */
	default User wrapUser(User user){
		user.setMyInviteCode(null);
		user.setPayPassword(null);
		user.setWalletUserNo(0);
		user.setDhMsgPublicKey(null);
		user.setDhMsgPrivateKey(null);
		user.setRsaMsgPublicKey(null);
		user.setRsaMsgPrivateKey(null);
		user.buildNoSelfUserVo(KConstants.ZERO);
		return user;
	}

}
