package com.basic.im.user.entity;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.entity.Config;
import com.basic.im.user.model.UserExample;
import com.basic.im.utils.SKBeanUtils;
import com.basic.utils.DateUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
@Document(value = "user")
public class User {

	public static String getDBName(){
		return "user";
	}


	private @Id Integer userId;// 用户Id

	//@JSONField(serialize = false)

	@Indexed
	private String userKey;// 用户唯一标识

	private String secretKey;

	/**
	 * 通讯账号 唯一
	 */
	@Indexed
	private String account;

	/**
	 * authkeys Id
	 */
	private Long accountId;

	/**
	 * account 加密后的通讯账号
	 */
	private String encryAccount;

	/**
	 * 修改账号次数
	 */
	private int setAccountCount;


	@JSONField(serialize = false)
	private String username; //用户名

	//@JSONField(serialize = false)
	private String password;

	private String appId; //ios 需要判断的包名

	// 用户类型：0=普通用户；  2=公众号-个人号  4-企业号 5-客服；
	private Integer userType=0;

	//消息免打扰
	private Integer offlineNoPushMsg=0;//1为开启  0为关闭

	private String openid;// 微信openId

	private String aliUserId; // 支付宝用户Id

	/**
	 * dh 消息公钥
	 */
	private String dhMsgPublicKey;
	/**
	 * dh 消息私钥
	 */
	private String dhMsgPrivateKey;

	/**
	 * rsa 消息公钥
	 */
	private String rsaMsgPublicKey;
	/**
	 * rsa 消息私钥
	 */
	private String rsaMsgPrivateKey;

	@Indexed
	private String areaCode;

	@Indexed
	private String telephone;

	@Indexed
	private String phone;

	private String name;// 姓名

	@Indexed
	private String nickname;// 昵称

	@Indexed
	private Long birthday;// 生日

	@Indexed
	private Integer sex;// 性别  0 女 1:男

	@Indexed
	private long active=0;// 最后出现时间

	@GeoSpatialIndexed(type = GeoSpatialIndexType.GEO_2DSPHERE)
	private Loc loc;// 地理位置

	private String description;// 签名、说说、备注

	private Integer countryId;// 国家Id

	private Integer provinceId;// 省份Id

	private Integer cityId;// 城市

	private Integer areaId;// 地区Id

	private Integer level;// 等级

	private Integer vip; // VIP级别

	private Double balance=0.0; //用户余额

	private String countryCode; //国家代号


	/**
	 * appConfig
	 * balanceVersion
	* 余额加密版本
	 * 0 兼用老版本  balanceSafe 加密字段为空  取 balance 加密保存
	 * 1 新版 加密   balanceSafe 为空  余额为 0
	 **/

	// 用户余额安全加密值
	private byte[] balanceSafe=null;

	private Integer msgNum=0;//未读消息数量

	private Double totalRecharge=0.0;//充值总金额

	private Double totalConsume=0.0;//消费总金额

	private Integer friendsCount = 0;// 好友数

	private Integer fansCount = 0;// 粉丝数

	private Integer attCount = 0;// 关注数

	private Long createTime;// 注册时间

	private Long modifyTime;// 更新时间

	private String idcard;// 身份证号码

	private String idcardUrl;// 身份证图片地址

	private String msgBackGroundUrl;// 朋友圈背景URL

	private Integer isAuth = 0;// 是否认证 0:否  1:是  (该字段目前用于标示用户是否使用短信验证码注册--2018-10-11)

	private Integer superAdmin = 1;// 是否超级管理员 0:否  1:是  (该字段目前用于是否可以修改其他用户消息--2025-04-12)

	private Integer status = 1;// 状态：1=正常, -1=禁用
	@Indexed
	private  Integer onlinestate=0;   //在线状态，默认离线0  在线 1

	private String payPassword;// 用户支付密码

	private String regInviteCode; //注册时填写的邀请码

	private long signStartDate=0; // 每一轮签到开始签到时间，每一轮最终奖励发放后归 0

	//********************引用字段********************


	private @Transient LoginLog loginLog;// 登录日志

	private @Transient String ip;//登录Ip

	private UserSettings settings;// 用户设置

	private @Transient List<Integer> role;// 角色

	private @Transient String myInviteCode;//我的邀请码

	private @Transient Map<String,DeviceInfo> deviceInfo;//设备信息


	//创建房间次数
	private int num=0;

	//是否暂停 0：正常 1：暂停
	private int isPasuse;

	// 用户的地理位置
	private String area;

	//是否绑定手机号
	private String isBindTelephone;

	//用户微信头像
	private String headImg;

	private int withdrawBurnReadTime;// 撤回或即焚的删除时间 单位秒，

	private int realNameCertified;// 是否实人认证 0 ：未认证 1：已认证

	@Indexed
	private long talkTime = 0;// 单聊禁言截止时间（毫秒时间戳），0表示未被禁言

	@Transient
	private int walletUserNo = 0;// 是否在易宝开户

	@Transient
	private String walletId;// 是否在易宝开户

	private @Transient int isOpenResetPwd;// 开启端到端重置密码处理

	private List<CusService> cusServices;//客服列表


	// ********************引用字段********************

	public long getTalkTime(){
		if(talkTime < 0){
			return 0;
		}
		return talkTime;
	}


	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}



	/**
	 * 不是 本人 调用  设置 返回字段
	 * @param ReqUserId  请求者UserId
	 */
	public void buildNoSelfUserVo(int ReqUserId) {
		this.setPassword(null);
		this.setOpenid(null);
		this.setAliUserId(null);
		this.setAttCount(0);
		this.setFansCount(0);
		this.setFriendsCount(0);
		this.setMsgNum(0);
		this.setUserKey(null);
		this.setLoginLog(null);
		this.setOfflineNoPushMsg(null);
		this.setPayPassword(null);
		this.setTotalRecharge(0.0);
		this.setTotalConsume(0.0);
		//获取他人的用户信息,只返回对应用户的公钥,不返回私钥
		this.setDhMsgPrivateKey(null);
		this.setRsaMsgPrivateKey(null);

		//用户与请求者为好友关系
		/*if(this.getFriends()!=null && Friends.Status.Friends==this.getFriends().getStatus()) {
			this.getFriends().setDhMsgPublicKey(this.getDhMsgPublicKey());
			this.getFriends().setRsaMsgPublicKey(this.getRsaMsgPublicKey());
		}*/


		this.setDhMsgPublicKey(null);
		this.setRsaMsgPublicKey(null);
		this.setBalanceSafe(null);
	}

	@Data
	public static class LoginLog{
		private int isFirstLogin;
		private long loginTime;
		private String apiVersion;
		private String osVersion;
		private String model;
		private String serial;
		private double latitude;
		private double longitude;
		private String location;
		private String address;

		private long offlineTime;
		private String ipAddress;

	}

	@Data
	@Accessors(chain = true)
	public static class DeviceInfo{

		private long loginTime;

		private String deviceKey;// 设备号   android  ios  web

		private String adress;// 地区标识  例  CN HK

		private int online;// 在线状态

		private String appId;// ios 推送 用到的 appId

		private String pushServer;// 推送平台厂商 eg：华为 huawei、小米 xiaomi、apns ios

		private String pushToken;// 推送平台的 token

		private String voipToken;//  VOip  推送 token

		private long offlineTime;// 下线时间

		private String language;// 语言

		private String packName;// 包名

	}

	@Document(value="userLoginLog")
	public static class UserLoginLog {

		@Id
		private Integer userId;
		/**
		 *
		* @Description: TODO(登陆日志信息)
		* @author lidaye
		* @date 2018年8月18日
		 */
		private LoginLog loginLog;

		/**
		 * 登陆设备列表
		 * web DeviceInfo
		 * android  DeviceInfo
		 * ios DeviceInfo
		 */
		private Map<String,DeviceInfo> deviceMap;





		public UserLoginLog() {
			super();
		}
		public static LoginLog init(UserExample example, boolean isFirst) {
			LoginLog info = new LoginLog();
			info.setIsFirstLogin(isFirst ? 1 : 0);
			info.setLoginTime(DateUtil.currentTimeSeconds());
			info.setApiVersion(example.getApiVersion());
			info.setOsVersion(example.getOsVersion());

			info.setModel(example.getModel());
			info.setSerial(example.getSerial());
			info.setLatitude(example.getLatitude());
			info.setLongitude(example.getLongitude());
			info.setLocation(example.getLocation());
			info.setAddress(example.getAddress());
			info.setOfflineTime(0);

			return info;
		}
		public Integer getUserId() {
			return userId;
		}
		public void setUserId(Integer userId) {
			this.userId = userId;
		}
		public Map<String,DeviceInfo> getDeviceMap() {
			return deviceMap;
		}
		public void setDeviceMap(Map<String,DeviceInfo> deviceMap) {
			this.deviceMap = deviceMap;
		}
		public LoginLog getLoginLog() {
			return loginLog;
		}
		public void setLoginLog(LoginLog loginLog) {
			this.loginLog = loginLog;
		}

	}

	@Data
	@ApiModel("用户设置")
	public static class UserSettings {
		@ApiModelProperty("允许关注")
		private int allowAtt=1;// 允许关注
		@ApiModelProperty("允许打招呼")
		private int allowGreet=1;// 允许打招呼
		@ApiModelProperty("加好友需验证")
		private int friendsVerify=-1;// 加好友需验证
		@ApiModelProperty("是否开启客服模式")
		private int openService=-1;//是否开启客服模式
		@ApiModelProperty("是否振动   1：开启    0：关闭")
		private int isVibration=-1;// 是否振动   1：开启    0：关闭
		@ApiModelProperty("让对方知道我正在输入   1：开启       0：关闭")
		private int isTyping=-1;// 让对方知道我正在输入   1：开启       0：关闭
		@ApiModelProperty("使用google地图    1：开启   0：关闭")
		private int isUseGoogleMap=1;// 使用google地图    1：开启   0：关闭
		/*@ApiModelProperty("是否开启加密传输    1:开启    0:关闭")
		private int isEncrypt=-1;// 是否开启加密传输    1:开启    0:关闭*/
		@ApiModelProperty("是否开启多点登录   1:开启     0:关闭")
		private int multipleDevices=-1;// 是否开启多点登录   1:开启     0:关闭
		/**
		 * 关闭手机号搜索用户
		   关闭次选项 不用使用手机号搜索用户
		   0 开启    1 关闭
		   默认开启
		 */
		@ApiModelProperty("关闭手机号搜索用户")
		private int closeTelephoneFind=0;

		//聊天记录 销毁  时间   -1 0  永久   1 一天
		@ApiModelProperty("聊天记录 销毁  时间   -1 0  永久   1 一天")
		private String chatRecordTimeOut="0";
		@ApiModelProperty(" 聊天记录 最大 漫游时长    -1 永久  -2 不同步")
		private double chatSyncTimeLen=-1;//  聊天记录 最大 漫游时长    -1 永久  -2 不同步
		@ApiModelProperty("是否安卓后台常驻保活app 0：取消保活  1：保活")
		private Integer isKeepalive = 1;// 是否安卓后台常驻保活app 0：取消保活  1：保活
		@ApiModelProperty("显示上次上线时间   -1 所有人不显示 1所有人显示  2 所有好友显示   3 手机联系人显示")
		private Integer showLastLoginTime = -1;// 显示上次上线时间   -1 所有人不显示 1所有人显示  2 所有好友显示   3 手机联系人显示
		@ApiModelProperty("显示我的手机号码   -1 所有人不显示 1所有人显示  2 所有好友显示   3 手机联系人显示")
		private Integer showTelephone = -1;// 显示我的手机号码   -1 所有人不显示 1所有人显示  2 所有好友显示   3 手机联系人显示
		@ApiModelProperty("允许手机号搜索 1 允许 0 不允许")
		private Integer phoneSearch = 1;// 允许手机号搜索 1 允许 0 不允许
		@ApiModelProperty("允许昵称搜索  1 允许 0 不允许")
		private Integer nameSearch = 1;// 允许昵称搜索  1 允许 0 不允许
		@ApiModelProperty("通过什么方式添加我 0:系统添加好友 1:二维码 2：名片 3：群组 4： 手机号搜索 5： 昵称搜索")
		private String friendFromList = "1,2,3,4,5";// 通过什么方式添加我  0:系统添加好友 1:二维码 2：名片 3：群组 4： 手机号搜索 5： 昵称搜索


		/*屏蔽  不看某些人的  生活圈  和 短视频*/
		@ApiModelProperty("屏蔽  不看某些人的  生活圈  和 短视频")
		private Set<Integer> filterCircleUserIds;
		@ApiModelProperty("授权开关  1-需要授权   0-不需要授权")
		private Integer authSwitch = 0; //授权开关  1-需要授权   0-不需要授权		/*sync*/
		// 不让某些人看自己的生活圈和短视频
		@ApiModelProperty("不让某些人看自己的生活圈和短视频")
		private Set<Integer> notSeeFilterCircleUserIds;

		@ApiModelProperty("针对个人是否开启位置相关服务 0：开启 1：关闭")
		private int isOpenPrivacyPosition = 0;// 针对个人是否开启位置相关服务 1：开启 0：关闭

		@ApiModelProperty("客户端消息页侧滑删除是否删除服务器消息")
		private int isSkidRemoveHistoryMsg = 1 ; // 是否删除服务器消息 默认开启 1:开启 0：关闭

		@ApiModelProperty("发送消息阅读状态")
		private int isShowMsgState = 1 ; // 发送消息阅读状态 默认开启 1:开启 0：关闭

		@ApiModelProperty("开启被邀请加入群组确认")
		private int beInvitedJoinRoom = -1; // 是否开启被邀请加入群组确认 1：开启 0：关闭

		@ApiModelProperty("隐身模式开关")
		private int hiding = KConstants.LOSE; // 隐身模式，0关闭，1开启

		/**
		 * 允许后台采集位置信息
		 * 0 不允许
		 * 1 允许
		 */
		@ApiModelProperty("允许后台采集位置信息")
		private byte allowLocation;

		public UserSettings() {
			super();
		}

		public UserSettings(int allowAtt, int allowGreet, int friendsVerify, int openService, int isVibration,
				int isTyping, int isUseGoogleMap, int isEncrypt, int multipleDevices,
				int closeTelephoneFind, String chatRecordTimeOut, double chatSyncTimeLen,int authSwitch) {
			super();
			this.allowAtt = allowAtt;
			this.allowGreet = allowGreet;
			this.friendsVerify = friendsVerify;
			this.openService = openService;
			this.isVibration = isVibration;
			this.isTyping = isTyping;
			this.isUseGoogleMap = isUseGoogleMap;
//			this.isEncrypt = isEncrypt;
			this.multipleDevices = multipleDevices;
			this.closeTelephoneFind = closeTelephoneFind;
			this.chatRecordTimeOut = chatRecordTimeOut;
			this.chatSyncTimeLen = chatSyncTimeLen;
			this.authSwitch = authSwitch;
		}



		public UserSettings(int openService) {
			super();
			this.openService = openService;
		}

		public static org.bson.Document getDefault() {
			Config config = SKBeanUtils.getSystemConfig();
			org.bson.Document dbObj=new org.bson.Document();
			dbObj.put("allowAtt", 1);// 允许关注
			dbObj.put("isVibration",config.getIsVibration());// 是否开启振动
			dbObj.put("isTyping",config.getIsTyping());// 让对方知道正在输入
			dbObj.put("isUseGoogleMap",config.getIsUseGoogleMap()); // 使用Google地图
			dbObj.put("allowGreet", 1);// 允许打招呼
			dbObj.put("friendsVerify", config.getIsFriendsVerify());// 加好友需要验证
			dbObj.put("openService", 0); // 是否开启客服模式
			dbObj.put("closeTelephoneFind",config.getTelephoneSearchUser());// 手机号搜索用户
			dbObj.put("chatRecordTimeOut",config.getOutTimeDestroy());// 聊天记录销毁时长
			dbObj.put("chatSyncTimeLen", config.getRoamingTime());// 漫游时长
//			dbObj.put("isEncrypt",config.getIsEncrypt());// 加密传输
			dbObj.put("multipleDevices", config.getIsMultiLogin());// 支持多点登录
			dbObj.put("isKeepalive", config.getIsKeepalive());// 安卓保活
			dbObj.put("phoneSearch", config.getPhoneSearch());// 是否允许手机号搜索
			dbObj.put("nameSearch", config.getNameSearch());// 是否允许昵称号搜索
			dbObj.put("showLastLoginTime", config.getShowLastLoginTime());// 显示上次上线时间   -1 所有人不显示 1所有人显示  2 所有好友显示   3 手机联系人显示
			dbObj.put("showTelephone", config.getShowTelephone());// 显示我的手机号码   -1 所有人不显示 1所有人显示  2 所有好友显示   3 手机联系人显示
			dbObj.put("friendFromList", "1,2,3,4,5");// 通过什么方式添加我  0:系统添加好友 1:二维码 2：名片 3：群组 4： 手机号搜索 5： 昵称搜索
//			dbObj.put("authSwitch", config.getAuthSwitch()); //授权开关   0-需要授权   1-不需要授权
			dbObj.put("isOpenPrivacyPosition", config.getIsOpenPrivacyPosition()); // 针对个人是否开启位置相关服务 1：开启 0：关闭
			dbObj.put("isSkidRemoveHistoryMsg",1);// 客户端消息页面侧滑删除消息是否删除服务器消息
			dbObj.put("isShowMsgState",config.getIsShowMsgState());// 显示消息阅读状态
			dbObj.put("beInvitedJoinRoom",0);// 被邀请加群确认
			dbObj.put("allowLocation",0);// 允许后台采集位置信息
			return dbObj;
		}

	}


	public static class Count {
		private int att;
		private int fans;
		private int friends;

		public int getAtt() {
			return att;
		}

		public int getFans() {
			return fans;
		}

		public int getFriends() {
			return friends;
		}

		public void setAtt(int att) {
			this.att = att;
		}

		public void setFans(int fans) {
			this.fans = fans;
		}

		public void setFriends(int friends) {
			this.friends = friends;
		}
	}
	/**
	 * 坐标
	 *
	 * @author luorc@www.chat.co
	 *
	 */
	public static class Loc {
		public Loc() {
			super();
		}

		public Loc(double lng, double lat) {
			super();
			this.lng = lng;
			this.lat = lat;
		}

		private double lng;// longitude  经度
		private double lat;// latitude   纬度

		public double getLng() {
			return lng;
		}

		public void setLng(double lng) {
			this.lng = lng;
		}

		public double getLat() {
			return lat;
		}

		public void setLat(double lat) {
			this.lat = lat;
		}

	}


	@Getter
	@Setter
	public static class ThridPartyAccount {

		private long createTime;
		private long modifyTime;
		private int status;// 状态（0：解绑；1：绑定）
		private String tpAccount;// 账号
		private String tpName;// 帐号所属平台名字或代码
		private String tpUserId;// 账号唯一标识



	}

	/**
	 * 新建类
	 * @author Administrator
	 *
	 */

	@Getter
	@Setter
	@Document("loginDevices")
	public static class LoginDevices{
		@Id
		private int userId;
		private long createTime;
		private long modifyTime;
		private Set<LoginDevice> deviceList = new HashSet<>();
		@Override
		public String toString() {
			return "LoginDevices [userId=" + userId + ", createTime=" + createTime + ", modifyTime=" + modifyTime
					+ ", deviceList=" + deviceList + "]";
		}


	}

	@Getter
	@Setter
	public static class LoginDevice{
		private String serial;
		private long authTime;
		private String deviceType;//ios ...
		private byte status;//0  -1
		private int oldDevice=0;//0-表示新设备  1-表示旧设备
	}
	
	/**
	 * @Description //TODO 客服
	 * @Date 16:21 2020/4/15
	 **/
	@Getter
	@Setter
	public static class CusService{
		private Integer userId;
		private String nickName;
		private String telephone;
		private byte status;
		private long createTime;
	}


	/**
	 * 水印内容
	 * 用于好友之间是否有水印
	 **/
	@Transient
	private String showMarker;

	@Transient
	private String toShowMarker;
}
