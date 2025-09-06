package com.basic.im.admin.entity;


import com.basic.im.entity.ClientConfig;
import com.basic.im.entity.Config;
import com.basic.im.entity.PayConfig;
import com.basic.im.utils.MoneyUtil;
import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 
 * @Description: TODO(config数据返回封装类)
 * @author zhm
 * @date 2018年9月21日 下午5:44:40
 * @version V1.0
 */
@Data
public class ConfigVO {
	
	public String XMPPDomain;  // xmpp虚拟域名
	
	public String XMPPHost;  // xmpp主机host
	
	private String apiUrl;  // 接口URL
	
	private String downloadAvatarUrl;  // 头像访问URL
	
	private String downloadUrl;  // 资源访问URL
	
	private String uploadUrl;  // 资源上传URL
	
	private String liveUrl;  // 直播URL
	
	private String jitsiServer; // 视频服务器URL

	private String mallServerUrl;// 商城服务地址
	
	public int XMPPTimeout=15; // xmpp超时时间  秒 
	
	private int xmppPingTime=10;
	
	private int displayRedPacket=1;

	/**
	 * 视界评论
	 *  0 关闭
	 *  1 开启
	 **/
	private byte banComment;

	/**
	 * 是否支持端对端消息聊天
	 * 非对称消息加密
	 * 1：开启 0：关闭
	 */
	private byte isOpenSecureChat=1;

	private byte enablePayModule=0; //是否启用支付模块
	private byte enableMpModule=0; //是否启用公众号模块
	private byte enableOpenModule=0; //是否启用开放平台模块
	private byte enableOpenSquare=1; //是否启用广场功能

	private byte enableAliPay = 0; // 是否启用支付宝支付
	private byte enableWxPay = 0; // 是否启用微信支付

	private int fileValidTime=-1; //聊天内容的 文件有效期  默认  -1
	
	private int chatRecordTimeOut=-1; //聊天记录过期时间  -1 为永久  数值 为天数
	
	private byte isOpenSMSCode = 1; //是否开启短信验证码
	
	private int showContactsUser=1; 
	
	private int isOpenRegister = 1; //是否开启注册

	private byte isOpenUI = 1; //是否开启客户端Ui

	private byte isOpenReceipt=1;//是否启用 消息回执
	private byte isOpenReadReceipt=1;
	
	private byte isOpenCluster;// 是否开启集群
	
	private String helpUrl;
	
	private String videoLen;
	
	private String audioLen;
	
	private String shareUrl;
	
	private String softUrl;
	
	private int distance;
	
	private String address;// 根据ip得到的地址
	private String ipAddress;// 当前请求的ip地址
	private byte isOpenAPNSorJPUSH = 0;// IOS推送平台 0：APNS  1：极光推送开发版  2：极光推送生产版
	private String privacyPolicyPrefix; // 隐私设置URL地址前缀

	private ClientConfig.InterfaceOrder interfaceOrder; //用于控制界面顺序的字段


	/**
	 * 公司模块
	 */
	/*private byte userJoinCompanyIsNeedManagerConfirm; //用户主动加入公司是否需要创建者确认 0 否 1 是
	private byte inviteJoinCompanyISNeedUserConfirm; //邀请加入公司是否需要用户确认 0 否 1 是*/


	/*
	 * 以下为版本更新的字段
	*/
	
	private int androidVersion;  //Android 版本号
	private int iosVersion;     //ios版本号
	
	private String androidAppUrl;  //Android App的下载地址
	
	private String iosAppUrl;    // IOS App 的下载地址
	
	private String androidExplain; //Android 说明
	
	private String iosExplain;   // ios 说明
	
	private int pcVersion;     // pc版本号
	
	private String pcAppUrl;  // pc 软件的下载地址
	
	private String pcExplain; // pc 说明
	
	private int macVersion;     // mac版本号
	
	private String macAppUrl;  // mac 软件的下载地址
	
	private String macExplain; // mac 说明
	
	private String androidDisable;// android禁用版本号（凡低于此版本号的禁用）
	
	private String iosDisable;// ios禁用版本号（凡低于此版本号的禁用）
	
	private String pcDisable;// pc禁用版本号（凡低于此版本号的禁用）
	
	private String macDisable;// mac禁用版本号（凡低于此版本号的禁用）

	/**
	 * 广场 标签列表
	 */
	private String lableListStr;


	private String companyName;// 公司名称
	
	private String copyright;// 版权信息
	
	private int hideSearchByFriends=1;// 是否隐藏好友搜索功能 0:隐藏 1：开启
	
	private String appleId;// IOS AppleId
	
	private String website;// APP下载页网址
	
	private int regeditPhoneOrName = 0;// 0：使用手机号注册，1：使用用户名注册
	
	private int registerInviteCode = 0; //0:关闭 1:开启一对一邀请（一码一用，且必填）2:开启一对多邀请（一码多用，选填项）
	
	private int nicknameSearchUser  = 0; //昵称搜索用户  0：关闭   1：昵称精准搜索  2：昵称模糊搜索
	
	private int isCommonFindFriends = 0;// 普通用户是否能搜索好友 0:允许 1：不允许

	private int isCommonCreateGroup = 0;// 普通用户是否能建群 0:允许 1：不允许

	// 普通用户是否允许加好友 0 不允许  1 允许
	private byte allowAddFriend = 1;
	
	private int isOpenPositionService = 0;// 是否开启位置相关服务 0：开启 1：关闭 
	
	private String headBackgroundImg;// 头部导航背景图

	private String homeName;  //启动软件首页名称

	private String homeAddress;// 启动软件的首页地址

	private byte isOpenGoogleFCM;// 是否打开Android Google推送 1：开启 0：关闭
	
	private byte isOpenRoomSearch = 0;// 是否开启群组搜索 0：开启 1：关闭
	
	private byte isOpenOnlineStatus=0;//是否开启在线状态1：开启 0：关闭
	
	private String popularAPP;// 热门应用,1:开启，0：关闭， 示例:{\"lifeCircle\":1,\"videoMeeting\":1,\"liveVideo\":1,\"shortVideo\":0,\"peopleNearby\":0,\"scan\":0}", 

	public long videoLength = 25;// 短视频拍摄上限时长，单位秒

	private byte isOpenManualPay=0;// 是否开启扫码手动充值

	private byte isOpenCloudWallet = 0; //是否开启云钱包 1:开启 0:关闭

	private double manualPaywithdrawFee = 0.006; // 手动充值功能-提现的费率

	private byte isOpenAuthSwitch = 0;// 是否开启旧设备登录授权 0：关闭 1：开启

	public byte upperShelf = 0;// 应用上架模式

	/**
	 * 零钱提现费率
	 */
	private double myChangeWithdrawRate = 0.006;

	/**
	 * 银行卡 提现手续费
	 */
	private double upayWithdrawServiceCharge=0.1;

	/**
	 * 第三方登录免注册 0.关闭 1.开启
	 */
	private byte isNoRegisterThirdLogin=0;

	/**
	 * 单笔最大红包金额
	 */
	private double maxRedpacktAmount;

	/**
	 * 单笔最大转账金额
	 */
	private double maxTransferAmount;

	/**
     * 单笔群红包最大发送个数
     */
    private int maxRedpacktNumber;

	private List<String> virtualDeposit;

	private String defaultTelephones;// 注册默认自动添加为好友 的用户手机号

	/**
	 * 定位上传间隔(单位小时)
	 */
	private long locateInterval;
	/**
	 * 是否开启审核提现
	 */
	private byte isOpenAuditPay = 1;
	/**
	 * 是否开启自动提现
	 */
	private byte isOpenAutoPay = 1;
	/**
	 * 是否开启微信提现
	 */
	private byte isOpenWXPay = 0;
	/**
	 * 单笔最小充值金额
	 */
	private double minRechargeAmount = 0;
	/**
	 * 零钱提现费率
	 */
	private double myChangeWithdrawBase = 1.0;
	/**
	 * 单笔最小提现金额
	 */
	private double minWithdrawAmount=200.0;

	private byte isOpenDepositWithdrawal = 1;

	public byte isOpenCZ = 1;

	public byte videoVoice = 0;

	public ConfigVO() {}

	public ConfigVO(Config config, ClientConfig clientConfig, PayConfig payConfig) {
		XMPPDomain = clientConfig.getXMPPDomain();
		XMPPHost = clientConfig.getXMPPHost();
		this.apiUrl = clientConfig.getApiUrl();
		this.downloadAvatarUrl = clientConfig.getDownloadAvatarUrl();
		this.downloadUrl = clientConfig.getDownloadUrl();
		this.uploadUrl = clientConfig.getUploadUrl();
		this.liveUrl = clientConfig.getLiveUrl();
		this.mallServerUrl = clientConfig.getMallServerUrl();
		this.jitsiServer = clientConfig.getJitsiServer();
		this.androidVersion = clientConfig.getAndroidVersion();
		this.iosVersion = clientConfig.getIosVersion();
		this.androidAppUrl = clientConfig.getAndroidAppUrl();
		this.iosAppUrl = clientConfig.getIosAppUrl();
		this.androidExplain = clientConfig.getAndroidExplain();
		this.iosExplain = clientConfig.getIosExplain();
		this.pcAppUrl = clientConfig.getPcAppUrl();
		this.pcVersion = clientConfig.getPcVersion();
		this.pcExplain = clientConfig.getPcExplain();
		this.macAppUrl = clientConfig.getMacAppUrl();
		this.macVersion = clientConfig.getMacVersion();
		this.macExplain = clientConfig.getMacExplain();
		this.androidDisable = clientConfig.getAndroidDisable();
		this.iosDisable = clientConfig.getIosDisable();
		this.pcDisable = clientConfig.getPcDisable();
		this.macDisable = clientConfig.getMacDisable();
		this.displayRedPacket = clientConfig.getDisplayRedPacket();
		this.enablePayModule = clientConfig.getEnablePayModule();
		this.enableMpModule = clientConfig.getEnableMpModule();
		this.enableOpenModule = clientConfig.getEnableOpenModule();
		this.enableOpenSquare = clientConfig.getEnableOpenSquare();
		this.enableAliPay = clientConfig.getEnableAliPay();
		this.enableWxPay = clientConfig.getEnableWxPay();
		this.showContactsUser = clientConfig.getShowContactsUser();
		this.isOpenRegister = clientConfig.getIsOpenRegister();
		this.isOpenUI =  clientConfig.getIsOpenUI();
		this.XMPPTimeout = config.getXMPPTimeout();
		this.fileValidTime = config.getFileValidTime();
		this.chatRecordTimeOut = config.getChatRecordTimeOut();
		this.isOpenReceipt = config.getIsOpenReceipt();
		this.helpUrl = config.getHelpUrl();
		this.videoLen = config.getVideoLen();
		this.audioLen = config.getAudioLen();
		this.shareUrl = config.getShareUrl();
		this.softUrl = config.getSoftUrl();
		this.distance = config.getDistance();
		this.isOpenSMSCode = config.getIsOpenSMSCode();
		this.isOpenCluster = config.getIsOpenCluster();
		this.xmppPingTime = (int) (config.getXMPPTimeout() / 2.5);
		this.lableListStr=clientConfig.getLableListStr();
		this.companyName = clientConfig.getCompanyName();
		this.copyright = clientConfig.getCopyright();
		this.hideSearchByFriends = clientConfig.getHideSearchByFriends();
		this.appleId = clientConfig.getAppleId();
		this.website = clientConfig.getWebsite();
		this.regeditPhoneOrName = config.getRegeditPhoneOrName();
		this.registerInviteCode = config.getRegisterInviteCode();
		this.nicknameSearchUser = config.getNicknameSearchUser();
		this.isCommonFindFriends = clientConfig.getIsCommonFindFriends();
		this.isCommonCreateGroup = clientConfig.getIsCommonCreateGroup();
		this.allowAddFriend=clientConfig.getAllowAddFriend();

		this.isOpenPositionService = clientConfig.getIsOpenPositionService();
		this.address = clientConfig.getAddress();
		this.headBackgroundImg = clientConfig.getHeadBackgroundImg();
		this.isOpenGoogleFCM = config.getIsOpenGoogleFCM();
		this.popularAPP = clientConfig.getPopularAPP();
		this.ipAddress = config.getIpAddress();
		this.isOpenAPNSorJPUSH = clientConfig.getIsOpenAPNSorJPUSH();
		this.privacyPolicyPrefix = config.getPrivacyPolicyPrefix();
		this.interfaceOrder = clientConfig.getInterfaceOrder();
		this.isOpenRoomSearch = clientConfig.getIsOpenRoomSearch();
		this.isOpenOnlineStatus = config.getIsOpenOnlineStatus();
		this.videoLength = clientConfig.getVideoLength();
		this.isOpenSecureChat = clientConfig.getIsOpenSecureChat();
		this.isOpenManualPay = payConfig.getIsOpenManualPay();
		this.isOpenCloudWallet = payConfig.getIsOpenCloudWallet();
		this.upayWithdrawServiceCharge=payConfig.getUpayWithdrawServiceCharge();
		this.manualPaywithdrawFee = payConfig.getManualPaywithdrawFee();
		this.maxRedpacktAmount = payConfig.getMaxRedpacktAmount();
		this.maxRedpacktNumber = payConfig.getMaxRedpacktNumber();
		this.maxTransferAmount=payConfig.getMaxTransferAmount();
		this.isOpenAuthSwitch = clientConfig.getIsOpenAuthSwitch();
		this.homeName = clientConfig.getHomeName();
		this.homeAddress = clientConfig.getHomeAddress();
		this.isNoRegisterThirdLogin=clientConfig.getIsNoRegisterThirdLogin();
		this.upperShelf = clientConfig.getUpperShelf();
		this.myChangeWithdrawRate = payConfig.getMyChangeWithdrawRate();
		this.banComment = config.getBanComment();
		this.defaultTelephones = config.getDefaultTelephones();
		this.virtualDeposit = getVirtualDepositList(config.getVirtualDeposit());
		this.locateInterval = config.getLocationModiyTime();
		this.myChangeWithdrawBase=payConfig.getMyChangeWithdrawBase();
		this.isOpenAuditPay=payConfig.getIsOpenAuditPay();
		this.isOpenAutoPay=payConfig.getIsOpenAutoPay();
		this.isOpenWXPay=payConfig.getIsOpenWXPay();
		this.minRechargeAmount=payConfig.getMinRechargeAmount();

		this.minWithdrawAmount=payConfig.getMinWithdrawAmount();

		this.isOpenCZ = payConfig.getIsOpenCZ();
		this.isOpenDepositWithdrawal= payConfig.getIsOpenDepositWithdrawal();
		this.videoVoice = config.getVideoVoice();

	}

	private List<String> getVirtualDepositList(String virtualDeposit){
		List<String> result = new ArrayList<>();
		List<String> list = Arrays.asList(virtualDeposit.split(","));
		for (String s : list) {
			result.add(String.valueOf(MoneyUtil.fromCentToDouble(s)));
		}
		return result;
	}
	
}
