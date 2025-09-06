package com.basic.im.entity;



import com.alibaba.fastjson.JSON;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter
@Setter
@Document(value="config")
public class Config {

	private @Id long id = 10000;

	public int XMPPTimeout = 180;// xmpp超时时间  秒
	
	//聊天内容的 文件有效期  默认  -1
	private int fileValidTime = -1;
	
	//聊天记录// 过期销毁时长   -1、0:永久    0.04：一小时    1：1天     7：一周     30:一个月      120:一季     365：一年
	private int chatRecordTimeOut = -1;
	
	private byte telephoneSearchUser = 1;  //手机号搜索用户 0 :关闭      1:精确搜索    2:模糊搜索   默认精准搜索
	
	private byte nicknameSearchUser = 0; //昵称搜索用户  0 :关闭       1:精确搜索    2:模糊搜索   默认模糊搜索
	
	private byte isTelephoneLogin = 1; //手机号登陆   1 开启  0 关闭
	
	private byte isUserIdLogin = 1; //用户ID登陆   1 开启  0 关闭

	private String helpUrl;
	
	private String videoLen;
	
	private String audioLen;
	
	private String shareUrl;
	
	private String softUrl;


	private int distance;  //附近的人查询范围  单位:千米 例如设置为5,则查询附近5千米的用户

	private byte isAuthApi = 1; //是否开启 api 权限  1开启  0关闭
	
	private byte isKeyWord;  // 是否开启关键词过滤   1:开启    0:关闭

	private byte isSendKeyWordHint; //是否发送敏感词提示 1:是  0:否

	private byte isSaveMsg = 1;  // 是否保存单聊聊天记录    1:开启     0:关闭
	
	private byte isSaveMucMsg = 1; // 是否保存群聊聊天记录   1:是      0:否

	/**
	 * 是否保存服务器发送的系统消息
	 * 1:是      0:否
	 * 以下发送的做到不存盘
	 * 1.公众号发送的
	 * 2.全员群发的
	 * 3.后台发到群组的
	 * 4.压测的
	 */
	private byte isSaveSystemMsg = 1;
	
	private byte isMsgSendTime; // 是否强制同步消息发送时间   1:是   0:否
	
	private byte regeditPhoneOrName = 0;// 0：使用手机号注册，1：使用用户名注册

	private String userNicknameNotword = "";

	private byte registerInviteCode; // 注册邀请码 0:关闭 1:开启一对一邀请（一码一用，且必填）2:开启一对多邀请（一码多用，选填项）
	
	private byte isSaveRequestLogs = 0; // 是否保存接口请求日志 0:不保存 1：保存

	private String privacyPolicyPrefix; // 隐私设置URL地址前缀

	private byte createCompamyIsNeedCheck; //创建公司是否需要审核  0 否 1 是

	private byte userJoinCompanyIsNeedManagerConfirm; //用户主动加入公司是否需要创建者确认 0 否 1 是

	private byte inviteJoinCompanyIsNeedUserConfirm; //邀请加入公司是否需要用户确认 0 否 1 是

	/** 用户隐私设置参数  **/
	private double roamingTime = 30;// 漫游时长   -2:不漫游     -1、0:永久  0.04：一小时   1：1天    7:一周     30：一个月    120：一季    365：一年
	
	private double  outTimeDestroy = -1;// 过期销毁时长   -1、0:永久    0.04：一小时    1：1天     7：一周     30:一个月      120:一季     365：一年

	/**
	 * 消息延时删除时间
	 * 撤回，阅后即焚，删除
	 * 单位秒
	 */
	private int msgDelayedDeleteTime=3600;

	private String language="zh";// 客户端默认语种
	
	private byte isFriendsVerify = 1;// 是否需要好友验证   1:开启    0:关闭
	
//	private byte isEncrypt;// 是否开启加密传输    1:开启    0:关闭T
	
	private byte isMultiLogin=1;// 是否开启多点登录   1:开启     0:关闭
	
	private byte isVibration; // 是否振动   1：开启    0：关闭
	
	private byte isTyping; // 让对方知道我正在输入   1：开启       0：关闭
	
	private byte isUseGoogleMap;// 使用google地图    1：开启   0：关闭
	
	private byte isKeepalive = 1;// 是否安卓后台常驻保活app 0：取消保活  1：保活

	private byte isOpenPrivacyPosition = 1;// 针对个人是否开启位置相关服务 1：开启 0：关闭
	
	private byte phoneSearch = 1;// 允许手机号搜索 1 允许 0 不允许
	
	private byte nameSearch = 1;// 允许昵称搜索  1 允许 0 不允许
	
	private byte showLastLoginTime = -1;// 显示上次上线时间   -1 所有人不显示 1所有人显示  2 所有好友显示   3 手机联系人显示
	
	private byte showTelephone = -1;// 显示我的手机号码   -1 所有人不显示 1所有人显示  2 所有好友显示   3 手机联系人显示

	private int isShowMsgState = 1 ; // 显示消息阅读状态 默认开启 1:开启 0：关闭

//	private int authSwitch = 0; //授权开关  1-需要授权   0-不需要授权
	
	/*sync*/
	/**
	 * 第三方登录免注册 0.关闭 1.开启
	 */
	private byte isNoRegisterThirdLogin=0;

	/** 建立群组默认参数设置  **/

	private int maxUserSize=1000;// 群成员人数上限
	
	private byte isAttritionNotice=1;// 群组减员发送通知（踢人，退出） 1：开启  0：关闭
	
	private byte isLook=1;// 群组是否可见   0为可见   1为不可见
	
	private byte showRead=0;// 群主设置 群内消息是否发送已读 回执 显示数量  1：是 0：否
	
	private byte isNeedVerify=0; // 加群是否需要通过验证  0：不要   1：要
	
	private byte showMember=1;// 显示群成员给 普通用户   1 显示  0  不显示
	
	private byte allowSendCard=1;// 允许发送名片 好友  1 允许  0  不允许
	
	private byte allowInviteFriend=1;// 允许普通成员邀请好友  1：允许 0：不允许
	
	private byte allowUploadFile=1;// 允许群成员上传群共享文件 1：允许 0：不允许
	
	private byte allowConference=1;// 允许成员 召开会议  1：允许 0：不允许
	
	private byte allowSpeakCourse=1;// 允许群成员 开启 讲课  1：允许 0：不允许

	private String iosPushServer="apns";//   apns 推送
	
	private String SMSType = "aliyun";// 短信服务支持
	
	private byte imgVerificationCode = 0;// 图形验证码类型0：字母和数字混合，1：存数字
	
	private byte isAutoAddressBook;// 是否自动添加通讯录好友 1：开启 0：关闭
	
	private double giftRatio = 0.50;// 直播礼物分成比例
	
	private String promotionUrl;// 客服推广链接

	private String defaultTelephones="";// 注册默认自动添加为好友 的用户手机号
	
	private String defaultGroups = "";

	private byte isOpenSMSCode = 1; //是否开启短信验证码 1：开启 0：关闭
	
	private byte isOpenReceipt = 1;//是否启用 消息回执1：开启 0：关闭


	
	private byte isOpenOnlineStatus=0;//是否开启在线状态1：开启 0：关闭
	
	private byte isOpenCluster;// 是否开启集群 1：开启 0：关闭
	
//	private byte isOpenVoip;// 是否打开ios voip推送 1：开启 0：关闭

	private byte isOpenGoogleFCM;// 是否打开Android Google推送 1：开启 0：关闭

	private int maxCrowdNumber = 2000; // 所有群最大群人数

	private int adminMaxNumber = 50; // 最多可以设置管理员的个数,0:表示没有限制

	private byte isDelWithdrawMsg = 1;// 是否开启物理删除撤回的消息 1：开启 0：关闭

	@Transient
	private String ipAddress;// 当前请求的ip地址
	
	//以下为版本更新的字段
	private int androidVersion;  //Android 版本号
	
	private int iosVersion;  //ios版本号
	
	private String androidAppUrl;  //Android App的下载地址
	
	private String iosAppUrl;    // IOS App 的下载地址
	
	private String androidExplain; //Android 说明
	
	private String iosExplain;   // ios 说明

	private String roomNotice; // 注册是否默认创建群组的群公告

	/**
	 *  视界评论
	 *  0 关闭
	 *  1 开启
	 **/
	private byte banComment=0;

	/**
	 * 资源数据库url
	 **/
	private String resourceDatabaseUrl;


	/**
	 * 是否显示水印开关
	 * 1=显示水印, 0=关闭水印  用户建群时
	 **/
	private byte showMarker = 0;


	private String virtualDeposit = "100,500,1000,1500,2000,3000,";   // 提现金额区间配置，使用 , 进行分割,需要以 , 结尾以分为单位


	private Integer qps = 10000;   // 服务端 QOS 数量限制

	/**
	 *  对接系统集成接口配置
	 */
	public SystemApiConfig systemApiConfig;


	/**
	 * 发布朋友圈是否审核
	 * 0 不需要审核
	 * 1 需要审核
	 */
	private byte publishMsgWhetherCheck;

	/**
	 * 发布短视频是否审核
	 * 0 不需要审核
	 * 1 需要审核
	 */
	private byte publishMsgLableWhetherCheck;

	/**
	 * 更新用户地理位置频率
	 * 单位 毫秒
	 */
	private long locationModiyTime;

	/**
	 * 后台管理--短信验证码登录登录
	 * 1=打开  0关闭
	 */
	private byte openAdminLoginCode = 0;

	/**
	 * 员工模板地址
	 */
	private String employeeTemplateUrl;

	/**
	 * 部门模板地址
	 */
	private String departmentTemplateUrl;


	private byte googleVerification = 0;

	private byte videoVoice = 0;

	private String registerInviteCodeSetting;

	/**
	 * 禁言时间配置
	 * 默认禁言时间数值
	 */
	private int muteTime = 0;

	/**
	 * 禁言时间单位
	 * 1:秒 2:分 3:时 4:天
	 */
	private byte muteTimeUnit = 3;

	public Config() {
	}

	/**
	 * 获取禁言时间
	 */
	public int getMuteTime() {
		return muteTime;
	}

	/**
	 * 设置禁言时间
	 */
	public void setMuteTime(int muteTime) {
		this.muteTime = muteTime;
	}

	/**
	 * 获取禁言时间单位
	 */
	public byte getMuteTimeUnit() {
		return muteTimeUnit;
	}

	/**
	 * 设置禁言时间单位
	 */
	public void setMuteTimeUnit(byte muteTimeUnit) {
		this.muteTimeUnit = muteTimeUnit;
	}
	
	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}
	
}
