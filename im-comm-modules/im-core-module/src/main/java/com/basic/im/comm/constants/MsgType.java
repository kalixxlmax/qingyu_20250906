package com.basic.im.comm.constants;

import java.util.Arrays;
import java.util.List;

/**
* @Description: TODO(消息类型常量)
* @author lidaye
* @date 2018年2月24日 
*/
public interface MsgType {
	////////////////////////////以下为在聊天界面显示的类型/////////////////////////////////
	 int TYPE_TEXT = 1; // 文字
	 int TYPE_IMAGE = 2;// 图片
	 int TYPE_VOICE = 3;// 语音
	 int TYPE_LOCATION = 4; // 位置
	 int TYPE_GIF = 5;  // gif
	 int TYPE_VIDEO = 6;// 视频
	 int TYPE_SIP_AUDIO = 7;// 音频
	 int TYPE_CARD = 8;// 名片
	 int TYPE_FILE = 9;// 文件
	 int TYPE_TIP = 10;// 自己添加的消息类型,代表系统的提示

	 int TYPE_READ = 26;    // 是否已读的回执类型

	 int TYPE_RED = 28;     // 红包消息
	 int TYPE_TRANSFER = 29;// 转账消息

	 int TYPE_DICE = 40;// 骰子

	 int TYPE_RPS = 41;// 石头剪刀布
	 //分享朋友圈动态
	 int TYPE_SHARE_VLOG = 46;
	 int TYPE_SHARE_CONTENT = 47;//分享小程序
	 int TYPE_RICH_TEXT_MALL = 75;//商城富文本消息
	 int TYPE_RICH_TEXT = 77;//富文本消息
	 int MANUAL_RECHARGE = 78;// 扫码手动充值-后台审核后的通知
	 int MANUAL_WITHDRAW = 79;// 扫码手动提现-后台审核后的通知
	 int TYPE_IMAGE_TEXT = 80;     // 单条图文消息
	 int TYPE_IMAGE_TEXT_MANY = 81;// 多条图文消息
	 int TYPE_LINK = 82; // 链接
	 int TYPE_SHARE_LINK = 87; // 分享进来的链接
	 int TYPE_83 = 83;   // 某个成员领取了红包
	 int TYPE_SHAKE = 84;  // 戳一戳
	 int TYPE_CHAT_HISTORY = 85;  // 聊天记录
	 int TYPE_RED_BACK = 86;  // 红包退回通知
	 int TYPE_RECEIVETRANSFER = 88;// 转账领取
	 int TYPE_REFUNDTRANSFER = 89;// 转账退回
	 int CODEPAYMENT = 90;// 付款码已付款通知
	 int CODEARRIVAL = 91;// 付款码已到账通知
	 int CODERECEIPT = 92;// 二维码收款已付款通知
	 int CODEERECEIPTARRIVAL = 93;// 二维码收款已到账通知

	  int TYPE_REPLY = 94;//回复消息

	 int TYPE_SEND_ONLINE_STATUS = 200;// 在线情况
	 int TYPE_INPUT = 201;// 正在输入消息
	 int TYPE_BACK = 202; // 撤回消息
	 int TYPE_EDIT = 203; // 编辑消息

	////////////////////////////音视频通话/////////////////////////////////
	 int TYPE_IS_CONNECT_VOICE = 100;// 发起语音通话
	 int TYPE_CONNECT_VOICE = 102;// 接听语音通话
	 int TYPE_NO_CONNECT_VOICE = 103;// 拒绝语音通话 || 对来电不响应(30s)内
	 int TYPE_END_CONNECT_VOICE = 104;// 结束语音通话

	 int TYPE_IS_CONNECT_VIDEO = 110;// 发起视频通话
	 int TYPE_CONNECT_VIDEO = 112;// 接听视频通话
	 int TYPE_NO_CONNECT_VIDEO = 113;// 拒绝视频通话 || 对来电不响应(30s内)
	 int TYPE_END_CONNECT_VIDEO = 114;// 结束视频通话

	 int TYPE_IS_MU_CONNECT_Video = 115;// 视频会议邀请
	 int TYPE_IS_MU_END_CONNECT_VIDEO = 119;// 视频会议结束了
	 int TYPE_IS_MU_CONNECT_VOICE = 120;// 音频会议邀请

	 int TYPE_IN_CALLING = 123;// 通话中...
	 int TYPE_IS_BUSY = 124;// 忙线中...

	// 暂未用到
	 int TYPE_VIDEO_IN = 116;            // 视频会议进入
	 int TYPE_VIDEO_OUT = 117;           // 视频会议退出
	 int TYPE_OK_MU_CONNECT_VOICE = 121; // 音频会议进入了
	 int TYPE_EXIT_VOICE = 122;          // 音频会议退出了

	 int TYPE_IS_MU_END_CONNECT_VOICE = 129;// 音频会议结束了
	 int TYPE_IS_MU_CONNECT_TALK = 130;   // 发起对讲机
	 int TYPE_IS_SCREEN_SHARING = 140;   // 发起屏幕共享

	//----------朋友圈消息-----------------------------
	 int DIANZAN = 301; // 朋友圈点赞
	 int PINGLUN = 302; // 朋友圈评论
	 int ATMESEE = 304; // 提醒我看

	//---------我的同事模块相关消息-----------------
	int COMPANY_APPLY_JOIN = 320; //申请加入公司
	int COMPANY_INVITE_USER_JOIN = 321; //邀请用户加入公司通知

	//-------------新朋友消息----------------------
	 int TYPE_SAYHELLO = 500;// 打招呼
	 int TYPE_PASS = 501;    // 同意加好友
	 int TYPE_FEEDBACK = 502;// 回话
	 int TYPE_FRIEND = 508;//   直接成为好友
	 int TYPE_BLACK = 507; //   黑名单
	 int TYPE_REFUSED = 509;//  取消黑名单
	 int TYPE_DELALL = 505;//   彻底删除
	 int TYPE_CONTACT_BE_FRIEND = 510;   // 对方通过 手机联系人 添加我 直接成为好友
	 int TYPE_NEW_CONTACT_REGISTER = 511;// 我之前上传给服务端的联系人表内有人注册了，更新 手机联系人
	 int TYPE_REMOVE_ACCOUNT = 512;// 用户被后台删除，用于客户端更新本地数据 ，from是系统管理员 to是被删除人的userId，

	// 未用到
	 int TYPE_NEWSEE = 503;// 新关注
	 int TYPE_DELSEE = 504;// 删除关注
	 int TYPE_RECOMMEND = 506;// 新推荐好友

	//--------------群组协议------------------------
	 int TYPE_MUCFILE_ADD = 401; // 群文件上传
	 int TYPE_MUCFILE_DEL = 402; // 群文件删除
	 int TYPE_MUCFILE_DOWN = 403;// 群文件下载

	 int TYPE_CHANGE_NICK_NAME = 901; // 修改昵称
	 int TYPE_CHANGE_ROOM_NAME = 902; // 修改房间名
	 int TYPE_DELETE_ROOM = 903;// 删除房间
	 int TYPE_DELETE_MEMBER = 904;// 退出、被踢出群组
	 int TYPE_NEW_NOTICE = 905; // 新公告
	 int TYPE_GAG = 906;// 禁言/取消禁言
	 int NEW_MEMBER = 907; // 增加新成员
	 int TYPE_SEND_MANAGER = 913;// 设置/取消管理员

	 int TYPE_CHANGE_SHOW_READ = 915; // 设置群已读消息
	 int TYPE_GROUP_VERIFY = 916; // 群组验证消息
	 int TYPE_GROUP_LOOK = 917; // 群组是否公开
	 int TYPE_GROUP_SHOW_MEMBER = 918; // 群组是否显示群成员列表
	 int TYPE_GROUP_SEND_CARD = 919; // 群组是否允许发送名片
	 int TYPE_GROUP_ALL_SHAT_UP = 920; // 全体禁言
	 int TYPE_GROUP_ALLOW_NORMAL_INVITE = 921; // 允许普通成员邀请人入群
	 int TYPE_GROUP_ALLOW_NORMAL_UPLOAD = 922; // 允许普通成员上传群共享
	 int TYPE_GROUP_ALLOW_NORMAL_CONFERENCE = 923; // 允许普通成员发起会议
	 int TYPE_GROUP_ALLOW_NORMAL_SEND_COURSE = 924;// 允许普通成员发送讲课
	 int TYPE_GROUP_TRANSFER = 925; // 转让群组

	 int TYPE_UPDATE_ROLE = 930;// 设置/取消隐身人，监控人，
	 int TYPE_DISABLE_GROUP = 931;// 群组被后台锁定/解锁

	////////////////////////////直播协议/////////////////////////////////
	 int TYPE_SEND_DANMU = 910;// 弹幕
	 int TYPE_SEND_GIFT = 911; // 礼物
	 int TYPE_SEND_HEART = 912;// 点赞
	 int TYPE_SEND_ENTER_LIVE_ROOM = 914;// 加入直播间
	// 以前直播间和群组共用了部分协议，现独立出来
	 int TYPE_LIVE_LOCKING = 926; // 锁定直播间(后台可锁定用户直播间)
	 int TYPE_LIVE_EXIT_ROOM = 927;// 退出、被踢出直播间
	 int TYPE_LIVE_SHAT_UP = 928;// 禁言/取消禁言
	 int TYPE_LIVE_SET_MANAGER = 929;// 设置/取消管理员

	/**
	 * 是否禁用群成员修改名片功能
	 **/
	 int TYPE_MODITY_ROOM_ALLOWMODIFYCARD = 944;

	/**
	 * 刷新聊天背景水印
	 **/
	int TYPE_MODITY_SHOWMARKER = 945;


	 List<Integer> FileTypeArr =
			Arrays.asList(TYPE_IMAGE, TYPE_VOICE,
					TYPE_GIF, TYPE_VIDEO, TYPE_FILE);
}

