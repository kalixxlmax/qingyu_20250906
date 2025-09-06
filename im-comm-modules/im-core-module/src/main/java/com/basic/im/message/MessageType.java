package com.basic.im.message;

import java.util.HashSet;
import java.util.Set;

public interface MessageType {

    /**
     * 被邀请加入群组
     */
    int inviteJoinRoom = 45;

    //分享朋友圈动态
    int TYPE_SHARE_VLOG = 46;

    //扫码手动充值-后台审核后的通知
    //{
    // "type":78
    // "fromUserId":""
    // "fromUserName":""
    // "content":""
    // "timeSend":""
    //
    int MANUAL_RECHARGE = 78;

    //扫码手动提现-后台审核后的通知
    //{
    // "type":79
    // "fromUserId":""
    // "fromUserName":""
    // "content":""
    // "timeSend":""
    //
    int MANUAL_WITHDRAW = 79;

    //收红包
    //{
    //  "type":83
    //	"fromUserId":""
    //	"fromUserName":""
    //	"ObjectId":"如果是群聊，则为房间Id"
    //	"timeSend":123
    int OPENREDPAKET = 83;

    // 红包退款
    // {
    //	 "type":86
    //   "fromUserId":""
    //   "fromUserName":""
    // 	 "ObjectId":"如果是群聊，则为房间Id"
    //	 "timeSend":123
    int RECEDEREDPAKET = 86;

    // 转账收款
    // {
    //    "type":88
    //    "fromUserId":""
    //    "fromUserName":""
    //    "ObjectId":""
    //    "timeSend":123
    int RECEIVETRANSFER = 88;

    // 转账退回
    // {
    //    "type":89
    //    "fromUserId":""
    //    "fromUserName":""
    //    "ObjectId":""
    //    "timeSend":123
    int REFUNDTRANSFER = 89;

    // 付款码已付款通知
    // {
    //    "type":90
    //    "fromUserId":""
    //    "fromUserName":""
    //	  "ObjectId":""
    //    "timeSend":123
    int CODEPAYMENT = 90;

    // 付款码已到账通知
    // {
    //    "type":91
    //    "fromUserId":""
    //    "fromUserName":""
    //	  "ObjectId":""
    //    "timeSend":123
    int CODEARRIVAL = 91;

    // 二维码收款已付款通知
    // {
    //    "type":91
    //    "fromUserId":""
    //    "fromUserName":""
    //	  "ObjectId":""
    //    "timeSend":123
    int CODERECEIPT = 92;

    // 二维码收款已到账通知
    // {
    //    "type":93
    //    "fromUserId":""
    //    "fromUserName":""
    //	  "ObjectId":""
    //    "timeSend":123
    int CODEERECEIPTARRIVAL = 93;

    //回复消息
    int TYPE_REPLY=94;


    /**
      销毁聊天消息
     好友通过发送此协议来双向清除聊天记录、群主通过此协议来删除群聊记录、后台管理员通过此协议来删除所有数据

     fromUserId(操作者id)，fromUserName(好友昵称|群内操作者昵称|后台管理员)，objectId(如果是群聊则是群组jid)，
     content(为空则是清除聊天记录单聊或群聊、“claer_all”，则是清除所有数据)，isGroup=1则是群聊
     */
    int TYPE_DESTROY_MESSAGE=96;

    // 第三方应用调取IM支付成功通知
    int OPENPAYSUCCESS = 97;

    //上传文件
    //{
    //"type":401,
    //"content":"文件名",
    //"fromUserId":"上传者",
    //"fromUserName":"",
    //"ObjectId":"文件Id"
    //"timeSend":123
    //}
    int FILEUPLOAD = 401;

    //删除文件
    //{
    //"type":402,
    //"content":"文件名",
    //"fromUserId":"删除者",
    //"fromUserName":"",
    //"ObjectId":"文件Id",
    //"timeSend":123
    //}
    int DELETEFILE = 402;

    /**
     * 敏感词通知，单聊群聊都有发
     */
    int sensitiveWordsNotice = 518;


    /**
     * 后台删除好友（客户端自己封装的xmpp，这里用于后台的用户管理删除好友）
     * {
     * fromUserId:10005
     * "type":515
     * "objectId": 封装 fromUserId  toUserId 用于接收系统号发送xmpp消息的用户
     * }
     */
    int deleteFriends = 515;

    /**
     * 删除用户通知客户端退出app
     */
    int deleteUser = 516;

    /**
     * 后台加入黑名单（客户端自己封装的xmpp，这里用于后台的用户管理中的加入黑名单）
     * {
     * fromUserId:10005
     * "type":513
     * "objectId": 封装 fromUserId  toUserId 用于接收系统号发送xmpp消息的用户
     * }
     */
    int joinBlacklist = 513;

    /**
     * 后台移除黑名单（客户端自己封装的xmpp，这里用于后台的用户管理中的移除黑名单）
     * {
     * fromUserId:10005
     * "type":514
     * "objectId": 封装 fromUserId  toUserId 用于接收系统号发送xmpp消息的用户
     * }
     */
    int moveBlacklist = 514;

    /**
     * 通讯录批量添加好友
     * {
     * fromUserId:我的新通讯录好友
     * "type":510
     * "toUserId": 我
     * }
     */
    int batchAddFriend = 510;

    /**
     * 用户注册后更新通讯录好友
     * {
     * fromUserId:我的新通讯录好友
     * "type":511
     * "toUserId": 我
     * }
     */
    int registAddressBook = 511;

    /**
     * 后台删除用户用于客户端更新本地数据
     * {
     * fromUserId:系统用户
     * "type":512
     * "toUserId": 被删除用户的所有好友Id
     * "objectId" ： 被删除人的id
     * }
     */
    int consoleDeleteUsers = 512;


    /**
     * 拉黑好友、陌生人
     */
    int ADD_FRIENDS_BLACK_USER = 507;


    /**
     * 取消拉黑 好友、陌生人
     */
    int CANCEL_FRIENDS_BLACK_USER = 509;

    /**
     * 开始讲解商品
     */
    int START_EXPLAIN_PRODUCT = 650;

    /**
     * 结束讲解商品
     */
    int END_EXPLAIN_PRODUCT = 651;


    /**
     * 多点登录用户相关操作用于同步数据
     * {
     * fromUserId:自己
     * "type":800
     * "toUserId": 自己
     * "other": 0：修改密码，1：设置支付密码，2：用户隐私设置
     * }
     */
    int multipointLoginDataSync = 800;

    /**
     * 多点登录更新个人资料
     * {
     * fromUserId:自己
     * "type":801
     * "toUserId": 自己
     * }
     */
    int updatePersonalInfo = 801;


    /**
     * 多点登录更新群组相关信息
     * {
     * fromUserId:自己
     * "type":801
     * "toUserId": 自己
     * "objectId": "房间Id",
     * }
     */
    int updateRoomInfo = 802;

    /**
     * 给好友发送更新公钥的xmpp 消息 803
     */
    int updateFriendsEncryptKey = 803;

    /**
     * 变更与好友的消息加密方式
     */
    int updateFriendEncryptType = 813;

    /**
     * 授权消息通知
     */
    int AUTHLOGINDEVICE = 810;

    /**
     * 后台管理封号
     */
    int CONSOLELOCKUSER = 811;


    // 修改昵称
    // {
    // "type": 901,
    // "objectId": "房间Id",
    // "fromUserId": 10005,
    // "fromUserName": "10005",
    // "toUserId": 用户Id,
    // "toUserName": "用户昵称",
    // "timeSend": 123
    // }
    int CHANGE_NICK_NAME = 901;

    // 修改房间名
    // {
    // "type": 902,
    // "objectId": "房间Id",
    // "content": "房间名",
    // "fromUserId": 10005,
    // "fromUserName": "10005",
    // "timeSend": 123
    // }
    int CHANGE_ROOM_NAME = 902;

    // 删除成员
    // {
    // "type": 904,
    // "objectId": "房间Id",
    // "fromUserId": 0,
    // "fromUserName": "",
    // "toUserId": 被删除成员Id,
    // "timeSend": 123
    // }
    int DELETE_MEMBER = 904;
    // 删除房间
    // {
    // "type": 903,
    // "objectId": "房间Id",
    // "content": "房间名",
    // "fromUserId": 10005,
    // "fromUserName": "10005",
    // "timeSend": 123
    // }
    int DELETE_ROOM = 903;
    // 禁言
    // {
    // "type": 906,
    // "objectId": "房间Id",
    // "content": "禁言时间",
    // "fromUserId": 10005,
    // "fromUserName": "10005",
    // "toUserId": 被禁言成员Id,
    // "toUserName": "被禁言成员昵称",
    // "timeSend": 123
    // }
    int GAG = 906;
    // 新成员
    // {
    // "type": 907,
    // "objectId": "房间Id",
    // "fromUserId": 邀请人Id,
    // "fromUserName": "邀请人昵称",
    // "toUserId": 新成员Id,
    // "toUserName": "新成员昵称",
    // "content":"是否显示阅读人数",  1:开启  0：关闭
    // "timeSend": 123
    // }
    int NEW_MEMBER = 907;
    // 新公告
    // {
    // "type": 905,
    // "objectId": "房间Id",
    // "content": "公告内容",
    // "fromUserId": 10005,
    // "fromUserName": "10005",
    // "timeSend": 123
    // }
    int NEW_NOTICE = 905;
    //用户离线
    //
    //{
    // "type": 908,
    // "userId":"用户ID"
    // "name":"用户昵称"
    // "coment":"用户离线"
    //}
    int OFFLINE = 908;
    //用户上线
    //{
    // "type": 909,
    // "userId":"用户ID"
    // "name":"用户昵称"
    // "coment":"用户上线"
    //}
    int ONLINE = 909;

    //弹幕
    //{
    //	"type":910,
    //	"formUserId":"用户ID"
    //	"fromUserName":"用户昵称"
    //	"content":"弹幕内容"
    //	"timeSend": 123
    //}
    int BARRAGE = 910;

    //送礼物
    //{
    //	"type":911
    //	"fromUserId":"用户ID"
    //	"fromUserName":"用户昵称"
    //	"content":"礼物"
    //	"timeSend":123
    //}
    int GIFT = 911;

    //直播点赞
    //{
    //	"type":912
    //	}
    int LIVEPRAISE = 912;

    //设置管理员
    //{
    //	"type":913
    //	"fromUserId":"发送者Id"
    //	"fromUserName":"发送者昵称"
    //	"content":"1为启用  0为取消管理员"
    // 	"timeSend":123
    //}
    int SETADMIN = 913;

    //进入直播间
    // {
    //	"type":914
    //	"fromUserId":"发送者Id"
    //	"fromUserName":"发送者昵称"
    //	"objectId":"房间的JID"
    //	"timeSend":123
    //}
    int JOINLIVE = 914;


    /**
     * 显示阅读人数
     * {
     * "type":915
     * "objectId":"房间JId"
     * "content":"是否显示阅读人数" 1：开启 2：关闭
     * }
     */
    int SHOWREAD = 915;

    /**
     * 群组是否需要验证
     * {
     * "type":916
     * "objectId":"房间JId"
     * "content": 1：开启验证   0：关闭验证
     * }
     */
    int RoomNeedVerify = 916;

    /**
     * 房间是否公开
     * {
     * "type":917
     * "objectId":"房间JId"
     * "content": 1：不公开 隐私群   0：公开
     * }
     */
    int RoomIsPublic = 917;

    /**
     * 普通成员 是否可以看到 群组内的成员
     * 关闭 即普通成员 只能看到群主
     * {
     * "type":918
     * "objectId":"房间JId"
     * "content": 1：可见   0：不可见
     * }
     */
    int RoomShowMember = 918;
    /**
     * 群组允许发送名片
     * {
     * "type":919
     * "objectId":"房间JId"
     * "content": 1：   允许发送名片   0：不允许发送
     * }
     */
    int RoomAllowSendCard = 919;

    /**
     * 群组全员禁言
     * {
     * "type":920
     * "objectId":"房间JId"
     * "content": tailTime   禁言截止时间
     * }
     */
    int RoomAllBanned = 920;

    /**
     * 群组允许成员邀请好友
     * {
     * "type":921
     * "objectId":"房间JId"
     * "content": 1：  允许成员邀请好友   0：不允许成员邀请好友
     * }
     */
    int RoomAllowInviteFriend = 921;

    /**
     * 群组允许成员上传群共享文件
     * {
     * "type":922
     * "objectId":"房间JId"
     * "content": 1：  允许成员上传群共享文件   0：不允许成员上传群共享文件
     * }
     */
    int RoomAllowUploadFile = 922;
    /**
     * 群组允许成员召开会议
     * <p>
     * {
     * "type":923
     * "objectId":"房间JId"
     * "content": 1：  允许成员召开会议   0：不允许成员召开会议
     * }
     */
    int RoomAllowConference = 923;

    /**
     * 群组允许成员开启 讲课
     * {
     * "type":924
     * "objectId":"房间JId"
     * "content": 1：  允许成员开启 讲课   0：不允许成员开启 讲课
     * }
     */
    int RoomAllowSpeakCourse = 924;
    /**
     * 群组转让 接口
     * {
     * fromUserId:旧群主ID
     * "type":925
     * "objectId":"房间JId"
     * "toUserId": 新群组用户ID
     * }
     */
    int RoomTransfer = 925;

    /**
     * 房间是否锁定
     * {
     * "type":926
     * "objectId":"房间JId"
     * "content": 1：锁定房间   0：解锁房间
     * }
     */
    int RoomDisable = 926;

    /**
     * 直播间中退出、被踢出直播间
     * {
     * "type":927
     * "objectId":"房间JId"
     * "content": 退出被踢出直播间
     * }
     */
    int LiveRoomSignOut = 927;

    /**
     * 直播间中的禁言、取消禁言
     * {
     * "type":928
     * "objectId":"房间JId"
     * "content": 0：禁言，1：取消禁言
     * }
     */
    int LiveRoomBannedSpeak = 928;

    /**
     * 直播间中设置、取消管理员
     * {
     * "type":929
     * "objectId":"房间JId"
     * "content": 0:设置管理员  1:取消管理员
     * }
     */
    int LiveRoomSettingAdmin = 929;

    /**
     * 群组中设置 隐身人和监控人
     * {
     * "type":930
     * "objectId":"房间JId"
     * "content": 1:设置隐身人  -1:取消隐身人，2：设置监控人，0：取消监控人
     * }
     */
    int SetRoomSettingInvisibleGuardian = 930;

    /**
     * 后台锁定、取消锁定群组
     * {
     * fromUserId:系统用户
     * "type":931
     * "content":1：解锁，-1：锁定
     * "objectId" ： roomJid
     * }
     */
    int consoleProhibitRoom = 931;

    /**
     * 聊天记录超时设置
     * {
     * "type":932
     * "objectId":"房间JId"
     * "content": 1.0:保存一天  -1:永久保存  365.0保存一年
     * }
     */
    int ChatRecordTimeOut = 932;

    /**
     * {
     * "type":933
     * "objectId":"房间JId"
     * "content": 1
     * }
     */
    int LocationRoom = 933;

    /**
     * 修改群公告
     * {
     * "type":934
     * "objectId":"房间JId"
     * "content": notice
     * }
     */
    int ModifyNotice = 934;

    /**
     * 修改群组加密类型
     * {
     * "type":935
     * "objectId":"房间JId"
     * "content": encryptType
     * }
     */
    int ModifyEncryptType = 935;

    /**
     * 用户离线移出直播间
     * {
     * "type":936
     * "objectId":"房间JId"
     * "content": encryptType
     * "other":"创建者Id"
     * }
     */
    int RemoveLiveRoom = 936;

    /**
     * 群组内群成员开启直播间通知其他群成员
     */
    int OpenLiveRoom = 937;

    /**
     * 关闭群组内直播
     */
    int CloseLiveRoom = 938;

    /**
     * 群主或群管理员关闭直播间
     */
    int CloseLiveRoomByAdmin = 939;

    /**
     * 修改直播间的名称或公告
     */
    int UpdateLiveRoomNameOrNotive = 940;

    /**
     * 是否允许群成员开启群组直播
     */
    int IsOPenLiveRoomByMember = 941;



    /**
     * 群拉黑用户
     */
    int BLACK_USER = 943;

    /**
     * 删除群组公告
     */
    int DELETE_NOTICE = 946;

    // 敏感词触发禁言
    int KEEP_SILENT = 956;

    /**
     * 刷新服务器系统配置
     */
    int REFRESH_SYSCONFIG = 1000;

    // 点赞
    //{
    //	"type":301
    //
    //
    //}
    int PRAISE = 301;

    // 评论
    //{
    //	"type":302
    //}
    int COMMENT = 302;

    // 取消点赞
    //{
    //	"type":303
    //
    //
    //}
    int CANCELPRAISE = 303;


    //朋友圈的提醒
    //{
    //"type":304
    //}
    int REMIND = 304;

    /**
     * 生活圈新消息
     */
    int MSG_NEW = 305;

    /**
     *
     *--------------
     * -客服相关消息-
     * -------------
     */

    /**
     * 访客状态通知
     * content:0,1,2,3
     * 0:离开 1：访问 2:首次咨询
     */
    int VISITOR_STATUS_NOTICE=700;

    /**
     * 客服转接
     */
    int SERVICE_TRANSFER=705;


    /**
     * 访客新留言
     */
    int CUSTOMER_LEAVE=706;

    /**
     * 留言状态处理改变
     */
    int CUSTOMER_LEAVE_STATUS=707;


    /**
     * 菜单回复消息
     */
    int CUSTOMER_MENU=711;


    /**
     * 超时回复消息
     */
    int CUSTOMER_OUTTIME_MSG=712;

    Set<Integer> liveRoomType = new HashSet<Integer>() {{
        add(BARRAGE);
        add(GIFT);
        add(LIVEPRAISE);
        add(JOINLIVE);
        add(LiveRoomSignOut);
        add(LiveRoomBannedSpeak);
        add(LiveRoomSettingAdmin);
        add(RemoveLiveRoom);
    }};

}
