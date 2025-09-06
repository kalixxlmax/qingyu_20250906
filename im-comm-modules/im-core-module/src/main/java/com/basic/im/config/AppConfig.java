package com.basic.im.config;

import com.basic.im.model.Language;
import jdk.nashorn.internal.objects.annotations.Getter;
import jdk.nashorn.internal.objects.annotations.Setter;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
@Configuration
@ConfigurationProperties(prefix = "appconfig")
public class AppConfig {

    private String uploadDomain = "http://upload.server.com";//上传服务器域名

    private String apiKey;

    private int chatWarningKeywordNum = 5; //单聊普通敏感词预警敏感词数量

    private int chatWarningNotwordNum = 3; //单聊否词预警敏感词数量

    private int groupWarningKeywordNum = 5; //群聊普通敏感词预警敏感词数量

    private int groupWarningNotwordNum = 3; //群聊否词词预警敏感词数量

    private List<Language> languages; //语言

    private String buildTime;


    //注册是否默认创建群组
    private byte registerCreateRoom;

   //private String roomNotice;

    /**
     * ip 数据库目录
     */
    private String qqzengPath;

    private int openTask = 1; //是否开启定时任务

    private int distance = 20;

    /**
     * 余额加密版本
     * 0 兼用老版本   有老用户 版本升级使用
     * balanceSafe 加密字段为空  取 balance 加密保存
     *
     * 1 新版 加密   balanceSafe 为空  余额为 0
     * 新版 没有老用户 版本使用
     */
    private byte balanceVersion = 1;
    /*
     *
     * 禁用附近的人,1:禁用 0开启
     * 内部系统版本配置,严禁修改
     *
     * */
    private byte disableNearbyUser = 0;

    /**
     * 内部系统版本配置
     * 严禁修改
     */
    private String phone = "";


    private String flagPhone = "";

    /**
     * 内部系统版本配置
     * 严禁修改
     */
    private String servicePhones;

    /**
     * 内部系统版本配置
     * 严禁修改
     */
    private Set<Integer> serviceNoList = new HashSet<>();


    private byte isBeta = 0;//是否测试版本  测试版本 附近的人和 所有房间不返回值

    private byte isDebug = 1;//是否开启调试  打印日志用到

    //是否开启清除 admin token，开启后在项目启动时会清除redis里存的 admin token (admin token 用于管理后台、公众号页面、开放平台)
    private byte openClearAdminToken = 0;

    private String wxChatUrl;// 微信公众号群聊网页路径

    private byte isReturnSmsCode = 0;// 是否开启返回短信验证码
    /**
     * 每天注册多少用户通知提醒系统默认好友
     */
    private int registerUserNoticeNum = 0;

    private int version = 0;

}
