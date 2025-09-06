package com.basic.im.entity;

import io.swagger.annotations.ApiModel;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @Author xie yuan yang
 * @Date 2020/5/29
 **/
@ApiModel("推送配置实体")
@Data
@Document("pushConfig")
public class PushConfig {

    @Id
    private int id;

    private String packageName;// 安卓包名

    private String appStoreAppId;// appStore 版本App包名 (IOS个人版)

    private String betaAppId;// 企业版 app 包名(IOS企业版)

    private PushConfig.AndroidPush androidPush;// 安卓推送配置

    private PushConfig.IOSPush iosPush;// IOS推送配置

    /**
     * AndroidPush 安卓推送配置
     */
    @Data
    @Accessors(chain = true)
    public static class AndroidPush {

        // 小米
        private String xmAppSecret;
        // 是否创建自定义渠道
        private int xmChannle;

        // 华为
        private String hwAppSecret;
        private String hwAppId;
        private String hwTokenUrl;
        private String hwApiUrl;
        private String hwIconUrl;
        // 是否使用单独部署华为推送   1是    0：否
        private byte isOpen;
        // 服务器地区 例  CN、HK
        private String serverAdress="CN";

        // 极光
        private String jPushAppKey;
        private String jPushMasterSecret;

        // google FCM
        private String fcmDataBaseUrl;
        private String fcmKeyJson;

        // 魅族
        private String mzAppSecret;
        private long mzAppId;

        // VIVO
        private int vivoAppId;
        private String vivoAppKey;
        private String vivoAppSecret;

        // OPPO
        private String oppoAppKey;
        private String oppoMasterSecret;
    }

    /**
     * IOSPush ios推送配置
     */
    @Data
    @Accessors(chain = true)
    public static class IOSPush {

        private String appStoreAppId;// appStore 版本App包名 (IOS个人版)

        private String betaAppId;// 企业版 app 包名(IOS企业版)

        // appStore apns 推送证书 (IOS个人版)
        private String appStoreApnsPk;

        // 企业版 测试版 apns 推送证书
        private String betaApnsPk;

        // 证书 密码
        private String pkPassword;

        // 沙箱模式
        private byte isApnsSandbox = 0;

        // 调试模式  打印 log
        private byte isDebug = 0;

        // 百度
        private String bdAppStoreAppId;
        private String bdAppStoreAppKey;
        private String bdAppStoreSecretKey;
        private String[] bdAppKey;
        private String bdRestUrl;
        private String[] bdSecretKey;

        // 极光
        private String jPushAppKey;
        private String jPushMasterSecret;
    }


}
