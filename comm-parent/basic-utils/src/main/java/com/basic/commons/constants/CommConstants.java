package com.basic.commons.constants;

public class CommConstants  {
    public static boolean isDebug=true;

    public static final String PAGE_INDEX = "0";
    public static final String PAGE_SIZE = "15";

    public static final int MOENY_ADD = 1; //金钱增加
    public static final int MOENY_REDUCE = 2; //金钱减少
    public static final double LBS_KM=111.01;
    public static final int LBS_DISTANCE=50;

   


    /**
             * 用户ID 起始值
     */
    public static final int MIN_USERID=100000;
    /**
         * 数据库分表 取余  计算值
     * @author chat 
     *
     */
    public interface DB_REMAINDER{
       /**
                       *默认
         */
        public static final int DEFAULT=10000;
    }
   


    //订单状态
    public interface OrderStatus {
        public static final int CREATE = 0;// 创建
        public static final int END = 1;// 成功
        public static final int DELETE = -1;// 删除
    }
    //支付方式
    public interface PayType {
        public static final int ALIPAY = 1;// 支付宝支付
        public static final int WXPAY = 2;// 微信支付
        public static final int BALANCEAY = 3;// 余额支付
        public static final int SYSTEMPAY = 4;// 系统支付
    }
    

    //public static final KServiceException InternalException = new KServiceException(KConstants.ErrCode.InternalException,KConstants.ResultMsg.InternalException);

    public interface Expire {
        static final int DAY1 = 86400;
        static final int DAY7 = 604800;
        static final int HOUR12 = 43200;
        static final int HOUR=3600;
    }



    public interface ResultCode {

        //接口调用成功
        static final int Success = 1;

        //接口调用失败
        static final int Failure = 0;

        //请求参数验证失败，缺少必填参数或参数错误
        static final int ParamsAuthFail = 1010101;

        //缺少请求参数：
        static final int ParamsLack = 1010102;

        //接口内部异常
        static final int InternalException = 1020101;

        //链接已失效
        static final int Link_Expired = 1020102;

        //缺少访问令牌
        static final int TokenEillegal = 1030101;

        //访问令牌过期或无效
        static final int TokenInvalid = 1030102;

        //权限验证失败
        static final int AUTH_FAILED = 1030103;

        //帐号不存在
        static final int AccountNotExist = 1040101;

        //帐号或密码错误
        static final int AccountOrPasswordIncorrect = 1040102;

        //原密码错误
        static final int OldPasswordIsWrong = 1040103;

        //短信验证码错误或已过期
        static final int VerifyCodeErrOrExpired = 1040104;

        //发送验证码失败,请重发!
        static final int SedMsgFail = 1040105;

        //请不要频繁请求短信验证码，等待{0}秒后再次请求
        static final int ManySedMsg = 1040106;

        //手机号码已注册!
        static final int PhoneRegistered = 1040107;

        //余额不足
        static final int InsufficientBalance = 1040201;


        //请输入图形验证码
        static final int NullImgCode=1040215;

        //图形验证码错误
        static final int ImgCodeError=1040216;

        //没有选择支付方式!
        static final int NotSelectPayType = 1040301;

        //支付宝支付后回调出错：
        static final int AliPayCallBack_FAILED = 1040302;

        //你没有权限删除!
        static final int NotPermissionDelete = 1040303;

        //账号被锁定
        static final int ACCOUNT_IS_LOCKED = 1040304;

        // 第三方登录未绑定手机号码
        static final int UNBindingTelephone = 1040305;

        // 第三方登录提示账号不存在
        static final int SdkLoginNotExist = 1040306;

    }
}
