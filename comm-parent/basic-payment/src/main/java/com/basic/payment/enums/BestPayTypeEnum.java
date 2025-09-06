package com.basic.payment.enums;


import static com.basic.payment.enums.PayChannelEnum.PAY_CHANNEL_ALIPAY;
import static com.basic.payment.enums.PayChannelEnum.PAY_CHANNEL_WXPAY;

/**
 * description: 支付方式 枚举定义 <br>
 * date: 2020/6/28 0028  <br>
 * author: chat  <br>
 * version: 1.0 <br>
 */
public enum BestPayTypeEnum {

    /**
     * 支付方式
     */

    ALIPAY_APP("alipay_app", PAY_CHANNEL_ALIPAY, "支付宝app"),

    ALIPAY_PC("alipay_pc", PAY_CHANNEL_ALIPAY, "支付宝pc"),

    ALIPAY_WAP("alipay_wap", PAY_CHANNEL_ALIPAY, "支付宝wap"),

    ALIPAY_H5("alipay_h5", PAY_CHANNEL_ALIPAY, "支付宝统一下单(h5)"),

    WXPAY_MP("JSAPI", PAY_CHANNEL_WXPAY,"微信公众账号支付"),

    WXPAY_MWEB("MWEB", PAY_CHANNEL_WXPAY, "微信H5支付"),

    WXPAY_NATIVE("NATIVE", PAY_CHANNEL_WXPAY, "微信Native支付"),

    WXPAY_MINI("JSAPI", PAY_CHANNEL_WXPAY, "微信小程序支付"),

    WXPAY_APP("APP", PAY_CHANNEL_WXPAY, "微信APP支付"),
    ;

    private String code;

    private PayChannelEnum channelEnum;

    private String desc;

    BestPayTypeEnum(String code, PayChannelEnum channelEnum, String desc) {
        this.code = code;
        this.channelEnum = channelEnum;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public PayChannelEnum getChannelEnum() {
        return channelEnum;
    }

    public String getDesc() {
        return desc;
    }

    public static BestPayTypeEnum getByName(String code) {
        for (BestPayTypeEnum bestPayTypeEnum : BestPayTypeEnum.values()) {
            if (bestPayTypeEnum.name().equalsIgnoreCase(code)) {
                return bestPayTypeEnum;
            }
        }
        throw new RuntimeException("pay type error");
    }
}
