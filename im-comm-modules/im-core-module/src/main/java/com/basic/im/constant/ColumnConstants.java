package com.basic.im.constant;


public class ColumnConstants {
    // id类常量
    public final static String UNDERLINE_ID    = "_id";
    public final static String ID              = "id";
    public final static String USER_ID         = "userId";
    public final static String ROOM_ID         = "roomId";
    public final static String TO_USER_ID      = "toUserId";
    public final static String RED_PACKET_ID   = "redPacketId";


    // 名称类常量
    public final static String NAME            = "name";
    public final static String COMPANY_NAME    = "companyName";
    public final static String NICK_NAME       = "nickname";
    public final static String TO_NICK_NAME    = "toNickname";

    // 用户模块常用
    public final static String ON_LINE_STATE   = "onlinestate";
    public final static String AGE             = "age";
    public final static String LOC             = "loc";

    // 好友模块常量
    public final static String FROM_ADD_TYPE   = "fromAddType";

    // 动态模块常量
    public final static String VISIBLE         = "visible";
    public final static String STATE           = "state";

    // 金额类常量
    public final static String AMOUNT          = "amount";
    public final static String MONEY           = "money";
    public final static String STATUS          = "status";
    public final static String CHANGE_TYPE     = "changeType";
    public final static String PAY_TYPE        = "payType";
    public final static String IS_VALID        = "isValid";
    public final static String PRICE           = "price";
    public final static String CHARGE_PRICE    = "chargePrice";
    public final static String TOTAL_PRICE     = "totalPrice";

    // 时间类常量
    public final static String MODIFY_TIME     = "modifyTime";
    public final static String CREATE_TIME     = "createTime";
    public final static String START_TIME      = "startTime";
    public final static String END_TIME        = "endTime";
    public final static String TIME            = "time";

    // 统计类常量
    public final static String COUNT           = "count";

    // 类型常量
    public final static String TYPE           = "type";

    // 其他通用常量
    public final static String VALUE           = "value";
    public final static String CODE            = "code";
    public final static String PREFIX          = "prefix";

    public interface InviteHistory{
        String CONSUMER_PRICE    = "consumerPrice";
        String REBATE_PRICE      = "rebatePrice";
        String CONSUMER_COUNT    = "consumerCount";
        String REBATE_COUNT      = "rebateCount";
    }
}
