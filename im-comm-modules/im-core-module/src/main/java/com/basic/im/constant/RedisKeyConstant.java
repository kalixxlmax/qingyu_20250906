package com.basic.im.constant;

/**
 * description: RedisKey 常量定义 <br>
 * date: 2020/6/16 0016  <br>
 * author: lidaye <br>
 * version: 1.0 <br>
 */
public interface RedisKeyConstant {

    /**
     * 客服配置属性 Redis Map key
     */
    interface CustomerServiceKey {

        /**
         * 客服配置信息 Map
         */
        String CUSTOMER_SERVICE_KEY="customer:service:%s";

        /**
         * 公众号的客服列表 List
         */
        String CUSTOMER_SERVICE_LIST_KEY="customer:service_list:%s";

        /**
         * 新到访客推送
         */
        String NEW_VISITOR_PUSH="newVisitorHint";
        /**
         * 新对话推送
         */
        String NEW_TALK_PUSH="newTalkHint";
        /**
         * 新访客消息推送
         */
        String NEW_VISITOR_MSGPUSH = "newVisitorMsgHint";

        /**
         * 新到一个转接会话
         */
        String NEW_FOWARD_PUSH = "newFowardHint";

        /**
         * 新到一条同事消息
         */
        String NEW_INNERTALK_PUSH = "newInnerTalkHint";

    }

}
