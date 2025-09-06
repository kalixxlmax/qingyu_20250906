package com.basic.im.constant;


/**
 * 监听消息内容主题常量类
 */
public final class TopicConstant {

    /**
     * 延时任务主题
     */
    public static final String PUBLISH_DELAY_JOB="publish-delayjob";

    /**
     * 等待延时任务主题
     */
    public static final String PUBLISH_WAIT_DELAY_JOB="publish-waitdelayjob";

    /**
     * xmpp 消息发送到 message-push
     */
    public static final String XMPP_MESSAGE_TOPIC="xmppMessage";

    /**
     * 客服消息
     */
    public static final String CUSTOMER_MESSAGE_TOPIC= "customer_message_topic";

    /**
     * 华为推送主题
     */
    public static final String HW_PUSH_MESSAGE_TOPIC = "HWPushMessage";

    /**
     * 系统推送消息主题
     */
    public static final String PUSH_MESSAGE_TOPIC= "pushMessage";


    /**
     * 好友首字母修改主题
     */
    public static final String FIRST_NAME_CHANGE_TOPIC= "first_name_change_topic";

    /**
     * 客服菜单任务主题
     */
    public static final String CUSTOMER_MENU_TASK_TOPIC= "customer_menu_task_topic";
}
