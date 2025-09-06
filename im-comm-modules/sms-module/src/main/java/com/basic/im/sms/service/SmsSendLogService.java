package com.basic.im.sms.service;

import com.basic.im.sms.entity.SmsSendLog;

public interface SmsSendLogService {
    void saveSmsSendLog(SmsSendLog smsSendLog);

    /**
     * 检查指定时间前到当前时间区间内的短信调用次数
     * @param poor 指定时间
     */
    Long countSmsSendLog(Integer poor,Integer isSend);
    Long countSmsSendLogByIp(String ip,Integer poor,Integer isSend);
}
