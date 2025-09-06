package com.basic.im.sms.service.impl;

import com.basic.im.comm.utils.DateUtil;
import com.basic.im.sms.dao.SmsSendLogDao;
import com.basic.im.sms.entity.SmsSendLog;
import com.basic.im.sms.service.SmsSendLogService;
import com.basic.mongodb.wrapper.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SmsSendLogServiceImpl implements SmsSendLogService {

    @Autowired
    private SmsSendLogDao smsSendLogDao;

    @Override
    public void saveSmsSendLog(SmsSendLog smsSendLog) {
        smsSendLogDao.save(smsSendLog);
    }

    @Override
    public Long countSmsSendLog(Integer poor, Integer isSend) {
        QueryWrapper<SmsSendLog> wrapper = new QueryWrapper<>();
        wrapper.gte(SmsSendLog::getCreateTime, DateUtil.currentTimeSeconds() - poor);
        wrapper.eq(SmsSendLog::getIsSend,isSend);
        return this.smsSendLogDao.count(wrapper.build());
    }

    @Override
    public Long countSmsSendLogByIp(String ip, Integer poor, Integer isSend) {
        QueryWrapper<SmsSendLog> wrapper = new QueryWrapper<>();
        wrapper.gte(SmsSendLog::getCreateTime, DateUtil.currentTimeSeconds() - poor);
        wrapper.eq(SmsSendLog::getIsSend,isSend);
        wrapper.eq(SmsSendLog::getIp,ip);
        return this.smsSendLogDao.count(wrapper.build());
    }
}
