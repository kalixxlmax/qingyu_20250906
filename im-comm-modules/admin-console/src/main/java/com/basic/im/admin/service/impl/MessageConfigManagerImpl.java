package com.basic.im.admin.service.impl;

import com.basic.im.admin.dao.MessageConfigDao;
import com.basic.im.admin.service.MessageConfigManager;
import com.basic.im.sms.model.SmsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description: TODO （短信操作）
 * @Author xie yuan yang
 * @Date 2020/5/25
 **/
@Service
public class MessageConfigManagerImpl implements MessageConfigManager {

    @Autowired
    private MessageConfigDao messageConfigDao;

    @Autowired
    private SmsConfig smsConfig;

/*    @Override
    public MessageConfig getMessageConfig() {
        return messageConfigDao.getMessageConfig();
    }

    @Override
    public MessageConfig setMessageConfig(MessageConfig messageConfig) {
        MessageConfig config = messageConfigDao.setMessageConfig(messageConfig);
        smsConfig.setSmsConfig(setSmsConfig(config));
        return config;
    }

    @Override
    public SmsConfigModel setSmsConfig(MessageConfig messageConfig) {
        SmsConfigModel data = new SmsConfigModel();
        data.setOpenSMS(messageConfig.getOpenSMS());
        data.setHost(messageConfig.getHost());
        data.setPort(messageConfig.getPort());
        data.setUsername(messageConfig.getUsername());
        data.setPassword(messageConfig.getPassword());
        data.setTemplateChineseSMS(messageConfig.getTemplateChineseSMS());
        data.setTemplateEnglishSMS(messageConfig.getTemplateEnglishSMS());
        data.setProduct(messageConfig.getProduct());
        data.setDomain(messageConfig.getDomain());
        data.setAccesskeyid(messageConfig.getAccesskeyid());
        data.setAccesskeysecret(messageConfig.getAccesskeysecret());
        data.setSignname(messageConfig.getSignname());
        data.setChinase_templetecode(messageConfig.getChinase_templetecode());
        data.setInternational_templetecode(messageConfig.getInternational_templetecode());
        data.setCloudWalletVerification(messageConfig.getCloudWalletVerification());
        data.setCloudWalletNotification(messageConfig.getCloudWalletNotification());
        return data;
    }*/
}
