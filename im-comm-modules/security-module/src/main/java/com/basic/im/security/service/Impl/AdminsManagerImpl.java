package com.basic.im.security.service.Impl;

import com.basic.im.entity.Config;
import com.basic.im.security.dao.ConfigsDao;
import com.basic.im.security.service.AdminsManager;
import com.basic.im.utils.SKBeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description: TODO
 * @Author xie yuan yang
 * @Date 2020/3/4
 **/
@Slf4j
@Service
public class AdminsManagerImpl implements AdminsManager {

    @Autowired
    private ConfigsDao configsDao;

    @Override
    public Config getConfig() {
        Config config=null;
        try {
            config= SKBeanUtils.getImCoreService().getConfig();
            if(null==config){
                config = configsDao.getConfig();
                if(null==config) {
                    config=initConfig();
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            config = configsDao.getConfig();
        }

        return config;
    }

    @Override
    public Config initConfig() {
        Config config=new Config();
        try {
//			config.XMPPDomain="im.server.co";
//			config.setLiveUrl("rtmp://v1.one-tv.com:1935/live/");
            config.setShareUrl("");
            config.setSoftUrl("");
            config.setHelpUrl("");
            config.setVideoLen("20");
            config.setAudioLen("20");
            configsDao.addConfig(config);
//				initSystemNo();

            return config;
        } catch (Exception e) {
            e.printStackTrace();
            return null==config?null:config;
        }
    }
}
