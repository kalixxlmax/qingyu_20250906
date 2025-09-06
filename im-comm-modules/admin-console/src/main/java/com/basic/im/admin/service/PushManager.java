package com.basic.im.admin.service;

import com.basic.common.model.PageResult;
import com.basic.im.admin.entity.PushNews;
import com.basic.im.entity.PushConfig;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @ClassName PushManager
 * @Author xie yuan yuang
 * @date 2020.11.09 16:18
 * @Description
 */
public interface PushManager {

    //设备推送
    void pushDevice(String pushDevice,String title,String text,String pushToken,String pushPackageName) throws UnsupportedEncodingException;

    List<PushConfig> getPushConfigList();

    PushConfig addPushConfig(PushConfig pushConfig);

    PushConfig getPushConfigModelDetail(int id);

    boolean deletePushConfig(int id);

    int getRandomIntId();

    void sendSysNotice(Integer type,String body,String title,String url) throws UnsupportedEncodingException;

    /**
     * 推送信息列表
     **/
    PageResult<PushNews> getPushNewsList(String startTime,String endTime,int page,int limit,String type,String content);

    /**
     * 删除推送信息
     **/
    void deletePushNews(String id);
}
