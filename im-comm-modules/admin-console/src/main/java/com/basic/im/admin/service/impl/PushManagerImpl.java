package com.basic.im.admin.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.basic.common.model.PageResult;
import com.basic.im.admin.dao.PushConfigDao;
import com.basic.im.admin.dao.PushNewsDao;
import com.basic.im.admin.entity.PushNews;
import com.basic.im.admin.service.PushManager;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.entity.PushConfig;
import com.basic.im.utils.SKBeanUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.ServiceState;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Random;

/**
 * @ClassName PushManagerImpl
 * @Author xie yuan yang
 * @date 2020.11.09 16:18
 * @Description 推送相关逻辑
 */
@Service
public class PushManagerImpl implements PushManager {

    @Autowired
    private PushConfigDao pushConfigDao;

    @Autowired
    private PushNewsDao pushNewsDao;

    public static PushConfig pushConfig;

    @Value("${rocketmq.name-server}")
    private String mqNameServer;

    //设备推送
    @Override
    public void pushDevice(String pushDevice,String title,String text,String pushToken,String pushPackageName)throws UnsupportedEncodingException {
        JSONObject bodyObj = new JSONObject();
        bodyObj.put("content", text);
        bodyObj.put("title", title);
        bodyObj.put("pushToken", pushToken);
        bodyObj.put("pushPackageName", pushPackageName);

        //华为推送
        if (pushDevice.equals("华为")){
            bodyObj.put("type", 2);
            bodyObj.put("push_type", PushNews.Type.HUAWEI_PUSH);
            org.apache.rocketmq.common.message.Message message=
                    new org.apache.rocketmq.common.message.Message("fullPushMessage",bodyObj.toJSONString().getBytes("utf-8"));

            try {
                SendResult result = getPushProducer().send(message);
                if(SendStatus.SEND_OK!=result.getSendStatus()){
                    System.out.println(result.toString());
                }
            } catch (Exception e) {
                System.err.println("send  push Exception "+e.getMessage());
                restartProducer();
            }
        }

        //VIVO推送
        if(pushDevice.equals("VIVO")){
            bodyObj.put("type", 3);
            bodyObj.put("push_type", PushNews.Type.VIVO_PUSH);
            org.apache.rocketmq.common.message.Message message=
                    new org.apache.rocketmq.common.message.Message("fullPushMessage",bodyObj.toJSONString().getBytes("utf-8"));

            try {
                SendResult result = getPushProducer().send(message);
                if(SendStatus.SEND_OK!=result.getSendStatus()){
                    System.out.println(result.toString());
                }
            } catch (Exception e) {
                System.err.println("send  push Exception "+e.getMessage());
                restartProducer();
            }
        }

        //OPPO推送
        if(pushDevice.equals("OPPO")){
            bodyObj.put("type", 4);
            bodyObj.put("push_type", PushNews.Type.OPPO_PUSH);
            org.apache.rocketmq.common.message.Message message=
                    new org.apache.rocketmq.common.message.Message("fullPushMessage",bodyObj.toJSONString().getBytes("utf-8"));

            try {
                SendResult result = getPushProducer().send(message);
                if(SendStatus.SEND_OK!=result.getSendStatus()){
                    System.out.println(result.toString());
                }
            } catch (Exception e) {
                System.err.println("send  push Exception "+e.getMessage());
                restartProducer();
            }
        }

        //小米推送
        if(pushDevice.equals("小米")){
            bodyObj.put("type", 5);
            bodyObj.put("push_type", PushNews.Type.XIAOMI_PUSH);
            org.apache.rocketmq.common.message.Message message=
                    new org.apache.rocketmq.common.message.Message("fullPushMessage",bodyObj.toJSONString().getBytes("utf-8"));

            try {
                SendResult result = getPushProducer().send(message);
                if(SendStatus.SEND_OK!=result.getSendStatus()){
                    System.out.println(result.toString());
                }
            } catch (Exception e) {
                System.err.println("send  push Exception "+e.getMessage());
                restartProducer();
            }
        }

        //魅族推送
        if(pushDevice.equals("魅族")){
            bodyObj.put("type", 6);
            bodyObj.put("push_type", PushNews.Type.MEIZU_PUSH);
            org.apache.rocketmq.common.message.Message message=
                    new org.apache.rocketmq.common.message.Message("fullPushMessage",bodyObj.toJSONString().getBytes("utf-8"));

            try {
                SendResult result = getPushProducer().send(message);
                if(SendStatus.SEND_OK!=result.getSendStatus()){
                    System.out.println(result.toString());
                }
            } catch (Exception e) {
                System.err.println("send  push Exception "+e.getMessage());
                restartProducer();
            }
        }

        //IOS推送
        if(pushDevice.equals("APNS")){
            bodyObj.put("type", 7);
            bodyObj.put("push_type", PushNews.Type.APNS_PUSH);
            org.apache.rocketmq.common.message.Message message=
                    new org.apache.rocketmq.common.message.Message("fullPushMessage",bodyObj.toJSONString().getBytes("utf-8"));

            try {
                SendResult result = getPushProducer().send(message);
                if(SendStatus.SEND_OK!=result.getSendStatus()){
                    System.out.println(result.toString());
                }
            } catch (Exception e) {
                System.err.println("send  push Exception "+e.getMessage());
                restartProducer();
            }
        }

        //极光推送
        if(pushDevice.equals("极光")){
            bodyObj.put("type", 8);
            bodyObj.put("push_type", PushNews.Type.JPUSH_PUSH);
            org.apache.rocketmq.common.message.Message message =
                    new org.apache.rocketmq.common.message.Message("fullPushMessage",bodyObj.toJSONString().getBytes("utf-8"));
            try {
                SendResult result = getPushProducer().send(message);
                if(SendStatus.SEND_OK!=result.getSendStatus()){
                    System.out.println(result.toString());
                }
            } catch (Exception e) {
                System.err.println("send  push Exception "+e.getMessage());
                restartProducer();
            }
        }
        //保存推送记录
        pushNewsDao.sava(new PushNews(bodyObj.getString("title"),bodyObj.getString("content"),bodyObj.getIntValue("type"),bodyObj.getString("pushPackageName"),bodyObj.getString("pushToken"), DateUtil.currentTimeSeconds()));
    }

    @Override
    public List<PushConfig> getPushConfigList() {
        List<PushConfig> result = pushConfigDao.getPushConfigList();
        return result;
    }

    @Override
    public PushConfig addPushConfig(PushConfig pushConfig) {
        //id = 0; 新增配置
        if (0 == pushConfig.getId()){
            pushConfig.setId(getRandomIntId());
        }
        PushConfig result = pushConfigDao.addPushConfig(pushConfig);
        //重新初始化配置
        SKBeanUtils.getImCoreService().cleanPushConfig();
        SKBeanUtils.getImCoreService().initPushConfig();
        SKBeanUtils.getImCoreService().updatePushFlag(1);
        return result;
    }

    @Override
    public PushConfig getPushConfigModelDetail(int id) {
        PushConfig pushConfig = pushConfigDao.getPushConfigModelDetail(id);
        return pushConfig;
    }

    @Override
    public boolean deletePushConfig(int id) {
        boolean flag = pushConfigDao.deletePushConfig(id);
        if (flag){
            //重新初始化配置
            SKBeanUtils.getImCoreService().cleanPushConfig();
            SKBeanUtils.getImCoreService().initPushConfig();
            SKBeanUtils.getImCoreService().updatePushFlag(1);
        }
        return  flag;
    }


    private DefaultMQProducer pushProducer;
    public DefaultMQProducer getPushProducer() {
        if(null!=pushProducer) {
            return pushProducer;
        }

        try {
            pushProducer=new DefaultMQProducer("pushProducer");
            pushProducer.setNamesrvAddr(mqNameServer);
            //pushProducer.setVipChannelEnabled(false);
            //pushProducer.setCreateTopicKey("fullPushMessage");
            pushProducer.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return pushProducer;
    }
    public void restartProducer() {
        System.out.println("pushProducer restartProducer ===》 "+mqNameServer);
        try {
            if(null!=pushProducer&&null!=pushProducer.getDefaultMQProducerImpl()) {
                if(ServiceState.CREATE_JUST==pushProducer.getDefaultMQProducerImpl().getServiceState()) {
                    try {
                        pushProducer.start();
                    } catch (Exception e) {
                        pushProducer=null;
                        getPushProducer();
                    }
                }
            }else {
                pushProducer=null;
                getPushProducer();
            }
        } catch (Exception e) {
            System.err.println("restartProducer Exception "+e.getMessage());

        }
    }

    /**
     * @Description 随机获取一个正整数
     * @Date 15:02 2020/8/11
     **/
    @Override
    public int getRandomIntId(){
        int id = 0;
        boolean flag = true;
        while (flag){
            int ran = new Random().nextInt();
            if(ran <= 0){
                continue;
            }
            PushConfig pushConfig = pushConfigDao.getPushConfigModelDetail(ran);
            if (pushConfig == null){
                id = ran;
                flag = false;
            }
        };
        return id;
    }

    /**
     * 发送系统通知
     * @param type
     * @param body
     * @throws UnsupportedEncodingException
     */
    @Override
    public void sendSysNotice(Integer type,String body,String title,String url) throws UnsupportedEncodingException{
        JSONObject bodyObj = new JSONObject();
        if(type==1){// 版本更新
            bodyObj.put("objectId", url);
        }
        bodyObj.put("content", body);
        bodyObj.put("title", title);
        bodyObj.put("type", type);
        org.apache.rocketmq.common.message.Message message=
                new org.apache.rocketmq.common.message.Message("fullPushMessage",bodyObj.toJSONString().getBytes("utf-8"));

        try {
            SendResult result = getPushProducer().send(message);
            if(SendStatus.SEND_OK!=result.getSendStatus()){
                System.out.println(result.toString());
            }
            //保存推送记录
            pushNewsDao.sava(new PushNews(bodyObj.getString("title"),bodyObj.getString("content"),bodyObj.getString("objectId"),PushNews.Type.ALL_PUSH,DateUtil.currentTimeSeconds()));
        } catch (Exception e) {
            System.err.println("send  push Exception "+e.getMessage());
            restartProducer();
        }
    }


    @Override
    public PageResult<PushNews> getPushNewsList(String startTime, String endTime, int page, int limit, String type,String content) {
        PageResult<PushNews> result = pushNewsDao.getPushNewsList(startTime, endTime, page, limit, type,content);
        return result;
    }


    @Override
    public void deletePushNews(String id) {
        pushNewsDao.deletePushNews(new ObjectId(id));
    }

}
