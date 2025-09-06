package com.basic.im.utils;

import com.alibaba.fastjson.JSONObject;
import com.basic.im.constant.TopicConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

@Slf4j
public class MqMessageSendUtil {

    private static RocketMQTemplate rocketMQTemplate;

    static{
        rocketMQTemplate=SpringBeansUtils.getBean(RocketMQTemplate.class);
        checkConfig();
    }

    /**
     * 发送消息通用类
     * @param topic  接收此消息的主题
     * @param data   要发送的数据包
     * @param isLog  是否记录日志
     * @return       发送成功返回true
     */
    public static boolean sendMessage(String topic,Object data,boolean isLog){
        if (checkConfig()){
            try{
                SendResult sendResult = rocketMQTemplate.syncSend(topic, data);
                return resultBuild(sendResult,topic,data,isLog);
            }catch (Exception e){
                log.error("message send error,topic is {},data is {},errorMsg is {}",topic,data,e.getMessage());
            }
        }
        return false;
    }

    public static boolean sendMessage(String topic,Object data){
        return sendMessage(topic,data,false);
    }

    

    /**
     * 将消息重新放入队列
     * @param topic 主题
     * @param data  消息体内容
     */
    public static void convertAndSend(String topic,Object data){
        if (checkConfig()){
            try{
                rocketMQTemplate.convertAndSend(topic, convertObject(data));
            } catch (Exception e) {
                log.error("重新放入队列失败");
                e.printStackTrace();
            }
        }
    }


    public static boolean sendOrderly(String topic,Object data,String hashKey,boolean isLog){
        if (checkConfig()){
            try{
                SendResult sendResult = rocketMQTemplate.syncSendOrderly(TopicConstant.XMPP_MESSAGE_TOPIC, data,hashKey);
                return resultBuild(sendResult,topic,data,isLog);
            }catch (Exception e){
                log.error("message send error,topic is {},data is {},errorMsg is {}",topic,data,e.getMessage());
            }
        }
        return false;
    }

    /**
     * 处理结果数据
     */
    private static boolean resultBuild(SendResult sendResult,String topic,Object data,boolean isLog){
        if (sendResult.getSendStatus()== SendStatus.SEND_OK){
            if (isLog){
                log.info("message send to topic {} success,data is {}", topic,convertObject(data));
            }
            return true;
        }else{
            if (isLog){
                log.error("message send to topic {} failure,data is {}", topic,convertObject(data));
            }
            return false;
        }
    }

    /**
     * 内容转换
     */
    private static String convertObject(Object obj){
        if (obj instanceof String){
            return obj.toString();
        }else if (obj instanceof JSONObject){
            return ((JSONObject) obj).toJSONString();
        }else{
            return JSONObject.toJSONString(obj);
        }
    }


    /**
     * 检查 MQ 是否被正确的导入
     */
    private static boolean checkConfig(){
        if (rocketMQTemplate==null){
            log.error("rocketMqTemplate is null,Please check your configuration");
            return false;
        }
        return true;
    }

    /**
     * 获取 MQ 实例
     */
    public static RocketMQTemplate getMqTemplate() {
        return  checkConfig()?rocketMQTemplate:null;
    }
}
