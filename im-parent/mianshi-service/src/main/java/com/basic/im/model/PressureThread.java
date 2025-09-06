package com.basic.im.model;

import com.google.common.collect.Maps;
import com.chat.imserver.common.message.ChatMessage;
import com.chat.imserver.common.message.MessageHead;
import com.chat.imserver.common.packets.ChatType;
import com.chat.imserver.common.utils.StringUtils;
import com.basic.utils.DateUtil;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

//@Component
public class PressureThread implements Runnable{

    @Autowired(required = false)
    protected IMPushConfig imPushConfig;


    public static Logger logger = LoggerFactory.getLogger(PressureThread.class);

    private PressureParam param;

    private String jid;

    private String roomName;

    private AtomicInteger mySendCount;

//    private List<IMPushClient> mucChats = Collections.synchronizedList(new ArrayList<>());
    private Map<Integer, IMPushClient> userClient = Maps.newConcurrentMap();


    public PressureThread() {
        // TODO Auto-generated constructor stub
    }

    public PressureThread(String jid, String roomName, PressureParam param, Map<Integer,IMPushClient> userClient) {
        this.jid = jid;
        this.param = param;
        this.userClient = userClient;
        this.mySendCount = new AtomicInteger(0);
        this.roomName = roomName;
    }


    @Override
    public void run() {
            int i = mySendCount.get();
            if (i >= param.getSendMsgNum()) {
                return;
            }

            try {
                Collection<IMPushClient> values = userClient.values();
                if(values.isEmpty())
                    return;
                Random random = new Random();
                IMPushClient client = values.stream().collect(Collectors.toList()).get(random.nextInt(values.size()));
                runQueuePush(client,i);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                mySendCount.incrementAndGet();
            }
    }

    public void runQueuePush(IMPushClient client,int i){
        String content = "=== ";
        ChatMessage message=null;
        MessageHead messageHead=null;
            message=new ChatMessage();
            messageHead=new MessageHead();
            messageHead.setFrom(client.getUserId()+"/Server");
            messageHead.setTo(this.jid);
            messageHead.setChatType(ChatType.GROUPCHAT);
            message.setFromUserId(client.getUserId());
            message.setFromUserName(client.getUserId());
            message.setToUserId(this.jid);
            message.setToUserName("群组");
            message.setType((short)1);
            message.setTimeSend(System.currentTimeMillis());
            long timeSend = DateUtil.getSysCurrentTimeMillis_sync()+i;
//            message.setTimeSend(Long.valueOf(getTimeSend(timeSend)+""));
            message.setTimeSend(timeSend);
            /*message.setSeqNo(-1);*/
            message.setContent(param.getTimeStr()+" "+client.getUserId()+" "+ content + i);// 批次 + userId + 消息序号
            messageHead.setMessageId(StringUtils.newStanzaId());
            message.setMessageHead(messageHead);



        try {
            client.sendMessage(message);
//            logger.info("系统推送成功： to {},",message.getToUserId());
            logger.info(" timeStr {}  {}  === {}  sendMsg muc  {} count {}  {}  time {}",param.getTimeStr(),client.getUserId(),roomName,i,param.getAtomic().incrementAndGet(),message.getTimeSend());
        }  catch (Exception e) {
//            queue.offer(message);
            logger.error(e.getMessage(),e);
        }
    }

    @Getter
    @Setter
    public class ChatMessageVo {
        private long createTime;
        private ChatMessage message;
    }


    private double getTimeSend(long ts) {
        double time = (double) ts;
        DecimalFormat dFormat = new DecimalFormat("#.000");
        return new Double(dFormat.format(time / 1000));
    }

    public static String getFullString() {
        return new SimpleDateFormat("MM-dd HH:mm").format(currentTimeSeconds());
    }

    public static long currentTimeSeconds() {
        return System.currentTimeMillis();
    }
}
