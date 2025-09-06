package com.basic.push.rocketmq;

import java.util.ArrayList;

import org.junit.Test;


public class XmppMQProducerTest {
//	private DefaultMQProducer chatProducer;
//
//
//	private final String name_addr="47.101.137.26:9876";
//
//	public DefaultMQProducer getChatProducer() {
//		if(null!=chatProducer)
//			return chatProducer;
//
//			try {
//				chatProducer=new DefaultMQProducer("xmppProducer");
//				chatProducer.setNamesrvAddr(name_addr);
//				chatProducer.setVipChannelEnabled(false);
//				chatProducer.setCreateTopicKey("xmppTestMessage");
//				chatProducer.setSendMsgTimeout(30000);
//
//				chatProducer.start();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		return chatProducer;
//	}
//
////	public DefaultMQProducer getGroupProducer() {
////		if(null!=groupProducer)
////			return groupProducer;
////
////			try {
////				groupProducer=new DefaultMQProducer("groupProducer");
////				groupProducer.setNamesrvAddr(name_addr);
////				groupProducer.setCreateTopicKey("groupMessage");
////				groupProducer.start();
////			} catch (Exception e) {
////				e.printStackTrace();
////			}
////		return groupProducer;
////	}
//
//	@Test
//	public void testProducer(){
//		System.out.println("sssss");
//		MessageBean messageBean=new MessageBean();
//        messageBean.setContent("");
//        messageBean.setFromUserId("10005");
//        messageBean.setFromUserName("群主");
//        messageBean.setToUserId("10010004");
//        messageBean.setMsgType(0);
//        messageBean.setType(1);
//        messageBean.setMessageId(StringUtil.randomUUID());
//
//       // muc.sendMessage(messageBean.toString());
//        //muc.leave();
//
//        DefaultMQProducer producer = getChatProducer();
//       /* try {
//        	 producer.createTopic("chatMessage", "chatMessage", 1);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}*/
//
//        String content="单聊测试= ";
//        long start=System.currentTimeMillis();
//        Message message=null;
//       List<Message> list=new ArrayList<>();
//       for (int i = 1; i <=10;i++) {
////    	   for (int j = 0; j < 10; j++) {
////    		   messageBean.setTimeSend(DateUtil.currentTimeSeconds());
////        	   messageBean.setMessageId(StringUtil.randomCode());
////        	   messageBean.setContent(content+i);
////        	    message=new Message("xmppMessage",messageBean.toString().getBytes());
////        	   list.add(message);
////			 i++;
////    	   }
//    	   try {
//    		   messageBean.setTimeSend(DateUtil.currentTimeSeconds());
//        	   messageBean.setMessageId(StringUtil.randomCode());
//        	   messageBean.setContent(content+i);
//        	    message=new Message("xmppMessage",messageBean.toString().getBytes());
//    		   SendResult result = producer.send(message);
//    		   if(SendStatus.SEND_OK!=result.getSendStatus()){
//					System.out.println(result.toString());
//    		   }else{
//    			   System.out.println(messageBean.getContent());
//    		   }
//
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//
//
//       }
//       long count=(System.currentTimeMillis()-start)/1000;
//        System.out.println("send all timeCount "+count);
////        while (true) {
////
////		}
//	}
//
//	@Test
//	public void testGroupProducer(){
//		System.out.println("群xmpp消息测试");
//		MessageBean messageBean=new MessageBean();
//        messageBean.setContent("");
//        messageBean.setFromUserId("10005");
//        messageBean.setFromUserName("10005");
//        messageBean.setToUserId("10006");
//        messageBean.setRoomJid("8f07c8dbad2b4d4eb9bec4bbc903401d");
//        messageBean.setMessageId(StringUtil.randomUUID());
//        messageBean.setType(1);
//        messageBean.setMsgType(1);
//
//        DefaultMQProducer producer = getChatProducer();
//	       /* try {
//	        	 producer.createTopic("chatMessage", "chatMessage", 1);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}*/
//
//	        String content="群聊测试= ";
//	        long start=System.currentTimeMillis();
//	        Message message=null;
//
//	       for (int i = 1; i <=15; i++) {
//	    	   messageBean.setTimeSend(DateUtil.currentTimeSeconds());
//	    	   messageBean.setMessageId(StringUtil.randomCode());
//	    	   messageBean.setContent(content+i);
//
//	    	   message=new Message("xmppMessage",messageBean.toString().getBytes());
//	    	   try {
//	    		   SendResult result = producer.send(message);
//	    		   if(SendStatus.SEND_OK!=result.getSendStatus()){
//	    				System.out.println(result.toString());
//	    		   }else{
//	    			   System.out.println(messageBean.getContent());
//	    		   }
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//	       }
//	       long count=(System.currentTimeMillis()-start)/1000;
//	        System.out.println("send all timeCount "+count);
//	}
//
//	@Test
//	public void sendBroadCast(){
//		System.out.println("发送广播消息测试");
//		MessageBean messageBean=new MessageBean();
//        messageBean.setContent("");
//        messageBean.setFromUserId("10005");
//        messageBean.setFromUserName("群主");
//        messageBean.setToUserId("10006");
//        messageBean.setMsgType(2);
//        messageBean.setType(1);
//
//       // muc.sendMessage(messageBean.toString());
//        //muc.leave();
//
//        DefaultMQProducer producer = getChatProducer();
//       /* try {
//        	 producer.createTopic("chatMessage", "chatMessage", 1);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}*/
//
//        String content="=== ";
//        long start=System.currentTimeMillis();
//        Message message=null;
//
//
//	   messageBean.setTimeSend(DateUtil.currentTimeSeconds());
//	   messageBean.setMessageId(StringUtil.randomCode());
//	   messageBean.setContent(content+"广播消息");
//
//	   message=new Message("xmppMessage",messageBean.toString().getBytes());
//	   try {
//		   SendResult result = producer.send(message);
//		   if(SendStatus.SEND_OK!=result.getSendStatus()){
//				System.out.println(result.toString());
//		   }else{
//			   System.out.println(messageBean.getContent());
//		   }
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//       long count=(System.currentTimeMillis()-start)/1000;
//        System.out.println("send all timeCount "+count);
//	}
}
