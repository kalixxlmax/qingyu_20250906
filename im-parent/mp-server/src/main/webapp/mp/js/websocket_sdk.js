var IMSDK={

    cont:"imweb_",
    VERSION:4,
    websocket:null,
    serverUrl:null,
    /*登陆成功的回调*/
    logincallBack:null,
    /*收到消息的回调*/
    messageReceiver:null,
    /*消息回执 处理方法*/
    handlerMsgReceipt:null,
    handlerLoginConflict:null,
    userIdStr:null,
    token:null,
    /*心跳间隔时间*/
    pingTime:30,
    mcode:0,
    apiKey:"212i919292901",
    appName:null,
    companyName:null,
    /*最后一次传递消息的时间*/
    lastTransferTime:0,
    waitSendReceiptIds:"",
    initApi(url,userId,resource,token,pingTime,server,companyName,appName){
        
        IMSDK.token=token;
        IMSDK.serverUrl=url;
        IMSDK.server=server;
        IMSDK.resource=IMSDK.resource;
        IMSDK.userIdStr=userId+"/"+resource;
        IMSDK.pingTime=pingTime;
        IMSDK.server=server;
        IMSDK.companyName=companyName;
        IMSDK.appName=appName;

         /*IMSDK.messageReceiver=messageReceiver;
        IMSDK.handlerMsgReceipt=handlerMsgReceipt;
        IMSDK.handlerLoginConflict=handlerLoginConflict;*/
    },
    loginIM:function(callback,apiKey){
          try {
            IMSDK.apiKey=apiKey;
            if(callback)
                IMSDK.logincallBack=callback;

             IMSDK.websocket = new WebSocket(IMSDK.serverUrl);
             
             IMSDK.websocket.onopen=IMSDK.onopen;
             IMSDK.websocket.onmessage=IMSDK.onmessage;
             IMSDK.websocket.onerror=IMSDK.onerror;
             IMSDK.websocket.onclose=IMSDK.onclose;
             IMSDK.websocket.binaryType = "arraybuffer";
            
        } catch (e) {
          console.log(e.message);
        }
         
    },
    loginSuccess:function(message){
        if(IMSDK.logincallBack)
                IMSDK.logincallBack(message);
        if(IMSDK.userIdStr==message.from){
            clearInterval(IMSDK.ping);
            //XmppSdk.ping=null
            IMSDK.pingTime=IMSDK.pingTime*1000;
            IMSDK.ping = window.setInterval(function(){
                IMSDK.sendPing();
            },IMSDK.pingTime); 
        }else{
            /*其他设备登陆*/
        }

        
    },
    onopen:function(e){
         console.log("onopen  ===> "+e);
         var message=IMSDK.buildAuthMessage();
         message.version=IMSDK.VERSION;
        /* message.companyName=$.md5(IMSDK.companyName);
         message.appName=$.md5(IMSDK.appName);
         message.apiKey=$.md5(IMSDK.apiKey);*/

        var buffer=IMSDK.encodeMessage(message,Command.COMMAND_AUTH_REQ);
        
       IMSDK.sendBytes(buffer);
    },
    /*收到服务器消息*/
    onmessage:function(e){
        var dataArr = new Uint8Array(e.data) ; 
        // var cmd= ((dataArr[0]&0xff)+((dataArr[1]&0xff)<<8));
        var cmd = dataArr[0];
        /*if(cmd>127){
            cmd=cmd-256;
        }*/
        IMSDK.lastTransferTime=IMSDK.getCurrentSeconds();
        console.log("onmessage  cmd ===> "+cmd);
        if(0==cmd)
            return;
       var bytes= dataArr.subarray(1,dataArr.length);
       var message=IMSDK.decodeMessage(bytes,cmd);
       cmd=message.cmd;
       /*var dataStr=JSON.stringify(message);*/
      
       message=IMSDK.convertToClientMsg(message);
          
       IMSDK.handlerMessageBycmd(cmd,message); 
    },
    disconnect:function(e){
        clearInterval(IMSDK.ping);
        IMSDK.websocket.close();
    },
    isConnect:function(){
        if(!IMSDK.websocket)
            return false;
        return 1==IMSDK.websocket.readyState;
    },
    sendPing:function(){
         if(!IMSDK.isConnect())
            return;
        /*var currTime=IMSDK.getCurrentSeconds();
        if((currTime-IMSDK.pingTime)<IMSDK.lastTransferTime){
            //最后通讯时间相近 不需要发送心跳包
            //console.log("最后通讯时间相近 不需要发送心跳包");
            return ;
        }*/

        //console.log("发送心跳包");
        var message=IMSDK.buildPingMessage();
        var buffer=IMSDK.encodeMessage(message,Command.Ping_REQ);
        IMSDK.sendBytes(buffer);
    },
    onerror:function(e){
        console.log("onerror ====> "+e);
        IMSDK.loginIM();
    },
    onclose:function(e){
        console.log("onclose ====> "+e);
        
    },
    handlerMessageBycmd(cmd,message){
        /*发送客户端消息回执*/
        if(ChatType.CHAT==message.chatType&&85<message.type&&94>message.type){
            return;
        }else if(ChatType.GROUPCHAT==message.chatType&&IMSDK.userIdStr==message.fromJid){
           IMSDK.handlerMsgReceipt(message.messageId);
        }

        switch (cmd){
            case Command.COMMAND_CHAT:
                IMSDK.sendReceipt(message.messageId);
                IMSDK.messageReceiver(message);
                break;
            case Command.SUCCESS:
                IMSDK.handlerMsgReceipt(message.messageId);
                break;
            case Command.MESSAGE_RECEIPT:
                IMSDK.handlerMsgReceipt(message.messageId);
                break;
            case Command.PULL_BATCH_GROUP_MESSAGE_RESP:
                IMSDK.handlerGroupMessageResult(message);
                break;
            case Command.PULL_MESSAGE_RECORD_RESP:
                IMSDK.handlerHistoryMessageResult(message);
                break;
            case Command.CHAT_OFF_MESSAGE_RESP:
                IMSDK.messageReceiver(message);
                //IMSDK.handleChatOfflineMessageResult(message);
                break;
            case Command.COMMAND_AUTH_RESP:
               // this.mcode=message.mcode;
                IMSDK.loginSuccess(message);
                break;
            case Command.Login_Conflict:
                IMSDK.handlerLoginConflict();
                break;
           
            default://默认 其他
                content="";
                break ;

        }
    },
   /* handlerMsgReceipt(message){

    },*/

    /*
    发送消息 api  
    */
    sendMessage:function(msg,cmd){
        if(!cmd)
            cmd=Command.COMMAND_CHAT;
        var head=IMSDK.buildMessageHead(msg.to,msg.chatType);
        if(msg.messageId)
            head.messageId=msg.messageId;
        msg.messageHead=head;
        //delete msg["to"];
        var buffer=IMSDK.encodeMessage(msg,cmd);
       IMSDK.sendBytes(buffer);
    },
    sendBytes:function(bytes){
     if(!IMSDK.isConnect()) 
           sleep(1000);
        //console.log("sendBytes  ===>  "+bytes);
        IMSDK.websocket.send(bytes);
        
    },
    sendReceipt:function(messageId){
        IMSDK.waitSendReceiptIds+=(messageId+",");
        if(!IMSDK.sendReceiptTask){
            IMSDK.sendReceiptTask=window.setInterval(function(){
                if(""==IMSDK.waitSendReceiptIds)
                    return;
                 var receipt=IMSDK.buildReceiptMessage(IMSDK.waitSendReceiptIds,1,IMSDK.server);
                 var buffer=IMSDK.encodeMessage(receipt,Command.MESSAGE_RECEIPT);
                 IMSDK.sendBytes(buffer);
                 //console.log("sendReceipt ===> "+JSON.stringify(receipt))
                IMSDK.waitSendReceiptIds="";
            },3000); 
        }
       
    },
    /*转换为 客户端的 消息*/
    convertToClientMsg:function(msg){
      msg=JSON.parse(JSON.stringify(msg)) ;
        var message=msg;
        if(msg.messageHead){
            if(ChatType.GROUPCHAT==msg.messageHead.chatType){
                message.from=msg.messageHead.to;
                message.to=IMSDK.userIdStr;
                message.fromJid=msg.messageHead.from;
            }else{
                message.from=msg.messageHead.from;
                message.to=msg.messageHead.to;
            }
         message.messageId=msg.messageHead.messageId;
         message.chatType=msg.messageHead.chatType;
         message.offline=msg.messageHead.offline;
         message.timeLen=message.fileTime;
         if(message.locationX){
             message.location_x=message.locationX;
              delete message["locationX"];
         }
        if(message.locationY){
            message.location_y=message.locationY;
             delete message["locationY"];
         }
             
         delete message["messageHead"];
         delete message["fileTime"];

        }else{
            message.messageId=msg.messageId;
            message.chatType=msg.chatType;
        }
           
       /* var dataStr=JSON.stringify(message);
        console.log("convertToClientMsg end  ===> "+dataStr);*/
        return message;
       
    },
    buildChatMessage:function(){

    },
    /*创建消息头*/
    buildMessageHead:function(to,chatType){

        var head = {
                from:IMSDK.userIdStr,
                messageId:IMSDK.randomUUID(),
                chatType:!chatType?0:chatType,
                to:!to?"":(to+""),
            };
        return head;
    },
    buildAuthMessage:function(){
        var head=IMSDK.buildMessageHead("server",ChatType.AUTH);
        var message={
            messageHead:head,
            token:IMSDK.token,
        }
        return message;
    },
    buildPingMessage:function(){
        var head=IMSDK.buildMessageHead("server",ChatType.PING); 
        var message={
            messageHead:head,
         }
        return message;
    },
    buildReceiptMessage:function(messageId,chatType,to){
        var head=IMSDK.buildMessageHead(to,chatType);
        var message={
            messageHead:head,
            messageId:messageId,
            status:2,
         }
        return message;
    },
    /*加入群组*/
    joinGroupChat(jid,seconds){
        var head=IMSDK.buildMessageHead("server",ChatType.CHAT);
        var message={
            messageHead:head,
            jid:jid,
            seconds:seconds,
        }
        var buffer=IMSDK.encodeMessage(message,Command.JOINGROUP_REQ);
        IMSDK.sendBytes(buffer);
    },
    exitGroupChat(jid){
        var head=IMSDK.buildMessageHead("server",ChatType.CHAT);
        var message={
            messageHead:head,
            jid:jid,
        }
        var buffer=IMSDK.encodeMessage(message,Command.EXITGROUP_REQ);
        IMSDK.sendBytes(buffer);
    },
    /*批量请求 群组消息数量*/
    pullBatchGroupMessage:function(jidList){
       var head=IMSDK.buildMessageHead("server",ChatType.CHAT);
        var message={
            messageHead:head,
            jidList:jidList,
            endTime:IMSDK.getCurrentSeconds()
        }
        var buffer=IMSDK.encodeMessage(message,Command.PULL_BATCH_GROUP_MESSAGE_REQ);
        IMSDK.sendBytes(buffer);
       
    },
    /*请求漫游聊天记录*/
    pullHistoryMessage:function(chatType,jid,size,startTime,endTime){
        var head=IMSDK.buildMessageHead("server",chatType);

        var message={
            messageHead:head,
            jid:jid,
            size:size,
            startTime:startTime,
            endTime:endTime
        }
        var buffer=IMSDK.encodeMessage(message,Command.PULL_MESSAGE_RECORD_REQ);
         IMSDK.sendBytes(buffer);
    },
    /*解码*/
    decodeMessage:function(buffer,cmd,messageType){
        
        var message =null;
        if(cmd){
            var key= buffer.length;
           // if(this.mcode){
           //    cmd= (cmd^ (key*3)^ this.mcode) & 0xffff;
           //    /*for (var i = 0; i < buffer.length; i++) {
           //       buffer[i]= ((buffer[i] ^ (key*3+i)^this.mcode) & 0xff);
           //    }*/
           // }else{
           //    cmd= (cmd^ (key*3)) & 0xffff;
           //   /* for (var i = 0; i < buffer.length; i++) {
           //      buffer[i]= ((buffer[i] ^ (key*3+i)) & 0xff);
           //    }*/
           // }
          if(!messageType){
            messageType=IMSDK.getProtoMessageType(cmd);
          }
           message=messageType.decode(buffer);
        }else{
            message= messageType.decode(buffer);
        }
        message.cmd=cmd;

        return message;
    },
    /*编码*/
    encodeMessage:function(jsonMsg,cmd,messageType){
        if(!messageType)
            messageType=IMSDK.getProtoMessageType(cmd);

        var errMsg = messageType.verify(jsonMsg);
        if (errMsg){
            throw Error(errMsg);
        }
         var message = messageType.create(jsonMsg);

        var buffer = messageType.encode(message).finish();
        //console.log("encodeMessage cmd   > "+cmd);
        if(cmd){
            var bytes =new Uint8Array(buffer.length+1);
            var key=buffer.length;
            for (var i = 0; i < buffer.length; i++) {
                bytes[i+1]=buffer[i];
            }
            bytes[0]=cmd;
            // if(this.mcode){
            //     cmd= (cmd^ key ^ this.mcode)& 0xffff;
            //     /*for (var i = 0; i < buffer.length; i++) {
            //          bytes[i+2]=((bytes[i+2] ^(key+i) ^ this.mcode) & 0xff);
            //     }*/
            //     bytes[0]= (cmd & 0xff);
            //     bytes[1]= ((cmd >> 8) & 0xff);
            // }else{
            //     cmd= (cmd^ key)& 0xffff;
            //     /*for (var i = 0; i < buffer.length; i++) {
            //          bytes[i+2]=((bytes[i+2] ^(key+i)) & 0xff);
            //     }*/
            //     bytes[0]= (cmd & 0xff);
            //     bytes[1]= ((cmd >> 8) & 0xff);
            // }

            return bytes;
        }else{
            return buffer;
        }
    },
    /*根据 cmd 获取 proto 的编解码 MessageType */
    getProtoMessageType:function(cmd){
        var messageType=null;
         switch (cmd){
            case Command.COMMAND_CHAT:
                messageType=ProtoMessageType.chatMessage;
              break;
            case Command.COMMAND_AUTH_REQ:
                messageType=ProtoMessageType.authMessageReq;
              break;
            case Command.COMMAND_AUTH_RESP:
                messageType=ProtoMessageType.authMessageResp;
              break;
            case Command.MESSAGE_RECEIPT:
                messageType=ProtoMessageType.messageReceipt;
              break;
            case Command.PULL_MESSAGE_RECORD_REQ:
                messageType=ProtoMessageType.pullMessageHistoryRecordReq;
              break;
            case Command.PULL_MESSAGE_RECORD_RESP:
                messageType=ProtoMessageType.pullMessageHistoryRecordResp;
              break;
            case Command.PULL_BATCH_GROUP_MESSAGE_REQ:
                messageType=ProtoMessageType.pullBatchGroupMessageReq;
              break;
            case Command.PULL_BATCH_GROUP_MESSAGE_RESP:
                messageType=ProtoMessageType.pullBatchGroupMessageResp;
              break;
            case Command.CHAT_OFF_MESSAGE_RESP:
                 messageType=ProtoMessageType.chatOffMessageResp;
               break;
            case Command.SUCCESS:
                messageType=ProtoMessageType.commonSuccess;
              break;
            case Command.ERROR:
                messageType=ProtoMessageType.commonError;
              break;
            case Command.Ping_REQ:
                messageType=ProtoMessageType.pingMessage;
              break;
            case Command.JOINGROUP_REQ:
                messageType=ProtoMessageType.joinGroupMessage;
              break;
            case Command.EXITGROUP_REQ:
                messageType=ProtoMessageType.exitGroupMessage;
              break;
            case Command.GROUP_REQUEST_RESULT:
                messageType=ProtoMessageType.groupMessageResp;
              break;
            case Command.Login_Conflict:
                 messageType= ProtoMessageType.commonError;
              break;
            default://默认 其他
                break ;
            }

        return messageType;
    },
    getUserIdFromJid:function (jid){
        jid+="";
        return jid ? jid.split("/")[0] : "";
    },
    getBareJid: function (jid){
        jid+="";
        return jid ? jid.split("/")[0] : "";
    },
    getResource : function(jid) {
        if(mpCommon.isNil(jid))
            return "";
        jid+="";
        var resource = jid.substr(jid.indexOf("/")+1, jid.length);
        return resource;
    },
    /*是否为群组 Jid*/
    isGroup : function(userId) {
        var reg = /^[0-9]*$/;
        if(!reg.test(userId))
            return 1;
        else
            return 0;
    },
    randomUUID : function() {
        return IMSDK.cont+IMSDK.getCurrentSeconds()+Math.round(Math.random()*1000);
    },
    getCurrentSeconds:function(){
        return Math.round(new Date().getTime());
    },
       
}

var Command={
    /*握手请求，含http的websocket握手请求*/
    COMMAND_HANDSHAKE_REQ:1,
    /*握手响应，含http的websocket握手响应*/
    COMMAND_HANDSHAKE_RESP:2,
    /*登录消息请求*/
    COMMAND_AUTH_REQ:5,
    /*登录消息结果*/
    COMMAND_AUTH_RESP:6,
    /*关闭请求*/
    COMMAND_CLOSE:7,
    /*聊天请求*/
    COMMAND_CHAT:10,
    /*消息回执*/
    MESSAGE_RECEIPT:11,
    /*拉取 聊天历史记录 */
    PULL_MESSAGE_RECORD_REQ:12,
    /*拉取 聊天历史记录 结果*/
    PULL_MESSAGE_RECORD_RESP:13,
    /*批量拉取群组消息数量  请求*/
    PULL_BATCH_GROUP_MESSAGE_REQ:14,
    /*批量拉取群组消息数量  结果*/
    PULL_BATCH_GROUP_MESSAGE_RESP:15,
    /* 单聊离线消息 */
    CHAT_OFF_MESSAGE_RESP:16,
    /*失败错误*/
    ERROR:-1,
    /*登陆 被挤下线*/
    Login_Conflict:-3,
    /*加入群组*/
    JOINGROUP_REQ:20,
    /*退出群组*/
    EXITGROUP_REQ:21,
    /*群组请求结果协议*/
    GROUP_REQUEST_RESULT:22,
    /*心跳消息*/
    Ping_REQ:99,
    /*成功请求*/
    SUCCESS:100,
  }

 var ProtoMessageType={
    messageHead:null,
    chatMessage:null,
    authMessageReq:null,
    authMessageResp:null,
    messageReceipt:null,
    joinGroupMessage:null,
    exitGroupMessage:null,
    groupMessageResp:null,
    pullMessageHistoryRecordReq:null,
    pullMessageHistoryRecordResp:null,
    pullBatchGroupMessageReq:null,
    pullBatchGroupMessageResp:null,
    chatOffMessageResp:null,
    pingMessage:null,
    commonSuccess:null,
    commonError:null,
  };

  protobuf.load("proto/message.proto",function (err,root) {
    if(err)
        throw err;
   ProtoMessageType.messageHead=root.lookupType("Message.MessageHead");

   ProtoMessageType.chatMessage=root.lookupType("Message.ChatMessage");

   ProtoMessageType.authMessageReq=root.lookupType("Message.AuthMessage");

   ProtoMessageType.authMessageResp=root.lookupType("Message.AuthRespMessageProBuf");

    ProtoMessageType.messageReceipt=root.lookupType("Message.MessageReceiptStatusProBuf");

    ProtoMessageType.joinGroupMessage=root.lookupType("Message.JoinGroupMessageProBuf");

    ProtoMessageType.exitGroupMessage=root.lookupType("Message.ExitGroupMessageProBuf");

    ProtoMessageType.groupMessageResp=root.lookupType("Message.GroupMessageRespProBuf");

    ProtoMessageType.pullMessageHistoryRecordReq=root.lookupType("Message.PullMessageHistoryRecordReqProBuf");
    
    ProtoMessageType.pullMessageHistoryRecordResp=root.lookupType("Message.PullMessageHistoryRecordRespProBuf");
    
    ProtoMessageType.pullBatchGroupMessageReq=root.lookupType("Message.PullBatchGroupMessageReqProBuf");

    ProtoMessageType.pullBatchGroupMessageResp=root.lookupType("Message.PullGroupMessageRespProBuf");

    ProtoMessageType.chatOffMessageResp=root.lookupType("Message.OffChatMessage")

    ProtoMessageType.pingMessage=root.lookupType("Message.PingMessageProBuf");

    ProtoMessageType.commonSuccess=root.lookupType("Message.CommonSuccessProBuf");
    
    ProtoMessageType.commonError=root.lookupType("Message.CommonErrorProBuf");

});
var ChatType={
     UNKNOW:0,
    /**
     * 单聊
     */
    CHAT:1,
    /**
     * 群聊
     */
    GROUPCHAT:2,
    /**
     * 广播
     */
    ALL:3,

    /*授权*/
    AUTH:5,
    
    /**
     *心跳消息
     */
    PING:9,
    /**
     * 返回结果
     */
    RESULT:10,
    /**
     * 消息回执
     */
    RECEIPT:11,
}


   

/*var ClientMessage={
    from:null,
    to:null,
    messageId,
    buildFromNetMsg:function(msg){

    },
}*/
  
