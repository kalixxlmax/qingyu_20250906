var SKIMSDK={

    cont:"skimweb_",
    connection:null,
    serverUrl:null,
    /*服务器的 domain*/
    server:null,
    /*登陆成功的回调*/
    logincallBack:null,
    /*收到消息的回调*/
    messageReceiver:null,
    /*消息回执 处理方法*/
    handlerMsgReceipt:null,
    handlerLoginConflict:null,
    userIdStr:null,
    resource:null,
    token:null,
    /*心跳间隔时间*/
    pingTime:30,
    /*最后一次传递消息的时间*/
    lastTransferTime:0,
    /*是否启用批量发送消息回执 */
    enableAckReceipt:false,
    waitSendReceiptIds:"",
    /*自定义 服务器送达机制的 命名空间*/
    NS_SKACK:"xmpp:chat:ack",


    initApi(url,userId,resource,token,pingTime,server){
        
        SKIMSDK.token=token;
        SKIMSDK.serverUrl=url;
        SKIMSDK.server=server;
        SKIMSDK.resource=SKIMSDK.resource;
        SKIMSDK.userIdStr=SKIMSDK.getFullUserJid(userId)+"/"+resource;
        SKIMSDK.pingTime=pingTime;
        /*SKIMSDK.messageReceiver=messageReceiver;
        SKIMSDK.handlerMsgReceipt=handlerMsgReceipt;
        SKIMSDK.handlerLoginConflict=handlerLoginConflict;*/
        
    },
    loginIM:function(callback){
          try {
            if(callback)
                SKIMSDK.logincallBack=callback;

            SKIMSDK.connection=null;
			SKIMSDK.connection = new Strophe.Connection(SKIMSDK.serverUrl,{'keepalive': true});	
		console.log("xmpp开始链接-----");
		SKIMSDK.connection.xmlInput = SKIMSDK.onmessage;
		SKIMSDK.connectingCount=0;
		SKIMSDK.connect();	
        } catch (e) {
           console.log(e.name + ": " + e.message);
        }
         
    },
    loginSuccess:function(message){
    	 SKIMSDK.sendEnableAckReceipt();
        if(SKIMSDK.logincallBack)
                SKIMSDK.logincallBack(message);
       clearInterval(SKIMSDK.ping);
        //XmppSdk.ping=null
        SKIMSDK.reConn=true;
        SKIMSDK.connectingCount=0;
        SKIMSDK.send($pres());
        //SKIMSDK.enableStream();
        SKIMSDK.pingTime=SKIMSDK.pingTime*1000;
        SKIMSDK.ping = window.setInterval(function(){
            SKIMSDK.sendPing();
        },SKIMSDK.pingTime); 
       

        
    },
    connect:function(){
    	SKIMSDK.connection.connect(
				 SKIMSDK.userIdStr,
				 SKIMSDK.token,
				 SKIMSDK.onConnect,
				 50
			);
    },
	onConnect:function(status) {
		/*xmpp 链接成功*/
		if (status == Strophe.Status.CONNECTED) {
			console.log("xmpp连接成功-----");
			SKIMSDK.loginSuccess();
			

		} else if (status = Strophe.Status.CONNECTING) {
			SKIMSDK.connectingCount++;
			console.log("xmpp连接中 。。。"+SKIMSDK.connectingCount);
			UI.offline();
			if(SKIMSDK.connectingCount>3){
				SKIMSDK.loginIM();
			}
		} else if(status = Strophe.Status.CONNFAIL){
			UI.offline();
		} else if(status = Strophe.Status.DISCONNECTED){
			UI.offline();
			SKIMSDK.disconnect();
		}else {
			UI.offline();
			console.log("xmpp连接失败 ！！！"+status);
			
		}
	},
   
    /*收到服务器消息*/
    onmessage:function(elem){
    	 //if("body"==elem.nodeName)
       		//chatLog("onmessage  "+Strophe.serialize(elem));
      
       if(SKIMSDK.checkConflict(elem))
			return null;
		/*检查 是否 开启自定义送达机制*/
       if(!SKIMSDK.enableAckReceipt){
       	  if(SKIMSDK.handlerEnableAckResult(elem)){
       	 		return;
       	  }
       }
		/*else if(elem.nodeName!="message"){
			return null;
		}*/
		SKIMSDK.lastTransferTime=SKIMSDK.getCurrentSeconds();
		var msgArr=SKIMSDK.getMessages(elem,"message");
		if(null==msgArr)
			return;	
		var message=null;
		for (var i = 0; i < msgArr.length; i++) {
			 message=msgArr[i];
			 if(!message)
			 	continue;
			else if(SKIMSDK.checkReceived(message)){
				continue;
		 	}
		 	message=SKIMSDK.convertToClientMsg(message);
		 	if(!message)
		 		continue;
			 //chatLog("收到 receiver  "+Strophe.serialize(message));
			 //处理单条消息
			SKIMSDK.messageReceiver(message);
			
		}

       /*var dataStr=JSON.stringify(message);*/
    },
    disconnect:function(e){
    	clearInterval(SKIMSDK.ping);
        SKIMSDK.connection.disconnect("离线");
        SKIMSDK.connection=null;
    },
    isConnect:function(){
        if(!SKIMSDK.connection)
            return false;
        return SKIMSDK.connection.connected;
    },
    sendPing:function(){
        /*var currTime=SKIMSDK.getCurrentSeconds();
        if((currTime-SKIMSDK.pingTime)<SKIMSDK.lastTransferTime){
            //最后通讯时间相近 不需要发送心跳包
            //console.log("最后通讯时间相近 不需要发送心跳包");
            return ;
        }*/

        //console.log("发送心跳包");
       var messageId=SKIMSDK.randomUUID();
		var iq = $iq({
			id : messageId,
			to :SKIMSDK.server,
			type : "get"
		}).c("ping", {
			xmlns : "urn:xmpp:ping"
		}, null);
		SKIMSDK.send(iq.tree());
    },
    onerror:function(e){
        console.log("onerror ====> "+e);
    },
    onclose:function(e){
        console.log("onclose ====> "+e);
        SKIMSDK.loginIM();
    },
    handlerMessageBycmd(cmd,message){
       switch (cmd){
            case Command.COMMAND_CHAT:
                SKIMSDK.sendReceipt(message);
                SKIMSDK.messageReceiver(message);
                break;
            case Command.SUCCESS:
                SKIMSDK.handlerMsgReceipt(message);
                break;
            case Command.MESSAGE_RECEIPT:
                SKIMSDK.handlerMsgReceipt(message);
                break;
          	default://默认 其他
                content="";
                break ;

        }
    },
   
    /*
    发送消息 api  
    */
    sendMessage:function(message){
       var elem=SKIMSDK.buildMessage(message);
       SKIMSDK.send(elem.tree());
    },
    sendBytes:function(bytes){
        //console.log("sendBytes  ===>  "+bytes);
        SKIMSDK.connection.send(bytes);
    },
    send:function(elem){
    	SKIMSDK.connection.send(elem);
    },
  	/*转换为 客户端的 消息*/
    convertToClientMsg:function(elem){
     	var bodyElem = elem.getElementsByTagName('body')[0];
		var type = elem.getAttribute('type');		
		 if ((type !=ChatType.CHAT&& type !=ChatType.GROUPCHAT) || bodyElem == undefined || bodyElem.length <= 0) {
			//chatLog("跳过： type "+type+"  "+ bodyElem);
			return null;
		}
			
		var bodyText = Strophe.getText(bodyElem);
		if ("{" != bodyText.charAt(0) || "}" != bodyText.charAt(bodyText.length - 1)) {
			//chatLog("跳过：" + bodyText);
			return null;
		}
		
		
		var message = JSON.parse(bodyText.replace(/&quot;/gm, '"'));
		console.log("========>>>> receiver  msg bodyTesxt : " + bodyText.replace(/&quot;/gm, '"'));

		message.chatType=type;
		
		message.from = elem.getAttribute('from');
		message.to=elem.getAttribute('to');
		
		var fromResource=SKIMSDK.getResource(message.from);
		if(fromResource && ChatType.CHAT==type){
		    message.from=SKIMSDK.getUserIdFromJid(message.from)+"/"+fromResource;
		    var toResource = SKIMSDK.getResource(message.to);
		    if(toResource)
				message.to=SKIMSDK.getUserIdFromJid(message.to)+"/"+toResource;
			else 
				message.to=SKIMSDK.getUserIdFromJid(message.to);
		}else{
			message.from=SKIMSDK.getUserIdFromJid(message.from);
			message.to=SKIMSDK.getUserIdFromJid(message.to);
		}
		
		message.messageId=elem.getAttribute('id');

		if(SKIMSDK.enableAckReceipt&&message.messageId){
			/*
			开启了 送达机制的 情况  发送回执
			*/
		   SKIMSDK.sendAckReceiptIds(message.messageId);
		}
		if(type ==ChatType.CHAT){//单聊 
			// 收到消息立即发送回执给发送者
		  var delay=elem.getAttribute("delay");//有这个字段就代表是离线消息
		  if(!delay){
		  	//离线消息 不发送达回执
		  	/*if(SKIMSDK.hasRequestReceipt(elem)){
		  	   SKIMSDK.sendReceipt(elem.getAttribute('from'),message.messageId);
		  	}*/
		  	
		  } 
		    	
		}	
       /* var dataStr=JSON.stringify(message);
        console.log("convertToClientMsg end  ===> "+dataStr);*/
        return message;
    },
    buildChatMessage:function(){

    },
    /*创建消息*/
    buildMessage:function(msgObj){
    	var elem=null;
    	var message=msgObj;
    	message.isReadDel = msgObj.isReadDel ? 1 : 0;
    	var chatType=message.chatType;
    	//delete message['chatType'];
    	var to=WEBIM.getUserIdFromJid(message.to);
       if (SKIMSDK.isGroup(to)) {
		   elem = $msg({
				from:SKIMSDK.userIdStr,
				to : SKIMSDK.getFullGroupJid(message.to),
				type : 'groupchat',
				id : message.messageId
			}).c("body", null, JSON.stringify(message));

		} else {
			var resource=SKIMSDK.getResource(message.to);
			if(resource)
				to=SKIMSDK.getFullUserJid(to)+"/"+resource;
			else
				to=SKIMSDK.getFullUserJid(to);
			elem = $msg({
				from:SKIMSDK.userIdStr,
				to :to,
				type : 'chat',
				id : message.messageId
			}).c("body", null, JSON.stringify(message));
			
		}
	    /**
         *需要序列号 1-9、26、28、29、40、41、80、81、82、84、85、87
         **/
        if(9>=message.type || message.type == 26 || message.type == 28 || message.type == 29 ||
        message.type == 40 || message.type == 41 || message.type == 80 || message.type == 81 ||
        message.type == 82 || message.type == 84 || message.type == 85 || message.type == 87){
            elem.c("subject", null, '-1'); //请求序列号
        }
		elem.c("request", {
			xmlns : "urn:xmpp:receipts"
		}, null);
		var text=Strophe.serialize(elem);
		console.log("send Message"+text.replace(/&quot;/gm, '"'));
		return elem;
    },
  	/*加入群组*/
    joinGroupChat(jid,userId,seconds){
        SKIMSDK._XEP_0045_037(jid,userId,seconds);
    },
    exitGroupChat(jid){
        SKIMSDK._XEP_0045_038(jid,userId);
    },
   //检测是否为下线消息
	checkConflict:function(elem){
		var type = elem.getAttribute('type');
		var condition=elem.getAttribute('condition');
		if(!type||!condition)
			return false;
		if("remote-stream-error"==condition&&"terminate"==type){
			SKIMSDK.handlerLoginConflict();
			return true;
		}
		return false;
	},
	//检测是否为消息回执
	checkReceived:function(message){
		var received=message.getElementsByTagName('received')[0];
		var from = message.getAttribute('from');
		if(!received)
			return false;
		//chatLog("收到回执 checkReceived  "+Strophe.serialize(message));
		var id = received.getAttribute('id');
		var xmlns=received.getAttribute('xmlns');
		if(!xmlns&&!id){
			return false;
		}

		//多设备模块的  回执处理
		/*if(1==mpMyData.multipleDevices && getLoginData().userId==SKIMSDK.getUserIdFromJid(from) && 
			-1!=DeviceManager.allDeviceArr.indexOf(WEBIM.getResource(from))){
			chatLog("更新多设备状态  ===>>>> resources "+ WEBIM.getResource(from));
			DeviceManager.updateDeviceStatus(WEBIM.getResource(from), 1);
		   //return true;
		}*/

		SKIMSDK.handlerMsgReceipt(id);
		return true;
	},
	hasRequestReceipt:function(elem){
		/*是否要求 发送消息回执*/
		var request=elem.getElementsByTagName('request')[0];
		if(mpCommon.isNil(request)){
			return false;
		}
		return "urn:xmpp:receipts"==request.getAttribute('xmlns');
	},
	//消息回执
	sendReceipt : function(from, id) {
		var receipt = $msg({
			to : from,
			from : SKIMSDK.userIdStr,
			type : 'chat'
		}).c("received", {
			xmlns : "urn:xmpp:receipts",
			id : id
		}, null);
		SKIMSDK.send(receipt.tree());

		chatLog("发送回执： messageId " + id);
	},
	/*
	批量发送 到服务器 消息回执
	*/
	sendAckReceiptIds:function(messageId){
	  SKIMSDK.waitSendReceiptIds+=(messageId+",");
		if(!SKIMSDK.sendReceiptTask){
			SKIMSDK.sendReceiptTask=window.setInterval(function(){
				if(""==SKIMSDK.waitSendReceiptIds)
					return;
				var receipt= $iq({
				  "from":SKIMSDK.userIdStr,
			      "to": SKIMSDK.server,
			      "type":'set',
			      "version": '1.0'
		    	}).c("body",{xmlns :SKIMSDK.NS_SKACK},SKIMSDK.waitSendReceiptIds);
				SKIMSDK.send(receipt.tree());
				SKIMSDK.waitSendReceiptIds="";
				//console.log("sendReceipt ===> "+Strophe.serialize(receipt))
	        },3000); 
		}
		
	},
	sendEnableAckReceipt:function(){
		/*发送启用批量发送消息回执机制命令*/
		 var enable= $iq({
		 	      "from":SKIMSDK.userIdStr,
			      "to": SKIMSDK.server,
			      "type":'set',
			      "version": '1.0'
		    	}).c("enable",{xmlns :SKIMSDK.NS_SKACK},"enable");
		 console.log("sendEnableAckReceipt > "+Strophe.serialize(enable));
		 SKIMSDK.send(enable.tree());
	},
	handlerEnableAckResult:function(elem){
		if("iq"==elem.nodeName&&elem.firstChild){
			if("enable"==elem.firstChild.nodeName&&SKIMSDK.NS_SKACK==elem.firstChild.namespaceURI){
       	 		 chatLog("handlerEnableAckResult  "+Strophe.serialize(elem));
       	 		/*启用成功发送消息回执机制*/
				SKIMSDK.enableAckReceipt=true;
				chatLog("启用消息送达机制 成功=====>");
				return true;
       	 	}
       }
		return false;
		
	},
	getMessages:function(elem,nodeName){
		let msgArr=new Array();
		if (elem.firstChild&&elem.firstChild.nodeName != "message") {
			msgArr.push(elem);
			return msgArr;
		}else if (elem.childNodes.length == 0){
			msgArr.push(elem);
			return msgArr;
		}
		
		let child=null;
		for (let i = 0; i < elem.childNodes.length; i++) {
                child = elem.childNodes[i];
                if (nodeName!=child.nodeName) 
                	continue;
               	msgArr.push(child);
        }

		return msgArr;
		
	},
	getFullUserJid:function(userId){
		if(!userId)
			return userId;
		return userId+"@"+SKIMSDK.server;
	},
	getFullGroupJid:function(jid){
		if(!jid)
			return jid;
		return jid+"@muc."+SKIMSDK.server;
	},
	getUserIdFromJid:function (jid){
		/*debugger*/
    	jid+="";
        return jid ? jid.split("@")[0] : "";
    },
	getBareJid: function (jid){
        jid+="";
        return jid ? jid.split("/")[0] : "";
    },
    getResource : function(jid) {
    	if(!jid)
    		return null;
    	jid+="";
		var arr = jid.split("/");
        if (arr.length < 2) { return null; }
        arr.splice(0, 1);
        return arr.join('/');
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
        return SKIMSDK.cont+SKIMSDK.getCurrentSeconds()+Math.round(Math.random()*1000);
    },
    getCurrentSeconds:function(){
        return Math.round(new Date().getTime());
    },
	_XEP_0045_018 : function(to) {
		var id = SKIMSDK.randomUUID();
		var pres = $pres({
			id : id,
			to : to
		}).c("x", {
			xmlns : "http://jabber.org/protocol/muc"
		});
		// 发送报文
		SKIMSDK.send(pres.tree());
	},
	_XEP_0045_018 : function(groupId, userId) {
		var id = SKIMSDK.randomUUID();
		var to =SKIMSDK.getFullGroupJid(groupId)+ "/" + userId;
		var pres = $pres({
			id : id,
			to : to
		}).c("x", {
			xmlns : "http://jabber.org/protocol/muc"
		});
		// 发送报文
		SKIMSDK.send(pres.tree());
	},
	/**
	 * [_XEP_0045_0137 用户请求不发送历史消息]
	 * @param  {[type]} groupId [群组JID]
	 * @param  {[type]} userId [用户userId]
	 * @return {[type]}         [description]
	 */
	_XEP_0045_037 : function(groupId,userId,seconds) {
		var id = SKIMSDK.randomUUID();
		var to =SKIMSDK.getFullGroupJid(groupId)+ "/" + userId;
		
		if(!seconds){
		 	var logOutTime=DataUtils.getLogoutTime();
			if(logOutTime>0)
			  seconds=getCurrentSeconds()-DataUtils.getLogoutTime();
			else
			    seconds=0;
		}
		
		chatLog(to+" to _XEP_0045_037 seconds "+seconds);
		var pres = $pres({
			id : id,
			to : to
		}).c("x", {
			xmlns : "http://jabber.org/protocol/muc"
		}).c("history",{seconds:seconds});
		///maxstanzas:'100',
		
		// 发送报文
		SKIMSDK.send(pres.tree());
		//console.log(" 037 "+pres.tree());
	},
	/**
	 * [_XEP_0045_038 退出群聊]
	 * @param  {[type]} groupId [群组JID]
	 * @param  {[type]} userId  [用户userId]
	 * @return {[type]}         [description]
	 */
	_XEP_0045_038 : function(groupId,userId) {
		var id = SKIMSDK.randomUUID();
		var to =SKIMSDK.getFullGroupJid(groupId)+ "/" + userId;
		var pres = $pres({
			id : id,
			to : to,
			type : 'unavailable'
		});
		// 发送报文
		SKIMSDK.send(pres.tree());
		
	},
	/**
	 * 143. Jabber用户新建一个群组并声明对多用户聊天的支持
	 * 
	 * @param groupId
	 * @param groupName
	 * @param groupDesc
	 * @param userId
	 * @param cb
	 */
	_XEP_0045_143 : function(groupId, groupName, groupDesc, userId, cb) {
		var id = SKIMSDK.randomUUID();
		var to =SKIMSDK.getFullGroupJid(groupId)+ "/" + userId;
		var pres = $pres({
			id : id,
			to : to
		}).c("x", {
			xmlns : "http://jabber.org/protocol/muc"
		});
		// 监听回调
		/*var handler = GroupManager.getCon().addHandler(function(stanza) {
			// 回调成功
		}, null, 'presence', null, id, null, null);*/
		
		// 发送报文
		SKIMSDK.connection.sendIQ(pres.tree(),function(stanza){
				 console.log("_XEP_0045_143 result "+Strophe.serialize(stanza));
		},function(stanza){
			 console.log("_XEP_0045_143 error "+Strophe.serialize(stanza));
		},null);

		SKIMSDK._XEP_0045_144(groupId, groupName, groupDesc, userId, cb);	
	},
	/**
	 * 144. 服务承认群组新建成功
	 * 
	 * @param groupId
	 * @param groupName
	 * @param groupDesc
	 * @param userId
	 * @param cb
	 */
	_XEP_0045_144 : function(groupId, groupName, groupDesc, userId, cb) {
		// 服务承认群组新建成功
		SKIMSDK._XEP_0045_146(groupId, groupName, groupDesc, userId, cb);
	},
	/**
	 * 146. 所有者请求配置表单
	 * 
	 * @param groupId
	 * @param groupName
	 * @param groupDesc
	 * @param userId
	 * @param cb
	 */
	_XEP_0045_146 : function(groupId, groupName, groupDesc, userId, cb) {
		var id = SKIMSDK.randomUUID();
		var to =SKIMSDK.getFullGroupJid(groupId)+ "/" + userId;
		var iq = $iq({
			id : id,
			to : to,
			type : "get"
		}).c("query", {
			xmlns : "http://jabber.org/protocol/muc#owner"
		}, null);
		SKIMSDK.connection.sendIQ(iq, function(stanza) {
			// 147. 服务发送配置表单
			// 148. 服务通知所有者没有配置可用
			SKIMSDK._XEP_0045_149(groupId, groupName, groupDesc, userId, cb);
		}, function(stanza) {
			// 请求配置表单失败
			//cb(0, "请求配置表单失败");
		});
		cb(0, "");
	},
	/**
	 * 149. 所有者提交配置表单
	 * 
	 * @param groupId
	 * @param groupName
	 * @param groupDesc
	 * @param userId
	 * @param cb
	 */
	_XEP_0045_149 : function(groupId, groupName, groupDesc, userId, cb) {
		var x = $build("x", {
			xmlns : "jabber:x:data",
			type : "submit"
		});
		x.cnode($build("field", {
			"var" : "muc#roomconfig_roomname",
			"type" : "text-single"
		}).c("value", null, groupName).tree());
		x.up().cnode($build("field", {
			"var" : "muc#roomconfig_roomdesc",
			"type" : "text-single"
		}).c("value", null, groupDesc).tree());
		x.up().cnode($build("field", {
			"var" : "muc#roomconfig_persistentroom",
			"type" : "boolean"
		}).c("value", null, 1).tree());
		x.up().cnode($build("field", {
			"var" : "muc#roomconfig_publicroom",
			"type" : "boolean"
		}).c("value", null, 1).tree());
		x.up().cnode($build("field", {
			"var" : "muc#roomconfig_enablelogging",
			"type" : "boolean"
		}).c("value", null, 1).tree());

		var id = SKIMSDK.randomUUID();
		var to =SKIMSDK.getFullGroupJid(groupId)+ "/" + userId;
		var iq = $iq({
			id : id,
			to : to,
			type : 'set'
		}).c("query", {
			xmlns : "http://jabber.org/protocol/muc#owner"
		}, null).cnode(x.tree());

		SKIMSDK.connection.sendIQ(iq.tree(), function(stanza) {
			// 150. 服务通知新群组所有者成功
			// 151. 服务通知所有者请求的配置选项不被接受
			//cb(0, "");
		}, function(stanza) {
			//cb(0, "");
			//cb(1, "提交配置表单失败");
		});
	},
	enableStream:function(){
		/*启用流管理*/

			/*
			启用流管理  XEP-0198
			*/
			SKIMSDK.connection.streamManagement.logging = true;
			var streamId=SKIMSDK.connection.streamId;
			if(!mpCommon.isNil(streamId)){
				SKIMSDK.connection.streamManagement.enable();
				//this.resumeStream(streamId);
			}else{
				SKIMSDK.connection.streamManagement.enable();
			}

			//if(1!=AppConfig.isOpenReceipt){
				
			//}

			/*您还可以启用该选项以在每个收到的节上发送请求响应*/
			//connection.streamManagement.autoSendCountOnEveryIncomingStanza = true;
			//connection.streamManagement.returnWholeStanza = true;
			//connection.streamManagement.requestAcknowledgement();

			
	},
	resumeStream:function(streamId){
		/*0198 恢复流*/
		
		if(!mpCommon.isNil(streamId)){
			SKIMSDK.connection.streamManagement.resumeStream(streamId);
		}
		
	},
	enabledStream:function(streamId){
		/*启用成功流*/
		SKIMSDK.connection.streamId=streamId;
		DataUtils.setStreamId(streamId);

		SKIMSDK.connection.streamManagement.requestResponseInterval =1;
		SKIMSDK.connection.streamManagement.addAcknowledgedStanzaListener(function(id){
			chatLog("streamManagement ack  "+id);
			ConversationManager.processReceived(id);
		});
	},
	selfSend:function(stanza,callback){
		
	    var xhr=this.newXHR();
	    var data=Strophe.serialize(stanza);
		console.log("selfSend  "+data);
		xhr.responseCall=callback;
	    xhr.send(data);

	    SKIMSDK.connection.xmlOutput(stanza);
	     SKIMSDK.connection.rawOutput(stanza);
         
         
	},
	newXHR: function () {
        var xhr = null;
        if (window.XMLHttpRequest) {
            xhr = new XMLHttpRequest();
            if (xhr.overrideMimeType) {
                xhr.overrideMimeType("text/xml; charset=utf-8");
            }
        } else if (window.ActiveXObject) {
            xhr = new ActiveXObject("Microsoft.XMLHTTP");
        }
        //xhr.responseType = "document";
        // use Function.bind() to prepend ourselves as an argument
        xhr.onreadystatechange =function(){
        	if (xhr.readyState == 4 && xhr.status == 200) {
		       console.log("onreadystatechange  responseText "+xhr.responseText);
		       var node=null;
		        if (xhr.responseXML && xhr.responseXML.documentElement) {
	            	node = xhr.responseXML.documentElement;
	            } else if (xhr.responseText) {
		        	node = new DOMParser().parseFromString(xhr.responseText, 'application/xml').documentElement;
		        }
		       if(xhr.responseCall){
		       		xhr.responseCall(node);
		       }
		    }else {
		       //console.log("onreadystatechange  statusText "+xhr.statusText);
		    }
        };
        xhr.ontimeout = function(e) {
        	console.log("ontimeout   "+e);
        };
  		xhr.onerror = function(e) { 
  			console.log("onerror   "+e);
  		 };
        var contentType = SKIMSDK.connection.options.contentType || "text/xml; charset=utf-8";
        //myConnection.options.sync ? false : true
         xhr.open("POST", SKIMSDK.connection.service,true);
	    if (typeof xhr.setRequestHeader !== 'undefined') {
	        // IE9 doesn't have setRequestHeader
	        xhr.setRequestHeader("Content-Type", contentType);
	       
	     }
	      //xhr.withCredentials = true;
	    if (SKIMSDK.connection.options.withCredentials) {
	        xhr.withCredentials = true;
	    }
	    xhr.getResponse=function () {
	        var node = null;
	        if (xhr.responseXML && xhr.responseXML.documentElement) {
	            node = xhr.responseXML.documentElement;
	            if (node.tagName === "parsererror") {
	                
	                console.log("responseText: " + xhr.responseText);
	                console.log("responseXML: " +
	                              Strophe.serialize(xhr.responseXML));
	                throw "parsererror";
	            }
	        } else if (xhr.responseText) {
	            // In React Native, we may get responseText but no responseXML.  We can try to parse it manually.
	           
	            node = new DOMParser().parseFromString(xhr.responseText, 'application/xml').documentElement;
	            if (!node) {
	                throw new Error('Parsing produced null node');
	            } else if (node.querySelector('parsererror')) {
	                console.log("invalid response received: " + node.querySelector('parsererror').textContent);
	                console.log("responseText: " + xhr.responseText);
	                throw "badformat";
	            }
	        }
       	 return node;
    	};
        return xhr;
    },

       
}

var ChatType={
     UNKNOW:0,
    /**
     * 单聊
     */
    CHAT:"chat",
    /**
     * 群聊
     */
    GROUPCHAT:"groupchat",
    /**
     * 广播
     */
    ALL:3,

    /*授权*/
    AUTH:5,
    
    /**
     *心跳消息
     */
    PING:"ping",
    /**
     * 返回结果
     */
    RESULT:"result",
    /**
     * 消息回执
     */
    RECEIPT:11,
}

