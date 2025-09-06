var NetWork={
	online:true,
	//监听网络状态
	networkListener:function(onCallBack,offCallBack){
		window.addEventListener('online',  function(){
		 	 console.log("===========网络链接了.....");
		 	 NetWork.online=true;
		 	 onCallBack();
		});
		window.addEventListener('offline', function(){
		 	 console.log("=======网络断开了.....");
       WEBIM.disconnect();
       mpDataUtils.setLogoutTime(WEBIM.getTimeSecond());
		 	 NetWork.online=false;
		 	 offCallBack();
	   });
	}
};

//网络链接上了
function onNet(){
	//网络链接上了
	setTimeout(function(){
		WEBIM.loginIM(function(){
			UI.online();
		});
	},2000);
};

//网络断开了
function offNet(){
	 UI.offline();
};

//检查网络状态 和xmpp 链接状态
function checkNetAndXmppStatus(){
	console.log("当前网络状态 "+NetWork.online);
	if(NetWork.online){

	}else{
	    ownAlert(3,mpLanguage.getLanguageName('network_disconnection'));
	}
};

function sleep(numberMillis) {
    var now = new Date();
    var exitTime = now.getTime() + numberMillis;
    while (true) {
        now = new Date();
        if (now.getTime() > exitTime)
            return;
    }
};

/**
* 聊天相关
**/
var MpChat = {

	isOpen : false,// 聊天窗口是否打开
	to : null,// 目标用户
    toNickName : null,// 目标用户昵称
	toUserId:null,
	toResource:null,//目标用户的 设备标识
	talkTime:0,//禁言时间

	isShowNewMsgCut:true,//是否显示新消息分割线,初始为需要显示
	isShowUnreadCount:true,//是否需要显示新消息未读统计，初始需要显示

	minTimeSend:0,//当前聊天好友的 历史记录 最早时间
	/**
	* 打开聊天面板
	**/
	openChatPanel : function(to,name){
		

		//判断目标会话界面是否已经打开
		if (this.isOpen && this.to==to && this.toUserId==WEBIM.getUserIdFromJid(to)){
			return;
		}

		this.isOpen = true;
		this.to = to;
		this.toUserId = WEBIM.getUserIdFromJid(to);
		this.toNickName = name;
		this.toResource = WEBIM.getResource(to);

		//初始化当前界面的新消息未读数据
		this.isShowNewMsgCut=true;
		this.isShowUnreadCount=true;

		mpChatData.timeSendLogMap[this.toUserId]=0;	

		this.minTimeSend=0;
		//将获取消息历史记录的数据进行临时存储
		mpChatData.msgHistory["to"] = to;
		mpChatData.msgHistory["index"] = 0; //将聊天记录的页码数进行初始化

		//更新聊天面板顶部的头像和昵称
		this.showAvatarAndNickname(this.toUserId,this.toNickName);

		//打开聊天面板
		layui.layer.open({
		    title:"",
		    type: 1,
		    shade: false,
		    area: ['550px', '660px'],
		    shadeClose: true, //点击遮罩关闭
		    content: $("#mp_chatPanel"),
		    cancel: function(index, layero){ //关闭聊天面板后执行
			  
		
			  layer.close(index)
			  //清空页面上的聊天消息
			  $("#mp_chatPanel #messageContainer").empty();
			  $("#mp_chatPanel").hide();
			  MpChat.isOpen=false;

			  return false; 
			},
			success : function(layero,index){  //弹窗打开成功后的回调
				
				$(".nano").nanoScroller();

				//加载历史消息
				MpChat.loadHistoryMsg(to,1);
				
				//加载后清除该用户的未读消息数量
				MpChat.clearUserMsgNum(MpChat.toUserId);

				$("#messageBody").empty();

				
			}
		});

	},

	/**
	*  加载历史消息
	**/
	loadHistoryMsg : function(to,type){

		//先读取本地消息记录
		var localMsgRecordList = mpDataUtils.getMsgRecordList(to);
		//读取用户的未读消息
		var localUnReadMsgList = mpDataUtils.getUnReadMsgList(to);

		if(type==1 && (localMsgRecordList.size()>0 || !mpCommon.isNil(localUnReadMsgList) )){
		
		
			if(!mpCommon.isNil(localMsgRecordList)){
				for (var i = 0; i < localMsgRecordList.size(); i++) {
					var msg = localMsgRecordList.get(localMsgRecordList.keys[i]); 
					MpChat.showHistoryToHtml(msg);
				}
			}
			
			//判断是否存在本地未读消息
			if(!mpCommon.isNil(localUnReadMsgList) ){ 

				for (var i = 0; i < localUnReadMsgList.length; i++) {
						if(mpCommon.isNil(localUnReadMsgList[i]))
							continue;
						var msg = localUnReadMsgList[i];

						MpChat.showHistoryToHtml(msg);

						if(WEBIM.isChatType(msg.chatType)&&1!=msg.isRead&&getLoginData().userId==msg.toUserId){//单聊
							
								MpChat.sendReadReceipt(to, msg); //发送已读回执
								//发送已读回执后将该消息加入本地聊天记录中储存
								mpDataUtils.putMsgRecordList(to,msg);
						}
				}
				//加载完本地未读消息到页面后清空该用户的未读消息列表
				mpDataUtils.deleteUnReadMsgList(to);
			}

			MpChat.showLoadHistoryIcon(1); //加载消息历史结束后，显示消息历史的相关Icon	
				
			setTimeout(function(){ //将滚动条移动到最下方
				$(".nano").nanoScroller();//刷新滚动条
				MpChat.scrollToEnd(); //滚动到底部
			},400);


		}else{

			//加载历史消息
			mpHttpApi.showHistory(0, function(status, result) {

				if(mpCommon.isNil(result))
					return;

				if (0!= status)
					ownAlert(2,result);



				var pageMsgHtml="";

				var length = parseInt(result.length);

				//for (var i = length-1; i >= 0; i--) {
				for (var i = 0; i < length; i++) {
					var o = result[i];
					var msg=mpDataUtils.getMessage(MpChat.toUserId,o.messageId,1);
					if(mpCommon.isNil(msg)){
						msg = WEBIM.convertToClientMsg(JSON.parse(o.message.replace(/&quot;/gm, '"')));
					}
					msg.isRead = o.isRead;
					if(msg.isEncrypt)
						msg.content=WEBIM.decryptMessage(msg);
					mpDataUtils.putMsgRecordList(MpChat.toUserId,msg,1);
					
					//记录 当前会话 聊天记录最小时间  拉取漫游使用
					if(0==MpChat.minTimeSend)
						MpChat.minTimeSend=msg.timeSend;
					else if(MpChat.minTimeSend>msg.timeSend){
						MpChat.minTimeSend=msg.timeSend;
					}
					/*if(msg.type>100)
						return;*/

					//if(2==type){
						pageMsgHtml = MpChat.showHistoryToHtml(msg,1)+pageMsgHtml;
					//}else{
						//消息显示到Html中
						//MpChat.showHistoryToHtml(msg);
					//}
					
				}

				if(2==type){
					//为滚动条添加锚点
				 	pageMsgHtml += "<div id='msgAnchor'></div>";   
				 	$("#messageContainer").prepend(pageMsgHtml);

				 	//让滚动条移动翻页之前的消息位置
					$("#messagePanel").nanoScroller({ scrollTo: $('#messageContainer #msgAnchor') });
				
					//清除此次翻页的锚点
					$("#messageContainer #msgAnchor").remove();
				
				}else{

					$("#messageContainer").prepend(pageMsgHtml);

					setTimeout(function(){ //将滚动条移动到最下方
						$(".nano").nanoScroller();//刷新滚动条
						MpChat.scrollToEnd(); //滚动到底部
					},400);
				}

			
				if( length<10){
					//调用方法显示消息记录翻页相关状态 3:没有更多记录了
					MpChat.showLoadHistoryIcon(3); 
				}else{
					MpChat.showLoadHistoryIcon(1);
				}

			
			});
		}

	},
	showLoadHistoryIcon : function(type){ // type:1 查看更多消息   type:2 loading  type:3 没有更多消息了
		
		var logHtml ="<div id='loadHistoryIcon' class='loadHistoryIcon' >";
		if (1==type) { //查看更多消息
			logHtml += "<img src='images/msgHistory.png' style='width:25px; height=25px;display:inline;'>"
					+  "<a href='#' style='font-size: 12px;' onclick='MpChat.loadHistoryMsg("+MpChat.toUserId+",2);'>"+ mpLanguage.getLanguageName('more_news') +"</a>";
		}else if (2==type) { //loading   
			logHtml += "<img src='img/loading.gif'>";
		}else if(3==type){ //没有更多消息了
			logHtml += "<span style='font-size: 12px;'>"+ mpLanguage.getLanguageName('no_news') +"</span>";
		}			
		logHtml+="</div>";
		//清除原有的历史记录Icon显示
		$("#messageContainer #loadHistoryIcon").remove();
		$("#messageContainer").prepend(logHtml);
		// UI.scrollToEnd();

	},
	/*isTop  是否加载历史记录  需要 添加到顶部
	返回 内容*/
	showHistoryToHtml :function(msg,isTop){ //显示历史消息记录
		
		if(mpCommon.isNil(msg))
			return "";

		if(!mpCommon.isNil($("#messageContainer #msg_"+msg.messageId).prop("outerHTML")))
			return "";

		var itemHtml = null;
		
		itemHtml= this.createItem(msg, msg.fromUserId,getLoginData().userId!=msg.fromUserId?0:1);

		if(mpCommon.isNil(itemHtml))   
			return "";

		//记录 当前会话 聊天记录最小时间  拉取漫游使用
		if(0==MpChat.minTimeSend)
			MpChat.minTimeSend=msg.timeSend;
		else if(MpChat.minTimeSend>msg.timeSend){
			MpChat.minTimeSend=msg.timeSend;
		}
		
		if(isTop)
			return itemHtml;
		else
			$("#messageContainer").append(itemHtml);





	},
	//处理发送消息
	processSendMsg:function(msg,toJid){

		msg.id=msg.messageId;
		
		if(mpCommon.isNil(toJid))
		 	toJid = MpChat.to;
		if(mpCommon.isNil(msg.toUserId)){
		 	msg.toUserId=WEBIM.getUserIdFromJid(toJid);
		}

		var content=msg.content;

		MpChat.sendMsg(msg,function(){
			//接受方 为当前打开界面目标用户 才添加消息到界面
			if(MpChat.to==msg.toJid){
				msg.content=content;
					
				$("#messageBody").val("");
				
				if(!(msg.type==2||msg.type==9)||((msg.type==2||msg.type==9)&&1==msg.forward)){ 
					//图片type=2、文件 type=9 消息上传前已做了UI预加载，这里不再次显示UI
					//转发的图片需要显示
					MpChat.showMsg(msg,getLoginData().userId,1);
				}
				
			}

			//发送消息后将该消息加入本地聊天记录中储存
			mpDataUtils.putMsgRecordList(MpChat.toUserId,msg);

			//当消息列表页面打开时更新最后一条消息及时间
			MpChat.showAndUpdateMsgNum(MpChat.toUserId,msg,0);

			//更新本地存储的消息列表顺序
			MpChat.updateMsgListOrder(MpChat.toUserId,content);

			//更新最后一条消息存储记录
			mpDataUtils.putLastMsg(MpChat.toUserId,msg);

				
		},toJid);
	},
	sendMsg : function(msg,callback,toJid) {
		//发送消息开始
		//检查Xmpp 是否在线
		if(WEBIM.isConnect()){
			//检查消息是否需要加密
			 MpChat.sendMsgAfter(msg,toJid);
			  if(callback)	
					callback();
			
		}else{
		
			ownAlert(4,mpLanguage.getLanguageName('drops'),function(){

					WEBIM.loginIM(function(){
						UI.online();
		           		//GroupManager.joinMyRoom(); //xmpp 加群
						MpChat.sendMsgAfter(msg,toJid);

						if(callback)	
						 	callback();
						
					});

			});

		}
	},
	sendMsgAfter:function(msg,toJid){
		//组装xmpp 消息体 继续发送
		var type=msg.type;
		var from = WEBIM.userIdStr;
		// toJid指定的消息接受者
		// Temp.toJid 临时的消息接受者
		// ConversationManager.from  聊天框的消息接受者
		
		
		/*if(mpCommon.isNil(toJid))
		 	toJid = ConversationManager.from;
		if(mpCommon.isNil(msg.toUserId)){
		 	msg.toUserId=WEBIM.getUserIdFromJid(toJid);
		}*/

		msg.to=toJid;
		msg.toJid=toJid;
		var content=msg.content;
		var msgObj=msg;
		//判断是否需要加密消息内容
		msgObj.content = MpChat.checkEncrypt(msgObj);

		//发送消息
		WEBIM.sendMessage(msgObj); 

		
		
		//设置发送消息重发次数
		if(msg.type<100)
			msg.reSendCount=5;
		else
			msg.reSendCount=0;
		msg.content=content;
		// DataUtils.saveMessage(msg);
		return msg;
	},
	sendTimeout:function(msgId){
		var msg=mpDataUtils.getMessage(MpChat.toUserId,msgId,2);
		if(mpCommon.isNil(msg)){
			this.showReSendImg(msgId);
			console.log("sendTimeout  消息找不到了");
			return;
		}
		//检查网络状态
		checkNetAndXmppStatus();
		if(msg.reSendCount>0){
			console.log(" 消息自动重发 "+msgId+"  type "+msg.type+" content ==  "+msg.content+"  reSendCount "+msg.reSendCount);
			msg.reSendCount=msg.reSendCount-1;
			mpDataUtils.saveMessage(msg);
			WEBIM.sendMessage(msg,msgId);
		}else{
			console.log(" showReSendImg "+msgId+"  type "+msg.type+" content ==  "+msg.content+"  reSendCount "+msg.reSendCount);
			this.showReSendImg(msgId);
		}
	},
	checkEncrypt:function(msg,callback){
		//检测消息加密  如果加密 调用接口加密
		var content=msg.content;
		if(mpChatData.isEncrypt == 1 || mpChatData.isEncrypt=='1'){
			return WEBIM.encryptMessage(msg);
		}else{
			return msg.content;
		}
	},
	sendGif:function(gifName){
		$("#emojl-panel #gifList").getNiceScroll().hide(); //隐藏滚动条
		$("#emojl-panel").hide();//隐藏表情面板
		var msg = WEBIM.createMessage(5, gifName);
		UI.sendMsg(msg);
	},
	showMsg:function(msg, fromUserId, direction,isSend,isNewMsg) {
		/**
		 * direction  1 自己发送的   0 别人发送的
		 * isSend 是否 显示发送等待符
		 * isNewMsg 是否为新消息  "newMsg"表示新消息
		 */
		if(mpCommon.isNil(msg.content))
			return;
		if(mpCommon.isNil(isSend))
			isSend=1;
		else
			isSend=0==isSend?0:1;
		var itemHtml = this.createItem(msg, fromUserId, direction,isSend);
		if(mpCommon.isNil(itemHtml))
			return "";
		//direction==0  表示收到消息 ，然后根据坐标判断用户是否在查看聊天记录,如果是则在新消息前添加分割标识
		if(isNewMsg =="newMsg" && direction==0 ){

			if(MpChat.isShowNewMsgCut){
				$("#messageContainer .message_system #newMsg_tips").remove();
				$("#messageContainer").append('<div class="message_system"><div id="newMsg_tips" class="content unread-content">'+
				'<div class="line-left"></div>'+ mpLanguage.getLanguageName('this_new_message') +'<div class="line-right"></div></div></div>');
			  	MpChat.isShowNewMsgCut = false;
			}
			
		}
		//将消息显示到界面		
		$("#messageContainer").append(itemHtml);
		// 滚动到底部
		setTimeout(function(){
			$(".nano").nanoScroller();//刷新滚动条

			if("newMsg"==isNewMsg){
				MpChat.scrollToEnd("receive");//滚动到底部
			}else{
				MpChat.scrollToEnd();
			}
		},500);
		
	},
	createItem : function(msg, fromUserId, direction,isSend) {   //消息Item
		//direction  1 自己发送的   0 别人发送的
		//isSend  发送后创建消息
			
		var contentHtml = this.createMsgContent(msg,direction,isSend);
		var html="";
		
		if(mpCommon.isNil(contentHtml))
			return "";
		var imgUrl=msg.imgUrl;
		//用户头像
		if(mpCommon.isNil(msg.imgUrl)){
			imgUrl=mpCommon.getAvatarUrl(WEBIM.CHAT==msg.chatType?fromUserId:msg.fromUserId);
		}
		var delayTime=msg.timeSend-mpChatData.timeSendLogMap[MpChat.toUserId];
		
		var timeSendStr = "";
		//10分钟
		if(delayTime>(10*60*1000)||delayTime< -(10*60*1000)){
				// var timeSendHtml=UI.createTimeSendLog(msg.timeSend);
				mpChatData.timeSendLogMap[MpChat.toUserId]=msg.timeSend;
				timeSendStr = getTimeText(msg.timeSend,0);
		}


		html+= "<div id=msg_"+msg.messageId+" msgType="+msg.type+" class='msgDiv' >"
            +		"<div class='clearfix'>"
			+			"<div style='overflow: hidden;' >"
			+	    		"<div  class='message " + (0 == direction ? "you" : "me")+"'>";
		if(!mpCommon.isNil(timeSendStr)){
		html+=			        "<div  class='message_system'>"
			+			        	"<div  class='content'>"+timeSendStr+"</div>"
			+			        "</div>";
		}
		html+=	        		"<img  class='avatar' onerror='this.src=\"./images/ic_avatar.png\"' src='" +imgUrl+ "' >"
			+	        		"<div class='content'>"
								//(msg type = 5 表示 gif 动态图 ，若为gif消息则不添加消息气泡背景 )
            +	            	"<div class='bubble js_message_bubble  "+ (0 == direction ?(5 == msg.type?"left'>":"bubble_default left'>"):(5 == msg.type?"right'>":"bubble_primary right'>"))  
			+	               		 "<div class='bubble_cont'>"
			+                          contentHtml
			+	               		 "</div>"
			+	            	"</div>"
			+	        	"</div>"
			+	    	"</div>"
			+		"</div>"
			+	"</div>"
       		+"</div>";
		                      

		return html;

	},
	/*type 为1  的文本消息*/
	createTextMsgContent:function(msg,direction,isSend,contentHtml,msgStatusHtml){
		var content = "";
		content=MpChat.parseContent(msg.content);
		
		contentHtml += "<div class='plain' length='"+msg.content.length+"' >";
		/*if(!WEBIM.isGroupChat(ConversationManager.chatType)&& 0==direction&&mpCommon.isReadDelMsg(msg)){
			contentHtml+= "<pre class='js_message_plain hide'>"+ content +"</pre>"
					   +  "<a href='javascript:void(0)' onclick='UI.showReadDelMsg(\""+msg.messageId+"\");' style=''> 点击查看  T</a>"
					   +  "<span id='msgReadTime_"+msg.messageId+"' class='msg_status_common read_del_time'>10</span>"
					   +"</div>";

			if(1==msg.isShow){
				setTimeout(function(){
					UI.showReadDelMsg(msg.id);
				},1000);
			}
		}else{*/
			contentHtml += "<pre class='js_message_plain'>"+ content +"</pre>"
								   + msgStatusHtml
								   + "</div";
		//}
		return contentHtml;
	},
	createMsgContent:function(msg,direction,isSend){

			var contentHtml = "";
			
			if(mpCommon.isNil(msg.content)){
				return null;
			}
			msg.content=(msg.content).replaceAll('\n','<br/>');

			var msgStatusHtml  ="";  
			this.createMsgStatusIteam(msg,direction,isSend);
			if(99<msg.type&&125>msg.type){
				contentHtml=this.createTextMsgContent(msg,direction,isSend,contentHtml,msgStatusHtml);
				return contentHtml;
			}
			   msgStatusHtml=this.createMsgStatusIteam(msg,direction,isSend);
			switch (msg.type){

				case 1:// 文字、表情
					contentHtml=this.createTextMsgContent(msg,direction,isSend,contentHtml,msgStatusHtml);
				  break;

				case 2:// 图片
                   	contentHtml +=  '<div class="picture">';
				 	if("true"==msg.isReadDel ||1==msg.isReadDel){ //判断是否为阅后即焚消息
				 		contentHtml += '<img id="readDelImg" class="shade" src="' + msg.content + '" style="max-width:320px;max-height:320px;"  onclick="MpChat.showImgZoom(\'' + msg.content + '\',\''+msg.messageId+'\')"/>';
				 	}else{ //不是
				 		contentHtml += '<img class="msg-img" onerror="this.src=\'images/overdue.png\'" src="' + msg.content + '" onclick="MpChat.showImgZoom(\'' + msg.content + '\')">';
				 	}

				 	contentHtml += '</div>'
				 				+msgStatusHtml;

				  break;

				case 3:// 语音 


					contentHtml = "<p id='voiceP_"+msg.messageId+"' class='chat_content' onclick='MpChat.showAudio(\"" + msg.content + "\",\"" + msg.messageId + "\",\""+(mpCommon.isNil(msg.isReadDel)?0:msg.isReadDel)+"\")'>"
							+     "<img id='voiceImg' src='./images/voice.png' style='width:25px; height:25px;margin-top:-2.5px'> <span style='display:inline; margin-left:15px; margin-right:10px;'>"+msg.timeLen+"\" </span>"
							+	  "<div id='voice_"+msg.messageId+"'></div>"
							+ "</p>"
							+msgStatusHtml;
				  break;

				case 4:// 位置
 					contentHtml = '<div class="location">'
 								+	'<a href="javascript:void(0)" onclick="showToMap(this)"'+' lng="'+msg.location_x+' "lat="'+msg.location_y+'">'
 								+		'<img alt="" class="img" src="'+BaiduMap.imgApiUrl+msg.location_y+','+msg.location_x+' ">'
 								+		'<p class="desc ng-binding">'+msg.objectId+'</p>'
 								+	'</a>'
 								+ '</div>'
 								+ msgStatusHtml;
     				break;				
				 	
				  
				case 5:// GIF 动画
					contentHtml = '<div class="emoticon">'
			                	+	'<img  class="custom_emoji msg-img"  src="./gif/'+ msg.content +'">'
			            		+ '</div>'
			            		+ msgStatusHtml;
				   break;

				case 6://视频

					contentHtml ="<div>"
								+	"<video class='video' controls  onended='MpChat.videoPlayEnd(\"" + msg.messageId + "\",\""+msg.isReadDel+"\")' style='width:240px;height:240px;'>"
				  	            +		"<source src='"+msg.content+"' type='video/mp4'>"
					            +	"</video>"
					            //+	"<script>plyr.setup();</script>"
					            +"</div>";
				  break;

				case 7:// 音频
				 
				  break;
				case 8: // 名片

					contentHtml =	'<div class="card" onclick="UI.showUser(\''+ msg.objectId + '\')">'
				                +        '<div class="card_bd">'
				                +            '<div class="card_avatar">'
				                +                '<img class="img"  onerror=\'this.src=\"./images/ic_avatar.png\"\' src=\'' + mpCommon.getAvatarUrl(msg.objectId) + '\' >'
				                +            '</div>'
				               	+            '<div class="info">'
				                +                '<h3 class="display_name">'+ msg.content +'</h3>'
				                +            '</div>'
				                +        '</div>'
				                +        '<div class="card_hd">'
				                +        	'<p class="">个人名片</p>'
				                +        '</div>'
				                +    '</div>'
								+ msgStatusHtml;
				  	break;
				  
				case 9:// 文件
					var fileStr="";
					var fileName="";
					if(!mpCommon.isNil(msg.fileName)){
						fileStr=msg.fileName.substring(msg.fileName.lastIndexOf("/")+1);
						fileName=fileStr.substring(0,fileStr.indexOf("."));
					}else{
						fileStr=msg.content.substring(msg.content.lastIndexOf("/")+1);
						fileName=fileStr.substring(0,fileStr.indexOf("."));
					}
					contentHtml = '<div class="bubble_cont primary">'
								+		'<div class="attach">'
				                +        	'<div class="attach_bd">'
				                +            	'<div class="cover">'
				                +                	'<i class="icon_file"></i>'
				                +           	'</div>'
				                +            	'<div class="cont">'
				                +                	'<p class="title">'+fileStr+'</p>'
				                +                	'<div class="opr">'
				                +                    	'<span  class="">'+mpCommon.parseFileSize(msg.fileSize)+'</span>'
				                +                    	'<span class="sep">  |  </span>'
				                +                   	'<a target="_blank" id="fileDown_'+msg.messageId+'" download="'+fileName+'"  filename="'+fileName+'"  href="' + msg.content+ '"  class="">'+ mpLanguage.getLanguageName('now_download') +'</a>';
				   	if(1 == direction && 1==isSend ){
					    contentHtml +=						'<p id="fileProgress_'+msg.messageId+'" class="progress_bar">'
					            	+                            '<span class="progress" style="width: 0%;"></span>'
					                +                       '</p>';
					}
				     contentHtml+=                	'</div>'
				                +            	'</div>'
				                +        	'</div>'
				                +    	'</div>'
								+ '</div>'
								+ msgStatusHtml;
				  	break;

				case 10://提醒
				 	MpChat.showMsgLog(msg.content,msg.messageId);
				 	return;
				  break;

				case 26:// 已读回执
					mpChatData.msgStatus[msg.content]  =  2; //将发送消息状态进行储存 2:已读
			 		var message = mpDataUtils.getMessage(msg.fromUserId,msg.content,1);
			 		if(mpCommon.isNil(message))
			 			return;
			 		
			 		$("#msgStu_"+msg.content+"").css("background-color" ,"#7BD286");
			 		$("#msgStu_"+msg.content+"").text(mpLanguage.getLanguageName('already_read')).show();
			 		return;
				  break;

				case 28://红包
					/*contentHtml = '<div onclick="UI.openRedPacket(\''+msg.objectId+'\')" class="" style="background:#ff8a2a;">'
								+		'<div class="attach" style="background:#ff8a2a;padding-top:4px">'
				                +        	'<div class="attach_bd">'
				                +            	'<div class="cover">'
				                +                	'<img src="./images/ic_chat_hongbao.png">'
				                +           	'</div>'
				                +            	'<div class="cont" style="line-height:60px;">'
				                +               '<p style="color:white">'+(msg.fileName==3?"口令: ":"")+msg.content+'</p>'
				                +            	'</div>'
				                +        	'</div>'
				                +    	'</div>'
								+ '</div>'
								+ msgStatusHtml;*/
					break;
				 
				  	
				  break;
				/*case 501://同意加好友
					UI.showMsgLog(msg.content);
					UI.showNewFriends(0);
				  break;*/
				case 201:// 正在输入
					
					$("#chatHint").empty().text(mpLanguage.getLanguageName('now_input'));
				 	$("#chatHint").show();
					setTimeout(function () {  //过5秒后隐藏
						$("#chatHint").empty();
		        		$("#chatHint").hide();
		    		}, 5000);
				  break;

				case 80://单条图文
				 	
				  break;

				case 81://多条图文
				 
				  break;

				case 202://消息撤回
					//msg.messageId=msg.content;
					var recallHtml ,msgText = "";

					msgText = (msg.fromUserId==getLoginData().userId) ? mpLanguage.getLanguageName('you_rollback_message') : msg.fromUserName+mpLanguage.getLanguageName('rollback_message');
					
					recallHtml = "<div class='logContent'><span>"+msgText+" ("+getTimeText(msg.timeSend,0)+") </span></div>";

					$("#messageContainer #msg_"+msg.content).remove();
					$("#messageContainer").append(recallHtml);

					//删除本地缓存中要撤回的消息
					mpDataUtils.removeMsgFromRecordList(msg.fromUserId,msg.content);

				 	return;
				  break;

				default://默认 其他
				contentHtml += "<p class='plain'>";
				contentHtml = mpLanguage.getLanguageName("web_not_support") + "</p>";
			}
			/*if(1==ConversationManager.isGroup&&msg.type>400){
				WEBIM.converGroupMsg(msg);
				UI.showMsgLog(msg.content);
				 	return;
			}*/
			return contentHtml;
	},
	//生成消息状态标签 (包括：loading  送达 已读  失败重发 阅后即焚计时 群已读人数等)
	createMsgStatusIteam:function(msg,direction,isSend){

		var  msgStatusHtml = "";

		//单聊消息
		//0 == direction  表示接受到的消息       1 == direction  表示用户自己发送的消息
		msgStatusHtml += ( 0 == direction ? "" : 1==isSend ? "<span id='msgStu_"+msg.messageId+"'  class='msg_status_common send_status'></span>"
			+"<img id='msgLoad_"+msg.messageId+"' class='msg_status_common ico_loading' src='./images/loading.gif'>" :
			(true == msg.isRead ?"<span class='msg_status_common send_status read_bg'>"+ mpLanguage.getLanguageName('already_read') +"</span>" :
				(1 == msg.isReadDel ? "<img class='msg_status_readDel ico_readDel' src='./images/fire.png'><span  id='msgStu_"+msg.messageId+"' class='msg_status_readDel send_status ok_bg'>"+ mpLanguage.getLanguageName('delivery') +"</span>" :
					"<span  id='msgStu_"+msg.messageId+"'  class='msg_status_common send_status ok_bg' style=''>"+ mpLanguage.getLanguageName('delivery') +"</span>")) );
   
		
		return  msgStatusHtml;
	},
	showReSendImg:function(messageId){
		
		 	//消息框显示重发标志
			$("#msgLoad_"+messageId+"").after("<i id='msgResend_"+messageId+"'  class='msg_status_common ico_fail web_wechat_message_fail'  onclick='UI.reSendMsg(\""+messageId+"\")'></i>");
			//移除loading
			$("#msgLoad_"+messageId+"").remove();	
		
	},
	//点击消息重发
	reSendMsg:function(msgId){
		//改变UI
		$("#msgResend_"+msgId+"").after("<img id='msgLoad_"+msgId+"' class='msg_status_common ico_loading' src='./img/loading.gif'>");
		$("#msgResend_"+msgId+"").remove();

		//消息重发
		var msg = mpDataUtils.getMessage(MpChat.toUserId,msgId,2);
		if(mpCommon.isNil(msg))
			return;
		MpChat.sendMsgAfter(msg);
		MpChat.sendTimeoutCheck(msgId); //调用方法进行重发检测
		
	},
	processReceived:function(id){
		
		console.log("收到送达回执 ："+id);
		/*处理收到的消息回执*/
		
		mpChatData.msgStatus[id] = 1; //将发送消息状态进行储存 1:送达
		//将对应消息的状态显示为送达
		$("#msgLoad_"+id+"").remove();
		/*if(MpChat.isReadDelMsg(DataUtils.getMessage(id))){ //阅后即焚消息
			$("#msgStu_"+id+"").before("<img class='msg_status_readDel ico_readDel' src='./images/fire.png'>");
			$("#msgStu_"+id+"").removeClass("msg_status_common").addClass("msg_status_readDel  ok_bg");
			$("#msgStu_"+id+"").text("送达").show();
		}else{*/
			$("#msgStu_"+id+"").addClass("ok_bg"); //改变背景
			$("#msgStu_"+id+"").text(mpLanguage.getLanguageName('delivery')).show();
		//}
		return true;
	},
	//处理收到的单条消息
	processMsg:function(msg){
		//消息去重
		/*if(DataUtils.getMessage(msg.messageId))
			return;	*/

		var type = msg.type;
		var from = msg.from;
		var toJid=msg.to;
		var resource=WEBIM.getResource(from);
		var fromUserId = WEBIM.getUserIdFromJid(from);
		//判断消息是否来自于黑名单用户，是则不接收
		/*if(!myFn.isNil(DataMap.blackListUIds[fromUserId])){
			return;
		}*/
		var contextType = msg.type;
		//消息的发送者userID  群组的Jid
		msg.fromId = fromUserId;
		//消息来源的JID  其他地方要用
		msg.fromJid = from;
		msg.toJid = toJid;

		var toUserId = WEBIM.getUserIdFromJid(msg.to);

		//mpDataUtils.saveMessage(msg);
		msg.content = WEBIM.decryptMessage(msg);

		this.receiverShowMsg(msg);

	},
	//接受的消息显示到页面
	receiverShowMsg:function(msg){
		
		//已经接受到的消息 返回
		/*if (!mpCommon.isNil(DataMap.msgIds[msg.id])) {
			return;
		}*/
		//消息type 大于100，且不是撤回消息
		if(msg.type>100 && msg.type!=MessageType.Type.REVOKE)
			return;

		var isFilter=false;
		var from =msg.from;
		var fromUserId = msg.fromUserId;

		var fromUserName=msg.fromUserName;//发送方的用户昵称
		
		var fromJid=MpChat.to;		
		
				
		//聊天界面没有打开//判断聊天面板是否打开
		var isOpen=true;
		if (!MpChat.isOpen)
			isOpen=false;
		else{
			//发送者不是 当前页面好友 或群组jid.
			if(WEBIM.getUserIdFromJid(fromJid)!=WEBIM.getUserIdFromJid(from))
				isOpen=false;
		}


		//聊天界面已打开 显示消息
		if(isOpen){
			MpChat.showMsg(msg, fromUserId,getLoginData().userId!=msg.fromUserId?0:1,getLoginData().userId!=msg.fromUserId?1:0,"newMsg");

			//显示消息后将该消息加入本地聊天记录中储存
			mpDataUtils.putMsgRecordList(fromUserId,msg);

			//当消息页面打开时更新最后一条消息及时间
			MpChat.showAndUpdateMsgNum(fromUserId,msg,0);

		}else{//聊天面板没有打开

			if(MessageType.Type.INPUT==msg.type||MessageType.Type.READ==msg.type){
				return;
			}


		   	//撤回消息
		   	if(MessageType.Type.REVOKE==msg.type){
		   		//本地储存的未读消息字符串
		   		var unReadMsgStr =  dbStorage.getItem("msgUnReadList_"+fromUserId);
		   		//若存在于未读消息中,撤回消息,新消息数量减1,不存在则不改变未读数
		   		if(!mpCommon.isNil(unReadMsgStr) && -1 < unReadMsgStr.indexOf(msg.content))
					this.showAndUpdateMsgNum(fromUserId,msg,-1);
				else
					this.showAndUpdateMsgNum(fromUserId,msg,0);
			}else
				//显示并更新消息数量
				this.showAndUpdateMsgNum(fromUserId,msg,1);

			//存储未读消息
		   	mpDataUtils.setUnReadMsgList(fromUserId,msg);
		}

		//更新\存储 消息列表的顺序
		this.updateMsgListOrder(fromUserId);

		//更新最后一条消息存储记录
		mpDataUtils.putLastMsg(fromUserId,msg);


		if(isOpen){
			if(WEBIM.isChatType(msg.chatType)&&getLoginData().userId!=msg.fromUserId){//单聊
				if(!MpChat.isReadDelMsg(msg))
					MpChat.sendReadReceipt(fromUserId, msg);
				else if(1!=msg.type&&2!=msg.type&&3!==msg.type&&6!==msg.type)
					MpChat.sendReadReceipt(fromUserId, msg); //发送已读回执
			}

		}
	},
	//发送已读回执
	sendReadReceipt : function(from,toMsg) {
		//isSendMyDevice 发送给我的其他设备
		if(toMsg.isPubsub)
			return;
		
		var msg=WEBIM.sendMessageReadReceipt(from,toMsg.messageId);
		
		//设置发送消息重发次数
		msg.reSendCount=3;
		//DataUtils.saveMessage(msg);
		console.log("发送已读回执："+msg.messageId+"       类型:"+toMsg.type);
		/*if(1==isSendMyDevice&&1==myData.multipleDevices){
			DeviceManager.sendMsgToMyDevices(msgObj);
		}*/
		//阅后即焚消息  删除
		if(MpChat.isReadDelMsg(toMsg)&&(3!=toMsg.type&&6!=toMsg.type))
			mpDataUtils.deleteMessage(toMsg.messageId);
		else{
			toMsg.isRead=1;
			mpDataUtils.saveMessage(toMsg);
		}
		

	},
	/*收到已读消息回执*/
	handlerReadReceipt:function(msg){
		if(WEBIM.CHAT==msg.chatType){
			var message = mpDataUtils.getMessage(msg.fromUserId,msg.content,1);
			
			if(MpChat.isReadDelMsg(message)) //消息为阅后即焚消息，在收到已读回执后从本地记录中移除
				mpDataUtils.removeMsgRecordList(msg.fromJid,msg.content);

			if(!mpCommon.isNil(message)){ //本地存在该消息，则更改该消息的组状态为已读
				message.isRead=1;
				mpDataUtils.updateMessage(message);
			}
		}
		this.receiverShowMsg(msg);
	},
	showAndUpdateMsgNum :function(userId,msg,num){

		if(!mpCommon.isNil(num) && num != 0 ){
			//更新消息未读数
			var msgNumData = mpDataUtils.updateMsgNum(userId,num);

			var msgNum = msgNumData.msgNum;
			var msgNumCount = msgNumData.msgNumCount;

			//更新消息管理菜单处的消息总数
			this.showMsgNumCount(msgNumCount);
		}

		//检查当前页面是否存在发送者的记录，如有，更新最后一条消息及未读数量到页面
	   	if($("#msg_manager").is(':visible')){//判断消息管理页面是否显示  显示：true 隐藏：false
	   		
	   		var itemHtml = $("#msg_manager #msgListItem_"+userId+"").prop("outerHTML");
	   		if(!mpCommon.isNil(itemHtml)){//在当前页面存在,则更新时间、未读消息数、最后一条消息等数据到页面
	   			
	   			if(!mpCommon.isNil(num) && num != 0 )
	   				this.showUserMsgNum(userId,msgNum);
	   			
	   			$("#msgListItem_"+userId+" #message_time").text(getTimeText(msg.timeSend,0));
	   			//var parseContent = this.parseContent(msg.content,1);
	   			$("#msgListItem_"+userId+" #lastMsgContent").html(WEBIM.parseShowMsgTitle(msg));
	   		
	   		}else{ //不存在，则创建一条记录插入到最前面

	   			var noDataHtml = $("#msg_manager #message_list .empty_tips").prop("outerHTML");
	   			
	   			if(!mpCommon.isNil(noDataHtml)){ //如果当前列表显示的是暂无数据，则先将暂无数据移除
	   				$("#msg_manager #message_list .empty_tips").remove();
	   			}
	   			
	   			var messageItemHtml = UI.createMessageItem(userId,(msg.fromUserId==getLoginData().userId) ? msg.toUserName : msg.fromUserName,1,getTimeText(msg.timeSend,0), WEBIM.parseShowMsgTitle(msg));

				$("#msg_manager #message_list").prepend(messageItemHtml);
	   		}

	   		
	   	}

	},
	//显示某个用户的未读消息数
	showUserMsgNum:function(userId,msgNum){

		if(0<msgNum){
				//更新某条记录的消息数
			if(99<msgNum)
				$("#msgListItem_"+userId+" #messageNumw_count").text("99+");
			else
				$("#msgListItem_"+userId+" #messageNumw_count").text(msgNum);

			$("#msgListItem_"+userId+" #messageNumw_count").removeClass("msgNumHide").addClass("msgNumShow");
		}else{
			$("#msgListItem_"+userId+" #messageNumw_count").text("");
			$("#msgListItem_"+userId+" #messageNumw_count").removeClass("msgNumShow").addClass("msgNumHide");
		}

	},
	//清空某个用户的消息未读数
	clearUserMsgNum:function(userId){
		
		//清空该用户的消息未读数 
		var msgNumCount = mpDataUtils.clearUserMsgNum(userId);

		
		//显示到页面
		if(!msgNumCount>0){
			$("#mp_navBar #messageNum_count").removeClass("msgNumShow");
			$("#mp_navBar #messageNum_count").addClass("msgNumHide");
			//更新首页新消息数量
			$("#newMsg_area #msgCount").text(0);
		}else{
			if(msgNumCount > 99){  //数量大于99 则显示99+
				$("#mp_navBar #messageNum_count").text("99+");
			}else{
				$("#mp_navBar #messageNum_count").text(msgNumCount);
			}
			//更新首页新消息数量
			$("#newMsg_area #msgCount").text(msgNumCount);

			$("#mp_navBar #messageNum_count").removeClass("msgNumHide");
			$("#mp_navBar #messageNum_count").addClass("msgNumShow");
		} 
		
		$("#msgListItem_"+userId+" #messageNumw_count").text("");
		
		$("#msgListItem_"+userId+" #messageNumw_count").removeClass("msgNumShow");
		$("#msgListItem_"+userId+" #messageNumw_count").addClass("msgNumHide");
		
		
	},
	//显示消息管理菜单处的未读消息总数
	showMsgNumCount : function(msgNumCount){
		
		if(0<msgNumCount){

			$("#msgCount").html(msgNumCount);

			//显示到消息管理菜单处
			if(msgNumCount > 99){  //数量大于99 则显示99+
				$("#mp_navBar #messageNum_count").text("99+");
			}else{
				$("#mp_navBar #messageNum_count").text(msgNumCount);
			}
			$("#mp_navBar #messageNum_count").removeClass("msgNumHide").addClass("msgNumShow");
		}else{
			$("#mp_navBar #messageNum_count").text("");
			$("#msgCount").html(0);
			$("#mp_navBar #messageNum_count").removeClass("msgNumShow").addClass("msgNumHide");

		}

	},
	/**
	* 更新消息列表顺序
	**/
	updateMsgListOrder:function(userId){

		//判断消息管理页面是否打开  打开：true 隐藏：false
		if($("#msg_manager").is(':visible')){

	   		var itemHtml = $("#msg_manager #msgListItem_"+userId+"").prop("outerHTML");

	   		if(!mpCommon.isNil(itemHtml)){ //在当前页面存在,则移动到顶部
	   			$("#msg_manager #msgListItem_"+userId+"").remove();
	   			$("#msg_manager #message_list").prepend(itemHtml);
	   		}
	   	}
	   	//更新本地存储中的消息列表顺序
		mpDataUtils.putMsgListOrder(userId);
	},
	/**
	 * [处理消息内容，将表情字符替换为图片]
	 * @param  {[type]} content [description]
	 * @param  type   不传或者0 表示
	 * 
	 */
	parseContent : function(content,type) {
		var emojlKeys = new Array();
		if(mpCommon.isNil(content))
			return content;
		var s = content;
		var fromIndex = 0;
		while (fromIndex != -1) {
			fromIndex = s.indexOf("[", fromIndex);
			if (fromIndex == -1)
				break;
			else {
				var stop = s.indexOf("]", fromIndex);
				if (stop == -1)
					break;
				else {
					var emojlKey = s.substring(fromIndex, stop + 1);
					emojlKeys.push(emojlKey);
					fromIndex = fromIndex + 1;
				}
			}
		}
		//表情
		if (emojlKeys.length != 0) {
			var key=null;
			var emojl=null;
			for (var i = 0; i < emojlKeys.length; i++) {
				 key = emojlKeys[i];
				 emojl=_emojl[key];
				 if(!mpCommon.isNil(emojl)){
				 	if(1==type)
				 		s = s.replace(key, "<img src='" + emojl + "' height='20' />");
				 	else
						s = s.replace(key, "<img src='" + emojl + "' height='25' />");
				 }
					
			}
			return s;
		}

		content=mpCommon.hrefEncode(content);
		
		return content;
		
	},
	/*是否为阅后即焚消息*/
	isReadDelMsg : function(msg){
		try {
			if(!msg.isReadDel){
				return false;
			}
			return ("true"==msg.isReadDel||1==msg.isReadDel);
		} catch (e) {
		   return false;
		}
		
	},
	//滚动到底部  type ：类型分为发送和接收两种 
	scrollToEnd : function() {

		/*if(type=="receive" && UI.getElementPos("messageEnd").y> UI.getElementPos("sendMsgScopeDiv").y+300){  //类型为接收
  			//根据坐标判断用户是否在浏览历史消息
		    return;
		}*/
		// $("#bottomUnreadCount").remove();

		$(".nano").nanoScroller({ scrollBottom: -100000000000});
	},
	/**
	* 显示聊天窗口顶部的头像和昵称
	**/
	showAvatarAndNickname : function(userId,nickname){ //显示聊天窗口顶部头像和昵称
		$("#mp_chatPanel #chatAvator").empty();
		$("#chatTitle").empty();

		var avatarHtml ="<img onerror='this.src=\"./images/ic_avatar.png\"' src='" +mpCommon.getAvatarUrl(userId)+ "' class='chat_content_avatar'>";
		            
		$("#chatAvator").append(avatarHtml);
		$("#mp_chatPanel #chatTitle").text(nickname);
	},
	//聊天图片上传
	uploadImg:function(){
		layui.upload.render({
		        elem: '#btnImg', //文件选择元素
		        accept:'images',
		        acceptMime: 'image/*',
		        url: mpCommon.uploadAddr+'',
		        size:5000,
		        auto: false, //选择文件后不自动上传
  				bindAction: '#imgUploadAction',//指向一个按钮触发上传
		        data:{id:""},
		        progress: function(e , percent) {
		          //console.log("进度：" + percent + '%');
		        },
		        choose: function(obj) {//选择完成事件
		          var imgMsg = WEBIM.createMessage(2, " ");
		          this.data.id=imgMsg.messageId;

		          $('#imgUploadAction').trigger("click");

		          obj.preview(function(index, file, result) {
					 	
						var reader = new FileReader();
						reader.onload = function(evt){
							imgMsg.content = evt.target.result;
		          			MpChat.showMsg(imgMsg,getLoginData().userId,1,1,"newMsg");
		          			mpDataUtils.saveMessage(imgMsg);//储存消息
						}
						reader.readAsDataURL(file);
		          });


		        },
		        done: function(res) { //上传完成事件
					var imgMsg=mpDataUtils.takeOutMessage(this.data.id);
					imgMsg.content = res.url;
		          	MpChat.processSendMsg(imgMsg);

		          //UI.sendImg();
		        },
		        error: function(res) {
		          //layui.layer.msg(res.msg);
		        }
		});	


	},
	uploadFile:function(){
		layui.upload.render({
		        elem: '#btnFile', //文件选择元素
		        accept:'file',
		        url: mpCommon.uploadAddr+'',
		        auto: false, //选择文件后不自动上传
  				bindAction: '#fileUploadAction',//指向一个按钮触发上传
		        data:{id:""},
		        progress: function(e , percent) {

		          //更新进度条
		          document.getElementById('fileProgress_'+this.data.id+'').childNodes[0].style.cssText="width: "+percent+"%;";
		        },
		        choose: function(obj) {//选择完成事件
		          var fileMsg = WEBIM.createMessage(9, "fileUrl");
		          this.data.id = fileMsg.messageId;

		          $('#fileUploadAction').trigger("click");

		          obj.preview(function(index, file, result) {
		          		//开始ui预加载
						fileMsg.fileName=file.name;
						fileMsg.fileSize=file.size;
	          			MpChat.showMsg(fileMsg,getLoginData().userId,1,1,"newMsg");
		           		mpDataUtils.saveMessage(fileMsg);//储存消息
		          });
		          
		        },
		        done: function(res) { //上传完成事件
		          
		          //移除进度条
		          $("#fileProgress_"+this.data.id+"").remove();
		          //替换下载url
		          $("#fileDown_"+this.data.id+"").attr("href",res.url);
		          //取出暂存的文件消息进行封装发送
		          var fileMsg=mpDataUtils.takeOutMessage(this.data.id);
				  fileMsg.content = res.url;
				  MpChat.processSendMsg(fileMsg);
		        },
		        error: function(res) {
		          if(res.msg)
		          		layui.layer.msg(res.msg);
		        }
		});	

	},
	showMsgLog:function(log,messageId){
		var logHtml ="<div class='logContent' >"
					+"	<span >"+log+"</span> "
					+"</div>";
		if(messageId){
			logHtml="<div id='msg_"+messageId+"' >"
			 +logHtml+ "</div>";
		}
		$("#messageContainer").append(logHtml);
		this.scrollToEnd();
	},
	/*撤回消息*/
	handlerRevokeMessage:function(msg){
		var targetMsgId = msg.content;

		this.receiverShowMsg(msg);
	},
	handlerLoginConflict:function(){
		WEBIM.disconnect();
		ownAlert(4,mpLanguage.getLanguageName('offline'),function(){
			setTimeout(function() {
                    location.replace("/mp/login.html");
            }, 1000);
		});
	},
	/**
	*	去除重复消息
	**/
	removeRepeatMsg : function (msg){
		//消息去重，丢弃已经收到过的消息
       if(mpDataUtils.getMessage(msg.fromUserId,msg.messageId,1)){
            console.log("丢弃重复消息 ===> "+msg.messageId);
            return;
       }
	},
	loadEmoji : function(){
		//加载表情
		var emojiHtml = "";  //emoji 的Html

		emojiList.forEach(function(info, index){
			emojiHtml +="<img src='emojl/"+info['filename']+".png' alt='"+info['chinese']+"' title='"+info['chinese']+"' onclick='MpChat.choiceEmojl(\"" +"["+info['english']+"]"+ "\")' />";
		});
		$("#emojl-panel #emojiList").append(emojiHtml);
		//初始化GIF动画的滚动条
		$("#emojl-panel #emojiList").niceScroll({
			  cursorcolor: "rgb(113, 126, 125)",
	          cursorwidth: "5px", // 滚动条的宽度，单位：便素
	          autohidemode: true, // 隐藏滚动条的方式
	          railoffset: 'top', // 可以使用top/left来修正位置
	          enablemousewheel: true, // nicescroll可以管理鼠标滚轮事件
	          smoothscroll: true, // ease动画滚动  
	          cursorminheight: 32, // 设置滚动条的最小高度 (像素)
	          iframeautoresize: true,//iframeautoresize: true
	          bouncescroll: false,
	          railalign: 'right',
		});
		$("#emojl-panel #emojiList").getNiceScroll().show(); //显示滚动条
	},
	choiceEmojl : function(key) {
		
		$("#messageBody").val($("#messageBody").val()+key);
		//$("#emojl-panel #gifList").getNiceScroll().hide();//隐藏滚动条
		//$("#emojl-panel #emojiList").getNiceScroll().hide();
		$("#emojl-panel").hide();
	},
	showImgZoom : function(src,messageId) {
		//return;
		this.previewImgs(src);

		//阅后即焚消息
     	if(!mpCommon.isNil(messageId)){
     		var msg=mpDataUtils.getMessage(MpChat.toUserId,messageId,1);
     		if(getLoginData().userId==msg.fromUserId)
     			return;
     		MpChat.sendReadReceipt(MpChat.toUserId, messageId,1);
     		if(!mpCommon.isNil(msg))
     			mpHttpApi.deleteFile(msg.content);
			$("#messageContainer #msg_"+messageId).remove();

			//将此条阅后即焚消息从缓存消息中删除
			mpDataUtils.removeMsgFromRecordList(MpChat.toUserId,messageId);
		}

	},
	videoPlayEnd : function(messageId,readDel) { //视频播放结束后执行
		if(readDel=="true" || readDel==1){ //判断是否为阅后即焚消息
			//播放结束后显示阅后即焚图片
			$("#messageContainer #msg_"+messageId+"").remove();
			var msg=mpDataUtils.getMessage(MpChat.toUserId,messageId,1);
			MpChat.sendReadReceipt(MpChat.toUserId, messageId,1);
     		if(!mpCommon.isNil(msg))
     			mpHttpApi.deleteFile(msg.content);

			//将此条阅后即焚消息从缓存消息中删除
			mpDataUtils.removeMsgFromRecordList(MpChat.toUserId,messageId);
		}

	},
	showAudio : function(videoUrl,messageId,readDel) {
		var type=videoUrl.substr(videoUrl.lastIndexOf('.')+1,videoUrl.length);
		if("amr"==type){
			var url=mpCommon.uploadAddr.substr(0,mpCommon.uploadAddr.lastIndexOf('/'))+"/amrToMp3";
			var data=WEBIM.createOpenApiSecret();
			data.paths=videoUrl;

			$.ajax({
				type:"POST",
				url:url,
				jsonp:"callback",
				data:data,
				success : function(result) {
					//if (1 == result.resultCode) {
						var res = eval("(" + result + ")");
						videoUrl=res.data[0].oUrl;
						MpChat.showAudioInUrl(videoUrl,messageId,readDel);
					//}
				},
				error : function(result) {

				}
			});

		}else if("wav"==type){
			// videoUrl = videoUrl.substr(0, videoUrl.lastIndexOf('.')) + ".mp3";
			this.showAudioInUrl(videoUrl,messageId,readDel);
		}else{
			videoUrl = videoUrl.substr(0, videoUrl.lastIndexOf('.')) + ".mp3";
			this.showAudioInUrl(videoUrl,messageId,readDel);
			// videoUrl =
			// "http://96.f.1ting.com/56401610/e02941274d5babb817f2abcf5f6d0220/zzzzzmp3/2009fJun/10/10gongyue/02.mp3";

		}
	},
	//根据url 开始播放语音
	showAudioInUrl:function(videoUrl,msgId,readDel){
		//将页面上的语音消息都恢复成静态图片
		$("#messageContainer #voiceImg").attr("src","images/voice.png");

		//ownAlert(3,"开始播放语音");

		var voiceHtml = '<audio id="audio" autoplay="autoplay">'
					  +		'<source src="'+videoUrl+'" type="audio/mpeg"/>'
					  +'</audio>'
		$("#messageContainer #voice_"+msgId+"").empty().append(voiceHtml);
		//显示播放的gif图
		$("#messageContainer #voiceP_"+msgId+" #voiceImg").attr("src","images/voice.gif");

		//播放结束后恢复
		$("#messageContainer #voice_"+msgId+" #audio").bind('ended',function () {

			if(readDel=="true" || readDel==1 ){ //判断是否为阅后即焚消息
				$("#messageContainer #msg_"+msgId+"").remove();
				var msg=mpDataUtils.getMessage(MpChat.toUserId,msgId,1);
     			if(!mpCommon.isNil(msg))
     				mpHttpApi.deleteFile(msg.content);

     			MpChat.sendReadReceipt(ConversationManager.from, msgId,1);

				setTimeout(function(){
					UI.scrollToEnd();
				},400);

				//将此条阅后即焚消息从缓存消息中删除
				mpDataUtils.removeMsgFromRecordList(MpChat.toUserId,msgId);

			}else{ // 不是阅后即焚 则恢复为静态图片
				$("#messageContainer #voiceP_"+msgId+" #voiceImg").attr("src","images/voice.png");
			}


		});


	},
	//原图预览
    previewImgs : function(src) {
        var maxWidth = (undefined!=document.body.clientWidth)?document.body.clientWidth-100:800;
        var maxHeight = (undefined!=document.body.clientHeight)?document.body.clientHeight-80:700;

    	if(mpChatData.msgImgLayerOpen){
    		return;
    	}
       	$("#mp_imgZoom_div").append("<img src="+src+" class='mp_imgZoom_img' style='max-width:"+maxWidth+"px; max-height:"+maxHeight+"px;'/>");

        //弹出层
		layer.open({
            type: 1,
            title: false,
            btn:false,
            shade:0.6,
            closeBtn: 0,
            skin: 'layui-layer-nobg',//去掉背景
            shadeClose: true, //点击外围关闭弹窗
            scrollbar: false, //不现实滚动条
            maxWidth: maxWidth,
            maxHeight: maxHeight,
            //area:  ['auto', 'auto'],
            content: $("#mp_imgZoom_div"),//自定义 html 内容，注意这里的整个 html 内容背景肯定是透明的
            success: function(layero, index){
            	mpChatData.msgImgLayerOpen = true;
            },
            end: function(){
            	$("#mp_imgZoom_div").empty().hide();
            	mpChatData.msgImgLayerOpen = false;
            	return false;
            },
            cancel: function(index, layero){

				layer.close(index)
				$("#mp_imgZoom_div").empty().hide();
				mpChatData.msgImgLayerOpen = false;
				return false;
			},
        });

    },
    /*同步服务器 最近聊天列表*/
	getLastChatList:function(startTime){
		//var startTime=mpCommon.isNil(DataUtils.getUIMessageList())?0:DataUtils.getLogoutTime();
		if(startTime==0 && 0!=mpDataUtils.getMsgListLastTime()){ //已经加载过第一页数据到本地，不重复加载
			return;
		}

		mpHttpApi.getLastChatList(startTime,function(result){
			var msgListhtml = "";

			var message=null;

			for (var i = 0; i < result.length; i++) {

				message=result[i];

				message.fromUserId = WEBIM.getUserIdFromJid(message.from);

				if(message.isEncrypt)
					message.content = WEBIM.decryptMessage(message.content);

				//更新本地存储中的消息列表顺序
				mpDataUtils.putMsgListOrder(message.jid,1);

				//存储最后一条消息，用于消息列表的显示
				mpDataUtils.putLastMsg(message.jid,message);

				if(startTime>0){ //加载第一页时由于当前页非消息管理页,不更新UI
					msgListhtml = MpChat.loadMsgListHtml(message.jid);
					$("#msg_manager #message_list").append(msgListhtml);
				}
			}

			if(startTime!=0 && 0==result.length)  //startTime != 0 、length !=0   startTime == 0 length != 0 ;   startTime == 0 length == 0 ;
				$("#noDataMsgList").show().siblings().hide();
			else if(0!=result.length)
				$("#loadMoreMsgList").show().siblings().hide();
		});
	},
	loadMsgListHtml:function(userId){
		var html = "";
		// var lastMsg = mpDataUtils.getLastMsgFromUnReadMsgList(userId);
		var lastMsg = mpDataUtils.getLastMsg(userId);
		if(!mpCommon.isNil(lastMsg)){
			var nickName = (lastMsg.userId==getLoginData().userId ) ? ( mpCommon.isNil(lastMsg.toUserName) ? IMSDK.getUserIdFromJid(lastMsg.toJid) : lastMsg.toUserName )  : lastMsg.fromUserName;
  			html+=UI.createMessageItem(userId, nickName,mpDataUtils.getMsgNum(userId),getTimeText(lastMsg.timeSend,0), WEBIM.parseShowMsgTitle(lastMsg));
		}

		return html;
	},


}