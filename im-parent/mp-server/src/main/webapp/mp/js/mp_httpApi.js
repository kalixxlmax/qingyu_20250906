/**
调用http 接口的相关函数
**/
var mpHttpApi = {
	
	getConfig:function(callback){
			mpCommon.invoke({
				type:"GET",
				url : '/mp/config',
				data : {},
				async:false,
				success : function(result) {
					if (1 == result.resultCode) {
						callback(result.data);
					} 
				},
				error : function(result) {
					if(1030102==result.resultCode){
						window.location.href = "login.html";
					}
				}
			});
	},
	getCurrentTime:function(callback){
		mpCommon.invoke({
			url : mpCommon.apiAddr+'/getCurrentTime',
			data : {},
			isImapi : 1,
			success : function(result) {
				if (1 == result.resultCode) {
					if(callback)
						callback(result.data);
				} 				mpCommon.timeDelay=mpCommon.getMilliSeconds()-result.currentTime;
				console.log("timeDelay   ====> "+mpCommon.timeDelay);
			}
		});
	},
	getMessageForServer:function(msgId,chatType){
		
		mpCommon.invoke({
			    url : '/mp/getMessage',
				data:{
					messageId:msgId,
					type:chatType
				},
				success:function(result){
					if(1==result.resultCode){
						return result.data;
					}
				}			
		});	
	},
	updateHomeCount : function(){
		mpCommon.invoke({
			url : '/mp/getHomeCount',
			data : {},
			success : function(result) {
				if (result.resultCode == 1) {
					//$("#msgCount").html(result.data.msgCount);
					$("#fansCount").html(result.data.fansCount);
					//$("#userCount").html(result.data.userCount);
				} 
			},
			error : function(result) {
				console.log("update msgCount error ====> ");
			}
		});
	},
	deleteFile:function(url,callback){
		//删除文件服务器文件
		var data=WEBIM.createOpenApiSecret(); 
		data.paths=url;
		$.ajax({
			type:'POST',
			url: mpCommon.uploadAddr.substr(0,mpCommon.uploadAddr.lastIndexOf('/'))+"/deleteFileServlet",
			data:data,
			success:function(result){
				callback(result);
			},
			error : function(result) {
				//ownAlert(2,result);
			}
		});	
		
	},
	showHistory : function(pageIndex, cb,endTime) {
        //console.log("历史记录当前页码数:"+pageIndex);
        endTime =MpChat.minTimeSend;

        if(mpCommon.isNil(endTime)){
            endTime = 0;
        }
        var url = mpCommon.apiAddr+'/tigase/chat_msgs';
        var params = {
            pageIndex : pageIndex,
            pageSize : 20,
            endTime : parseInt(endTime*1000),
            maxType:200,
            receiver:WEBIM.getUserIdFromJid(MpChat.to),
            sender:getLoginData().userId
        };
        //params["receiver"] = WEBIM.getUserIdFromJid(MpChat.to);
        mpCommon.invoke({
            url : url,
            data : params,
            isImapi : 1,
            success : function(result) {
                if (1 == result.resultCode) {
                    cb(0, result.data);
                } else {
                    cb(1, result.resultMsg);
                }
            },
            error : function(result) {
                cb(1, null);
            }
        });
    },
	logout:function(){
		mpCommon.invoke({
			url : '/mp/logout',
			data : {},
			success : function(result) {
				if (1 == result.resultCode) {
					localStorage.removeItem("loginData");
					localStorage.removeItem("mp_system_language");
					location.replace("/mp/login.html");
				}
			}
		});
	},
	getLastChatList:function(startTime,callback){

		mpCommon.invoke({
			url :'/mp/getLastChatList',
			data : {
				startTime:startTime,
				pageSize:20
			},
			async:false,
			success : function(result) {
				if (1 == result.resultCode) {
					callback(result.data);
				}
			},
			error : function(result) {
			}
		});
	},


};