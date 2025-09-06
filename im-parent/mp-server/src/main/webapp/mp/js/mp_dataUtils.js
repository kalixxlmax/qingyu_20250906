var mpMyData = {

	isEncrypt:0,
	isReadDel:0,
	jid : null,
	_connection : null,

};


/**
*聊天相关数据储存
**/
var mpChatData = {

	unReadMsg : {}, //存放未读消息    key : 发送方的userId  value: Array[] 存放该用户的所有未读消息(保证先后顺序)
	//msgEndTime : {}, //存放消息记录的结束时间   key: 发送方的userId   value: 结束时间

 	//=========使用的===============
 	msgStatus:{}, //存放发送消息的状态   key messageId  value 1:送达 2:已读  
 	msgHistory:{},  //储存用于获取聊天历史记录的数据
 	timeSendLogMap:{}, //消息发送时间保存
 	msgRecordList:{},//好友聊天记录
 	msgUnReadList:{}, //未读消息列表
 	msgImgLayerOpen:false,
};



var mpDataUtils = {
	

	getMessageKey:function(messageId){
		return "msg_"+messageId;
	},
	/**
	* userId : 消息发送者的userId
	* type : 1 只从本地获取  2 本地没有数据就从服务器获取
	*/
	getMessage:function(userId,messageId,type){
		
		var msgRecordMap = this.getMsgRecordList(userId);

		var msg = msgRecordMap.get(messageId);

		if( type==2 && mpCommon.isNil(msg) ){  //本地没有就从网络获取
			msg = mpHttpApi.getMessageForServer(messageId,1);		
		}		

		return msg;

	},
	saveMessage:function(msg){
		dbStorage.setItem(this.getMessageKey(msg.messageId),JSON.stringify(msg));
	},
	takeOutMessage:function(msgId){
		var message = dbStorage.getItem(this.getMessageKey(msgId));
		if(!mpCommon.isNil(message)){
			return  JSON.parse(message);
		}
	},
	updateMessage:function(newMsg){
		var msgRecordMap = this.getMsgRecordList(newMsg.fromUserId);

		var oldMsg = msgRecordMap.get(newMsg.messageId);

		if(!mpCommon.isNil(oldMsg)){ //消息在本地存在，则更新

			msgRecordMap.set(oldMsg.messageId,newMsg);
			
			this.setMsgRecordList(newMsg.fromUserId,msgRecordMap);
		}
	},
	deleteMessage:function(messageId){
		return dbStorage.removeItem(this.getMessageKey(messageId));
	},
	getMsgReadList:function(messageId){
		var key="msgReadList_"+messageId;
		var readList=dbStorage.getItem(key);
		if(mpCommon.isNil(readList)){
			readList=new Array();
		}else{
			readList=JSON.parse(readList);
		}
		return readList;
	},
	//取出用户未读消息集合
	getUnReadMsgList:function(userId){
		var unReadMsgList = mpChatData.msgUnReadList[userId];
		if(mpCommon.isNil(unReadMsgList)){
			unReadMsgList = dbStorage.getItem("msgUnReadList_"+userId);

			if(mpCommon.isNil(unReadMsgList))
				unReadMsgList = new Array();
			else
				unReadMsgList = JSON.parse(unReadMsgList);

			mpChatData.msgUnReadList[userId] = unReadMsgList;
		}
		return unReadMsgList;
	},
	//将消息存到未读消息集合
	setUnReadMsgList:function(userId,msg){
		var unReadMsgList = this.getUnReadMsgList(userId);
		unReadMsgList.push(msg)
		mpChatData.msgUnReadList[userId]=unReadMsgList;

		dbStorage.setItem("msgUnReadList_"+userId,JSON.stringify(unReadMsgList));
	},
	//从未读消息列表取出最后一条消息
	getLastMsgFromUnReadMsgList:function(userId){
		var unReadMsgList = this.getUnReadMsgList(userId);
		if (unReadMsgList.length>0) {
			return unReadMsgList.slice(-1)[0];
		}else{
			var msgRecordMap = this.getMsgRecordList(userId);
			if(msgRecordMap.size()>0){
				return msgRecordMap.get(msgRecordMap.keys[msgRecordMap.size()-1]);
			}

		}
		return "";
	},
	deleteUnReadMsgList:function(userId){
		delete mpChatData.msgRecordList[userId];
		dbStorage.removeItem("msgUnReadList_"+userId);
	},
	putLastMsg:function(userId,msg){

		var lastMsgMap = dbStorage.getItem("lastMsgStr");
		if(mpCommon.isNil(lastMsgMap)){
			lastMsgMap = new Map();
		}else{
			lastMsgMap = new Map(JSON.parse(lastMsgMap));
		}
		lastMsgMap.set(userId,msg);

		dbStorage.setItem("lastMsgStr",JSON.stringify([...lastMsgMap]));
	},
	getLastMsg:function(userId){

        var latsMsg = "";

		var lastMsgMap = dbStorage.getItem("lastMsgStr");

		if(!mpCommon.isNil(lastMsgMap)){

				lastMsgMap = new Map(JSON.parse(lastMsgMap));
				latsMsg = lastMsgMap.get(userId);
		}
		return latsMsg;
	},
	/*获取与粉丝的聊天记录*/
	getMsgRecordList:function(userId){
		//msgRecordMap={msgId:msg}

		if(mpCommon.isNil(mpChatData.msgRecordList[userId]) || mpChatData.msgRecordList[userId].size()==0){

			mpChatData.msgRecordList[userId] = new mpMap();

			var msgRecordMap = dbStorage.getItem("msgRecordList_"+userId);

			if(!mpCommon.isNil(msgRecordMap)){

				msgRecordMap=JSON.parse(msgRecordMap);

				for(var messageId in msgRecordMap){
					mpChatData.msgRecordList[userId].putBottom(messageId,msgRecordMap[messageId]);
				}
			}

		}
		return mpChatData.msgRecordList[userId];
	},
	setMsgRecordList:function(userId,msgRecordMap){
		mpChatData.msgRecordList[userId] = msgRecordMap;
		dbStorage.setItem("msgRecordList_"+userId , msgRecordMap.toString());
	},
	putMsgRecordList:function(userId,msg,inTop){

		var msgRecordMap = this.getMsgRecordList(userId);

		//消息存在则不继续执行
		if(!mpCommon.isNil(msgRecordMap.get(msg.messageId)))
			return;


		/*本地记录 最多只保存最新50条*/
		if(1==inTop){
			if(50>msgRecordMap.size()){
				msgRecordMap.putTop(msg.messageId,msg);
			}
		}else{
			if(50>msgRecordMap.size()){
				msgRecordMap.putBottom(msg.messageId,msg);
			}else{
				msgRecordMap.remove(msgRecordMap.keys[0]);
				msgRecordMap.putBottom(msg.messageId,msg);
			}
		}

		this.setMsgRecordList(userId,msgRecordMap);
	},
	removeMsgFromRecordList:function(userId,msgId){
		var msgRecordMap = this.getMsgRecordList(userId);
		//消息存在则不继续执行
		if(!mpCommon.isNil(msgRecordMap.get(msgId)))
			return;

		msgRecordMap.remove(msgId);

		this.setMsgRecordList(userId,msgRecordMap);
	},
	/*清除 好友 或群组的 消息记录*/
	clearMsgRecordList:function(userId){
		userId=this.getMsgRecordListKey(userId);
		delete mpChatData.msgRecordList[userId];
		dbStorage.removeItem("msgRecordList_"+userId);
	},
	/*获取未读消息 总数*/
	getMsgNumCount:function(){
		var num=dbStorage.getItem(getLoginData().userId+"_msgUnReadNumCount");
		
		num = mpCommon.isNil(num) ? 0 : parseInt(num);
		
		return num;
	},
	/*更新消息总数*/
	setMsgMumCount:function(num){

		dbStorage.setItem(getLoginData().userId+"_msgUnReadNumCount",num);
	},
	//获取某个用户的消息未读数量
	getMsgNum:function(userId){
		
		var msgNumMap = dbStorage.getItem(getLoginData().userId+"_msgUnReadNum");
			
		msgNumMap = mpCommon.isNil(msgNumMap) ? new mpMap() : JSON.parse(msgNumMap);
		
		var num = mpCommon.isNil(msgNumMap[userId]) ? 0 : parseInt(msgNumMap[userId]);

		return num;

	},
	getMsgNumMap:function(){

		var msgNumMap = dbStorage.getItem(getLoginData().userId+"_msgUnReadNum");
			
		msgNumMap = mpCommon.isNil(msgNumMap) ? new Map() : JSON.parse(msgNumMap);

		return msgNumMap;	

	},
	setMsgNum:function(userId,num){
		var msgNumMap = this.getMsgNumMap();
		msgNumMap[userId] = num;
		dbStorage.setItem(getLoginData().userId+"_msgUnReadNum",JSON.stringify(msgNumMap));
	},
	updateMsgNum:function(userId,num){

		var msgNumData = {}; 

	 	var msgNum = mpDataUtils.getMsgNum(userId);
		var msgNumCount = mpDataUtils.getMsgNumCount();
		
		if(mpCommon.isNil(num)){
		    msgNum += 1; 
		    msgNumCount += 1;  //将好友发送的未读消息汇总
		}else{
			msgNum+=num;
			msgNumCount+=num; 	
		} 
		
		msgNumData["msgNum"]=msgNum;
		msgNumData["msgNumCount"]=msgNumCount;

		this.setMsgNum(userId,msgNum);
			
		this.setMsgMumCount(msgNumCount);
		
		return msgNumData;
	},
	//清除某个用户的未读消息数
	clearUserMsgNum:function(userId){

		var msgNum = mpDataUtils.getMsgNum(userId);
		var msgNumCount = mpDataUtils.getMsgNumCount();
		
		if(msgNum < 0)
			msgNum = 0;
		
		msgNumCount-=msgNum;
		
		if(msgNumCount<0)
			msgNumCount=0;
		
		msgNum = 0;

		this.setMsgNum(userId,msgNum) ;
			
		this.setMsgMumCount(msgNumCount);

		return msgNumCount;
	},
	/*
	* 消息列表的顺序
	* type  默认是增加到开头，当 type = 1 时增加到末尾
	*/
	putMsgListOrder:function(userId,type){
		var msgListOrder = this.getMsgListOrder();
		msgListOrder.remove(userId);
		if(1==type) //增加到末尾
			msgListOrder.push(userId);
		else //增加到开头
			msgListOrder.unshift(userId);

		dbStorage.setItem(getLoginData().userId+"_msgListOrder",JSON.stringify(msgListOrder));
	},
	getMsgListOrder:function(){
		var msgListOrderStr = dbStorage.getItem(getLoginData().userId+"_msgListOrder");

		if(!mpCommon.isNil(msgListOrderStr)){
			return JSON.parse(msgListOrderStr);
		}

		return new Array();
	},
	getLogoutTime:function(){
		var  key ="logOutTime_";
		var value=dbStorage.getItem(key);
		if(mpCommon.isNil(value)){
			value=0;
		}else{
			value=Number(value);
		}
		return value;
	},
	getMsgListLastTime:function(){
		var lastTime = 0;
		var lastUserId = mpDataUtils.getMsgListOrder().slice(-1)[0];
		if(!mpCommon.isNil(lastUserId)){
			//获取最后一条消息的顺序依次为未读消息表，本地消息记录表，
			var lastMsg = mpDataUtils.getLastMsg(lastUserId);
			if(!mpCommon.isNil(lastMsg)){
				lastTime = lastMsg.timeSend;
			}

		}
		return lastTime;
	},
};



//本地存储，localStorage类没有存储空间的限制，而cookieStorage有存储大小限制
//在不支持localStorage的情况下会自动切换为cookieStorage
window.dbStorage = (new (function(){
 
    /*var storage;    //声明一个变量，用于确定使用哪个本地存储函数
 
    if(window.localStorage){
        storage = localStorage;     //当localStorage存在，使用H5方式
    }
    else{
        storage = cookieStorage;    //当localStorage不存在，使用兼容方式
    }*/

    this.userId="";

    this.getKey=function(key){
        return this.userId+"_"+key;
    };
    this.setItem = function(key, value){
      
       try{
           localStorage.setItem(this.getKey(key), value);
        }catch(oException){
            if(oException.name == 'QuotaExceededError'){
                console.log('超出本地存储限额！');
                //如果历史信息不重要了，可清空后再设置
                localStorage.clear();
                localStorage.setItem(this.getKey(key), value);
            }
      }
    };
 
    this.getItem = function(key){
        var value=localStorage.getItem(this.getKey(key));
       /* if(!(undefined==value||null==value||""==value||"null"==value||NaN==value))
            console.log("dbStorageLog ==> getItem key > "+key);*/
        return value;
    };
 
    this.removeItem = function(key){
        console.log("dbStorageLog ==> removeItem key > "+key);
        localStorage.removeItem(this.getKey(key));
    };
 
    this.clear = function(){
        console.log("dbStorageLog ==> clearAll =====>");
        localStorage.clear();
    };

})());








/**
自定义数据对象
类似 map key,value 形式

**/

Array.prototype.remove = function(val) {
	
	var index = this.indexOf(val); 
	if (index > -1) 
		this.splice(index, 1); 
    /*for (var i = 0; i < this.length; i++) {     
        if (s == this[i])     
            this.splice(i, 1);     
    }     */
}     
    
/**   
 * Simple Map   
 *    
 *    
 * var m = new Map();   
 * m.put('key','value');   
 * ...   
 * var s = "";   
 * m.each(function(key,value,index){   
 *      s += index+":"+ key+"="+value+"/n";   
 * });   
 * alert(s);   
 *    
 * @author hsg   
 * @date 2019-08-31  
 */    
function mpMap() {  

    /** 存放键的数组(遍历用到) */    
    this.keys = new Array();     
   
    /** 存放数据 */    
    this.data = new Object();     
         
    /**   
     * 从底部放入一个键值对   
     * @param {String} key   
     * @param {Object} value   
     */    
    this.putBottom = function(key, value) {     
        if(this.keys.indexOf(key)==-1){     
            this.keys.push(key);     
        }     
        this.data[key] = value;     
    };

    /**   
     * 从开头放入一个键值对   
     * @param {String} key   
     * @param {Object} value   
     */    
    this.putTop = function(key, value){

    	if(this.keys.indexOf(key)==-1){     
            this.keys.unshift(key);     
        }     
        this.data[key] = value;

    };     
         
    /**   
     * 获取某键对应的值   
     * @param {String} key   
     * @return {Object} value   
     */    
    this.get = function(key) {     
        return this.data[key];     
    };     
    

    /**
    * 设置data中的 value 值，不更新keys的数据，
    * 用于对某个key对应的value 值进行更新
    * 
    */
    this.set = function(key,value){
    	this.data[key] = newValue; 
    }


    /**   
     * 删除一个键值对   
     * @param {String} key   
     */    
    this.remove = function(key) {     
        this.keys.remove(key);     
        delete this.data[key];     
    };     
         
    /**   
     * 遍历Map,执行处理函数   
     *    
     * @param {Function} 回调函数 function(key,value,index){..}   
     */    
    this.each = function(fn){     
        if(typeof fn != 'function'){     
            return;     
        }     
        var len = this.keys.length;     
        for(var i=0;i<len;i++){     
            var k = this.keys[i];     
            fn(k,this.data[k],i);     
        }     
    };     
         
    /**   
     * 获取键值数组(类似Java的entrySet())   
     * @return 键值对象{key,value}的数组   
     */    
    this.entrys = function() {     
        var len = this.keys.length;     
        var entrys = new Array(len);     
        for (var i = 0; i < len; i++) {     
            entrys[i] = {     
                key : this.keys[i],     
                value : this.data[i]     
            };     
        }     
        return entrys;     
    };     
         
    /**   
     * 判断Map是否为空   
     */    
    this.isEmpty = function() {     
        return this.keys.length == 0;     
    };     
         
    /**   
     * 获取键值对数量   
     */    
    this.size = function(){     
        return this.keys.length;     
    };     
         
    /**   
     * 重写toString    
     */    
    this.toString = function(){     
        var s = "{";     
        for(var i=0;i<this.keys.length;i++){     
            var k = this.keys[i];     
            s +='"'+k+'":'+JSON.stringify(this.data[k])+((this.keys.length>1 && i!=(this.keys.length-1))?",":"");     
        }     
        s+="}";     
        return s;     
    };


    /**
    * json 字符串转换为mpMap 对象
    **/
    this.toMpMap = function(mapStr){

    	for(var key in mapStr){

			this.keys.push(key);     
             
        	this.data[key] = mapStr[key];
        }
    };


} 


var emojiList = [
	{"filename":"e-01","english":"smile","chinese":"微笑"},
	{"filename":"e-02","english":"joy","chinese":"快乐"},
	{"filename":"e-03","english":"heart-eyes","chinese":"色咪咪"},
	{"filename":"e-04","english":"sweat_smile","chinese":"汗"},
	{"filename":"e-05","english":"laughing","chinese":"大笑"},
	{"filename":"e-06","english":"wink","chinese":"眨眼"},
	{"filename":"e-07","english":"yum","chinese":"百胜"},
	{"filename":"e-08","english":"relieved","chinese":"放松"},
	{"filename":"e-09","english":"fearful","chinese":"可怕"},
	{"filename":"e-10","english":"ohYeah","chinese":"欧耶"},
	{"filename":"e-11","english":"cold-sweat","chinese":"冷汗"},
	{"filename":"e-12","english":"scream","chinese":"尖叫"},
	{"filename":"e-13","english":"kissing_heart","chinese":"亲亲"},
	{"filename":"e-14","english":"smirk","chinese":"得意"},
	{"filename":"e-15","english":"angry","chinese":"害怕"},
	{"filename":"e-16","english":"sweat","chinese":"沮丧"},
	{"filename":"e-17","english":"stuck","chinese":"卡住"},
	{"filename":"e-18","english":"rage","chinese":"愤怒"},
	{"filename":"e-19","english":"etriumph","chinese":"生气"},
	{"filename":"e-20","english":"mask","chinese":"面具"},
	{"filename":"e-21","english":"confounded","chinese":"羞愧"},
	{"filename":"e-22","english":"sunglasses","chinese":"太阳镜"},
	{"filename":"e-23","english":"sob","chinese":"在"},
	{"filename":"e-24","english":"blush","chinese":"脸红"},
	{"filename":"e-26","english":"doubt","chinese":"疑惑"},
	{"filename":"e-27","english":"flushed","chinese":"激动"},
	{"filename":"e-28","english":"sleepy","chinese":"休息"},
	{"filename":"e-29","english":"sleeping","chinese":"睡着"},
	{"filename":"e-30","english":"disappointed_relieved","chinese":"失望"},
	{"filename":"e-31","english":"tire","chinese":"累"},
	{"filename":"e-32","english":"astonished","chinese":"惊讶"},
	{"filename":"e-33","english":"buttonNose","chinese":"抠鼻"},
	{"filename":"e-34","english":"frowning","chinese":"皱眉头"},
	{"filename":"e-35","english":"shutUp","chinese":"闭嘴"},
	{"filename":"e-36","english":"expressionless","chinese":"面无表情"},
	{"filename":"e-37","english":"confused","chinese":"困惑"},
	{"filename":"e-38","english":"tired_face","chinese":"厌倦"},
	{"filename":"e-39","english":"grin","chinese":"露齿而笑"},
	{"filename":"e-40","english":"unamused","chinese":"非娱乐"},
	{"filename":"e-41","english":"persevere","chinese":"坚持下去"},
	{"filename":"e-42","english":"relaxed","chinese":"傻笑"},
	{"filename":"e-43","english":"pensive","chinese":"沉思"},
	{"filename":"e-44","english":"no_mouth","chinese":"无嘴"},
	{"filename":"e-45","english":"worried","chinese":"担心"},
	{"filename":"e-46","english":"cry","chinese":"哭"},
	{"filename":"e-47","english":"pill","chinese":"药"},
	{"filename":"e-48","english":"celebrate","chinese":"庆祝"},
	{"filename":"e-49","english":"gift","chinese":"礼物"},
	{"filename":"e-50","english":"birthday","chinese":"生日 "},
	{"filename":"e-51","english":"paray","chinese":"祈祷"},
	{"filename":"e-52","english":"ok_hand","chinese":"好"},
	{"filename":"e-53","english":"first","chinese":"冠军"},
	{"filename":"e-54","english":"v","chinese":"耶"},
	{"filename":"e-55","english":"punch","chinese":"拳头"},
	{"filename":"e-56","english":"thumbsup","chinese":"赞"},
	{"filename":"e-57","english":"thumbsdown","chinese":"垃圾"},
	{"filename":"e-58","english":"muscle","chinese":"肌肉"},
	{"filename":"e-59","english":"maleficeent","chinese":"鼓励"},
	{"filename":"e-60","english":"broken_heart","chinese":"心碎"},
	{"filename":"e-61","english":"heart","chinese":"心 "},
	{"filename":"e-62","english":"taxi","chinese":"出租车"},
	{"filename":"e-63","english":"eyes","chinese":"眼睛"},
	{"filename":"e-64","english":"rose","chinese":"玫瑰"},
	{"filename":"e-65","english":"ghost","chinese":"鬼"},
	{"filename":"e-66","english":"lip","chinese":"嘴唇"},
	{"filename":"e-67","english":"fireworks","chinese":"烟花"},
	{"filename":"e-68","english":"balloon","chinese":"气球"},
	{"filename":"e-69","english":"clasphands","chinese":"握手"},
	{"filename":"e-70","english":"bye","chinese":"抱拳"}
];

var _emojl = {
	"[smile]" : "emojl/e-01.png",
	"[joy]" : "emojl/e-02.png",
	"[heart-eyes]" : "emojl/e-03.png",
	"[sweat_smile]" : "emojl/e-04.png",
	"[laughing]" : "emojl/e-05.png",
	"[wink]" : "emojl/e-06.png",
	"[yum]" : "emojl/e-07.png",
	"[relieved]" : "emojl/e-08.png",
	"[fearful]" : "emojl/e-09.png",
	"[ohYeah]" : "emojl/e-10.png",
	"[cold-sweat]" : "emojl/e-11.png",
	"[scream]" : "emojl/e-12.png",
	"[kissing_heart]" : "emojl/e-13.png",
	"[smirk]" : "emojl/e-14.png",
	"[angry]" : "emojl/e-15.png",
	"[sweat]" : "emojl/e-16.png",
	"[stuck]" : "emojl/e-17.png",
	"[rage]" : "emojl/e-18.png",
	"[etriumph]" : "emojl/e-19.png",
	"[mask]" : "emojl/e-20.png",
	"[confounded]" : "emojl/e-21.png",
	"[sunglasses]" : "emojl/e-22.png",
	"[sob]" : "emojl/e-23.png",
	"[blush]" : "emojl/e-24.png",
	"[doubt]" : "emojl/e-26.png",
	"[flushed]" : "emojl/e-27.png",
	"[sleepy]" : "emojl/e-28.png",
	"[sleeping]" : "emojl/e-29.png",
	"[disappointed_relieved]" : "emojl/e-30.png",
	"[tire]" : "emojl/e-31.png",
	"[astonished]" : "emojl/e-32.png",
	"[buttonNose]" : "emojl/e-33.png",
	"[frowning]" : "emojl/e-34.png",
	"[shutUp]" : "emojl/e-35.png",
	"[expressionless]" : "emojl/e-36.png",
	"[confused]" : "emojl/e-37.png",
	"[tired_face]" : "emojl/e-38.png",
	"[grin]" : "emojl/e-39.png",
	"[unamused]" : "emojl/e-40.png",
	"[persevere]" : "emojl/e-41.png",
	"[relaxed]" : "emojl/e-42.png",
	"[pensive]" : "emojl/e-43.png",
	"[no_mouth]" : "emojl/e-44.png",
	"[worried]" : "emojl/e-45.png",
	"[cry]" : "emojl/e-46.png",
	"[pill]" : "emojl/e-47.png",
	"[celebrate]" : "emojl/e-48.png",
	"[gift]" : "emojl/e-49.png",
	"[birthday]" : "emojl/e-50.png",
	"[paray]" : "emojl/e-51.png",
	"[ok_hand]" : "emojl/e-52.png",
	"[first]" : "emojl/e-53.png",
	"[v]" : "emojl/e-54.png",
	"[punch]" : "emojl/e-55.png",
	"[thumbsup]" : "emojl/e-56.png",
	"[thumbsdown]" : "emojl/e-57.png",
	"[muscle]" : "emojl/e-58.png",
	"[maleficeent]" : "emojl/e-59.png",
	"[broken_heart]" : "emojl/e-60.png",
	"[heart]" : "emojl/e-61.png",
	"[taxi]" : "emojl/e-62.png",
	"[eyes]" : "emojl/e-63.png",
	"[rose]" : "emojl/e-64.png",
	"[ghost]" : "emojl/e-65.png",
	"[lip]" : "emojl/e-66.png",
	"[fireworks]" : "emojl/e-67.png",
	"[balloon]" : "emojl/e-68.png",
	"[clasphands]" : "emojl/e-69.png",
	"[bye]" : "emojl/e-70.png"
};

var gifList = [
	{"filename":"eight.gif","english":"eight"},
	{"filename":"eighteen.gif","english":"eighteen"},
	{"filename":"eleven.gif","english":"eleven"},
	{"filename":"fifity.gif","english":"fifity"},
	{"filename":"fifity_four.gif","english":"fifity_four"},
	{"filename":"fifity_one.gif","english":"fifity_one"},
	{"filename":"fifity_three.gif","english":"fifity_three"},
	{"filename":"fifity_two.gif","english":"fifity_two"},
	{"filename":"fifteen.gif","english":"fifteen"},
	{"filename":"five.gif","english":"five"},
	{"filename":"forty.gif","english":"forty"},
	{"filename":"forty_eight.gif","english":"forty_eight"},
	{"filename":"forty_five.gif","english":"forty_five"},
	{"filename":"forty_four.gif","english":"forty_four"},
	{"filename":"forty_nine.gif","english":"forty_nine"},
	{"filename":"forty_one.gif","english":"forty_one"},
	{"filename":"forty_seven.gif","english":"forty_seven"},
	{"filename":"forty_three.gif","english":"forty_three"},
	{"filename":"forty_two.gif","english":"forty_two"},
	{"filename":"fourteen.gif","english":"fourteen"},
	{"filename":"nine.gif","english":"nine"},
	{"filename":"nineteen.gif","english":"nineteen"},
	{"filename":"one.gif","english":"one"},
	{"filename":"seven.gif","english":"seven"},
	{"filename":"seventeen.gif","english":"seventeen"},
	{"filename":"sixteen.gif","english":"sixteen"},
	{"filename":"ten.gif","english":"ten"},
	{"filename":"thirteen.gif","english":"thirteen"},
	{"filename":"thirty.gif","english":"thirty"},
	{"filename":"thirty_eight.gif","english":"thirty_eight"},
	{"filename":"thirty_five@.gif","english":"thirty_five@"},
	{"filename":"thirty_four.gif","english":"thirty_four"},
	{"filename":"thirty_nine.gif","english":"thirty_nine"},
	{"filename":"thirty_seven.gif","english":"thirty_seven"},
	{"filename":"thirty_six.gif","english":"thirty_six"},
	{"filename":"thirty_three.gif","english":"thirty_three"},
	{"filename":"thirty_two.gif","english":"thirty_two"},
	{"filename":"thirty-one.gif","english":"thirty-one"},
	{"filename":"three.gif","english":"three"},
	{"filename":"twelve.gif","english":"twelve"},
	{"filename":"twenty.gif","english":"twenty"},
	{"filename":"twenty_eight.gif","english":"twenty_eight"},
	{"filename":"twenty_five.gif","english":"twenty_five"},
	{"filename":"twenty_four.gif","english":"twenty_four"},
	{"filename":"twenty_nine.gif","english":"twenty_nine"},
	{"filename":"twenty_one.gif","english":"twenty_one"},
	{"filename":"twenty_seven.gif","english":"twenty_seven"},
	{"filename":"twenty_six.gif","english":"twenty_six"},
	{"filename":"twenty_three.gif","english":"twenty_three"},
	{"filename":"twenty_two.gif","english":"twenty_two"}
];