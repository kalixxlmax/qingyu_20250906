/**
 * 公共的 js
 */
var ivKey=[1,2,3,4,5,6,7,8];
var iv=getStrFromBytes(ivKey);
var ConfigData=null;
function getStrFromBytes (arr) {
    var r = "";
    for(var i=0;i<arr.length;i++){
        r += String.fromCharCode(arr[i]);
    }
    return r;
}
function getTime() {
    console.log("服务器时间差  "+localStorage.getItem("currentTime"));
    let time=parseInt(Math.round(((new Date().getTime()))))+parseInt(localStorage.getItem("currentTime"));
    return parseInt(time/1000);
}

function request(url){
    var time = getTime();
    var obj=localStorage.getItem("apiKey")+time+localStorage.getItem("account")+localStorage.getItem("access_Token");
    url=url+"?secret="+$.md5(obj)+"&time="+time+"&access_token="+localStorage.getItem("access_Token");
    return url;
}

function checkRequst(result){
    // 访问令牌过期或无效
    if(result.resultCode==1030102){
        layer.confirm(result.resultMsg,{icon:2, title:'提示消息',yes:function () {

          window.top.location.href="/pages/console/login.html";
        },btn2:function () {

          window.top.location.href="/pages/console/login.html";
        },cancel:function () {

          window.top.location.href="/pages/console/login.html";
        }});
    }else if (result.resultMsg == "权限不足"){
        layer.confirm('您没权限访问！请让管理员授予权限！', {icon: 3, title:'提示'}, function(index){
            window.top.location.href="/pages/console/index.html";
            layer.close(index);
        });
    }else if(result.resultCode==0){
        if(!Common.isNil(result.resultMsg)){
            layer.msg(result.resultMsg,{icon: 2});
        }

    }else if(result.resultMsg == "抱歉! 尚未开通支付相关功能!"){
        layer.msg(result.resultMsg,{icon: 2});
    }
}

var Config={
    getConfig:function(){
        if(ConfigData==null){
            $.ajax({
                type:'POST',
                url:'/config',
                data:{},
                async:false,
                success:function(result){
                    ConfigData=result.data;
                }
            })
        }
        return ConfigData;
    }
}

var Common = {
	// layui table重载刷新 解决零界值删除不刷新到上页的问题(支持删除，多选删除)
	// 	currentCount ：当前table总数量, currentPageIndex ： 当前页数, checkLength ：选中的个数, tableId : table的ID){
	tableReload : function(currentCount,currentPageIndex,checkLength,tableId){
		var remainderIndex = (currentCount - checkLength) % Common.limit;
        console.log("currentCount : "+currentCount+"  checkLength : "+checkLength+"  Common.limit : "+Common.limit+"  currentPageIndex : "+currentPageIndex+"  remainderIndex : "+remainderIndex);
        if(0 == remainderIndex)
            currentPageIndex = currentPageIndex - 1;
        layui.table.reload(tableId,{
            page: {
                curr: currentPageIndex, //重新从当前页开始
            }
        })
    },

    // 分页公共参数
    limit:15,
    limits:[15,50,100,1000,10000],

	/** 调用接口通用方法  */
	invoke : function(obj){
		jQuery.support.cors = true;
		//layer.load(1); //显示等待框

		var params = {
			type : "POST",
			url : obj.url,
			data : obj.data,
			contentType : 'application/x-www-form-urlencoded; charset=UTF-8',
			dataType : 'JSON',
            async:obj.async==false?false:true,
			success : function(result) {
				/*layer.closeAll('loading');*/
				if(1==result.resultCode){
                    obj.success(result);
					if(obj.successMsg!=undefined&&obj.successMsg!=""&&obj.successMsg!=null){
                        layer.msg(obj.successMsg,{icon:1});
                    }
				}else if(-1==result.resultCode){
					//缺少访问令牌
					layer.msg("缺少访问令牌",{icon: 3});
					window.location.href = "/pages/console/login.html";
				}else if(1030102==result.resultCode){// 访问令牌过期
                    checkRequst(result);
                }else{
				    if(obj.error!=undefined &&obj.error!=""&&obj.error!=null){
                        obj.error(result);
                    }
					if(!Common.isNil(result.resultMsg))
						layer.msg(result.resultMsg,{icon: 2,time: 2000});
					else
						layer.msg(obj.errorMsg,{icon: 2,time: 2000});

                    // obj.error(result);
				}
				return;

			},
			error : function(result) {
				layer.closeAll('loading');
				if(!Common.isNil(result.resultMsg)){
					layer.msg(result.resultMsg,{icon: 2});
				}else{
					layer.msg(obj.errorMsg,{icon: 2});
				}
				// obj.error(result);//执行失败的回调函数
				return;
			},
			complete : function(result) {
				layer.closeAll('loading');
			}
		}
		params.data["access_token"] = localStorage.getItem("access_Token");
		$.ajax(params);
	}
	,isNil : function(s) {
		return undefined == s || null == s || $.trim(s) == "" || $.trim(s) == "null";
	},
    JSONLength:function (obj) {
            var size = 0;
            var key;
            for (key in obj) {   //obj中存在几个关键字
                if (obj.hasOwnProperty(key))
                    size++;
            }
            return size;
    },
    // 校验是否为json数据
    isJSON:function (str) {
        try {
            if (typeof JSON.parse(str) == "object") {
                return true;
            }
        } catch(e) {
            return false;
        }
        return false;
    },

	formatDate : function (time,fmt,type) { //type : 类型 0:时间为秒  1:时间为毫秒
		var date = new Date((type==0?(time * 1000):time));
	    var o = {
	        "M+": date.getMonth() + 1, //月份
	        "d+": date.getDate(), //日
	        "h+": date.getHours(), //小时
	        "m+": date.getMinutes(), //分
	        "s+": date.getSeconds(), //秒
	        "q+": Math.floor((date.getMonth() + 3) / 3), //季度
	        "S": date.getMilliseconds() //毫秒
	    };
	    if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (date.getFullYear() + "").substr(4 - RegExp.$1.length));
	    for (var k in o)
	    	if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
	    return fmt;
	},
	// 消息类型
	msgType : function(type) {
        if(1 == type){
            return "文本消息";
        }else if(2 == type ){
            return "图片消息";
        }else if(3 == type){
            return "语音消息";
        }else if(4 == type){
            return "位置消息";
        }else if(5 == type){
            return "动画消息";
        }else if(6 == type){
            return "视频消息";
        }else if(7 == type){
            return "音频消息";
        }else if(8 == type){
            return "名片消息";
        }else if(9 == type){
            return "文件消息";
        }else if(10 == type){
            return "提醒消息";
        }else if (28 == type) {
			return "红包消息";
		}else if(83 == type){
            return "领取红包消息";
		}else if(86 == type){
            return "红包退回消息";
        }else if(29 == type){
            return "转账消息";
        }else if(80 == type){
            return "单条图文消息";
        }else if(81 == type){
            return "多条图文消息";
        }else if(84 == type){
            return "戳一戳消息";
        }else if(85 == type){
            return "聊天记录消息";
        }else if(88 == type){
            return "转账被领取消息";
        }else if(89 == type){
            return "转账已退回消息";
        }else if(90 == type){
            return "付款码已付款通知消息";
        }else if(91 == type){
            return "付款码已到账通知消息";
        }else if(92 == type){
            return "收款码已付款通知消息";
        }else if(93 == type){
            return "收款码已到账通知消息";
        }else if(95 == type){
            return "阅后即焚消息截屏消息";
        }else if(96 == type){
            return "双向撤回消息";
        }else if(201 == type){
            return "正在输入消息";
        }else if(202 == type){
            return "撤回消息";
        }else if(100 == type){
            return "发起语音通话消息";
        }else if(102 == type){
            return "接听语音通话消息";
        }else if(103 == type){
            return "拒绝语音通话消息";
        }else if(104 == type){
            return "结束语音通话消息";
        }else if(110 == type){
            return "发起视频通话消息";
        }else if(112 == type){
            return "接听视频通话消息";
        }else if(113 == type){
            return "拒绝视频通话消息";
        }else if(114 == type){
            return "结束视频通话消息";
        }else if(115 == type){
            return "会议邀请消息";
        }else if(901 == type){
            return "修改昵称消息";
        }else if(902 == type){
            return "修改房间名消息";
        }else if(903 == type){
            return "解散房间消息";
        }else if(904 == type){
            return "退出房间消息";
        }else if(905 == type){
            return "新公告消息";
        }else if(906 == type){
            return "禁言、取消禁言消息";
        }else if(907 == type){
            return "增加成员消息";
        }else if(913 == type){
            return "设置、取消管理员消息";
        }else if(915 == type){
            return "设置群已读消息";
        }else if(916 == type){
            return "群验证消息";
        }else if(917 == type){
            return "群组是否公开消息";
        }else if(918 == type){
            return "是否展示群成员列表消息";
        }else if(919 == type){
            return "允许发送名片消息";
        }else if(920 == type){
            return "全员禁言消息";
        }else if(921 == type){
            return "允许普通群成员邀请好友加群";
        }else if(922 == type){
            return "允许普通成员上传群共享";
        }else if(923 == type){
            return "允许普通成员发起会议";
        }else if(924 == type){
            return "允许普通成员发送讲课";
        }else if(925 == type){
            return "转让群组";
        }else if(930 == type){
            return "设置、取消隐身人，监控人";
        }else if(931 == type){
            return "群组被后台封锁/解封";
        }else if(932 == type) {
            return "聊天记录超时设置";
        }else if(933 == type){
            return "面对面建群";
        }else if(934 == type){
            return "修改群公告";
        }else if(935 == type){
            return "修改群组加密类型";
        }else if(1000 == type){
            // type = 1000 后台管理表示阅后即焚做展示
            return "阅后即焚消息"
        }
        else{
			return "其他消息类型";
		}
    },
	// 消息解密
    decryptMsg:function(content,msgId,timeSend) {
	    timeSend = parseInt(timeSend);
        console.log("content:"+content+"  msgId: "+msgId+"  timeSend: "+timeSend);
        var key=Common.getMsgKey(msgId,timeSend)
        var desContent = content.replace(" ", "");
        var content
        try {
            content=Common.decryptDES(desContent,key);
        }catch (e) {
            console.log("des解密失败：  "+e)
            return content;
        }
        return content;
    },
    getMsgKey:function(msgId,timeSend){
        var key= localStorage.getItem("apiKey")+timeSend+msgId;
        return $.md5(key);
    },
    decryptDES:function(message,key){
        console.log("key1: "+key);
        //把私钥转换成16进制的字符串
        var keyHex = CryptoJS.enc.Utf8.parse(key);

        //把需要解密的数据从16进制字符串转换成字符byte数组
        var decrypted = CryptoJS.TripleDES.decrypt({
            ciphertext: CryptoJS.enc.Base64.parse(message)
        }, keyHex, {
            iv:CryptoJS.enc.Utf8.parse(iv),
            mode: CryptoJS.mode.CBC,
            padding: CryptoJS.pad.Pkcs7
        });
        //console.log("decryptDES   "+ decrypted);
        //以utf-8的形式输出解密过后内容
        var result = decrypted.toString(CryptoJS.enc.Utf8);
        console.log("decryptDES   "+ result);
        return result;
    },
    getValueForElement:function(elementStr){
       return xssFilters.inHTMLData($(elementStr).val());
    },
    filterHtmlData:function(data){
        return xssFilters.inHTMLData(data);
    },
    isUndefined:function(data){
	    return data == undefined ? "" : data;
    },


    /**
     * AES密文解密
     * @param messageId
     * @param content
     * @returns {*}
     */
    decryptAESMessage: function (messageId, content) {
        var key = this.getAESMsgKey(messageId);
        let decryptContent = EncryptUtils.decryptAES(content, key);
        decryptContent = CryptoJS.enc.Utf8.stringify(decryptContent);
        console.log("AES decryptAESMessage content :" + decryptContent);
        return decryptContent;
    },
    /**
     * AES加解密通用处理
     *  获取秘钥key
     */
    getAESMsgKey:function(messageId){
        var key= localStorage.getItem("apiKey") + messageId;
        return CryptoJS.MD5(key);
    },

    /**
     * 当前时间戳，单位毫秒
     */
    getCurrentMilliSeconds: function () {
        return Math.round(new Date().getTime());
    }

    //为空返回 空字符串
    , isNull : function (s) {
        if (undefined == s || null == s || $.trim(s) == "" || $.trim(s) == "null"){
            return "";
        }
        return s;
    }
    //为空 返回0
    ,isNumberNullOne : function (s) {
        if (undefined == s || null == s || $.trim(s) == "" || $.trim(s) == "null"){
            return 0;
        }
        return s;
    }
    //为空 返回0
    ,isNumberNullOne : function (s) {
        if (undefined == s || null == s || $.trim(s) == "" || $.trim(s) == "null"){
            return 0;
        }
        return s;
    }
    //为空 返回 -1
    ,isNumberNullTwo : function (s) {
        if (undefined == s || null == s || $.trim(s) == "" || $.trim(s) == "null"){
            return -1;
        }
        return s;
    }
    //格式化限制数字文本框输入，只能正数保存两位小数
    ,input_num_negative:function (obj){
        obj.value = obj.value.replace(/[^\d.]/g,""); //清除"数字"和"."以外的字符
        obj.value = obj.value.replace(/^\./g,""); //验证第一个字符是数字
        obj.value = obj.value.replace(/\.{2,}/g,"."); //只保留第一个, 清除多余的
        obj.value = obj.value.replace(".","$#$").replace(/\./g,"").replace("$#$",".");
        obj.value = obj.value.replace(/^(\-)*(\d+)\.(\d\d).*$/,'$1$2.$3'); //只能输入两个小数
    }
    //判断当前用户是不是同一个
    ,isThisUser:function (userId) {
        var flag;
        var loginUserId = localStorage.getItem("userId");
        if (userId === loginUserId){
            flag = true
        }else{
            flag = false
        }
        return flag;
    }
    // 公众号获取头像
    ,getAvatarUrl : function(userId,update) {
        if(10000==userId)
            return "./images/im_10000.png";
        var imgUrl = Config.getConfig().downloadAvatarUrl+"avatar/o/"+ (parseInt(userId) % 10000) + "/" + userId + ".jpg";
        if(1==update)
            imgUrl+="?x="+Math.random()*10;
        return imgUrl;
    }
    /**
     * 消息解密
     */
    ,decodeContent:function (d) {
        if(Common.isNil(d.content)){
            return "";
        }
        if(1 == d.isEncrypt){
            // des解密
            var desContent = Common.decryptMsg(d.content,d.messageId,d.timeSend);
            return desContent;
        }else if (2 == d.isEncrypt){
            // aes解密
            let testContent = Common.decryptAESMessage(d.messageId,d.content);
            return testContent;
        }

        var text = (Object.prototype.toString.call(d.content) === '[object Object]' ? JSON.stringify(d.content) : d.content)
        return text;
    }
    //金额 分转元
    ,getMoneyCentConvertYuan:function (fen) {
        var num = fen;
        num=fen*0.01;
        num+='';
        var reg = num.indexOf('.') >-1 ? /(\d{1,3})(?=(?:\d{3})+\.)/g : /(\d{1,3})(?=(?:\d{3})+$)/g;
        num=num.replace(reg,'$1');
        num = Common.toDecimal2(num)
        return num
    }
    //金额 分转元
    , getMoneyCentConvertYuan_: function (fen) {
        var num = fen;
        num = fen * 0.01;
        num += '';
        var reg = num.indexOf('.') > -1 ? /(\d{1,3})(?=(?:\d{3})+\.)/g : /(\d{1,3})(?=(?:\d{3})+$)/g;
        num = num.replace(reg, '$1');
        return num
    }
    ,//强制保留2位小数，如：2，会在2后面补上00.即2.00
    toDecimal2:function (x) {
        var f = parseFloat(x);
        if (isNaN(f)) {
            return false;
        }
        var f = Math.round(x * 100) / 100;
        var s = f.toString();
        var rs = s.indexOf('.');
        if (rs < 0) {
            rs = s.length;
            s += '.';
        }
        while (s.length <= rs + 2) {
            s += '0';
        }
        return s;
    }
    ,
    //元转分 - 解决精度问题 yuan:要转换的钱，单位元； digit：转换倍数 1000倍数为分
    regYuanToFen:function (yuan,digit) {
        var m=0,
            s1=yuan.toString(),
            s2=digit.toString();
        try{m+=s1.split(".")[1].length}catch(e){}
        try{m+=s2.split(".")[1].length}catch(e){}
        return Number(s1.replace(".",""))*Number(s2.replace(".",""))/Math.pow(10,m)
    },
}; /*End Common*/


//Array 获取该值下标
Array.prototype.indexOf = function (val) {
    for (var i = 0; i < this.length; i++) {
        if (this[i] == val) return i;
    }
    return -1;
};

//Array 删除改值
Array.prototype.remove = function (val) {
    var index = this.indexOf(val);
    if (index > -1) {
        this.splice(index, 1);
    }
};