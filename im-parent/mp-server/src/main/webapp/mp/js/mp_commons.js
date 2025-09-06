var mpCommon = {

	apiAddr:"",
	imServerAddr:"",
	fileAddr:"",
	uploadAddr:"",
	uploadAvatarAddr:"",
	apiKey:getLoginData().apiKey,
	keepalive:15,
	timeDelay:0,
	serviceUrl:'', //客服模块api调用地址
	//mpCommon.serviceUrl = mpCommon.nullData(data.serviceUrl);
	limit:15,
    limits:[15,20,30,50],

	invoke : function(obj) {

		jQuery.support.cors = true;

		if(1==obj.isImapi){
			obj.data = mpCommon.imapiCreateCommApiSecret(obj.data,obj.url);
		}else{
			mpCommon.createCommApiSecret(obj.data);
		}
		var params = {
			type : (mpCommon.isNil(obj.type)?"POST":"GET"),
			url : obj.url,
			data : obj.data,
			contentType : 'application/x-www-form-urlencoded; charset=UTF-8',
			dataType : 'JSON',
			async:false,
			success : function(result) {
				//1030101 缺少访问令牌    1030102 访问令牌无效或过期
				if(1030101 == result.resultCode  || 1030102 == result.resultCode ){
					tokenInvalid();
					return;
				}
				obj.success(result);
			},
			error : function(result) {
				if(obj.error)
					obj.error(result);
			},
			complete : function() {
			}
		};

		params.data["access_token"] = getLoginData().access_Token;
		$.ajax(params);
	},

	invoke2 : function(obj) {

		jQuery.support.cors = true;

		if(1==obj.isImapi){
			obj.data = mpCommon.imapiCreateCommApiSecret(obj.data,obj.url);
		}else{
			mpCommon.createCommApiSecret(obj.data);
		}
		obj.url = mpCommon.serviceUrl + obj.url;
		var params = {
			type : (mpCommon.isNil(obj.type)?"POST":"GET"),
			url : obj.url,
			data : obj.data,
			contentType : 'application/x-www-form-urlencoded; charset=UTF-8',
			dataType : 'JSON',
			async:false,
			success : function(result) {

				//1030101 缺少访问令牌    1030102 访问令牌无效或过期
				if(1030101 == result.resultCode  || 1030102 == result.resultCode ){
					tokenInvalid();
					return;
				}

				obj.success(result);

			},
			error : function(result) {
				if(obj.error)
					obj.error(result);
			},
			complete : function() {
			}
		};
		params.data["access_token"] = getLoginData().access_Token;
		$.ajax(params);
	},
	//这个方法是为了数组到服务器----zhm
	invokeByArray : function(obj) {

		jQuery.support.cors = true;
		mpCommon.createCommApiSecret(obj.data);
		var params = {
			type : "POST",
			url : obj.url,
			data : obj.data,
			contentType : 'application/x-www-form-urlencoded; charset=UTF-8',
			dataType : 'JSON',
			async:false,
			traditional: true,
			success : function(result) {

				//1030101 缺少访问令牌    1030102 访问令牌无效或过期
				if(1030101 == result.resultCode  || 1030102 == result.resultCode ){
					tokenInvalid();
					return;
				}

				obj.success(result);

			},
			error : function(result) {
				if(obj.error)
					obj.error(result);
			},
			complete : function() {
			}
		};

		params.data["access_token"] = getLoginData().access_Token;
		$.ajax(params);
	},
	// 公众号获取头像
	getAvatarUrl : function(userId,update) {
		if(10000==userId)
			return "./images/im_10000.png";

		if (mpCommon.isNil(JSON.parse(localStorage.getItem('loginData')).officialHeadImg)){
			var imgUrl = this.fileAddr+"avatar/o/"+ (parseInt(userId) % 10000) + "/" + userId + ".jpg";
			if(1==update)
				imgUrl+="?x="+Math.random()*10;
			return imgUrl;
		}else{
			return JSON.parse(localStorage.getItem('loginData')).officialHeadImg;
		}

	},
	//创建 密钥
	createCommApiSecret : function (obj){
		obj.time=mpCommon.getServerTimeSecond();
		var key = getLoginData().apiKey+obj.time+getLoginData().userId
			+getLoginData().access_Token;
		obj.secret=$.md5(key);
		return obj;
	},
	//调用 imapi 接口生成验证参数
    imapiCreateCommApiSecret:function(obj,url){
        let timesend = obj.salt;
        delete obj.salt;
        var key="";
        return mpCommon.imapiAuthoLoginCreateSecret(obj,timesend,url);
    },
    imapiAuthoLoginCreateSecret:function(obj,timeSend,url){
       // console.log("login 后的url : "+url);
        let sortParam = EncryptUtils.paramKeySort(obj);

        if(mpCommon.isNil(timeSend))
            timeSend = Math.round(new Date().getTime());
        obj.secret = getLoginData().apiKey+getLoginData().userId+getLoginData().access_Token+sortParam+timeSend;
        obj.secret = EncryptUtils.encryptMacToBase64(obj.secret,CryptoJS.enc.Base64.parse(getLoginData().httpKey));
        obj.salt = timeSend;
        return obj;
    },
	getServerTimeSecond:function(){
    	return Math.round((this.getMilliSeconds()-mpCommon.timeDelay)/1000);
    },
	getMilliSeconds:function(){
		return Math.round(new Date().getTime());
    },
   	parseFileSize : function(value){
	    if(null==value||value==''){
	        return "0 B";
	    }
	    var unitArr = new Array("B","KB","MB","GB");
	    var index=0;
	    var srcsize = parseFloat(value);
	    index=Math.floor(Math.log(srcsize)/Math.log(1024));
	    var size =srcsize/Math.pow(1024,index);
	    size=size.toFixed(2);//保留的小数位数
	    return size+unitArr[index];
	},
	isContains: function(str, substr) {
    	return str.indexOf(substr) >= 0;
	},
	isNil : function(s) {
		return undefined == s || $.trim(s) == "undefined" || null == s || $.trim(s) == "" || $.trim(s) == "null" || JSON.stringify(s) == JSON.stringify({}) || JSON.stringify(s) == JSON.stringify("{}");
	},
	notNull : function(s) {
		return undefined != s && null != s && $.trim(s) != "" && $.trim(s) != "null";
	},
	strToJson : function(str) {
		return eval("(" + str + ")");
	},
	setCookie:function(key,value){
		$.cookie(key,JSON.stringify(value));
	},
	getCookie:function(key){
		var value=$.cookie(key);
		return this.strToJson(value);
	},
	removeCookie:function(key){
		return $.removeCookie(key);
	},
	randomUUID : function() {
		var chars = myData.charArray, uuid = new Array(36), rnd = 0, r;
		for (var i = 0; i < 36; i++) {
			if (i == 8 || i == 13 || i == 18 || i == 23) {
				uuid[i] = '-';
			} else if (i == 14) {
				uuid[i] = '4';
			} else {
				if (rnd <= 0x02)
					rnd = 0x2000000 + (Math.random() * 0x1000000) | 0;
				r = rnd & 0xf;
				rnd = rnd >> 4;
				uuid[i] = chars[(i == 19) ? (r & 0x3) | 0x8 : r];
			}
		}
		return uuid.join('').replace(/-/gm, '').toLowerCase();
	},
	regText :"(\\s|\\n|<br>|^)(http(s)?://.)?(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?(&|&amp;)//=]*)",

	hrefEncode:function  (e) {
      var a = e.match(/&lt;a href=(?:'|").*?(?:'|").*?&gt;.*?&lt;\/a&gt;/g);
      if (a) {
          for (var n, i, o = 0, r = a.length; o < r; ++o)
             n = /&lt;a href=(?:'|")(.*?)(?:'|").*?&gt;.*?&lt;\/a&gt;/.exec(a[o]),
              n && n[1] && (i = n[1],this.isUrl(i) && (e = e.replace(n[0], this.htmlDecode(n[0])).replace(n[1], n[1])));
          return e
      }
      return e.replace(new RegExp(this.regText, "ig"), function () {
          return '<a target="_blank" href="' + arguments[0].replace(/^(\s|\n)/, "") + '">' + arguments[0] + "</a> "
      })
   },
   isUrl:function(e) {
      return new RegExp(this.regText, "i").test(e)
   },
   htmlDecode:function (e){
     return e && 0 != e.length ? e.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&") : ""
   },
   /* 初始化配置 */
   initConfig:function(){
		mpHttpApi.getConfig(function(result){
			console.log("====> initConfig > "+JSON.stringify(result));
			if(mpCommon.isNil(result))
				return;
			if(result.apiAddr.endWith("/")){
				result.apiAddr+="#";
				result.apiAddr=result.apiAddr.replace("/#","");
			}
			if(result.isOpenWss == 1){
				mpCommon.imServerAddr = "wss://"+result.imServerAddr+":5260";
			}else{
				mpCommon.imServerAddr = "ws://"+result.imServerAddr+":5260";
			}
			mpCommon.apiAddr = result.apiAddr;
			mpCommon.fileAddr = result.fileAddr;
			mpCommon.uploadAddr = result.uploadAddr+"upload/UploadifyServlet";
			mpCommon.uploadAvatarAddr = result.uploadAddr+"upload/UploadifyAvatarServlet";
		});

		if(mpCommon.isNil(mpCommon.imServerAddr)){
			
			setTimeout(function(){
				if(mpCommon.isNil(mpCommon.imServerAddr)){
					mpCommon.initConfig();
				}else{
					return;
				}
					
			},2000);
		}

	},
	//数据回显判断是否为空
	nullData:function (data){
		if(data == '' || data == "undefined" || data==null){
			return "";
		}else{
			return data;
		}
	}
	,getHeadImg:function (userId) {
		return mpCommon.isNil(JSON.parse(localStorage.getItem('loginData')).officialHeadImg) ? mpCommon.getAvatarUrl(userId) : JSON.parse(localStorage.getItem('loginData')).officialHeadImg;
	}

	//元转分
	,regYuanToFen:function (yuan,digit) {
		var m=0,
			s1=yuan.toString(),
			s2=digit.toString();
		try{m+=s1.split(".")[1].length}catch(e){}
		try{m+=s2.split(".")[1].length}catch(e){}
		return Number(s1.replace(".",""))*Number(s2.replace(".",""))/Math.pow(10,m)
	}
	//强制保留2位小数，如：2，会在2后面补上00.即2.00
	,toDecimal2:function (x) {
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

	//分转元
	,getMoneyCentConvertYuan:function (fen) {
		var num = fen;
		num=fen*0.01;
		num+='';
		var reg = num.indexOf('.') >-1 ? /(\d{1,3})(?=(?:\d{3})+\.)/g : /(\d{1,3})(?=(?:\d{3})+$)/g;
		num=num.replace(reg,'$1');
		return num
	}
};


function getLoginData() {
		var loginData = JSON.parse(localStorage.getItem('loginData'));
		if(loginData==undefined || loginData == null){
			tokenInvalid();
			return;
		}
		return loginData;
};

/**
* token 无效跳转到登录页面
**/
function tokenInvalid(){

	/*layer.alert(mpLanguage.getLanguageName('access_token_invalid'),{
	 title:false,
	 icon:2,
	 closeBtn:0,
	 btn: [''+mpLanguage.getLanguageName('determine')],
	 btnAlign: 'c'
	},function(index){
		layer.close(index);
		//跳转到登录页面
		window.top.location.href="/mp/login.html";
	});*/

	layer.alert("访问令牌过期或无效",{
	 title:false,
	 icon:2,
	 closeBtn:0,
	 btn: ['确定'],
	 btnAlign: 'c'
	},function(index){
		layer.close(index);
		//跳转到登录页面
		window.top.location.href="/mp/login.html";
	});

}




function request(url,isImapi){
    /*if(1==isImapi){
        let sortParam = EncryptUtils.paramKeySort(obj);
        let timeSend = Math.round(new Date().getTime());

        let secret = getLoginData().apiKey+getLoginData().userId+getLoginData().access_Token+sortParam+timeSend;
        secret = EncryptUtils.encryptMacToBase64(secret,CryptoJS.enc.Base64.parse(getLoginData().httpKey));
        let salt = timeSend;

        url=url+"?secret="+secret+"&salt="+salt+"&access_token="+getLoginData().access_Token;

    }else{*/
    	var time = mpCommon.getServerTimeSecond();
    	var key = getLoginData().apiKey+time+getLoginData().userId+getLoginData().access_Token;
		var secret = $.md5(key);
		url=url+"?secret="+secret+"&time="+time+"&access_token="+getLoginData().access_Token;
    // }
    return url;

}


/**
 * 
 * @param type //type : 1 成功 2:失败 3：提示 4:询问
 * @param infoText
 * @param okCallback
 * @returns
 */
function ownAlert(type,infoText,okCallback){  //自定义的弹框  

	if(type==1)
		layer.msg(infoText, {icon: 1});

	if(type==2)
		layer.msg(infoText, {icon: 5});
	if(type==3)
		layui.layer.open({
		  title: false,
		  closeBtn: 0,
		  btnAlign: 'c',
		  skin: 'my-skin',
		  content: '<div style="text-align:center;">'+infoText+'</div>' 
		});
	if(type==4)

		layui.layer.confirm(
				'<div style="text-align:center;">'+infoText+'</div>', 
				{icon: 3, title:false, closeBtn: 0,btnAlign: 'c',skin: 'my-skin'}, 
				function(index){
				   if(okCallback)	
						 okCallback();
                   layui.layer.close(index);

			    }
		);

};


String.prototype.replaceAll  = function(s1,s2){   

    return this.replace(new RegExp(s1,"gm"),s2);   
};

String.prototype.endWith = function(str){
   if(str==null || str=="" || this.length == 0 ||str.length > this.length){ 
       return false;
   }
  if(str==this.substring(this.length - str.length)){
     return true;
   }else{
     return false;
   }
};

//时间转换
Date.prototype.format = function(fmt) { 
     var o = { 
        "M+" : this.getMonth()+1,                 //月份 
        "d+" : this.getDate(),                    //日 
        "h+" : this.getHours(),                   //小时 
        "m+" : this.getMinutes(),                 //分 
        "s+" : this.getSeconds(),                 //秒 
        "q+" : Math.floor((this.getMonth()+3)/3), //季度 
        "S"  : this.getMilliseconds()             //毫秒 
    }; 
    if(/(y+)/.test(fmt)) {
            fmt=fmt.replace(RegExp.$1, (this.getFullYear()+"").substr(4 - RegExp.$1.length)); 
    }
     for(var k in o) {
        if(new RegExp("("+ k +")").test(fmt)){
             fmt = fmt.replace(RegExp.$1, (RegExp.$1.length==1) ? (o[k]) : (("00"+ o[k]).substr((""+ o[k]).length)));
         }
     }
    return fmt; 
};


var mpLanguage = {
	//国际化初始化方法
	loadProperties:function(language){
		localStorage.setItem('mp_system_language',language);
		$.i18n.properties({
			name: 'strings',    //属性文件名     命名格式： 文件名_国家代号.properties
			path: '/mp/common/i18n/',   //注意这里路径是你属性文件的所在文件夹
			mode: 'map',
			language: language,     //这就是国家代号 name+language刚好组成属性文件名：strings+zh -> strings_zh.properties
			callback: function () {
				$("[data-locale]").each(function () {
					if($(this).attr('placeholder')){
						$(this).attr('placeholder', $.i18n.prop($(this).data('locale')));

					}if ($(this).attr('title')){
						$(this).attr('title', $.i18n.prop($(this).data('locale')));
					}else{
						$(this).html($.i18n.prop($(this).data("locale")));
					}
				});

			}
		});

		layui.form.render();
	}

	//获取国际化名称
	,getLanguageName:function (data) {
		mpLanguage.loadProperties(mpLanguage.getLanguage());
		try {
			return $.i18n.prop(data);
		} catch (e) {
			return data;
		}
	}
	//获取当前国际化语言
	,getLanguage:function() {
		var languaeg = mpLanguage.getSystemLanguaeg();
		if (mpCommon.isNil(languaeg)){
			return "zh";
		}else{
			return languaeg.toLowerCase();
		}
	}

	//获取当前存储语言
	,getSystemLanguaeg:function () {
		return localStorage.getItem('mp_system_language');
	}

	//提示框
	,Tips:function (place , lang) {
		layui.layer.tips(
			mpLanguage.getLanguageName(lang), '#'+ place +'',{
			tips: [4, '#01AAED']
		});
	}

	//初始化
	,initLanguage:function () {
		mpLanguage.loadProperties(mpLanguage.getLanguage());
	}

}
