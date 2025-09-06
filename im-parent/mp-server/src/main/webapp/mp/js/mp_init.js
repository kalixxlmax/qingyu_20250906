$(function() {

	//从服务器获取配置
	mpCommon.initConfig();

	//WEBIM.setUserIdAndToken(getLoginData().userId,getLoginData().access_Token);
	WEBIM.initWebIM(mpCommon.imServerAddr,getLoginData().userId,"mp"
			,getLoginData().access_Token,mpCommon.keepalive,getLoginData().nickname);


	$(".mp-head-nickname").text(getLoginData().nickname);
	$('#userInfo #avatar').attr('src',
		mpCommon.getAvatarUrl(getLoginData().userId,1)).attr("onerror","this.src='/mp/images/ic_avatar.png'");

	//监听头像点击上传
	UI.uploadhead();

	WEBIM.loginIM(function(){
        console.log("Mp loginSuccess =========>");
        // layui.layer.close(init_loading_index); //关闭loding
        UI.online();
	});

	//监听网络状态
	NetWork.networkListener(onNet,offNet);

	mpHttpApi.getCurrentTime();//获取和服务器时间的差值

	//加载首页
	$('#home').trigger("click");

	//判断是不是企业号
	var data = eval('(' + localStorage.getItem('loginData') + ')');
	if (data.userType != 4 ){
		$("#service_system").hide();
	}else{
		$("#service_system").show();
	}


	//初始化当前语言
	mpLanguage.loadProperties(mpLanguage.getLanguage());

	//加载消息管理最近会话列表
	MpChat.getLastChatList(0);

	//加载消息管理的消息未读数
	var msgNumCount = mpDataUtils.getMsgNumCount();
	MpChat.showMsgNumCount(msgNumCount);


	$("#index #newMsg_area").click(function(){
		$('#mp_navBar #li_six').trigger("click");
		$('#mp_navBar #li_six a').trigger("click");
	});

	$("#index #fansNum_area").click(function(){
		$('#mp_navBar #li_seven').trigger("click");
		$('#mp_navBar #li_seven a').trigger("click");
	});


	MpChat.uploadImg(); //监听图片上传

	MpChat.uploadFile(); //监听文件上传

	MpChat.loadEmoji(); //加载emoji 表情


	//监听发送消息按钮点击事件
	$("#btnSend").click(function(){ //点击详情按钮
			
		var content = $("#messageBody").val();
		if (mpCommon.isNil(content)) {
			ownAlert(3,mpLanguage.getLanguageName('input_send_context'));
			return;
		}
		var msg=WEBIM.createMessage(1, content);

		MpChat.processSendMsg(msg);

		$("#messageBody").val("");
		return;

	});

	//监听触发发送消息的按键事件
	document.getElementById("messageBody").onkeydown = function (e) {

        e = e || window.event;
        if (e.ctrlKey && e.keyCode == 13) {
            var $messageBody = $("#messageBody"), text = $messageBody.val();
            $messageBody.val(text + " \n ");
            return;
        }
        if (e.keyCode === 13) {
            var content = $("#messageBody").val();
            if (mpCommon.isNil(content)) {
                return;
            }
            var msg = WEBIM.createMessage(1, content);
            MpChat.processSendMsg(msg);
            setTimeout(function () {
                $("#messageBody").val(".").val("");
            }, 200);
        }
	};

	// 表情
	$("#btnEmojl").click(function(event) {
		//$("#userfulText-panel").hide();
		$("#gif-panel").hide();
		$("#gif-panel #gifList").getNiceScroll().hide();
		var e = window.event || event;
		
		if (e.stopPropagation) {
			e.stopPropagation();
			
		} else {
			e.cancelBubble = true;
		}
		$('#emojl-panel').toggle();
		$("#emojl-panel #emojiList").getNiceScroll().show();
		$("#emojl-panel #emojiList").getNiceScroll(0).doScrollTop(0, 0);

	});

	//加载更多消息列表
	$("#loadMoreMsgList").click(function(){
		$("#loadingMsgList").show().siblings().hide();
		MpChat.getLastChatList(mpDataUtils.getMsgListLastTime());
	});

});








