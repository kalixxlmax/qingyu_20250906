var tableInFans;

layui.use(['layer','table','util'],function(){
    var layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        table = layui.table,
        util = layui.util;

    	// 监听粉丝列表菜单操作项
        table.on('tool(fans_list)', function(obj){
            var layEvent = obj.event,  data = obj.data;

            if(layEvent==='send_msg'){ //编辑问题

                MpChat.openChatPanel(data.toUserId, data.toNickname);

            }else if(layEvent==='del_fans'){ //删除问题
                layer.confirm('确定删除该粉丝吗？', function(index){
                    UI.deleteFans(data.toUserId, tableInFans);
                    obj.del(); //删除对应行（tr）的DOM结构，并更新缓存
                    layer.close(index);
                });

            }

        });

});

var UI={
	getText:function(text,length){
		if(mpCommon.isNil(text))
			return  " ";
		text = text.replace(/<br\/>/g, '');
		if(!length)
			length=15;
		if (text.length<=length)
			return text;
		text = text.substring(0,length)+"...";
		return text;
	},
	// 面板切换
	switchTab:function(that){
		/*$("#"+that.id).css("background-color","#4E5465");
		$("#"+that.id).siblings().css("background-color","#393D49");*/
		//消息面板特殊处理
		if("msg"!=that.id){
			$("#msg_manager").hide();
		}

		if("fans"!=that.id){
			$("#fans_manager").hide();
		}

		$(that).css("background-color","#4E5465");
		$(that).siblings().css("background-color","#393D49");

		if(undefined==$(that).attr("data-url")){
			//结束加载
			return;
		}

		var pageContent = "<iframe src='"+$(that).attr("data-url")+"' style='position: absolute;height: 90%;width: 100%;border: none;'></frame>";
        $("#mpBodyTab").empty().append(pageContent);

	},
	// 消息管理
	load_messageList:function(){
		
		// $("#li_six").css("background-color","#4E5465");
		$("#mpBodyTab").empty();
		$("#fans_manager").hide();
		$("#msg_manager").show();


		var MsgListOrder =  mpDataUtils.getMsgListOrder();
		var html="";
		if (JSON.stringify(MsgListOrder) != JSON.stringify([])){
			//for(var userId in MsgNumMap){
			MsgListOrder.forEach(function(userId){
				/*var lastMsg = mpDataUtils.getLastMsgFromUnReadMsgList(userId);
				if(mpCommon.isNil(lastMsg))
					lastMsg = mpDataUtils.getLastMsg(userId);
				if(!mpCommon.isNil(lastMsg)){
					var nickName = (lastMsg.fromUserId==getLoginData().userId) ?
					 (mpCommon.isNil(lastMsg.toUserName) ? IMSDK.getUserIdFromJid(lastMsg.toJid) : lastMsg.toUserName)  : lastMsg.fromUserName;
		  			html+=UI.createMessageItem(userId, nickName,mpDataUtils.getMsgNum(userId),getTimeText(lastMsg.timeSend,0), WEBIM.parseShowMsgTitle(lastMsg));
				}*/
				html += MpChat.loadMsgListHtml(userId);
			});
			//}

		}else{
			html+="<p class='empty_tips'>" + mpLanguage.getLanguageName('index_14') + "</p>";
		}

		$("#msg_manager #message_list").empty();
		$("#msg_manager #message_list").append(html);

		
	},
	createMessageItem : function(userId,nickName,msgNum,timeStr,lastMsgStr){


		var messageItem ='<li class="message_item " id="msgListItem_'+userId+'">'
						+      '<div class="message_opr">'
						+          '<button onclick="MpChat.openChatPanel('+userId+',\''+nickName+'\')" class="layui-btn" style="height:30px;line-height:30px;border-radius:3px">'+ mpLanguage.getLanguageName('send_message') +'</button>'
						+       '</div>'
						+       '<div class="message_info">'
					    +          '<div id="message_time" class="message_time">'+timeStr+'</div>'
						+            '<div class="user_info">'
						+                '<p class="remark_name">'+nickName+'</p>'
						+                '<a target="_blank"  class="avatar">'
						+                    '<img class="roundAvatar" onerror="this.src=\'./images/ic_avatar.png\'" src="'+mpCommon.getAvatarUrl(userId)+'">'
						+          			 '<i id="messageNumw_count" class="message_num '+(msgNum>0 ? 'msgNumShow':'msgNumHide')+'">'+(msgNum>0 ? msgNum : "")+'</i>'
						+                '</a>'
						+            '</div>'
						+        '</div>'
						+        '<div class="message_content text">'
						+          '<div id="lastMsgContent">'+lastMsgStr+'</div>'
						+      '</div>'
						+ '</li>';

		return messageItem;

	},
	/*loadFansList:function(){

		$("#mpBodyTab").empty();
		$("#msg_manager").hide();
		$("#fans_manager").show();

		//粉丝列表
        tableInFans = layui.table.render({
            elem: '#fans_list'
            ,toolbar: '#fansTopBar'
            ,url: request("/mp/fans")
            ,id: 'fans_list'
            ,page: true
            ,curr: 0
            ,limit: mpCommon.limit
            ,limits: mpCommon.limits
            ,groups: 7
            ,cols: [[ //表头

                 // {type:'checkbox',fixed:'left'}// 多选
                {field: 'toUserId', title: mpLanguage.getLanguageName('fans_avatar'), width:120,templet:function(d){
                        return "<img width='30px' onerror='this.src=\"./images/ic_avatar.png\"'  src='"+mpCommon.getAvatarUrl(d.toUserId)+"'>";
                    }
                }
                ,{field: 'toUserId', title:  mpLanguage.getLanguageName('fans_Id'),sort:'true', width:120}
                ,{field: 'toNickname', title: mpLanguage.getLanguageName('fans_nickName'), width:300}
                ,{field: 'createTime', title: mpLanguage.getLanguageName('attention_time') ,sort:'true', width:200,templet:function(d){
                        return layui.util.toDateString(d.createTime*1000);
                    }
                }
                ,{fixed: 'right', title: mpLanguage.getLanguageName('operation'), align:'center', toolbar: '#fansOptionBar'}
            ]]
            ,done:function(res, curr, count){
                // checkRequst(res);

               //initLanguage();
                layui.form.render();
            }

        });

	},*/
	loadFansList:function(){

    		$("#mpBodyTab").empty();
    		$("#msg_manager").hide();
    		$("#fans_manager").show();

    		//粉丝列表
            tableInFans = layui.table.render({
                elem: '#fans_list'
                ,toolbar: '#fansTopBar'
                ,url: request("/mp/fans")
                ,id: 'fans_list'
                ,page: true
                ,curr: 0
                ,limit: mpCommon.limit
                ,limits: mpCommon.limits
                ,groups: 7
                ,cols: [[ //表头

                     // {type:'checkbox',fixed:'left'}// 多选
                    {field: 'toUserId', title: mpLanguage.getLanguageName('fans_avatar'), width:120,templet:function(d){
                            return "<img width='30px' onerror='this.src=\"./images/ic_avatar.png\"'  src='"+mpCommon.getAvatarUrl(d.toUserId)+"'>";
                        }
                    }
                    ,{field: 'toUserId', title:  mpLanguage.getLanguageName('fans_Id'),sort:'true', width:120}
                    ,{field: 'toNickname', title: mpLanguage.getLanguageName('fans_nickName'), width:300}
                    ,{field: 'createTime', title: mpLanguage.getLanguageName('attention_time') ,sort:'true', width:200,templet:function(d){
                            return layui.util.toDateString(d.createTime*1000);
                        }
                    }
                    ,{fixed: 'right', title: mpLanguage.getLanguageName('operation'), align:'center', toolbar: '#fansOptionBar'}
                ]]
                ,done:function(res, curr, count){
                    // checkRequst(res);

                   //initLanguage();
                    layui.form.render();
                }

            });

    	},

	deleteFans : function(fansId,){

		mpCommon.invoke({
			url : '/mp/fans/delete',
			data : {
				toUserId : fansId
			},
			success : function(result) {
				if (result.resultCode == 1) {
					layui.layer.msg(mpLanguage.getLanguageName('delete_success') ,{"icon":1});
                    //layui.layer.close(index);
                    tableInFans.reload(); //重载当前页

				} else {
					layui.layer.msg(mpLanguage.getLanguageName('delete_failed') ,{"icon":2});
				}
			},
			error : function(result) {
				layui.layer.msg(mpLanguage.getLanguageName('delete_failed') ,{"icon":2});
			}
		});

	},
	findMsgList:function(toUserId){
		$("#newMsg_item").show();
		$("#msg_item").show();
		$("#index_newMsg").hide();
		$("#msg_manager").hide();
		var html="";
		
		mpCommon.invoke({
			url : '/mp/msg/list',
			data : {
				toUserId:toUserId
			},
			success : function(result) {
				if(result.data==null){
				    //暂无数据
					html+="<tr><td>"+ mpLanguage.getLanguageName('index_1') +"</td><td></tr>";
				}else{
					for(var i=0;i<result.data.length;i++){
					    var msg = result.data[i];
						var sender = msg.sender;
						var plaintext = msg.content;
						// 消息解密处理
                        if(1 == msg.isEncrypt){
                            // 明文
                            plaintext = msgCommon.decryptMsg(msg.content,msg.messageId,msg.timeSend);
                            console.log(" msg : "+msg.content+"      role:   "+msg.isEncrypt+"       msgId:  "+msg.messageId+"    timeSend:  "+msg.timeSend +" plaintext:   "+plaintext);
                        }
						plaintext=UI.getText(plaintext,20);
						html+="<tr>"
						    +	"<td>"
						    +		"<img width='40px' onerror='this.src=\"./images/ic_avatar.png\"' src='"+mpCommon.getAvatarUrl(msg.receiver)+"'>"
						    +	"</td>"
						    +    "<td>"+msg.receiver+"</td>"
						    +	 "<td>"+result.data[i].nickname+"</td>"
						    +	 "<td>"+plaintext+"</td>"
						    +	 "<td>"
						    +		"<button onclick='MpChat.openChatPanel(" + msg.receiver + "," + result.data[i].nickname + ");'  class='layui-btn' >"+ mpLanguage.getLanguageName('index_16') +"</button>"
						    +	 "</td>"
						    + "</tr>";
					}
					$("#newMsg_body").empty();							
					$("#newMsg_body").append(html);
					$("#Msg_body").empty();
					$("#Msg_body").append(html);
				}
			},
			error : function(result) {
			    //加载数据失败
				layui.layer.alert(mpLanguage.getLanguageName('index_7'));
			}
		});
	},
	// select框变化
	change:function(){
		if($("#parentId").val()==0){
			$("#menu_url").hide();
			$("#menu_menuId").hide();
		}else{
			$("#menu_url").show();
			$("#menu_menuId").show();
		}
	},
	// 返回
	return_btn:function(){
		$("#menu").show();
		$("#update_menu").hide();
	},
	online:function(){
		// 用户在线
		$("#userInfo #avatar").removeClass("headChang");
		$("#userInfo #status").removeClass("user-back");
		$("#userInfo #status").addClass("user-online");
	},
	offline:function(){
		// 用户离线
		$("#userInfo #avatar").addClass("headChang");
		$("#userInfo #status").removeClass("user-online");
		$("#userInfo #status").addClass("user-back");
	},

	//全局配置
	mpConfig:function () {

		UI.getMpConfig();
	}

	/*//获取全局配置
	,getMpConfig:function () {
		mpCommon.invoke({
			url : '/mp/queryMpConfig',
			data : {},
			success : function(result) {
				if(result.resultCode == 1){
					let data=JSON.parse(result.data);
					$("#serviceUrl").val(mpCommon.nullData(data.serviceUrl));
					$("#uploadUrl").val(mpCommon.nullData(data.uploadUrl));
				}
			},
			error : function(result) {
				layui.layer.msg(mpLanguage.getLanguageName('index_26'));
			}
		});
	}

	//更新全局配置
	,savaMpConfig:function () {
		var systemMpConfig = {};
		var obj={};
		obj.serviceUrl = $("#serviceUrl").val()
		obj.uploadUrl = $("#uploadUrl").val()
		obj=JSON.stringify(obj);
		systemMpConfig.config=obj;

		mpCommon.invoke({
			url : '/mp/saveMpConfig',
			data : systemMpConfig,
			success : function(result) {
				if(result.resultCode == 1){
					layui.layer.msg(mpLanguage.getLanguageName('index_27'));
				}
			},
			error : function(result) {
				layui.layer.msg(mpLanguage.getLanguageName('index_28'));
			}
		});
	}*/

	//初始化信息
	,init_charge:function () {
		var data = eval('(' + localStorage.getItem('loginData') + ')');
		$(".charge_userName").val(data.nickname);

		mpCommon.invoke({
			url : '/mp/find/payAddFriend',
			data : {},
			success : function(result) {
				debugger
				if (result.data == -1){
					//关闭
					$("#switchCharge").removeAttr("checked");
					$('#switchCharge').attr("disabled");
					$("#switch_charge").val("-1");
					$(".payAddFriend")[0].classList.add("layui-disabled");
					$(".chargeDiv_form_sava")[0].classList.add("layui-btn-disabled");
					$(".chargeDiv_form_sava").attr("onclick","");
					$(".payAddFriend").val("");
					$(".payAddFriend").attr("disabled",true);
					layui.form.render();
				}else{
					$("#switchCharge").attr("checked");
					$('#switchCharge').removeAttr("disabled");
					$("#switch_charge").val("0");
					$(".payAddFriend")[0].classList.remove("layui-disabled");
					$(".chargeDiv_form_sava")[0].classList.remove("layui-btn-disabled");
					$(".chargeDiv_form_sava").attr("onclick","UI.charge_From_Submit()");
					$(".payAddFriend").attr("disabled",false);
					$(".payAddFriend").val(mpCommon.getMoneyCentConvertYuan(result.data));
					layui.form.render();
				}
			},
			error : function(result) {
				layui.layer.msg(mpLanguage.getLanguageName('index_29'));
			}
		});
	}

	//公众号收费表单提交
	,charge_From_Submit:function () {
		var payAddFriend;
		var data_1 = $("#switch_charge").val();
		var data_2 = $(".payAddFriend").val();

		if (data_1 == -1){
			payAddFriend = "-1";
		}else if (mpCommon.isNil(data_2)){
			payAddFriend = "0";
		}else{
			payAddFriend = data_2;
		}
		if (parseInt(payAddFriend) > 10){
			$(".prompt_charge").show();
			return;
		}else{
			$(".prompt_charge").hide();
		}

		payAddFriend = mpCommon.regYuanToFen(mpCommon.toDecimal2(payAddFriend),100);
		alert(payAddFriend);
		mpCommon.invoke({
			url : '/mp/set/payAddFriend',
			data : {
				payAddFriend:payAddFriend
			},
			success : function(result) {
				if (payAddFriend != -1){
					$(".payAddFriend").val(mpCommon.getMoneyCentConvertYuan(payAddFriend));
					layer.msg(mpLanguage.getLanguageName('index_30'), {icon: 1});
				}
			},
			error : function(result) {
				layer.msg(mpLanguage.getLanguageName('index_29'), {icon: 5});
			}
		});
	}
	//获取发红包是否打开
	,setOfficialPacketIsOpen:function (data) {
		mpCommon.invoke({
			url : '/mp/edit/officialPacket/open',
			data : {
				isOpen:data
			},
			success : function(result) {
				if (result.resultCode == 1){
					layui.layer.msg(mpLanguage.getLanguageName('index_31'));
				}
			},
			error : function(result) {
				layui.layer.msg(mpLanguage.getLanguageName('index_29'));
			}
		});
	}

	//获取发红包是否打开
	,getOfficialPacketIsOpen:function () {
		mpCommon.invoke({
			url : '/mp/find/officialPacketIsOpen',
			data : {},
			success : function(result) {
				console.log(result);
				if (result.resultCode == 1){
					if (result.data == undefined){
						//默认为关闭
						$('#switchReward').removeAttr("checked");
						$("#switchReward").attr("disabled");
						$(".switch_flag").val("0");
						$("#rewardDiv_form_sava").attr("onclick","");
						$("#rewardDiv_form_sava").addClass("layui-btn-disabled");
						layui.form.render();
						return;
					}
					if (result.data == 1){
						//设置开关为打开
						$("#switchReward").attr("checked");
						$('#switchReward').removeAttr("disabled");
						$(".switch_flag").val("1");
						$("#rewardDiv_form_sava").attr("onclick","UI.setOfficialPacketSettings()");
						$("#rewardDiv_form_sava").removeClass("layui-btn-disabled");
						layui.form.render();
					}else{
						//设置开关为关闭
						$('#switchReward').removeAttr("checked");
						$("#switchReward").attr("disabled");
						$(".switch_flag").val("0");
						$("#rewardDiv_form_sava").attr("onclick","");
						$("#rewardDiv_form_sava").addClass("layui-btn-disabled");
						layui.form.render();
					}
				}
			},
			error : function(result) {
				layui.layer.msg(mpLanguage.getLanguageName('index_32'));
			}
		});
	}

	//获取关注公众号红包设置
	,setOfficialPacketSettings:function () {
		var totalPrice = $(".totalPrice").val();
		if (mpCommon.isNil(totalPrice)){
			layui.layer.msg(mpLanguage.getLanguageName('reward_2'));
			return;
		}
		var sendCount = $(".sendCount").val();
		if (mpCommon.isNil(sendCount)){
			layui.layer.msg(mpLanguage.getLanguageName('index_33'));
			return;
		}
		var remark = $(".remark").val();
		if (mpCommon.isNil(remark)){
			layui.layer.msg(mpLanguage.getLanguageName('index_34'));
			return;
		}
		var delayDate = $(".delayDate").val();
		if (mpCommon.isNil(delayDate)){
			layui.layer.msg(mpLanguage.getLanguageName('index_35'));
			return;
		}

		if (parseInt(totalPrice) > 200){
			$(".reward_charge").show();
			return;
		}else {
			$(".reward_charge").hide();
		}
		mpCommon.invoke({
			url : '/mp/edit/officialPacket',
			data : {
				totalPrice:totalPrice,
				sendCount:sendCount,
				remark:remark,
				delayDate:delayDate,
				isOpen:$(".switch_flag").val()
			},
			success : function(result) {
				if (result.resultCode == 1){
					layui.layer.msg(mpLanguage.getLanguageName('order_8'));
				}else{
					layer.msg(result.resultMsg , {icon: 5});
				}
			},
			error : function(result) {
				layui.layer.msg(mpLanguage.getLanguageName('index_36'));
			}
		});
	},
	//头像上传
	uploadhead : function(docObj){

	    layui.upload.render({
		        elem: '#avatar', //文件选择元素
		        accept:'images',
		        url: mpCommon.uploadAvatarAddr+'',
		        data:{userId : getLoginData().userId},
		        done: function(res) { //上传完成事件

		          	//ownAlert(1,"头像上传成功！");
					//上传成功后更新头像
					$('#userInfo #avatar').attr('src',res.data.oUrl+"?"+Math.random()*1000);
		        },
		        error: function(res) {
		          if(res.msg){
		          		layui.layer.msg(res.msg);
		          }

		        }
		});



	}

	/*//头像上传
	,uploadhead:function(){
		$("#uploadUserHead").ajaxSubmit({
			uploadProgress: function (event, position, total, percentComplete) {
				percentVal = percentComplete + '%';

			},
			success:function(data){
				var obj = eval("("+data+")");
				console.log(obj.data.oUrl);
				$('#userInfo #avatar').attr('src',obj.data.oUrl+"?"+Math.random()*1000);
			}
		})
	}
	,selectHeadFile:function(data){
		$("#uploadUserHead").attr("action","http://upload.server.com:8088/upload/UploadifyAvatarServlet");
		localStorage.setItem("imgUrl",data);
		$("#userHeadUpload").click();
	}*/

	/*,initCustomLanguage:function () {
		// customer.renderLeyui();
		layui.form.render();
	}*/

	,zhzs:function (value){
		value = value.replace(/[^\d]/g,'');
		if(''!=value){
			value = parseInt(value);
		}
		return value;
	},
	//获取用户详细信息
	getUserInfo:function (userId) {
		mpCommon.invoke({
			url : '/mp/find/user/info',
			data : {
				userId:userId
			},
			success : function(result) {
				if(result.resultCode == 1){
					$(".user_info_headImg").attr("src",mpCommon.getAvatarUrl(result.data.userId));
					$(".userinfo_nickname").val(result.data.nickname);
					$(".userinfo_sex").val(result.data.sex==0?"男":"女");
					$(".userinfo_birthday").val(layui.util.toDateString(result.data.birthday * 1000));
					$(".userinfo_description").val(result.data.description);
					$(".userinfo_telephone").val(result.data.phone);
					$(".userinfo_account").val(result.data.account);
					$(".userinfo_area").val(result.data.area);
					layui.form.render();
					//弹出层
					layer.open({
						type: 1,
						title: "用户信息",
						area: ['680px','600px'],
						shadeClose: true,
						shade: false,
						maxmin: true, //开启最大化最小化按钮
						content: $("#showMsgInfo"),
						cancel:function () {
							$("#showMsgInfo").hide();
							layui.form.render();
						}
					});
				}
			},
			error : function(result) {
				layui.layer.msg("获取信息失败！");
			}
		});
	}

}

layui.use(['jquery','form','layer','laydate'],function(){
	var form = layui.form,
		layer = parent.layer === undefined ? layui.layer : top.layer,
		$ = layui.jquery,
		laydate = layui.laydate;

	//日期范围
	var time =  laydate.render({
		elem: '#globalDate'
		,range: "~"
		,done: function(value, date, endDate){  // choose end
			//console.log("date callBack====>>>"+value); //得到日期生成的值，如：2017-08-18
			var startDate = value.split("~")[0];
			var endDate = value.split("~")[1];
			UI.getVisitorCount(startDate,endDate,2);
		}
		,max: 0
	});

	//监听指定开关
	form.on('switch(switchCharge)', function(data){
		var flag = data.elem.checked;

		var content = flag ? mpLanguage.getLanguageName("index_37") : mpLanguage.getLanguageName('index_38') ;
		layer.confirm(content, {
			btn: [mpLanguage.getLanguageName('select_8'),mpLanguage.getLanguageName('select_7')] //按钮
		}, function(){
			if (flag){
				//开启
				$("#switch_charge").val("0");
				$(".payAddFriend")[0].classList.remove("layui-disabled");
				$(".chargeDiv_form_sava")[0].classList.remove("layui-btn-disabled");
				$(".chargeDiv_form_sava").attr("onclick","UI.charge_From_Submit()");
				$(".payAddFriend").attr("disabled",false);
				UI.charge_From_Submit();
			}else{
				//关闭
				$("#switch_charge").val("-1");
				$(".payAddFriend")[0].classList.add("layui-disabled");
				$(".chargeDiv_form_sava")[0].classList.add("layui-btn-disabled");
				$(".chargeDiv_form_sava").attr("onclick","");
				$(".payAddFriend").val("");
				$(".payAddFriend").attr("disabled",true);
				UI.charge_From_Submit();
			}
			layui.form.render();
			layer.msg(mpLanguage.getLanguageName('index_31'), {icon: 1})
		}, function(){
			data.elem.checked = !flag;
			form.render();
		});
	});

	//监听指定开关
	form.on('switch(switchReward)', function(data){
		var flag = data.elem.checked;
		var content  =  flag ? mpLanguage.getLanguageName('index_39') : mpLanguage.getLanguageName('index_40');
		layer.confirm( content , {
			btn: [mpLanguage.getLanguageName('select_8'),mpLanguage.getLanguageName('select_7')] //按钮
		}, function(){
			if (flag){
				$(".switch_flag").val("1");
				UI.setOfficialPacketIsOpen("1");
				$("#rewardDiv_form_sava").attr("onclick","UI.setOfficialPacketSettings()");
				$("#rewardDiv_form_sava").removeClass("layui-btn-disabled");
			}else{
				UI.setOfficialPacketIsOpen("0");
				$(".switch_flag").val("0");
				$("#rewardDiv_form_sava").attr("onclick","");
				$("#rewardDiv_form_sava").addClass("layui-btn-disabled");
			}

			layer.msg(mpLanguage.getLanguageName('index_31'), {icon: 1})
		}, function(){
				data.elem.checked = !flag;
				form.render();
		});
	});

	//初始化当前语言
	mpLanguage.loadProperties(mpLanguage.getLanguage());

	//去除
	$(".layui-nav-bar").hide();


	$("#FAQNoMatchReply").attr("placeholder",mpLanguage.getLanguageName("FAQNoMatchReply"))

})