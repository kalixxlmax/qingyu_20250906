var page=0;
var sum=0;
var lock=0;
var msgIds=new Array();
var commentIds = new Array();
var currentPageIndex;// 当前页码数
var currentCount;// 当前总数
var chooseDeleteCount = 0;
var chooseDeleteCount_common = 0;
layui.use(['form', 'layer', 'laydate', 'table', 'laytpl', 'util'], function () {
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table,
        util = layui.util;

    //直播间列表
    var tableIns = table.render({
      elem: '#msg_table'
      ,url:request("/console/getFriendsMsgList")
      ,id: 'msg_table'
      ,toolbar: '#toolbarDemo'
      ,page: true
      ,curr: 0
      ,limit:Common.limit
      ,limits:Common.limits
      ,groups: 7
      ,cols: [[ //表头
           {type:'checkbox',fixed:'left'}// 多选
          ,{field: 'msgId', title: '消息Id',width:'11%'}
          ,{field: 'userId', title: '发送人Id',width:'8%'}
          ,{field: 'nickname', title: '发送人昵称',width:'11%'}
          ,{field: 'body', title: '内容',width:'11%',templet:function (d) {
                    if (d.body.type == 1){
                        return "<button style='height: 26px;' class='layui-btn font_bule layui-btn-sm' onclick=\"Msg.showText('"+ d.body.text +"')\">预览</button>";
                    }else if (d.body.type == 5){
                        return "<button style='height: 26px;' class='layui-btn font_bule layui-btn-sm' onclick=\"Msg.showFile('"+ d.body.files[0].oUrl +"')\">预览</button>";
                    }else if (d.body.type == 2){
                        var arr = new Array();
                        if (d.body.images != undefined){
                            for(j = 0; j < d.body.images.length; j++) {
                                arr.push(d.body.images[j].tUrl);
                            }
                        }
                        var imgUrls = arr.join(",");
                        return "<button style='height: 26px;' class='layui-btn font_bule layui-btn-sm' onclick=\"Msg.showPicture('"+ imgUrls +"','"+ d.body.text +"' )\">预览</button>";
                    }else if (d.body.type == 3){
                        return "<button style='height: 26px;' class='layui-btn font_bule layui-btn-sm' onclick=\"Msg.showFile('"+ d.body.audios[0].oUrl +"')\">预览</button>";
                    }
                    var html = "";
                    try {
                        html = "<button style='height: 26px;' class='layui-btn font_bule layui-btn-sm' onclick=\"Msg.showVideo('"+ d.body.videos[0].oUrl +"','"+ d.body.text +"' )\">预览</button>";
                    }catch (e) {
                    }
                    return html;
                }}
          ,{field: 'location', title: '地址', width:'8%'}
          ,{field: 'model', title: '手机型号', width:'9%'}
          ,{field: 'visible', title: '可见类型', width:'8%',templet:function (d) {
          			var visiMsg;
					(d.visible == 1 ? visiMsg = "公开" : (d.visible == 2) ? visiMsg = "私密" : (d.visible == 3) ? visiMsg = "部分可见"
						:(d.visible == 4) ? visiMsg = "不给指定的人看" : (d.visible == 5 ) ? visiMsg = "@提示专属人看" : visiMsg = "暂无类型");
					return visiMsg;
                }}
          ,{field: 'time',title:'发送时间',width:'11%',templet: function(d){
          		return UI.getLocalTime(d.time);
          }}
          ,{fixed: 'right',title:"操作", align:'left',width:'24%', toolbar: '#msgListBar'}
        ]]
		,done:function(res, curr, count){
            checkRequst(res);
            //权限判断
            var arr=['msg-delete','msg-praiseMsg','msg-shutup','msg-shutup1'];
            manage.authButton(arr);

            //获取零时保留的值
            var last_value = $("#msg_limlt").val();
            //获取当前每页大小
            var recodeLimit =  tableIns.config.limit;
            //设置零时保留的值
            $("#msg_limlt").val(recodeLimit);
            //判断是否改变了每页大小数
            if (last_value != recodeLimit){
                // 刷新
                table.reload("msg_table",{
                    url:request("/console/getFriendsMsgList"),
                    page: {
                        curr: 1 //重新从第 1 页开始
                    },
                    where: {
                        type : Common.getValueForElement("#type")
                    }
                })
            }

          if(count==0&&lock==1){
                // layui.layer.alert("暂无数据",{yes:function(){
                //   renderTable();
                //   layui.layer.closeAll();
                // }});
                layer.msg("暂无数据",{"icon":2});
                renderTable();
              }
              lock=0;
			/*if(localStorage.getItem("role")==1 || localStorage.getItem("role")==4 || localStorage.getItem("role")==7){
				$(".delete").hide();
				$(".praiseMsg").hide();
				$(".commonMsg").hide();
				$(".shutup").hide();
				$(".cancelShutup").hide();
				$(".locking").hide();
				$(".unlock").hide();
				$(".deleteCommon").hide();
				$(".checkDelete").hide();

			}*/
            var pageIndex = tableIns.config.page.curr;//获取当前页码
            var resCount = res.count;// 获取table总条数
            currentCount = resCount;
            currentPageIndex = pageIndex;

            Msg.refreshMushDelete();
		}
    });

    //监听表格复选框选择
    table.on('checkbox(msg_table)', function (obj) {
        //当前页总数据量
        var thisCount = table.cache.msg_table.length;
        //每页数据量
        var pageCount = $(".layui-laypage-limits").find("option:selected").val();


        if (obj.type == 'one') {
            chooseDeleteCount = obj.checked == true ? chooseDeleteCount + 1 : chooseDeleteCount - 1;
        } else if (obj.type == 'all') {
            chooseDeleteCount = obj.checked == true ? pageCount > thisCount ? thisCount : pageCount : 0;
        }
        $("#friendMsgList .chooseDeleteCount").html("已选择 " + chooseDeleteCount + " 项")
        chooseDeleteCount < 1 ? $("#friendMsgList .mustDelete").hide() : $("#friendMsgList .mustDelete").show();
    });

    //监听表格复选框选择
    table.on('checkbox(commentMsg_table)', function (obj) {
        //当前页总数据量
        var thisCount = table.cache.commentMsg_table.length;
        //每页数据量
        var pageCount = $(".layui-laypage-limits").find("option:selected").val();


        if (obj.type == 'one') {
            chooseDeleteCount_common = obj.checked == true ? chooseDeleteCount_common + 1 : chooseDeleteCount_common - 1;
        } else if (obj.type == 'all') {
            chooseDeleteCount_common = obj.checked == true ? pageCount > thisCount ? thisCount : pageCount : 0;
        }
        $("#commonMsg .chooseDeleteCount").html("已选择 " + chooseDeleteCount_common + " 项")
        chooseDeleteCount_common < 1 ? $("#commonMsg .mustDelete").hide() : $("#commonMsg .mustDelete").show();
    });

    //朋友圈列表操作
    table.on('tool(msg_table)', function(obj){
         var layEvent = obj.event,
            data = obj.data;
            // console.log(data);
        if(layEvent === 'delete'){ //删除朋友圈
			// Msg.deleteFriendsMsg(data.userId,data.msgId,obj);
			Msg.checkDeleteFriendsMsg(data.msgId,1);
        }else if(layEvent === 'shutup') {// 锁定
			Msg.shutup(data.msgId,1);
        }else if(layEvent === 'cancelShutup'){// 取消锁定
        	Msg.shutup(data.msgId,0);
		}else if(layEvent === 'locking'){// 锁定该用户
            Msg.lockIng(data.userId,-1)
        }else if(layEvent === 'unlock'){// 解锁该用户
            Msg.lockIng(data.userId,1)
        }else if(layEvent === 'commonMsg'){ //评论详情
        	var tableInsCommon = table.render({
                  elem: '#commentMsg_table'
                  ,toolbar:'#toolbardeleteComment'
			      ,url:request("/console/commonListMsg")+"&msgId="+data.msgId
			      ,id: 'commentMsg_table'
			      ,page: true
			      ,curr: 0
                  ,limit:Common.limit
                  ,limits:Common.limits
			      ,groups: 7
			      ,cols: [[ //表头
                      {type:'checkbox',fixed:'left'}// 多选
			          ,{field: 'commentId', title: '评论Id', width:220}
			          ,{field: 'msgId', title: '所属消息Id', width:200}
                      ,{field: 'nickname', title: '评论用户昵称', width:200}
                      ,{field: 'body', title: '评论内容', width:200}
                      ,{field: 'toNickname', title: '被回复用户昵称', width:200}
                      ,{field: 'toBody', title: '回复内容', width:200}
			          ,{field: 'time', title: '评论时间', width:220,templet: function(d){
			          		return UI.getLocalTime(d.time);
			          }}
			          ,{fixed: 'right', width: 240,title:"操作", align:'left', toolbar: '#deleteCommonMsg'}
			        ]]
					,done:function(res, curr, count){
                        checkRequst(res);
                    	$("#friendMsgList").hide();
                        $(".serachMsg").hide();
                        $(".visitPathDiv").hide();
                    	$("#commonMsg").show();
						$("#save_roomId").val(Common.filterHtmlData(data.msgId));
                        var pageIndex = tableInsCommon.config.page.curr;//获取当前页码
                        var resCount = res.count;// 获取table总条数
                        currentCount = resCount;
                        currentPageIndex = pageIndex;
					}
			    });
        }else if(layEvent === 'praiseMsg'){// 点赞列表
        	var tableIns1 = table.render({
			      elem: '#praiseMsg_table'
                  , toolbar: '#toolbar'
			      ,url:request("/console/praiseListMsg")+"&msgId="+data.msgId
			      ,id: 'praiseMsg_table'
			      ,page: true
			      ,curr: 0
                  ,limit:Common.limit
                  ,limits:Common.limits
			      ,groups: 7
			      ,cols: [[ //表头
			           {field: 'praiseId', title: '点赞Id', width:220}
			          ,{field: 'msgId', title: '所属消息Id', width:200}
                        ,{field: 'userId', title: '点赞用户Id', width:200}
			          ,{field: 'nickname', title: '点赞用户昵称', width:220}
			          ,{field: 'time', title: '点赞时间', width:400,templet: function(d){
			          		return UI.getLocalTime(d.time);
			          }},
					]]
					,done:function(res, curr, count){
                        checkRequst(res);
						$("#friendMsgList").hide();
                        $(".visitPathDiv").hide();
                        $(".serachMsg").hide();
						$("#praiseMsg").show();
						$("#save_roomId").val(Common.filterHtmlData(data.msgId));
					}
			    });
        }else if (layEvent === 'checkMsg') {
            //审核
            layer.confirm('审核是否通过 ？', {
                title: '审核',
                skin: "layui-ext-motif",
                btn: ['通过', '驳回']
            }, function () {
                Msg.checkMsg(data.msgId, 0);
                layer.msg('操作成功', {icon: 1});
            }, function () {
                Msg.checkMsg(data.msgId, -1);
                layer.msg('操作成功', {icon: 1});
            });
        }
    });


    //评论列表操作
    table.on('tool(commentMsg_table)', function(obj){
         var layEvent = obj.event,
            data = obj.data;
         if(layEvent === 'deleteCommon'){ // 删除评论
         	Msg.checkDeleteCommentImpl(data.msgId,data.commentId,1);
         }
    })


    //头部搜索
    $(".search_live").on("click",function(){
            
            table.reload("msg_table",{
                url:request("/console/getFriendsMsgList"),
                where: {
                    nickname : Common.getValueForElement("#nickName"),  //搜索的关键字
					userId : Common.getValueForElement("#userId"),
                    type : Common.getValueForElement("#type")
                },
                page: {
                    curr: 1 //重新从第 1 页开始
                }
            })
            lock=1;
        $("#nickName").val("");
        $("#userId").val("");
        Msg.refreshMushDelete();
    });

});

//重新渲染表单
function renderTable(){
  layui.use('table', function(){
   var table = layui.table;//高版本建议把括号去掉，有的低版本，需要加()
   // table.reload("user_list");
   table.reload("msg_table",{
        page: {
            curr: 1 //重新从第 1 页开始
        },
        where: {
            nickname : Common.getValueForElement("#nickName"),  //搜索的关键字
            userId : Common.getValueForElement("#userId")
        }
    })
  });
 }

var Msg={
    //预览视频内容
    showVideo:function (url , text) {
        var html="";
        // 视频处理
        html += '<div class="layui-timeline-content layui-text">'
            +'<video class="layui-timeline-title" height="600px" width="400px" controls="controls" src=' + url + '></video>'
            +'</div>';

        html += '<div style="padding: 2%;"><label class="layui-text layui-word-aux" style="padding: 0 19px!important;">描述' + ": "+ text +'</label></div>';
        $("#msgContent").empty();
        $("#msgContent").append(html);

        //弹出层
        layer.open({
            type: 1,
            title: "视频预览",
            offset: ['50px'],
            area: ['700px','800px'],
            shadeClose: true,
            shade: false,
            maxmin: true, //开启最大化最小化按钮
            content: $("#showMsgInfo")
        });
    },

    //预览图片内容
    showPicture:function (pictures,text) {
        var html;
        if (pictures != ""){
            var arr = pictures.split(",");
            for (var i = 0; i < arr.length ; i++){
                if (i == 0){
                    html = '<img style="margin: 10px 0px 0px 10px;border: 1px solid red;" src=' + arr[i] + '>';
                }else{
                    html += '<img style="margin: 10px 0px 0px 10px;border: 1px solid red;" src=' + arr[i] + '>';
                }
            }

            html += '<div style="padding: 2%;"><label class="layui-text layui-word-aux" style="padding: 0 19px!important;">描述' + ": " + text + '</label></div>';
        }else{
            html = '<div style="padding: 2%;"><label class="layui-text layui-word-aux" style="padding: 0 19px!important;">描述' + ": "  + text + '</label></div>';
        }

        //弹出层
        layer.open({
            type: 1,
            title: "图片预览",
            offset: ['50px'],
            area: ['700px','700px'],
            shadeClose: true,
            shade: false,
            maxmin: true, //开启最大化最小化按钮
            content: html
        });
    },

    //预览文本内容
    showText:function (text) {
        var html = '<div class="wrap">';
        html += '<div style="padding: 2%;"><label class="layui-text layui-word-aux" style="padding: 0 19px!important;">'+ text +'</label></div>';
        html += '</div>';

        //弹出层
        layer.open({
            type: 1,
            title: "内容预览",
            offset: ['50px'],
            area: ['700px'],
            shadeClose: true,
            shade: false,
            maxmin: true, //开启最大化最小化按钮
            content: html
        });
    },

    //预览语言内容 .amr文件
    showFile:function (text) {
        var html;
        //获取后缀
        var index = text.lastIndexOf(".");
        var suffix = text.substr(index+1);

        if ('amr' == suffix){

            html = '<div style="margin-left: 20px;"> <a href="' + text +'" width="68px" height="68px" target="_blank" controls>下载录音</a></div>' +
                "<div style='padding:20px;margin:0 auto;margin-left: 31%';'>" +
                "<img id='playVoice' src='/pages/console/images/voice.png' onclick=\"Msg.playVoice('"+ text +"')\">" +
                "<img id='playVoiceGif' src='/pages/console/images/voice.gif' style='display: none;'>" +
                "<button class='layui-btn default_button_style stopVoiceBtn' style='margin-left: 31px;'>停止暂停</button>" +
                "</div>";

        }else if ('mp3' == suffix){

            html = '<audio controls style="margin-left: 29%;">' +
                '<source src="'+ text +'" type="audio/ogg">' +
                '</audio>';

        }else{

            html = '<div style="padding:20px;margin:0 auto;"> <a href="' + text +'" width="68px" height="68px" target="_blank" controls>点击查看文件点击查看文件</a></div>';

        }

        //弹出层
        layer.open({
            type: 1,
            title: "语音预览",
            offset: ['50px'],
            area: ['700px','150px'],
            shadeClose: true,
            shade: false,
            maxmin: true, //开启最大化最小化按钮
            content: html
        });
    },

    //播放amr语音
    playVoice:function(url){
        var amr = new BenzAMRRecorder();

        //初始化
        amr.initWithUrl(url).then(function() {
            console.log("语言时长（秒）：",amr.getDuration());
            $("#playVoice").hide();
            $("#playVoiceGif").show();
            amr.play();
        });

        amr.onEnded(function() {
            $("#playVoice").show();
            $("#playVoiceGif").hide();
        })

        //暂停播放
        $(".stopVoiceBtn").click(function () {
            amr.stop();
        })
    },
    // 朋友圈多选删除
    checkDeleteMsg: function () {
        var checkStatus = layui.table.checkStatus('msg_table'); //idTest 即为基础参数 id 对应的值

        for (var i = 0; i < checkStatus.data.length; i++) {
            msgIds.push(checkStatus.data[i].msgId);
        }
        console.log(msgIds);
        if (0 == checkStatus.data.length) {
            layer.msg("请勾选要删除的行");
            return;
        }
        Msg.checkDeleteFriendsMsg(msgIds.join(","), checkStatus.data.length);
    },

    // 多选删除朋友圈
    checkDeleteFriendsMsg:function(msgId,checkLength){
        layer.confirm('确定删除指定的朋友圈？',{icon:3, title:'提示信息',yes:function (index) {
                Common.invoke({
                    url:request('/console/deleteFriendsMsg'),
                    data:{
                        messageId:msgId
                    },
                    success:function(result){
                        if(result.resultCode==1){
                            layer.msg("删除成功",{"icon":1});
                        }
                        // 刷新
                        Common.tableReload(currentCount,currentPageIndex,checkLength,"msg_table");
                        msgIds =[];
                        // layui.table.reload("msg_table")
                    }
                })
            },btn2:function (index, layero) {
                msgIds =[];
            },cancel:function () {
                msgIds =[];
            }
            })

    },

    // 朋友圈评论多选删除
    checkDeleteComment:function(){

        var checkStatus = layui.table.checkStatus('commentMsg_table'); //idTest 即为基础参数 id 对应的值
        console.log("新版："+JSON.stringify(checkStatus.data)) //获取选中行的数据
        console.log("新版："+checkStatus.data.length) //获取选中行数量，可作为是否有选中行的条件
        console.log("新版："+checkStatus.isAll ) //表格是否全选

        var msgId;
        for (var i = 0; i < checkStatus.data.length; i++){
            commentIds.push(checkStatus.data[i].commentId);
            msgId = checkStatus.data[i].msgId;
        }
        console.log(commentIds);
        if(0 == checkStatus.data.length){
            layer.msg("请勾选要删除的行");
            return;
        }
        Msg.checkDeleteCommentImpl(msgId,commentIds.join(","),checkStatus.data.length);
    },

    checkDeleteCommentImpl : function(msgId,msgCommentIds,checkLength){
        console.log("参数 msgId："+msgId +"     "+msgCommentIds)
        layer.confirm('确定删除指定的朋友圈评论？',{icon:3, title:'提示信息', skin: "layui-ext-motif",yes:function (index) {
                Common.invoke({
                    url:request('/console/comment/delete'),
                    data:{
                        messageId:msgId,
                        commentId:msgCommentIds
                    },
                    success:function(result){
                        if(result.resultCode==1){
                            layer.msg("删除成功",{"icon":1});
                        }
                        // 刷新
                        Common.tableReload(currentCount,currentPageIndex,checkLength,"commentMsg_table");
                        // layui.table.reload("commentMsg_table")
                    }
                })
        },btn2:function (index, layero) {
                commentIds=[];
            },cancel:function () {
                commentIds=[];
            }
        })
    },

    // 删除评论
    deleteCommonMsg:function(msgId,commentId,obj){
        layer.confirm('确定删除该条评论？',{icon:3, skin: "layui-ext-motif", title:'提示信息'},function(index){
            Common.invoke({
                url:request('/console/comment/delete'),
                data:{
                    messageId:msgId,
                    commentId:commentId
				},
                success:function(result){
                    if(result.resultCode==1){
                        layer.msg("删除成功",{"icon":1});
                    }
                    obj.del();
                    layui.table.reload("commentMsg_table")
                }
            })
        })

    },

	// 朋友圈锁定取消
	shutup:function(msgId,state){
		var confMsg,successMsg="";
		(state == 0 ? confMsg = '确定解禁该朋友圈？':confMsg = '确定禁用该朋友圈？');
		(state == 0 ? successMsg = "解禁成功":successMsg ="禁用成功");
		layer.confirm(confMsg,{icon:3, skin: "layui-ext-motif", title:'提示信息'},function(index){

            Common.invoke({
                url : request('/console/lockingMsg'),
                data : {
					msgId:msgId,
                    state:state
                },
                successMsg : successMsg,
                errorMsg :  "加载数据失败，请稍后重试",
                success : function(result) {
                    layui.table.reload("msg_table",{
                    })
                },
                error : function(result) {
                }
            });

		})
	},
    // 朋友圈审核取消
    checkMsg:function(msgId,state){
        Common.invoke({
            url : request('/console/lockingMsg'),
            data : {
                msgId:msgId,
                state:state
            },
            success : function(result) {
                layui.table.reload("msg_table",{
                })
                console.log(result);
            },
            error : function(result) {
                console.log(result);
            }
        });
    },
	// 朋友圈锁定改用户
    lockIng:function(userId,status){
        var confMsg,successMsg="";
        (status == -1 ? confMsg = '确定对该用户做封号处理？':confMsg = '确定解封该用户账号？');
        (status == -1 ? successMsg = "封号成功":successMsg ="解封成功");
        layer.confirm(confMsg,{icon:3, skin: "layui-ext-motif", title:'提示信息'},function(index){

            Common.invoke({
                url : request('/console/changeStatus'),
                data : {
                    userId:userId,
                    status:status
                },
                successMsg : successMsg,
                errorMsg :  "加载数据失败，请稍后重试",
                success : function(result) {
                    layui.table.reload("msg_table",{})
                },
                error : function(result) {
                }
            });
        })
    },
    btn_back: function () {
        $("#friendMsg_div").show();
        $("#friendMsgList").show();
        $("#commonMsg").hide();
        $("#praiseMsg").hide();
        $(".visitPathDiv").show();
        $(".serachMsg").show();
        Msg.refreshMushDelete_common();
    },
    /**
     * 刷新多选删除
     */
    refreshMushDelete: function () {
        chooseDeleteCount = 0;
        $(".mustDelete").hide();
        userIds = [];
    },
    refreshMushDelete_common: function () {
        chooseDeleteCount_common = 0;
        $("#commonMsg .mustDelete").hide();
        commentIds = [];
    }

}
$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})