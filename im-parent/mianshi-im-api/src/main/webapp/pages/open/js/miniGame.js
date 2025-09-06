
$(function () {
    $("#uploadSmallFileFrom").attr("action", Config.uploadUrl);
    $("#uploadFilePath_update").attr("action", Config.uploadUrl);
    MiniGameOperation.miniGameList();
})

var MiniGameOperation = {
    //小游戏列表
    miniGameList:function () {
        var html='<thead style="width: 100%;height: 40px;background-color: #ebeef0;line-height: 40px;"><tr><th>应用名称</th><th>状态</th><th>操作</th></tr></thead><tbody>';
        myFn.invoke({
            type:'POST',
            url:'/open/appList',
            data:{
                userId:localStorage.getItem("userId"),
                type:3
            },
            success:function(result){
                console.log(result.data);
                for(var i=0;i<result.data.length;i++){
                    html+="<tr><td>"+result.data[i].appName+"</td><td>"+(result.data[i].status==-1?"已禁用":result.data[i].status==0?"审核中":result.data[i].status==1?"正常":"审核失败")+"</td><td><a href='#' onclick='MiniGameOperation.lookInfo(\""+result.data[i].id+"\")' style='color:#3292ff'>查看</a></td></tr>";
                }
                html+="</tbody>"
                $("#minigame_list_tab").empty();
                $("#minigame_list_tab").append(html);
            }
        })
    }
    //创建小游戏
    ,showMiniGameFrom:function () {
        $(".createMiniGame").show();
        $("#minigame_list_tab").hide();
        $("#minigameList").hide();
        $("#createMiniGame_div").show();
    },
    // 上传
    upload:function(){
        var uploadUrl = localStorage.getItem("miniGameImgUrl");
        layui.element.progress(uploadUrl, '0%');
        $("#uploadSmallFileFrom").ajaxSubmit({
            uploadProgress: function (event, position, total, percentComplete) {
                let percentVal = percentComplete + '%';
                layui.element.progress(uploadUrl, percentVal);
            },
            success:function(data){
                var obj = eval("("+data+")");
                $("#"+ uploadUrl).attr("src",obj.url);
            }
         })
    },
    selectSmallFile:function(data){
        $("#photoSmallUpload").click();
        localStorage.setItem("miniGameImgUrl",data);
    },
    updateUpload:function(){
        $("#uploadFilePath_update").ajaxSubmit(function(data){
            var obj = eval("(" + data + ")");
            console.log(obj);
            var uploadUrl = localStorage.getItem("miniGameSourceImgUrl");
            $("#"+ uploadUrl).html(obj.url);
        });
    },
    selectUploadFilePath:function(data){
        $("#uploadFilePathUrl_update").click();
        localStorage.setItem("miniGameSourceImgUrl",data);
    },
    miniGameSubmitFrom:function () {
        var appName = $(".appName").val();
        var appIntroduction = $(".appIntroduction").val();
        var appUrl = $(".appUrl").val();
        var logsUrl = $("#uploadSmall_url_loge").attr('src');
        if (MiniGameOperation.isNil(appName)){
            layer.msg("请输入游戏应用名称",{icon: 2,time: 2000});
            return;
        }
        if (MiniGameOperation.isNil(appIntroduction)){
            layer.msg("请输入应用简介",{icon: 2,time: 2000});
            return;
        }
        if (MiniGameOperation.isNil(appUrl)){
            layer.msg("请输入游戏应用地址",{icon: 2,time: 2000});
            return;
        }
        if (MiniGameOperation.isNil(logsUrl)){
            layer.msg("请上传小游戏图标",{icon: 2,time: 2000});
            return;
        }

        layer.open({
            type: 1,
            title: "校验信息",
            offset: ['50px'],
            btn: ['确定', '取消'],
            area: ['600px','350px'],
            shadeClose: true,
            shade: [0.5, '#393D49'],
            content: $("#showMsgInfo"),
            yes: function(index, layero){
                if (MiniGameOperation.isNil($("#sub_telephone").val()) || MiniGameOperation.isNil($("#sub_password").val())){
                    layer.msg("请输入账号或密码！",{icon: 2,time: 2000});
                    return;
                }
                layui.layer.close(index);
                MiniGameOperation.createApp(appName,appIntroduction,appUrl,logsUrl);
            }
        });
    }
    //判空
    ,isNil : function(s) {
        return undefined == s || null == s || $.trim(s) == "" || $.trim(s) == "null";
    }
    //限制字符长度
    , widthCheck:function (str, maxLen) {
        var w = 0;
        var tempCount = 0;
        //length 获取字数数，不区分汉子和英文
        for (var i=0; i<str.value.length; i++) {
            //charCodeAt()获取字符串中某一个字符的编码
            var c = str.value.charCodeAt(i);
            //单字节加1
            if ((c >= 0x0001 && c <= 0x007e) || (0xff60<=c && c<=0xff9f)) {
                w++;
            } else {
                w+=2;
            }
            if (w > maxLen) {
                str.value = str.value.substr(0,i);
                break;
            }
        }
    }
    ,createApp:function (appName,appIntroduction,appUrl,logsUrl) {
        myFn.invoke({
            url:'/open/createApp',
            data:{
                appName : appName,
                appIntroduction : appIntroduction,
                appUrl : appUrl,
                appImg : logsUrl,
                accountId : localStorage.getItem("userId"),
                telephone : "86"+$("#sub_telephone").val(),
                password : $.md5($("#sub_password").val()),
                appType : 3,
            },
            success:function(result){
                if(result.resultCode==1){
                    layui.layer.alert("创建成功");
                    MiniGameOperation.createSuccess();
                    App.appList();
                }else{
                    layui.layer.alert(result.resultMsg);
                }
            }
        })
    }
    ,createSuccess:function () {
        $("#showMsgInfo").hide();
        $(".appName").val('');
        $(".appIntroduction").val('');
        $(".appUrl").val('');
        $("#uploadSmall_url_loge").attr('src','');
    }
    //详情
    ,lookInfo:function (id) {
        $("#WebAppItem").show();
        $("#minigameList").hide();
        var html="";
        myFn.invoke({
            url:'/open/appInfo',
            data:{
                id:id
            },
            success:function(result){
                $("#app_url").attr("src",result.data.appImg);
                $("#app_name").empty();
                $("#app_name").append(result.data.appName);
                $("#app_id").empty();
                $("#app_id").append(result.data.appId);
                $("#AppSecret").empty();
                $("#AppSecret").append("<a onclick='Web.lookAppSecret()' style='color: #3292ff;cursor:pointer;'>查看</a>");
                html+="<button class='layui-btn layui-btn-danger' onclick='Web.deleteWebApp(\""+result.data.id+"\")'>删除应用</button>";
                $("#app_delete").empty();
                $("#app_delete").append(html);
            }
        })
        $("#applist").hide();
        $("#createApp_div").hide();
        $("#WebAppItem").show();
    }
}