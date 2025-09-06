//填充数据方法
function fillParameter(data){
    //数据回显
    $(".maxUserSize").val(data.maxUserSize);
    $(".adminMaxNumber").val(data.adminMaxNumber);
    $(".isLook").val(data.isLook);
    $(".showRead").val(data.showRead);
    $(".isNeedVerify").val(data.isNeedVerify);
    $(".isAttritionNotice").val(data.isAttritionNotice);// 群组减员发送通知
    $(".showMember").val(data.showMember);
    $(".allowSendCard").val(data.allowSendCard);
    $(".allowInviteFriend").val(data.allowInviteFriend);
    $(".allowUploadFile").val(data.allowUploadFile);
    $(".allowConference").val(data.allowConference);
    $(".allowSpeakCourse").val(data.allowSpeakCourse);
    $(".showMarker").val(data.showMarker);//是否显示水印开关
    $(".roomNotice").val(data.roomNotice);
    layui.form.render();
}

layui.use(['form','jquery',"layer"],function() {
    var form = layui.form,
        $ = layui.jquery,
        layer = parent.layer === undefined ? layui.layer : top.layer;


    //获取当前系统配置
    if(window.sessionStorage.getItem("systemConfig")){
        var systemConfig = JSON.parse(window.sessionStorage.getItem("systemConfig"));
        fillParameter(systemConfig);
    }else{
        Common.invoke({
            url :request('/console/config'),
            data : {},
            successMsg : false,
            errorMsg : "获取数据失败,请检查网络",
            success : function(result) {
                fillParameter(result.data);
            },
            error : function(result) {
            }

        });
    }
    //非管理员登录屏蔽操作按钮
    if(localStorage.getItem("IS_ADMIN")==0){
      $(".save").remove();
    }

    //提交保存配置
    form.on("submit(systemConfig)",function(data){
        /** 建立群组默认参数设置 **/
        var maxUserSize = $(".maxUserSize").val();
        var adminMaxNumber = $(".adminMaxNumber").val();
        if(parseInt(adminMaxNumber) > parseInt(maxUserSize)){
            layer.alert("管理员人数不能超过群组最大人数");
            return;
        }

        //弹出loading
        //var index = top.layer.msg('数据提交中，请稍候',{icon: 16,time:false,shade:0.8});
        Common.invoke({
            url : request('/console/config/default/room/set'),
            data : {
                id : 10000,
                distance : 0,
                maxUserSize : $(".maxUserSize").val(),
                adminMaxNumber : $(".adminMaxNumber").val(),
                isLook : $(".isLook").val(),
                showRead : $(".showRead").val(),
                isNeedVerify : $(".isNeedVerify").val(),
                isAttritionNotice : $(".isAttritionNotice").val(),
                showMember : $(".showMember").val(),
                allowSendCard : $(".allowSendCard").val(),
                allowInviteFriend : $(".allowInviteFriend").val(),
                allowUploadFile : $(".allowUploadFile").val(),
                allowConference : $(".allowConference").val(),
                allowSpeakCourse : $(".allowSpeakCourse").val(),
                roomNotice : $(".roomNotice").val(),
                showMarker : $(".showMarker").val()
            },
            successMsg : "系统配置修改成功",
            errorMsg : "修改系统配置失败,请检查网络",
            success : function(result) {
                layer.msg('系统配置修改成功', {icon: 1});
                localStorage.setItem("registerInviteCode",systemConfig.registerInviteCode); //更新系统邀请码模式
            },
            error : function(result) {

            }

        });

        return false;
    });
})
$(function () {
    //权限判断
    var arr=['sys-sava'];
    manage.authButton(arr);
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})



