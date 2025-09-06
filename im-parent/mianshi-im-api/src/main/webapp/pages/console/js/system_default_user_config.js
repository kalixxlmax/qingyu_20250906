//填充数据方法
function fillParameter(data){
    //数据回显
    $(".roamingTime").val(data.roamingTime);// 默认漫游时长
    $(".outTimeDestroy").val(data.outTimeDestroy);// 默认过期销毁时长
    $(".isFriendsVerify").val(data.isFriendsVerify);// 是否需要好友验证
    $(".isMultiLogin").val(data.isMultiLogin);// 是否支持多点登录
    $(".isVibration").val(data.isVibration);// 是否振动
    $(".isTyping").val(data.isTyping); // 让对方知道我正在输入
    $(".isUseGoogleMap").val(data.isUseGoogleMap);// 使用Google地图
    $(".phoneSearch").val(data.phoneSearch);// 允许通过手机号搜索我
    $(".nameSearch").val(data.nameSearch);// 允许通过昵称搜索我
    $(".isKeepalive").val(data.isKeepalive);// 允许安卓APP进程保活
    $(".isOpenPrivacyPosition").val(data.isOpenPrivacyPosition);// 针对个人是否开启位置相关服务
    $(".isShowMsgState").val(data.isShowMsgState);// 针对个人是否开启位置相关服务
    $(".showLastLoginTime").val(data.showLastLoginTime);// 允许别人看见我的上次上线时间
    $(".showTelephone").val(data.showTelephone);// 允许别人看见我的手机号
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
        Common.invoke({
            url : request('/console/config/default/user/set'),
            data : {
                id : 10000,
                distance : 0,
                roamingTime : $(".roamingTime").val(),
                outTimeDestroy : $(".outTimeDestroy").val(),
                isFriendsVerify : $(".isFriendsVerify").val(),
                isMultiLogin : $(".isMultiLogin").val(),
                isVibration : $(".isVibration").val(),
                isTyping : $(".isTyping").val(),
                isUseGoogleMap : $(".isUseGoogleMap").val(),
                phoneSearch : $(".phoneSearch").val(),
                nameSearch : $(".nameSearch").val(),
                isKeepalive : $(".isKeepalive").val(),
                isOpenPrivacyPosition : $(".isOpenPrivacyPosition").val(),
                isShowMsgState : $(".isShowMsgState").val(),
                showLastLoginTime : $(".showLastLoginTime").val(),
                showTelephone : $(".showTelephone").val(),
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



