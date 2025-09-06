/**
 * 系统配置页面相关的js
 */
var obj;
function num(){
	obj = $(".giftRatio").val();
	var reg = new RegExp("^[0-9]+(.[0-9]{0,2})?$", "g");
	(!reg.test(obj)?layer.alert("直播礼物分成比率请保持小数点后两位有效数字"):"");
	return;
}

$(function () {
    $("#checkFarme").hide();
    $("#showSystemApiConfig").hide();
    //权限判断
    var arr=['sys-sava'];
    manage.authButton(arr);
})

//填充数据方法
function fillParameter(data){
    //判断字段数据是否存在
    function nullData(data){
        return Common.isNil(data) == true ? "" : data ;
    }
    //数据回显
    $(".XMPPTimeout").val(nullData(data.XMPPTimeout));
    // 直播分成比率
    $(".giftRatio").val(nullData(data.giftRatio));
    $(".promotionUrl").val(nullData(data.promotionUrl));
    $(".defaultTelephones").val(nullData(data.defaultTelephones));
    $(".defaultGroups").val(nullData(data.defaultGroups));

    $(".privacyPolicyPrefix").val(nullData(data.privacyPolicyPrefix));
    //我的同事模块
    $(".createCompamyIsNeedCheck").val(nullData(data.createCompamyIsNeedCheck));
    $(".userJoinCompanyIsNeedManagerConfirm").val(nullData(data.userJoinCompanyIsNeedManagerConfirm));
    $(".inviteJoinCompanyIsNeedUserConfirm").val(nullData(data.inviteJoinCompanyIsNeedUserConfirm));

    $(".telephoneSearchUser").val(data.telephoneSearchUser);
    $(".nicknameSearchUser").val(data.nicknameSearchUser);
    $(".regeditPhoneOrName").val(data.regeditPhoneOrName); //使用手机号或者用户名注册
    $(".userNicknameNotword").val(data.userNicknameNotword); //用户昵称否词
    $(".registerInviteCode").val(data.registerInviteCode);  //注册邀请码
    $(".authApi").val(data.isAuthApi);
    $(".isOpenCluster").val(data.isOpenCluster);
    $(".isOpenGoogleFCM").val(data.isOpenGoogleFCM);// 是否开启Google推送
    $(".isKeyWord").val(data.isKeyWord);// 关键词
    $(".isSaveMsg").val(data.isSaveMsg);// 保存单聊消息
    $(".isSaveMucMsg").val(data.isSaveMucMsg);// 保存群聊消息
    //保存系统消息
    $(".isSaveSystemMsg").val(data.isSaveSystemMsg);
    $(".isOpenSMSCode").val(data.isOpenSMSCode);//是否开启短信验证码
    $(".isOpenOnlineStatus").val(data.isOpenOnlineStatus);//是否开启客户端在线状态
    $(".SMSType").val(data.sMSType);// 短信服务支持
    $(".imgVerificationCode").val(data.imgVerificationCode);// 短信服务支持
    $(".isAutoAddressBook").val(data.isAutoAddressBook);// 通讯录自动添加好友
    $(".isSaveRequestLogs").val(data.isSaveRequestLogs);// 是否保存接口请求日志
    $("#maxCrowdNumber").val(data.maxCrowdNumber);//最大群人数
    $(".msgDelayedDeleteTime").val(data.msgDelayedDeleteTime/3600);// 消息延时删除时间
    $(".banComment").val(data.banComment);// 视界评论评论
    $(".publishMsgWhetherCheck").val(data.publishMsgWhetherCheck);//发布朋友圈是否审核
    $(".publishMsgLableWhetherCheck").val(data.publishMsgLableWhetherCheck);//发布短视频是否审核
    $(".locationModiyTime").val(data.locationModiyTime);//更新用户地理位置频率
    $(".resourceDatabaseUrl").val(data.resourceDatabaseUrl);
    $(".openAdminLoginCode").val(getAdminPhone());//后台管理短信验证码登录
    $(".employeeTemplateUrl").val(data.employeeTemplateUrl);
    $(".requestApiList").val(data.systemApiConfig.requestApiList.join(","));
    localStorage.setItem("configPublicKey",data.systemApiConfig.publicKey);
    localStorage.setItem("configApiSecret",data.systemApiConfig.apiSecret);
    $(".departmentTemplateUrl").val(data.departmentTemplateUrl);
    $(".googleVerification").val(data.googleVerification);
    $(".videoVoice").val(data.videoVoice);
    $(".registerInviteCodeSetting").val(data.registerInviteCodeSetting);
    
    // 禁言时间配置
    $(".muteTime").val(nullData(data.muteTime));
    $(".muteTimeUnit").val(data.muteTimeUnit || 3);

    //重新渲染
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

        //直播分成比例
       var giftRatio = $(".giftRatio").val();
        if($(".giftRatio").val() == ""){
        	layer.alert("直播礼物分成比率不能为空");
        	return false;
        }else if(obj > 1){
        	layer.alert("直播礼物分成比率要小于等于1");
        	return false;
        }else if(obj == 0){
        	layer.alert("直播礼物分成比率不能为0");
        	return false;
        }

        /** 建立群组默认参数设置 **/
        var maxUserSize = $(".maxUserSize").val();
        var adminMaxNumber = $(".adminMaxNumber").val();
        if(parseInt(adminMaxNumber) > parseInt(maxUserSize)){
            layer.alert("管理员人数不能超过群组最大人数");
            return;
        }

        var openAdminLoginCode;
        var areaCode = $("#account").intlTelInput("getSelectedCountryData").dialCode;
        var countryCode  = $("#account").intlTelInput("getSelectedCountryData").iso2;
        if (Common.isNil($(".openAdminLoginCode").val())){
            openAdminLoginCode = 0;
            setAdminPhone(countryCode,areaCode,$(".openAdminLoginCode").val());
        }else{
            openAdminLoginCode = 1;
            setAdminPhone(countryCode,areaCode,areaCode+$(".openAdminLoginCode").val());
        }

        //弹出loading
        //var index = top.layer.msg('数据提交中，请稍候',{icon: 16,time:false,shade:0.8});
        Common.invoke({
            url : request('/console/config/running/set'),
            data : {
                id : 10000,
                distance : 0,
                XMPPTimeout :  $(".XMPPTimeout").val(),
                giftRatio : giftRatio,
                promotionUrl : $(".promotionUrl").val(),
                defaultTelephones : $(".defaultTelephones").val(),
                defaultGroups : $(".defaultGroups").val(),
                privacyPolicyPrefix :   $(".privacyPolicyPrefix").val(),
                createCompamyIsNeedCheck : $(".createCompamyIsNeedCheck").val(),
                userJoinCompanyIsNeedManagerConfirm : $(".userJoinCompanyIsNeedManagerConfirm").val(),
                inviteJoinCompanyIsNeedUserConfirm : $(".inviteJoinCompanyIsNeedUserConfirm").val(),
                telephoneSearchUser : $(".telephoneSearchUser").val(),
                nicknameSearchUser : $(".nicknameSearchUser").val(),
                regeditPhoneOrName : $(".regeditPhoneOrName").val(),
                userNicknameNotword : $(".userNicknameNotword").val(),
                registerInviteCode : $(".registerInviteCode").val(),
                isAuthApi : $(".authApi").val(),
                isOpenCluster : $(".isOpenCluster").val(),
                isOpenGoogleFCM : $(".isOpenGoogleFCM").val(),
                isKeyWord : $(".isKeyWord").val(),
                isSaveMsg : $(".isSaveMsg").val(),
                isSaveMucMsg : $(".isSaveMucMsg").val(),
                isSaveSystemMsg : $(".isSaveSystemMsg").val(),
                isOpenSMSCode : $(".isOpenSMSCode").val(),
                isOpenOnlineStatus : $(".isOpenOnlineStatus").val(),
                SMSType : $(".SMSType").val(),
                imgVerificationCode : $(".imgVerificationCode").val(),
                isAutoAddressBook : $(".isAutoAddressBook").val(),
                isSaveRequestLogs : $(".isSaveRequestLogs").val(),
                maxCrowdNumber : $(".maxCrowdNumber").val(),
                msgDelayedDeleteTime : $(".msgDelayedDeleteTime").val() * 3600,
                banComment : $(".banComment").val(),
                resourceDatabaseUrl : $(".resourceDatabaseUrl").val(),
                publishMsgWhetherCheck : $(".publishMsgWhetherCheck").val(),
                publishMsgLableWhetherCheck : $(".publishMsgLableWhetherCheck").val(),
                locationModiyTime : $(".locationModiyTime").val(),
                openAdminLoginCode:openAdminLoginCode,
                employeeTemplateUrl:$(".employeeTemplateUrl").val(),
                departmentTemplateUrl:$(".departmentTemplateUrl").val(),
                requestApiList: $(".requestApiList").val(),
                googleVerification:$(".googleVerification").val(),
                videoVoice:$(".videoVoice").val(),
                registerInviteCodeSetting:$(".registerInviteCodeSetting").val(),
                muteTime: $(".muteTime").val() || 0,
                muteTimeUnit: $(".muteTimeUnit").val() || 3
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

    //注册区号
    $("#account").intlTelInput({
        autoHideDialCode: false,
        autoPlaceholder: "off",
        dropdownContainer: "body",
        geoIpLookup: null,
        //初始国家
        initialCountry: getAdminCountryCode(),
        nationalMode: false,
        preferredCountries: ['cn', 'my'],
        separateDialCode: true,
        utilsScript: "",
    });
})

$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})

function setAdminPhone(countryCode,areaCode,phone) {
    Common.invoke({
        url : request('/console/set/admin/phone'),
        data : {
            "phone":phone,
            "areaCode":areaCode,
            "countryCode":countryCode
        },
        successMsg : "设置成功",
        errorMsg :  "设置失败，请稍后重试",
        success : function(result) {
            //关闭弹框
            layui.layer.close(index);
        },
        error : function(result) {
        }
    });
}

//获取管理员国家代号
function getAdminCountryCode() {
    var countryCode = "cn";
    $.ajax({
        url:request('/console/getUpdateUser'),
        data:{
            userId:1000
        },
        dataType:"json",
        async: false,
        success:function(result){
            console.log(result);
            if(result.data!=null && !Common.isNil(result.data.countryCode)){
                countryCode = result.data.countryCode;
            }
        },
        error:function (error) {
            countryCode = "cn";
        }
    });
    return countryCode;
}


//获取管理员手机号
function getAdminPhone() {
    var phone = "";
    $.ajax({
        url:request('/console/getUpdateUser'),
        data:{
            userId:1000
        },
        dataType:"json",
        async: false,
        success:function(result){
            console.log(result);
            if(result.data!=null && !Common.isNil(result.data.phone)){
                var areaCodeLength = result.data.areaCode.length;
                phone = result.data.phone.slice(areaCodeLength,result.data.phone.length);
            }
        },
        error:function (error) {
            phone = "";
        }
    });
    return phone;
}

layui.use('upload', function () {
    var upload = layui.upload;

    var uploadMusicUrl = Config.uploadUrl;
    if (uploadMusicUrl == null || uploadMusicUrl == "") {
        uploadMusicUrl = Config.getConfig().uploadUrl + "/upload/UploadifyServlet";
    }
    //员工模板上传
    upload.render({
        elem: '#employeeTemplateUpload'
        , url: uploadMusicUrl
        , accept: 'file'
        , before: function (obj) {
            layer.load(); //上传loading
        }, done: function (res) {
            layer.closeAll('loading'); //关闭loading
            console.log(res);
            $(".employeeTemplateUrl").val(res.url);
        }
        , error: function () {
            layer.closeAll('loading');
            layer.msg('上传失败！', {icon: 2});
        }
    });

    //部门模板上传
    upload.render({
        elem: '#departmentTemplateUpload'
        , url: uploadMusicUrl
        , accept: 'file'
        , before: function (obj) {
            layer.load(); //上传loading
        }, done: function (res) {
            layer.closeAll('loading'); //关闭loading
            console.log(res);
            $(".departmentTemplateUrl").val(res.url);
        }
        , error: function () {
            layer.closeAll('loading');
            layer.msg('上传失败！', {icon: 2});
        }
    });
});

$(".plaintext").click(function (){
    layui.layer.open({
        title: "输入密码",
        skin: 'layui-ext-motif',
        type: 1,
        shade: 0.3,
        btn: ["确定", "取消"],
        area: ['500px', '200px'],
        offset: 'auto',
        shadeClose: true, //点击遮罩关闭
        content: $("#checkFarme"),
        yes: function (index, layero) {
            layer.close(index);
            checkPassword();
        }
    });
})

/**
 * 查看第三方系统集成对接参数
 */
function showSystemApiConfig(){
    layui.layer.open({
        title: "查看",
        skin: 'layui-ext-motif',
        type: 1,
        shade: 0.3,
        btn: ["确定", "取消"],
        area: ['700px', '250px'],
        offset: 'auto',
        shadeClose: true, //点击遮罩关闭
        content: $("#showSystemApiConfig"),
    });
}

/**
 * 验证账号密码是否正确
 */
function checkPassword(){
    $.ajax({
        url:request('/console/verify/password'),
        data:{
            password: $.md5($("#adminPassword").val()),
            account: localStorage.getItem("account"),
            areaCode: localStorage.getItem("areaCode"),
        },
        dataType:"json",
        async: false,
        success:function(res){
            if (res.resultCode == 2){
                layer.msg(res.data, {icon: 2});
            }else{
                $("#apiSecret").val(localStorage.getItem('configApiSecret'));
                $("#publicKey").val(localStorage.getItem('configPublicKey'));
                layui.form.render();
                showSystemApiConfig();
            }
        }
    });
}




