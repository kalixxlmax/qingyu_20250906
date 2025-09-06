layui.use(['form'], function() {
    var form = layui.form;
    
    //提交
    form.on('submit(sysUser_login)', function(obj) {
        obj.field.password = $.md5(obj.field.password);
        obj.field.areaCode = $("#account").intlTelInput("getSelectedCountryData").dialCode;
        console.log("登录密码："+obj.field.password);
        console.log("___"+obj.field.areaCode);
        /*layer.load(1);*/
        if (operation.openAdminLoginSms(obj.field)){
            var resultCode = operation.getAdminPhone(obj.field.areaCode,obj.field.account);
            if (resultCode == -1){
                layer.msg("该用户不存在,请填写正确的区号或账号",{icon: 2});

            }else{
                layui.layer.open({
                    title:'填写验证码',
                    type: 1,
                    btn:["确定","取消"],
                    area: ['440px','220px'],
                    btnAlign: 'c',
                    content: '<div id="mdifyPassword" class="layui-form" style="margin:22px 40px 10px 64px;">'
                        + ' <div class="layui-form-mid layui-word-aux sendSmsHint">短信验证码发送至手机 : '+ operation.getAdminPhone(obj.field.areaCode,obj.field.account) +'</div>'
                        +'<div class="layui-form-item">'
                        +'<label class="layui-form-label">验证码</label>'
                        +'<div class="layui-input-inline" style="width: 178px;">'
                        +'<input type="text" id="randcode" placeholder="请输入验证码" class="layui-input">'
                        +'</div>'
                        +'<button class="layui-btn layui-btn-primary chat-code code" id="sendCode" onclick="operation.sendCode(\'' + obj.field + '\',\'' + obj.field.areaCode + '\',\'' + obj.field.account + '\')">获取验证码</button>'
                        +'</div>'
                        +'</div>'
                    ,yes: function(index, layero){
                        var flag = operation.checkSmsCode(obj.field.areaCode,obj.field.account,$("#randcode").val());
                        if (flag){
                            operation.userLogin(obj.field);
                        }
                    },btn2: function(index, layero){
                        operation.clearCountdown();
                    },
                    cancel: function(){
                        operation.clearCountdown();
                    }
                });
            }
        }
        return false; //阻止表单跳转。如果需要表单跳转，去掉这段即可。
    });
    
    //表单输入效果
    $(".loginBody .input-item").click(function(e){
        e.stopPropagation();
        $(this).addClass("layui-input-focus").find(".layui-input").focus();
    })
    $(".loginBody .layui-form-item .layui-input").focus(function(){
        $(this).parent().addClass("layui-input-focus");
    })
    $(".loginBody .layui-form-item .layui-input").blur(function(){
        $(this).parent().removeClass("layui-input-focus");
        if($(this).val() != ''){
            $(this).parent().addClass("layui-input-active");
        }else{
            $(this).parent().removeClass("layui-input-active");
        }
    })

    //注册区号
    $("#account").intlTelInput({
        autoHideDialCode: false,
        autoPlaceholder: "off",
        dropdownContainer: "body",
        geoIpLookup: null,
        //初始国家
        initialCountry: "cn",
        nationalMode: false,
        preferredCountries: ['cn', 'my'],
        separateDialCode: true,
        utilsScript: "",
    });
});

var timer = null;
var count = 60;

var operation= {
    /*倒计时60秒*/
    countdown: function () {
        var codeText = $('.code').text();
        if (codeText == '获取验证码') {
            $('#sendCode').attr('disabled',"true");
            timer = setInterval(function () {
                count--;
                $('.code').text(count + '后获取验证码');
                if (count <= 0) {
                 operation.clearCountdown();
                }
            }, 1000);
        }
    },
    clearCountdown:function(){
        clearInterval(timer);
        $('#sendCode').removeAttr("disabled");
        $('.code').text('获取验证码');
    },
    /*发送短信*/
    sendCode:function (field,areaCode,account) {
        $.ajax({
            type:"POST",
            url:"/console/randcode/sendSms",
            dataType:"json",
            data:{
                areaCode:areaCode,
                account:account
            },
            async: false,
            success:function(result){
                if (result.resultCode != 1){
                    layer.msg(result.resultMsg,{icon: 2});
                } else if (result.data.sendStatus == -1){
                    layer.msg("短信发送失败,账号不存在 或者 用户手机号格式错误",{icon: 2});
                }else if (result.data.sendStatus == 200){
                    layer.msg("短信验证码发送成功",{icon: 1});
                    operation.countdown();
                }else if (result.data.sendStatus == 0){
                    operation.userLogin(field);
                }else {
                    layer.msg("短信发送失败",{icon: 2});
                }
            },
            error:function(error){
                layer.msg("短信发送失败:"+error,{icon: 2});
            }
        });
    },
    googleVerification:function (field){
        var flag = false;
        $.ajax({
            type:"POST",
            url:"/console/find/googleVerification",
            dataType:"json",
            async: false,
            success:function(result){
                console.log(result);
                if (result.data == 1){
                    operation.userLogin(field,true);
                }else{
                    operation.userLogin(field,false);
                }
            },
            error:function(error){
                flag = false;
            }
        });
        return flag;
    },
    /*获取配置*/
    openAdminLoginSms:function (field) {
        var flag = false;
        $.ajax({
            type:"POST",
            url:"/console/find/openAdminLoginCode",
            dataType:"json",
            async: false,
            success:function(result){
                console.log(result);
                if (result.data == 1){
                    flag = true;
                }else{
                    operation.userLogin(field);
                }
            },
            error:function(error){
                flag = false;
            }
        });
        return flag;
    },

    /*用户登录*/
    userLogin:function (field,google) {
        $.post("/console/login",field, function(data) {
            console.log(data);
            if (data.resultCode == 1) {
                $.ajax({
                    url:'/getCurrentTime',
                    data:{
                    },
                    async:false,
                    success:function(result){
                        localStorage.setItem("currentTime",result.data-Math.round(((new Date().getTime()))));
                    }
                })

                $.ajax({
                    type:"POST",
                    url:"/console/find/googleVerification",
                    dataType:"json",
                    async: false,
                    success:function(result){

                        if(result.data == 1) {

                            layer.open({
                                type: 1,
                                title: '请输入验证码',
                                area: ['300px', '180px'], // 弹窗大小
                                content: `
                <div style="padding: 20px;">
                    <input type="text" id="userInput" class="layui-input" maxlength="6" placeholder="请输入6位数字" style="1px solid  #ff6700;" />
                    <button id="confirmButton" class="layui-btn layui-btn-normal" style="margin-top: 20px;width: 35%;float: right;background-color: #009688;">确定</button>
                </div>
            `,
                                success: function (layero, index) {
                                    $('#userInput').on('input', function () {
                                        var value = $(this).val();
                                        // 只允许输入数字
                                        $(this).val(value.replace(/\D/g, ''));
                                    });

                                    // 确定按钮点击事件
                                    $('#confirmButton').click(function () {
                                        var inputValue = $('#userInput').val(); // 获取输入框的值

                                        if (inputValue === "") {
                                            layer.msg('请输入内容！');
                                        } else {
                                            $.ajax({
                                                url: '/console/checkCode?userId=' + data.data.userId + "&code=" + inputValue, // 替换为你的后端接口
                                                type: 'GET',
                                                success: function (response) {
                                                    if (response.resultCode == 1) {
                                                        layer.msg("登录成功", {icon: 1});
                                                        console.log("Login data:" + JSON.stringify(data))
                                                        localStorage.setItem("access_Token", data.data.access_Token);
                                                        localStorage.setItem("role", data.data.role);
                                                        localStorage.setItem("userId", data.data.userId);
                                                        localStorage.setItem("areaCode", data.data.areaCode);
                                                        localStorage.setItem("account", data.data.account);
                                                        localStorage.setItem("adminId", data.data.adminId);
                                                        localStorage.setItem("apiKey", data.data.apiKey);
                                                        localStorage.setItem("nickname", data.data.nickname);
                                                        localStorage.setItem("registerInviteCode", data.data.registerInviteCode); //系统邀请码模式
                                                        localStorage.setItem("userAuth_resource", data.data.userAuth);//用户权限
                                                        localStorage.setItem("userName_resource", data.data.userName);//用户权限

                                                        setTimeout(function () {
                                                            location.replace("/pages/console/index.html");
                                                        }, 1000);
                                                    } else {
                                                        layer.msg(response.resultMsg, {icon: 2});
                                                    }
                                                    //
                                                    // $('#qrcodeImage').attr('src', response.data.url);
                                                    // $("#id-secretKey").val(response.data.secretKey);
                                                },
                                                error: function () {
                                                    alert('失败');
                                                }
                                            });
                                            // 关闭弹窗
                                            layer.close(index);
                                        }
                                    });
                                }
                            });
                        }else{
                            layer.msg("登录成功", {icon: 1});
                            console.log("Login data:" + JSON.stringify(data))
                            localStorage.setItem("access_Token", data.data.access_Token);
                            localStorage.setItem("role", data.data.role);
                            localStorage.setItem("userId", data.data.userId);
                            localStorage.setItem("areaCode", data.data.areaCode);
                            localStorage.setItem("account", data.data.account);
                            localStorage.setItem("adminId", data.data.adminId);
                            localStorage.setItem("apiKey", data.data.apiKey);
                            localStorage.setItem("nickname", data.data.nickname);
                            localStorage.setItem("registerInviteCode", data.data.registerInviteCode); //系统邀请码模式
                            localStorage.setItem("userAuth_resource", data.data.userAuth);//用户权限
                            localStorage.setItem("userName_resource", data.data.userName);//用户权限

                            setTimeout(function () {
                                location.replace("/pages/console/index.html");
                            }, 1000);
                        }
                        // if (result.data == 1){
                        //     operation.userLogin(field,true);
                        // }else{
                        //     operation.userLogin(field,false);
                        // }
                    },
                    error:function(error){
                        flag = false;
                    }
                });




                // }else{

            //         layer.open({
            //             type: 1,
            //             title: '请扫码',
            //             area: ['300px', '350px'], // 弹窗大小
            //             content: `
            //                 <div style="text-align: center; padding: 20px;">
            //         <img id="qrcodeImage" style="width: 200px; height: 200px;" />
            //         <input type="hidden" id="id-secretKey">
            //         <button id="confirmButton" class="layui-btn layui-btn-normal" style="margin-top: 20px;">确定已扫码</button>
            //     </div>
            // `,
            //             success: function(layero, index){
            //                 // 发起Ajax请求，获取二维码URL
            //                 $.ajax({
            //                     url: '/console/getQrcode?name='+data.data.userId, // 替换为你的后端接口
            //                     type: 'GET',
            //                     success: function(response) {
            //
            //                         $('#qrcodeImage').attr('src', response.data.url);
            //                         $("#id-secretKey").val(response.data.secretKey);
            //                     },
            //                     error: function() {
            //                         alert('二维码获取失败');
            //                     }
            //                 });
            //
            //                 $('#confirmButton').click(function() {
            //                     // 弹出二次确认框
            //                     layer.confirm('确认已经使用谷歌验证器扫码了吗？', {icon: 3, title: '确定扫码'}, function(confirmIndex) {
            //                         // 用户确认后，关闭二次确认弹窗
            //                         layer.close(confirmIndex);
            //
            //                         // 二次确认后发起请求到后台
            //                         $.ajax({
            //                             url: '/console/saveSecretKey?secretKey='+$("#id-secretKey").val()+"&userId="+data.data.userId, // 替换为你确认扫码的后台接口
            //                             method: 'GET',
            //                             success: function(response) {
            //                                 // 关闭二维码弹窗
            //                                 layer.close(index);
            //                             },
            //                             error: function() {
            //                                 alert('确认失败，请重试');
            //                             }
            //                         });
            //                     });
            //                 });
            //             }
            //         });
             //   }

            } else if(data.resultCode != 1) {
                layer.closeAll('loading');
                //layer.msg(data.resultMsg,{icon: 2});
                layer.msg(data.resultMsg,{icon: 2});

            }
        }, "json");
    },
    /*检验短信验证码*/
    checkSmsCode:function (areaCode,account,code) {
        var flag = false;
        $.ajax({
            type:"POST",
            url:"/console/check/sms",
            data:{
                code:code,
                areaCode,areaCode,
                account:account
            },
            dataType:"json",
            async: false,
            success:function(result){
                console.log(result);
                if (result.data.param == 200){
                    flag = true;
                }else{
                    layer.msg("短信验证码错误或已过期",{icon: 2});
                    flag = false;
                }
            },
            error:function(error){
                layer.msg(error.toString(),{icon: 2});
                flag = false;
            }
        });
        return flag;
    },
    /*获取管理员手机号*/
    getAdminPhone:function (areaCode,account) {
        var phone = 0;
        $.ajax({
            type:"POST",
            url:"/console/find/admin/phone",
            data:{
                areaCode:areaCode,
                account:account
            },
            dataType:"json",
            async: false,
            success:function(result){
                if (result.resultCode == 1){
                    phone = result.data;
                }else{
                    phone = -1;
                    console.log("",result.resultMsg);
                }
            },
            error:function(error){
                layer.msg(error.toString(),{icon: 2});
                phone = 0;
            }
        });
        return phone;

    }
}
