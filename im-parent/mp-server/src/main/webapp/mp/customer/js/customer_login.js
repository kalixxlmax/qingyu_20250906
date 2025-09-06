var item;
layui.config({
    base:'./common/step-lay/'
}).use(['form', 'step','element'], function() {
    var $ = layui.$, form = layui.form, step = layui.step;

    //-------------------------------------注册账号 JS-----------------------------------------
    //初始化地址
    for (area in Addr){
        $("#country").append("<option value='"+area+"'>"+area+"</option>");
    }
    $("#payMoney").add("display","none");
    /*oper.getAliPayInfo();*/
    layui.form.render();

    //注册区号
    $("#telephone").intlTelInput({
        autoHideDialCode: false,
        autoPlaceholder: "off",
        dropdownContainer: "body",
        geoIpLookup: null,
        //初始国家
        initialCountry: "cn",
        nationalMode: false,
        preferredCountries: ['cn', 'my'],
        separateDialCode: true,
    });

    /*进入注册页面*/
    $('#chat-register').on('click',function(){
        $("body").removeClass("loginBody");
        $("#registerOper").show();
        $("#loginOper").hide();
        $("#privateAccount").hide();
        $("#companyAccount").hide();
    });

    step.render({
        elem: '#stepForm',
        filter: 'stepForm',
        width: '100%', //设置容器宽度
        stepWidth: '750px',
        height: '500px',
        stepItems: (item == 0? [{title: getLanguageName('select_type')},{title: getLanguageName('register_info')}, {title: getLanguageName('basic_info')}, {title: getLanguageName('complete')}]
                          : [{title: getLanguageName('select_type')},{title: getLanguageName('register_info')}, {title: getLanguageName('basic_info')}, {title: getLanguageName('complete')}] )
    });

    //上一步
    form.on('submit(formStep)', function (data) {
        var num = $("#position").html();//获取当前步骤
        if (num == 1){
            var type = $("#officialType").val();
            if (type == 2){
                $("#privateAccount").show();
                $("#companyAccount").hide();
            }

            if (type == 4){
                $("#privateAccount").hide();
                $("#companyAccount").show();
            }
        }
        step.next('#stepForm');
        return false;
    });

    //下一步 数据判断
    form.on('submit(formStep2)', function (data) {
        var num = $("#position").html();//获取当前步骤
        var type = $("#officialType").val();//注册类型

        if (num == 4){
            step.next('#stepForm');
            return false;
        }

        /* 判断注册信息是否正确 */
        if (num == 2 && type == 4){
            var flag = oper.isBusinessLicense();
            if (flag){
                step.next('#stepForm');
                return false;
            }else{
                return false;
            }
        }

        if (num == 2 && type == 2){
            var flag = oper.isIdentity();
            if (flag){
                step.next('#stepForm');
                return false;
            }else{
                return false;
            }
        }


        /* 基本信息校验 */
        if (num == 3){
            if (item == 0){
                $("#payFrom").hide();
                $("#successPage").show();

                var flag = oper.isIdentity();
                if (flag){
                    step.next('#stepForm');
                    return false;
                }else {
                    return false;
                }
            }else if (type == 4 || type == 2){
                $("#payFrom").show();
                $("#successPage").hide();

                var flag = oper.checkInfo();
                if (flag){
                    oper.idCheck();
                    step.next('#stepForm');
                    return false;
                }else {
                    return false;
                }
            }
        }
        return false;
    });

    //上一步
    $('.pre').click(function () {
        step.pre('#stepForm');
    });

    //下一步
    $('.next').click(function () {
        step.next('#stepForm');
    });

    //返回
    $(".backLogin").click(function () {
        window.location.href="/mp/login.html";
    })


    //地址三级联动
    let country='';
    layui.form.on('select(country)',function(data){
        country=data.value;
        $("#province").empty();
        for(s in Addr[data.value]){
            $("#province").append("<option value='"+s+"'>"+s+"</option>");
        }
        layui.form.render();
    })


    layui.form.on('select(province)',function(data){
        $("#city").empty();
        $("#province").append("<option value=''>请选择省份</option>");
        for(c in Addr[country][data.value]){
            $("#city").append("<option value='"+Addr[country][data.value][c]+"'>"+Addr[country][data.value][c]+"</option>");
        }
        layui.form.render();
    })

    //注册付款    监听多选框是否已经点击
    form.on('checkbox(number)', function (data) {
        if (data.elem.checked){
            $("#payNext").css("pointer-events","auto");
            $("#payNext").removeClass(" layui-btn-disabled");
        }else{
            $("#payNext").css("pointer-events","none");
            $("#payNext").addClass(" layui-btn-disabled");
        }
     })


    //初始话国际化语言
    loadLanguage(getLanguage());
    layui.form.render();
});


var oper={
    // 上传
    upload:function(){
        var imgurl = localStorage.getItem("imgUrl");
        layui.element.progress(imgurl, '0%');
        $("#uploadSmallFileFrom").ajaxSubmit({
            uploadProgress: function (event, position, total, percentComplete) {
                percentVal = percentComplete + '%';
                layui.element.progress(imgurl, percentVal);
                console.log(percentVal, position, total);
            },
            success:function(data){
                var obj = eval("("+data+")");
                console.log(obj.url);
                $("#photoSmallUrl").val(obj.url);
                var imgurl = localStorage.getItem("imgUrl");
                $("#" + imgurl + "").attr('src',obj.url);
            }
        })
    }


    // 选择文件
    ,selectSmallFile:function(data){
        localStorage.setItem("imgUrl",data);
        $("#photoSmallUpload").click();
    }

    //身份证验证
    ,isIdentity:function () {
        var identityName = $("#identityName").val();
        if (oper.isNil(identityName)){
            layer.msg(getLanguageName('input_identityName'), {icon: 5});
            return false;
        }

        var identity = $("#identity").val();
        if (oper.isNil(identity)){
            layer.msg(getLanguageName('input_identity'), {icon: 5});
            return false;
        }

        var flag;
        var positive_url = $("#positive_url").attr('src');
        if (oper.isNil(positive_url)){
            layer.msg(getLanguageName('select_identity_from_picturn'), {icon: 5});
            return false;
        }

        var negative_url = $("#negative_url").attr('src');
        if (oper.isNil(negative_url)){
            layer.msg(getLanguageName('select_input_upload_identity_reverse_side'), {icon: 5});
            return false;
        }

        $.ajax({
            type:"POST",
            url:"/mp/isIdentity",
            dataType:"json",
            data:{
                identity:$("#identity").val(),
            },
            async:false,
            success : function(result) {
                if (result.resultCode == 1){
                    flag = true;
                }else{
                    layer.msg(result.resultMsg, {icon: 5});
                    flag = false;
                }
            },
            error : function(result) {
                layer.msg(result.resultMsg, {icon: 5});
                flag = false;
            }
        })
        return flag;
    }

    //审核营业执照
    ,isBusinessLicense:function () {
        var companyName = $("#companyName").val();
        if (oper.isNil(companyName)){
            layer.msg(getLanguageName('input_company_name'), {icon: 5});
            return false;
        }

        var businessLicense = $("#businessLicense").val();
        if (oper.isNil(businessLicense)){
            layer.msg(getLanguageName('input_business_license_number'), {icon: 5});
            return false;
        }

        var country = $("#country").val();
        if (oper.isNil(country)){
            layer.msg(getLanguageName('select_countries'), {icon: 5});
            return false;
        }

        var desc = $("#desc").val();
        if (oper.isNil(desc)){
            layer.msg(getLanguageName('input_detailed_address'), {icon: 5});
            return false;
        }

        var flag;
        var imgUrl = $("#businessLicense_url").attr('src');
        if (oper.isNil(imgUrl)){
            layer.msg(getLanguageName('order_3'), {icon: 5});
            return false;
        }
        $.ajax({
            type:"POST",
            url:"/mp/isBusinessLicense",
            dataType:"json",
            data:{
                companyBusinessLicense:$("#businessLicense").val(),
            },
            async:false,
            success : function(result) {
                if (result.resultCode == 1){
                    flag = true;
                }else{
                    layer.msg(result.resultMsg, {icon: 5});
                    flag = false;
                }
            },
            error : function(result) {
                layer.msg(result.resultMsg, {icon: 5});
                flag = false;
            }
        })
        return flag;
    }

    //基本信息校验
    ,checkInfo:function(){
        var flag;
        var passwatd = $("#password").val();
        var agreePasswatd = $("#agreePassword").val();
        if (passwatd != agreePasswatd){
            layer.msg(getLanguageName('order_4'),{icon: 5});
            return false;
        }
        $.ajax({
            type:"POST",
            url:"/mp/checkInfo",
            dataType:"json",
            data:{
                officialType:$("#officialType").val(),
                companyName:$("#companyName").val(),
                companyBusinessLicense:$("#businessLicense").val(),
                country:$("#country").val(),
                province:$("#province").val(),
                city:$("#city").val(),
                desc:$("#desc").val(),
                industryImg:$("#businessLicense_url").attr('src'),
                adminName:$("#adminName").val(),
                adminID:$("#adminIdentity").val(),
                telephone:$("#telephone").val(),
                areaCode:$("#telephone").intlTelInput("getSelectedCountryData").dialCode,
                randcode:$("#chat-code").val(),
                password:$("#password").val(),
                identityName:$("#identityName").val(),
                identity:$("#identity").val(),
                positiveUrl:$("#positive_url").attr('src'),
                negativeUrl:$("#negative_url").attr('src'),
                officialHeadImg:$("#officialHeadImg").attr('src'),
            },
            async:false,
            success : function(result) {
                if (result.resultCode == 1){
                    flag = true;
                }else{
                    layer.msg(result.resultMsg, {icon: 5});
                    flag = false;
                }
            },
            error : function(result) {
                layer.msg(result.resultMsg, {icon: 5});
                flag = false;
            }
        })
        return flag;
    }


    //注册账号
    ,idCheck:function(){
        var flag;
        var passwatd = $("#password").val();
        var agreePasswatd = $("#agreePassword").val();
        if (passwatd != agreePasswatd){
            layer.msg(getLanguageName('order_4'),{icon: 5});
            return false;
        }
        $.ajax({
            type:"POST",
            url:"/mp/registerOfficial",
            dataType:"json",
            data:{
                officialType:$("#officialType").val(),
                companyName:$("#companyName").val(),
                companyBusinessLicense:$("#businessLicense").val(),
                country:$("#country").val(),
                province:$("#province").val(),
                city:$("#city").val(),
                desc:$("#desc").val(),
                industryImg:$("#businessLicense_url").attr('src'),
                adminName:$("#adminName").val(),
                adminID:$("#adminIdentity").val(),
                telephone:$("#telephone").val(),
                areaCode:$("#telephone").intlTelInput("getSelectedCountryData").dialCode,
                randcode:$("#chat-code").val(),
                password:$("#password").val(),
                identityName:$("#identityName").val(),
                identity:$("#identity").val(),
                positiveUrl:$("#positive_url").attr('src'),
                negativeUrl:$("#negative_url").attr('src'),
                officialHeadImg:$("#officialHeadImg").attr('src'),
            },
            async:false,
            success : function(result) {
                if (result.resultCode == 1){
                    $("#successTest").html(getLanguageName('register_successfully') + "  " + $("#telephone").val() + "  "  + getLanguageName('register_info_desc'));
                    flag = true;
                }else{
                    layer.msg(result.resultMsg, {icon: 5});
                    flag = false;
                }
            },
            error : function(result) {
                layer.msg(result.resultMsg, {icon: 5});
                flag = false;
            }
        })
        return flag;
    }
    //倒计时
    ,sendCodeCountDown:function () {
        //倒计时60秒
        var count = 60;
        var codeText = $('.code').text();
        if (codeText == getLanguageName('getCode')) {
            $('.code').css("pointer-events","none");
            timer = setInterval(function () {
                count--;
                $('.code').text(count + '秒');
                if (count <= 0) {
                    clearInterval(timer);
                    $('.code').css("pointer-events","auto");
                    $('.code').text(getLanguageName('getCode'));
                }
            }, 1000);
        }
    }

    //获取验证码
    ,getCode:function () {
        var adminTelephone = $("#telephone").val();
        if (adminTelephone == ""){
            layer.msg(getLanguageName('input_phone_number'), {icon: 5});
            return;
        }
        var data = {
            telephone:$("#telephone").val(),
            areaCode:$("#telephone").intlTelInput("getSelectedCountryData").dialCode
        }
        $.ajax({
            type:"POST",
            url:"/mp/sendCode",
            dataType:"json",
            data:data,
            contentType : 'application/x-www-form-urlencoded; charset=UTF-8',
            async:false,
            traditional: true,
            success : function(result) {
                console.log(result)
                if (result.resultCode == 1){
                    oper.sendCodeCountDown();
                    layer.msg(getLanguageName('send_success'));
                }else {
                    layer.msg(result.resultMsg, {icon: 5});
                }
            },
            error : function(result) {
                layer.msg(getLanguageName('order_9'), {icon: 5});
            }
        })
    }

    ,isNil : function(s) {
        return undefined == s || null == s || $.trim(s) == "" || $.trim(s) == "null";
    }
   
}
