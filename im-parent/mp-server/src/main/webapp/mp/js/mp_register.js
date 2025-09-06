$(function () {

    for (area in Addr){
        $("#country").append("<option value='"+area+"'>"+area+"</option>");
    }
    layui.form.render();


    /*验证密码格式*/
     $("#password").blur(function () {
         var password = $("#password").val();
         if (password.length < 6){
             layer.msg('密码小于六位请重新输入！', {icon: 5});
             $("#password").val("");
         }
     })

    /*判断密码是否一致*/
    $("#againPassword").blur(function () {
        var password1 = $("#password").val();
        var password = $("#againPassword").val();
        if (password != password1){
            layer.msg('密码不一致请重新输入！', {icon: 5});
            $("#againPassword").val("");
        }
    })

    /*发送验证码*/
    $(".chat-code").click(function () {
        var name = $("#telephone").val();
        if (name === ""){
            layer.msg("请输入手机号码！", {icon: 5});
            return;
        }

        /*验证手机号格式*/
        if (isNaN(name)){
            layer.msg('请您输入正确的手机号！', {icon: 5});
            $("#telephone").val("");
            return;
        }

        var data={
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
               if (result.resultCode != 1){
                   layer.msg(result.resultMsg, {icon: 5});
               }else {
                   info.sendCode();
                   layer.msg("发送成功！");
               }
            },
            error : function(result) {
                layer.msg(result.resultMsg, {icon: 5});
            }
        })
    })

    //自定义省份
    $("#customProvince").click(function () {
        var country = $("#country").val();
        if (country === ''){
            layer.msg("请优先选择国家！");
            return;
        }
        info.contomAddress(1);
    })

    //自定义城市
    $("#customCity").click(function () {
        var country = $("#country").val();
        var province = $("#province").val();
        if (country === ''){
            layer.msg("请优先选择国家！");
            return;
        }
        if (province === ''){
            layer.msg("请优先选择省份！");
            return;
        }
        info.contomAddress(2);
    })

})

let country='';
layui.form.on('select(country)',function(data){
    console.log(data.value);
    country=data.value;
    $("#province").empty();
    $("#province").append("<option value=''>请选择省份</option>");
    for(s in Addr[data.value]){
        $("#province").append("<option value='"+s+"'>"+s+"</option>");
    }
    layui.form.render();
})

layui.form.on('select(province)',function(data){
    $("#city").empty();
    $("#city").append("<option value=''>请选择市区</option>");
    for(c in Addr[country][data.value]){
        $("#city").append("<option value='"+Addr[country][data.value][c]+"'>"+Addr[country][data.value][c]+"</option>");
    }
    layui.form.render();
})

/*提交表单*/
layui.form.on('submit(formDemo)', function (data) {
    /*营业执照号判断*/
    var cbl = $("#companyBusinessLicense").val();
    if (cbl.length != 18 && cbl.length != 15 ){
        layer.msg("请输入正确的营业执照号！", {icon: 5});
        $("#companyBusinessLicense").val("");
        return;
    }

    /*校验地址*/
    var desc = $("#desc").val();
    if (desc == '' && desc.length == 0){
        layer.msg("详细地址信息不能为空！", {icon: 5});
        return;
    }

    /*管理员身份证号校验*/
    var id = $("#adminID").val();
    if (id.length > 18 || id.length < 18){
        layer.msg("请输入正确的身份证号码！", {icon: 5})
        $("#adminID").val("")
        return;
    }

    /*管理员手机号格式校验*/
    var tele = $("#adminTelephone").val();
    if (isNaN(tele)){
        layer.msg('请您输入正确的手机号！', {icon: 5});
        $("#adminTelephone").val("");
        return;
    }

    var img = $("#uploadSmall_url").val();
    if (img == null){
        layer.msg('请您上传工商执照！', {icon: 5});
        return;
    }

    //JSON.stringify(data.field)   这是表单中所有的数据
    var data = {
        telephone:$("#telephone").val(),
        password:$("#password").val(),
        companyName:$("#companyName").val(),
        companyBusinessLicense:$("#companyBusinessLicense").val(),
        companyType:$(".companyType").val(),
        adminName:$("#adminName").val(),
        adminID:$("#adminID").val(),
        adminTelephone:$("#adminTelephone").val(),
        country:$("#country").val(),
        province:$("#province").val(),
        city:$("#city").val(),
        desc:$("#desc").val(),
        randcode:$("#randcode").val(),
        industryImg:$("#uploadSmall_url")[0].src,
        areaCode:$("#telephone").intlTelInput("getSelectedCountryData").dialCode
    }

    console.log(data)
    $.ajax({
        type:"POST",
        url:"/mp/opffcialInfoRegister",
        dataType:"json",
        data:data,
        async:false,
        success : function(result) {
            console.log(result)
           if (result.resultCode == 1){
               location.href="/mp/success.html";
           }else if(result.resultCode == 1040104){
               layer.msg("验证码过期或失效！", {icon: 5});
           }
           layer.msg(result.resultMsg, {icon: 5});
           return false;

        },
        error : function(result) {
            layer.msg(result.resultMsg, {icon: 5});
        }
    })
})

var timer = null;
var count = 60;

var info= {
    sendCode: function () {
        var codeText = $('.code').text();
        if (codeText == '获取验证码') {
            $('#sendCode').attr('disabled',"true");
            timer = setInterval(function () {
                count--;
                $('.code').text(count + '后获取验证码');
                if (count <= 0) {
                    clearInterval(timer);
                    $('#sendCode').removeAttr("disabled");
                    $('.code').text('获取验证码');
                }
            }, 1000);
        }
    }
}

var timer = null;
var count = 60;

var info= {
    sendCode: function () {
        var codeText = $('.code').text();
        if (codeText == '获取验证码') {
            $('#sendCode').attr('disabled',"true");
            timer = setInterval(function () {
                count--;
                $('.code').text(count + '后获取验证码');
                if (count <= 0) {
                    clearInterval(timer);
                    $('#sendCode').removeAttr("disabled");
                    $('.code').text('获取验证码');
                }
            }, 1000);
        }
    }

    //自定义地址
    ,contomAddress:function (data) {
        layui.layer.open({
            title:data == 1?'自定义省份':'自定义城市',
            type: 1,
            btn:["确定","取消"],
            area: ['300px'],
            content: '<div id="mdifyPassword" class="layui-form" style="margin:20px 40px 10px 40px;;">'
                +   '<div class="layui-form-item">'
                +      '<div class="layui-input-block" style="margin: 0 auto;">'
                +        '<input type="text" required  lay-verify="required" placeholder="请输入名称" autocomplete="off" class="layui-input address">'
                +      '</div>'
                +    '</div>'
                +'</div>'

            ,yes: function(index, layero){ //确定按钮的回调

                var address = $("#mdifyPassword .address").val();
                if(address === ''){
                    layui.layer.msg("请输入名称！",{"icon":2});
                    return;
                }

                if (data == 1){
                    $("#province").append("<option value='"+address+"' selected>"+address+"</option>");
                    layui.layer.close(index); //关闭弹框
                    layui.form.render();
                }else if(data == 2){
                    $("#city").append("<option value='"+address+"' selected>"+address+"</option>");
                    layui.layer.close(index); //关闭弹框
                    layui.form.render();
                }
            }

        });
    }
}

var UploadInfo={
    // 上传
    upload:function(){
        $("#uploadSmallFileFrom").ajaxSubmit(function(data){
            var obj = eval("("+data+")");
            console.log(obj.url);
            $("#photoSmallUrl").val(obj.url);
            $("#uploadSmall_url").attr("src",obj.url);
        })
    },
    // 选择文件
    selectSmallFile:function(){
        $("#photoSmallUpload").click();
    }
}
$(function(){
    $("#uploadSmallFileFrom").attr("action",Config.uploadUrl);
})


