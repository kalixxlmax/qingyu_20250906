layui.config({
    base:'./common/step-lay/'
}).use(['form', 'step'], function() {
    var $ = layui.$, form = layui.form, step = layui.step;

    //提交
    form.on('submit(mpUser_login)', function(obj) {

        obj.field.password = $.md5(obj.field.password);
        //obj.field.areaCode = $("#account").intlTelInput("getSelectedCountryData").dialCode;
        obj.field.account = $("#account").intlTelInput("getSelectedCountryData").dialCode + obj.field.account;
        layer.load(1);

        $.post("/mp/login",obj.field, function(result) {
            if (result.resultCode == 1) {
                layer.msg(getLanguageName("login_success"),{icon: 1});
                result.data["password"] = obj.field.password;
                localStorage.setItem('loginData',JSON.stringify(result.data));

                setTimeout(function() {
                    location.replace("/mp/index.html");
                }, 1000);

            } else if (result.resultCode == 101981){
                sessionStorage.setItem("reviseInfo",$("#account").val())
                window.location.href="/mp/reviseInfo.html";
            }else{
                layer.closeAll('loading');
                layer.msg(result.resultMsg,{icon: 2});
            }
        }, "json");
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
        utilsScript: "assets/js/utils.js",
    });
    $("#registerOper").hide();


    //选择国际化语言
    var selectId = document.getElementById("selectLanguage");//获取ID
    selectId.onchange = function(){
        var language = $("#selectLanguage").val();
        if (language === ""){
            return;
        }
        loadLanguage(language)
        layui.form.render();
    }
});


//初始化 i18n 插件
function loadLanguage(language){
    localStorage.setItem('mp_system_language',language);
    $.i18n.properties({
        name: 'strings',    //属性文件名     命名格式： 文件名_国家代号.properties
        path: './common/i18n/',   //注意这里路径是你属性文件的所在文件夹
        mode: 'map',
        language: language,     //这就是国家代号 name+language刚好组成属性文件名：strings+zh -> strings_zh.properties
        callback: function () {
            $("[data-locale]").each(function () {
                if($(this).attr('placeholder')){
                    $(this).attr('placeholder', $.i18n.prop($(this).data('locale')));

                }if ($(this).attr('title')){
                    $(this).attr('title', $.i18n.prop($(this).data('locale')));
                }else{
                    $(this).html($.i18n.prop($(this).data("locale")));
                }
            });

        }
    });
}

//获取当前国际化语言
function getLanguage(){
    var language = localStorage.getItem('mp_system_language');
    if (language == undefined){
        return "zh";
    }else{
        return language;
    }
}

//获取国际化语言  根据key值获取value值；
function getLanguageName(name){
    loadLanguage(getLanguage());
    try {
        return $.i18n.prop(name);
    } catch (e) {
        return name;
    }
}

$(function () {
    //初始化 国际化插件
    loadLanguage(getLanguage());
})








