layui.use(['form','jquery',"layer"],function() {
    var form = layui.form,
        $ = layui.jquery,
        layer = parent.layer === undefined ? layui.layer : top.layer;

    form.on("submit(systemConfig)",function(data){
        var host = $(".host").val();
        var api = $(".api").val();
        var username = $(".username").val();
        var password = $(".password").val();
        var templateChineseSMS = $(".templateChineseSMS").val();
        var templateEnglishSMS = $(".templateEnglishSMS").val();
        var port = $(".port").val();
        var smsSendCount = $(".smsSendCount").val();
        var smsSendTime = $(".smsSendTime").val();
        var smsSendBlackCount = $(".smsSendBlackCount").val();
        var smsSendBlackTime = $(".smsSendBlackTime").val();
        var allSmsSendCount = $(".allSmsSendCount").val();
        var allSmsSendTime = $(".allSmsSendTime").val();


        Common.invoke({
            url : request('/console/sms/tistilo/config/set'),
            data : {
                host:Common.isNull(host),
                api:Common.isNull(api),
                username:Common.isNull(username),
                password:Common.isNull(password),
                templateChineseSMS:Common.isNull(templateChineseSMS),
                templateEnglishSMS:Common.isNull(templateEnglishSMS),
                port:Common.isNull(port),
                smsSendCount:Common.isNumberNullTwo(smsSendCount),
                smsSendTime:Common.isNumberNullTwo(smsSendTime),
                smsSendBlackCount:Common.isNumberNullTwo(smsSendBlackCount),
                smsSendBlackTime:Common.isNumberNullTwo(smsSendBlackTime),
                allSmsSendCount:Common.isNumberNullTwo(allSmsSendCount),
                allSmsSendTime:Common.isNumberNullTwo(allSmsSendTime)

            },
            successMsg : '短信配置修改成功',
            errorMsg : '修改短信配置失败,请检查网络',
            success : function(result) {
            },
            error : function(result) {
            }
        });
        return false;
    })

    form.render();

});


$(function () {
    //加载数据
    Common.invoke({
        url : request('/console/smsConfig'),
        data : {},
        errorMsg : '加载数据失败，请稍后重试',
        success : function(result) {
            if (1 == result.resultCode){
                if (!Common.isNil(result.data.host)){ $(".host").val(result.data.host);}
                if (!Common.isNil(result.data.api)){$(".api").val(result.data.api);}
                if (!Common.isNil(result.data.product)){$(".product").val(result.data.product);}
                if (!Common.isNil(result.data.username)){$(".username").val(result.data.username);}
                if (!Common.isNil(result.data.password)){$(".password").val(result.data.password);}
                if (!Common.isNil(result.data.templateChineseSMS)){$(".templateChineseSMS").val(result.data.templateChineseSMS);}
                if (!Common.isNil(result.data.product)){$(".templateEnglishSMS").val(result.data.templateEnglishSMS);}
                if (!Common.isNil(result.data.product)){$(".port").val(result.data.port == -1 ? "" : result.data.port);}
                if (!Common.isNil(result.data.smsSendCount)){$(".smsSendCount").val(result.data.smsSendCount);}
                if (!Common.isNil(result.data.smsSendTime)){$(".smsSendTime").val(result.data.smsSendTime);}
                if (!Common.isNil(result.data.smsSendBlackCount)){$(".smsSendBlackCount").val(result.data.smsSendBlackCount);}
                if (!Common.isNil(result.data.smsSendBlackTime)){$(".smsSendBlackTime").val(result.data.smsSendBlackTime);}
                if (!Common.isNil(result.data.allSmsSendCount)){$(".allSmsSendCount").val(result.data.allSmsSendCount);}
                if (!Common.isNil(result.data.allSmsSendTime)){$(".allSmsSendTime").val(result.data.allSmsSendTime);}
            }

            layui.use('form', function() {
                var form = layui.form; //只有执行了这一步，部分表单元素才会自动修饰成功
                form.render();
            });
        },
        error : function(result) {
        }
    });

    //权限判断
    var arr=['smsConfig-sava'];
    manage.authButton(arr);

    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})