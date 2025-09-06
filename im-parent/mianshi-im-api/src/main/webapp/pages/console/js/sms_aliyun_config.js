layui.use(['form','jquery',"layer"],function() {
    var form = layui.form,
        $ = layui.jquery,
        layer = parent.layer === undefined ? layui.layer : top.layer;

    form.on("submit(systemConfig)",function(data){
        var product = $(".product").val();
        var domain = $(".domain").val();
        var accesskeyid = $(".accesskeyid").val();
        var accesskeysecret = $(".accesskeysecret").val();
        var signname = $(".signname").val();
        var chinase_templetecode = $(".chinase_templetecode").val();
        var international_templetecode = $(".international_templetecode").val();
        var cloudWalletVerification = $(".cloudWalletVerification").val();
        var cloudWalletNotification = $(".cloudWalletNotification").val();
        var bizType = $(".bizType").val();
        Common.invoke({
            url : request('/console/sms/aliyun/config/set'),
            data : {
                product:Common.isNull(product),
                domain:Common.isNull(domain),
                accesskeyid:Common.isNull(accesskeyid),
                accesskeysecret:Common.isNull(accesskeysecret),
                signname:Common.isNull(signname),
                chinase_templetecode:Common.isNull(chinase_templetecode),
                international_templetecode:Common.isNull(international_templetecode),
                cloudWalletVerification:Common.isNull(cloudWalletVerification),
                cloudWalletNotification:Common.isNull(cloudWalletNotification),
                bizType:Common.isNull(bizType),
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
                if (!Common.isNil(result.data.product)){$(".product").val(result.data.product);}
                if (!Common.isNil(result.data.domain)){ $(".domain").val(result.data.domain);}
                if (!Common.isNil(result.data.accesskeyid)){ $(".accesskeyid").val(result.data.accesskeyid);}
                if (!Common.isNil(result.data.accesskeysecret)){ $(".accesskeysecret").val(result.data.accesskeysecret);}
                if (!Common.isNil(result.data.signname)){$(".signname").val(result.data.signname);}
                if (!Common.isNil(result.data.chinase_templetecode)){$(".chinase_templetecode").val(result.data.chinase_templetecode);}
                if (!Common.isNil(result.data.international_templetecode)){ $(".international_templetecode").val(result.data.international_templetecode);}
                if (!Common.isNil(result.data.cloudWalletVerification)){ $(".cloudWalletVerification").val(result.data.cloudWalletVerification);}
                if (!Common.isNil(result.data.cloudWalletNotification)){$(".cloudWalletNotification").val(result.data.cloudWalletNotification);}
                if (!Common.isNil(result.data.bizType)){$(".bizType").val(result.data.bizType);}
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