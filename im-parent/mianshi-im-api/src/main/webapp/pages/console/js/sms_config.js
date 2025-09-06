layui.use(['form','jquery',"layer"],function() {
    var form = layui.form,
        $ = layui.jquery,
        layer = parent.layer === undefined ? layui.layer : top.layer;

    form.on("submit(systemConfig)",function(data){
        var openSMS = $(".openSMS").val();
        Common.invoke({
            url : request('/console/sms/application/config/set'),
            data : {
                openSMS:Common.isNull(openSMS)
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
                $(".openSMS").val(Common.isNull(result.data.openSMS));
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