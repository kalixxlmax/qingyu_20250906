var page = 0;
var sum = 0;
layui.use(['form', 'layer', 'laydate', 'table', 'laytpl'], function () {
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;

    // 绑定列表
    var tableIns = table.render({
        elem: '#bindingSDK_table'
        ,url:request("/console/getSdkLoginInfoList")
        ,toolbar: '#toolbar'
        ,page: {
            layout: [ 'prev', 'page', 'next','limit', 'count', 'skip'] //自定义分页布局
            ,groups: 3 //只显示 1 个连续页码
            ,first: false //不显示首页
            ,last: false //不显示尾页
        }
        ,curr: 0
        ,limit:Common.limit
        ,limits:Common.limits
        ,groups: 7
        ,cols: [[ //表头
            {field: 'id', title: '绑定Id',width:'15%'}
            ,{field: 'userId', title: '绑定用户Id',width:'15%'}
            ,{field: 'type', title: '第三方登录类型', width:'15%',templet : function (d) {
                    return (d.type==1?"QQ":d.type==2?"微信":d.type==3?"一键登录":"其他");
                }}
            ,{field: 'loginInfo', title: '登录标识', width:'18%'}
            ,{field: 'createTime', title: '绑定时间', width:'15%',templet : function (d) {
                    return UI.getLocalTime(d.createTime);
                }}
            ,{fixed: 'right',title:"操作", align:'left', toolbar: '#bindingSDKListBar'}

        ]]
        ,done:function(res, curr, count){
            checkRequst(res);

            //获取零时保留的值
            var last_value = Common.getValueForElement("#binding_limlt");
            //获取当前每页大小
            var recodeLimit = tableIns.config.limit;
            //设置零时保留的值
            $("#binding_limlt").val(recodeLimit);
            //判断是否改变了每页大小数
            if (last_value != recodeLimit) {
                // 刷新
                table.reload("bindingSDK_table", {
                    url: request("/console/getSdkLoginInfoList"),
                    page: {
                        curr: 1 //重新从第 1 页开始
                    }
                })
            }

        }
    });

    //列表操作
    table.on('tool(bindingSDK_table)', function (obj) {
        var layEvent = obj.event,
            data = obj.data;

        if (layEvent === 'delete') {// 删除
            if (3 == data.type) {
                return;
            }
            layer.confirm('确定解绑指定用户',{icon:3, title:'提示消息',skin : "layui-ext-motif",yes:function () {
                    layer.closeAll();
                    SDK.deleteBindingSDK(data.id);
                    obj.del();

                }
            });
        }
    });

    //首页搜索
    $(".search_bindingSDK").on("click", function () {
        table.reload("bindingSDK_table", {
            where: {
                userId: Common.getValueForElement(".bindingSDK_keyword") //搜索的关键字
            },
            page: {
                curr: 1 //重新从第 1 页开始
            }
        })
        $(".bindingSDK_keyword").val("");
    });


});

//重新渲染表单
function renderTable() {
    layui.use('table', function () {
        var table = layui.table;//高版本建议把括号去掉，有的低版本，需要加()
        table.reload("bindingSDK_table");
    });
}

var SDK = {
    // 删除第三方绑定
    deleteBindingSDK: function (id) {
        Common.invoke({
            url: request('/console/deleteSdkLoginInfo'),
            data: {
                id: id
            },
            success: function (result) {
                if (result.resultCode == 1)
                    layui.layer.alert("删除成功");
                renderTable();
            }
        })
    }
}

$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})