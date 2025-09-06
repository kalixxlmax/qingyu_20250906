var page = 0;
var sum = 0;
var lock = 0;
layui.use(['form', 'layer', 'laydate', 'table', 'laytpl'], function () {
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;


    console.log("页面加载");
    // 红包列表
    var tableIns = table.render({

        elem: '#redEnvelope_table'
        , url: request("/console/redPacketList")
        , id: 'redEnvelope_table'
        ,toolbar: '#toolbar'
        , page: true
        , curr: 0
        , limit: Common.limit
        , limits: Common.limits
        , totalRow: true
        , groups: 7
        , cols: [[ //表头
            {field: 'userId', title: '用户Id', width: '10%', totalRowText: '合计'}
            , {field: 'userName', title: '昵称', width: '10%'}
            , {field: 'money', title: '红包金额(元)',  width: '10%', totalRow: true}
            , {field: 'count', title: '红包个数',  width: '10%', totalRow: false}
            , {
                field: 'sendDirection', title: '发送类型', width: '10%', templet: function (d) {
                    if (!Common.isNil(d.roomJid)) {
                        return "群聊";
                    } else if (!Common.isNil(d.toUserId)) {
                        return "一对一";
                    } else {
                        return "其他";
                    }
                }
            }
            , {
                field: 'type', title: '红包类型', width: '10%', templet: function (d) {
                    var typeMsg;
                    (d.type == 1 ? typeMsg = "普通红包" : (d.type == 2) ? typeMsg = "拼手气红包" : (d.type == 3) ? typeMsg = "口令红包" : typeMsg = "其他")
                    return typeMsg;
                }
            }
            , {
                field: 'status', title: '红包状态', width: '10%', templet: function (d) {
                    var statusMsg;
                    (d.status == 1 ? statusMsg = "发出" : (d.status == 2) ? statusMsg = "已领完" : (d.status == -1) ? statusMsg = "已退款" : (d.status == 3) ? statusMsg = "未领完退款" : "")
                    return statusMsg;
                }
            }
            , {
                field: 'sendTime', title: '发送时间', width: '10%', templet: function (d) {
                    return UI.getLocalTime(d.sendTime);
                }
            }
            , {
                field: 'outTime', title: '退回时间', width: '10%', templet: function (d) {
                    if (d.status == -1) {
                        return UI.getLocalTime(d.outTime);
                    } else {
                        return "";
                    }
                }
            }
            , {fixed: 'right', title: "操作", align: 'left', toolbar: '#redPageListBar'}
        ]]
        , done: function (res, curr, count) {
            checkRequst(res);

            //权限判断
            var arr = ['redback-info'];
            manage.authButton(arr);

            //获取零时保留的值
            var last_value = $("#redEnvlope_limlt").val();
            //获取当前每页大小
            var recodeLimit = tableIns.config.limit;
            //设置零时保留的值
            $("#redEnvlope_limlt").val(recodeLimit);
            //判断是否改变了每页大小数
            if (last_value != recodeLimit) {
                // 刷新
                table.reload("redEnvelope_table", {
                    url: request("/console/redPacketList"),
                    page: {
                        curr: 1 //重新从第 1 页开始
                    }
                })
            }


            if (count == 0 && lock == 1) {
                // layui.layer.alert("暂无数据",{yes:function(){
                //   renderTable();
                //   layui.layer.closeAll();
                // }});
                layer.msg("暂无数据", {"icon": 2});
                renderTable();
            }
            lock = 0;

        }
    });

    // 列表操作
    table.on('tool(redEnvelope_table)', function (obj) {
        var layEvent = obj.event,
            data = obj.data;
        console.log(data);
        if (layEvent === 'receiveWaterInfo') {// 红包领取详情
            var tableIns1 = table.render({
                elem: '#receiveWater_table'
                , url: request("/console/receiveWater") + "&redId=" + data.id
                , id: 'receiveWater_table'
                ,toolbar: '#toolbar'
                , page: true
                , curr: 0
                , limit: 10
                , limits: [10, 20, 30, 40, 50, 100, 1000, 10000]
                , groups: 7
                , cols: [[ //表头
                    {field: 'id', title: '领取流水Id'}
                    , {field: 'redId', title: '红包Id'}
                    , {field: 'sendName', title: '发送红包用户昵称'}
                    , {field: 'userName', title: '领取红包用户昵称'}
                    , {field: 'reply', title: '红包回复语'}
                    , {field: 'money', title: '领取金额'}
                    , {field: 'time', title: '领取时间',  templet: function (d) {
                            return UI.getLocalTime(d.time);
                        }}]]
                , done: function (res, curr, count) {
                    checkRequst(res);
                    $("#redEnvelope").hide();
                    $(".redPage_btn_div").hide();
                    $(".visitPathDiv").hide();
                    $("#receiveWater").show();
                }
            });
        }
    });

    //首页搜索
    $(".search_live").on("click", function () {
        // 关闭超出宽度的弹窗
        $(".layui-layer-content").remove();
        table.reload("redEnvelope_table", {
            url: request("/console/redPacketList"),
            where: {
                userName: Common.getValueForElement("#toUserName"),  //搜索的关键字
                status: Common.getValueForElement("#status")
            },
            page: {
                curr: 1 //重新从第 1 页开始
            }
        })
        lock = 1;
    });

});

//重新渲染表单
function renderTable() {
    layui.use('table', function () {
        var table = layui.table;//高版本建议把括号去掉，有的低版本，需要加()
        // table.reload("user_list");
        table.reload("redEnvelope_table", {
            url: request("/console/redPacketList"),
            where: {
                userName: Common.getValueForElement("#toUserName")  //搜索的关键字
            },
            page: {
                curr: 1 //重新从第 1 页开始
            }
        })
    });
}

var Red = {
    btn_back: function () {
        $("#redEnvelope").show();
        $(".redPage_btn_div").show();
        $(".visitPathDiv").show();
        $("#receiveWater").hide();
    },
    //计算红包金额
    calculateRedPacketMoney: function () {
        Common.invoke({
            url: request('/console/get/redpacket/money'),
            data: {},
            async: false,
            success: function (result) {
                if (result.resultCode == 1) {
                    $(".totle").html(result.data.totle);
                    $(".remain").html(result.data.remain);
                    $(".receive").html(result.data.receive);
                }
            }
        });
    }
}

$(function () {
    Red.calculateRedPacketMoney();
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})