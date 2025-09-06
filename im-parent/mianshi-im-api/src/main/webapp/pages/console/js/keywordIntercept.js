layui.use(['form', 'layer', 'laydate', 'table', 'laytpl'], function () {
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;

    //消息拦截列表
    var tableInsLiveRoom = table.render({

        elem: '#keywordIntercept_table'
        , url: request("/console/keywordDenyRecordList")
        , id: 'keywordIntercept_table'
        ,toolbar: '#toolbar'
        , page: {
            layout: [ 'prev', 'page', 'next','limit', 'count', 'skip'] //自定义分页布局
            ,groups: 3 //只显示 1 个连续页码
            ,first: false //不显示首页
            ,last: false //不显示尾页
        }
        , curr: 0
        , limit: Common.limit
        , limits: Common.limits
        , groups: 7
        , cols: [[ //表头
            {field: 'id', title: 'id', width: '10%'}
            , {field: 'fromUserId', title: '发送者Id', width: '8%'}
            , {field: 'fromUserName', title: '发送者昵称', width: '8%'}
            , {field: 'toUserId', title: '接收者Id', width: '8%'}
            , {field: 'toUserName', title: '接收昵称', width: '7%'}
            , {field: 'msgContent', title: '消息内容', width: '10%'}
            , {
                field: 'chatType', title: '消息类型', width: '10%', templet: function (d) {
                    return d.chatType == 1 ? "单聊" : "群聊";
                }
            }
            , {
                field: 'keywordType', title: '敏感词类型', width: '10%', templet: function (d) {
                    return d.keywordType == 1 ? "否词" : "普通敏感词";
                }
            }
            , {field: 'keyword', title: '包含的的敏感词/否词', width: '10%'}
            , {
                field: 'createTime', title: '时间', width: '10%', templet: function (d) {
                    return UI.getLocalTime(d.createTime / 1000);
                }
            }
            , {fixed: 'right', title: "操作", align: 'left', toolbar: '#keywordInterceptListBar'}
        ]]
        , done: function (res, curr, count) {
            checkRequst(res);

            //权限判断
            var arr = ['keywordInter-delete'];
            manage.authButton(arr);

            console.log("shuju", res)
            //获取当前每页大小
            var recodeLimit = $(".layui-laypage-limits").find("option:selected").val();
            if (undefined != recodeLimit) {
                //获取零时保留的值
                var last_value = $("#keywordInt_limlt").val();
                //设置零时保留的值
                $("#keywordInt_limlt").val(recodeLimit);
                //判断是否改变了每页大小数
                if (last_value != recodeLimit) {
                    // 刷新
                    table.reload("keywordIntercept_table", {
                        url: request("/console/keywordDenyRecordList"),
                        page: {
                            curr: 1 //重新从第 1 页开始
                        }
                    })
                }
            }


            /* if(localStorage.getItem("role")==1 || localStorage.getItem("role")==7){
              $(".delete").hide();
             }*/
            var pageIndex = tableInsLiveRoom.config.page.curr;//获取当前页码
            var resCount = res.count;// 获取table总条数
            currentCount = resCount;
            currentPageIndex = pageIndex;
        }
    });


    //列表操作
    table.on('tool(keywordIntercept_table)', function (obj) {
        var layEvent = obj.event,
            data = obj.data;
        if (layEvent === 'delete') { //删除
            Intercept.deleteKeywordIntercept(data.id);
        }
    });

    //搜索
    $(".search_live").on("click", function () {
        // 关闭超出宽度的弹窗
        $(".layui-layer-content").remove();
        table.reload("keywordIntercept_table", {
            where: {
                userId: Common.getValueForElement("#sender"),  //搜索的关键字
                toUserId: Common.getValueForElement("#receiver"),
                content: Common.getValueForElement("#content"),
                type: Common.getValueForElement("#complaint_select")
            },
            page: {
                curr: 1 //重新从第 1 页开始
            }
        })
        $("#sender").val("");
        $("#receiver").val("");
        $("#content").val("");
    });
});


var Intercept = {
    getKeywordInterceptList: function () {
        if ($("#complaint_select").val() == 0) {
            $("#receiver").attr("placeholder", "接收人Id")
            var tableInsLiveRoom = layui.table.render({

                elem: '#keywordIntercept_table'
                , url: request("/console/keywordDenyRecordList")
                , id: 'keywordIntercept_table'
                ,toolbar: '#toolbar'
                , page: true
                , curr: 0
                , limit: Common.limit
                , limits: Common.limits
                , groups: 7
                , cols: [[ //表头
                    {field: 'id', title: 'id', width: '10%'}
                    , {field: 'fromUserId', title: '发送者Id', width: '8%'}
                    , {field: 'fromUserName', title: '发送者昵称', width: '8%'}
                    , {field: 'toUserId', title: '接收者Id', width: '8%'}
                    , {field: 'toUserName', title: '接收昵称', width: '8%'}
                    , {field: 'msgContent', title: '消息内容', width: '10%'}
                    , {
                        field: 'chatType', title: '消息类型', width: '10%', templet: function (d) {
                            return d.chatType == 1 ? "单聊" : "群聊";
                        }
                    }
                    , {
                        field: 'keywordType', title: '敏感词类型', width: '10%', templet: function (d) {
                            return d.keywordType == 1 ? "否词" : "普通敏感词";
                        }
                    }
                    , {field: 'keyword', title: '包含的的敏感词/否词', width: 160}
                    , {
                        field: 'createTime', title: '时间', templet: function (d) {
                            return UI.getLocalTime(d.createTime / 1000);
                        }
                    }

                    , {fixed: 'right', title: "操作", align: 'left', toolbar: '#keywordInterceptListBar'}
                ]]
                , done: function (res, curr, count) {
                    checkRequst(res);
                    /* if(localStorage.getItem("role")==1 || localStorage.getItem("role")==7){
                      $(".delete").hide();
                     }*/
                    var pageIndex = tableInsLiveRoom.config.page.curr;//获取当前页码
                    var resCount = res.count;// 获取table总条数
                    currentCount = resCount;
                    currentPageIndex = pageIndex;
                }
            });
        } else if ($("#complaint_select").val() == 1) {
            $("#receiver").attr("placeholder", "群组Jid")
            var tableInsLiveRoom = layui.table.render({
                elem: '#keywordIntercept_table'
                , url: request("/console/keywordDenyRecordList") + "&type=1"
                ,toolbar: '#toolbar'
                , id: 'keywordIntercept_table'
                , page: true
                , curr: 0
                , limit: Common.limit
                , limits: Common.limits
                , groups: 7
                , cols: [[ //表头

                    /* {field: 'id', title: 'id',width:120}
                     ,{field: 'sender', title: '发送人Id',width:120}
                     ,{field: 'senderName', title: '发送人',width:120}
                     ,{field: 'roomJid', title: '群组Jid', width:120}
                     ,{field: 'roomName', title: '群组名称', width:120}
                     ,{field: 'content', title: '消息内容', width:120}
                     ,{field: 'createTime', title: '时间', width:300,templet: function(d){
                       return UI.getLocalTime(d.createTime);
                      }}*/

                    {field: 'id', title: 'id', width: '10%'}
                    , {field: 'fromUserId', title: '发送者Id', width: '8%'}
                    , {field: 'fromUserName', title: '发送者昵称', width: '8%'}
                    , {
                        field: 'toUserId', title: '接收者Id', width: '8%', templet: function (d) {
                            return d.chatType == 1 ? d.toUserId : d.roomJid;
                        }
                    }
                    , {field: 'toUserName', title: '接收昵称', width: '8%'}
                    , {field: 'msgContent', title: '消息内容', width: '10%'}
                    , {
                        field: 'chatType', title: '消息类型', width: '10%', templet: function (d) {
                            return d.chatType == 1 ? "单聊" : "群聊";
                        }
                    }
                    , {
                        field: 'keywordType', title: '敏感词类型', width: '10%', templet: function (d) {
                            return d.keywordType == 1 ? "否词" : "普通敏感词";
                        }
                    }
                    , {field: 'keyword', title: '包含的的敏感词/否词', width: '10%'}
                    , {
                        field: 'createTime', title: '时间',width:'10%', templet: function (d) {
                            return UI.getLocalTime(d.createTime / 1000);
                        }
                    }

                    , {fixed: 'right',title: "操作", align: 'left', toolbar: '#keywordInterceptListBar'}
                ]]
                , done: function (res, curr, count) {
                    checkRequst(res);
                    /*if(localStorage.getItem("role")==1 || localStorage.getItem("role")==7){
                     $(".delete").hide();
                    }*/
                    var pageIndex = tableInsLiveRoom.config.page.curr;//获取当前页码
                    var resCount = res.count;// 获取table总条数
                    currentCount = resCount;
                    currentPageIndex = pageIndex;
                }
            });
        }
    },
    deleteKeywordIntercept: function (id) {
        layer.confirm('确定删除？', {icon: 3, title: '提示信息',skin : "layui-ext-motif"}, function (index) {

            Common.invoke({
                url: request('/console/deleteMsgIntercept'),
                data: {
                    id: id
                },
                success: function (result) {
                    layer.msg("删除成功", {"icon": 1});
                    layui.table.reload("keywordIntercept_table");
                }
            })
        });

    }
}

$(function () {
 //调用父级页面的Js函数
 window.parent.getJointVisitPath();
})
