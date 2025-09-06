var page = 0;
var html = "";
var messageIds = new Array();
var currentPageIndex;// 当前页码数
var currentCount;// 当前总数
var isExecute = 0;
var count = 0;//总数据量
var chooseDeleteCount = 0;
var isReferTable = 0;
layui.use(['form', 'layer', 'laydate', 'table', 'laytpl'], function () {
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;

    /*//非管理员登录屏蔽操作按钮
    if(localStorage.getItem("IS_ADMIN")==0){
    	$(".msg_btn_div").empty();
    }*/

    //单聊消息列表
    var tableIns = table.render({
        elem: '#message_table'
        , toolbar: '#toolbarMessageList'
        , url: request("/console/chat_logs_all")
        , id: 'message_table'
        , page: {
            layout: ['prev', 'page', 'next', 'limit', 'count', 'skip'] //自定义分页布局
            , groups: 3 //只显示 1 个连续页码
            , first: false //不显示首页
            , last: false //不显示尾页
        }
        , curr: 0
        , limit: Common.limit
        , limits: Common.limits
        , groups: 7
        , cols: [[ //表头
            {type: 'checkbox', fixed: 'left'}// 多选
            , {field: 'sender', title: '发送者Id', sort: true, width: '10%'}
            , {field: 'sender_nickname', title: '发送者', sort: true, width: '10%'}
            , {field: 'receiver', title: '接收者Id', sort: true, width: '10%'}
            , {field: 'receiver_nickname', title: '接收者', sort: true, width: '10%'}
            , {
                field: 'contentType', title: '消息类型', sort: true, width: '10%', templet: function (d) {
                    return Common.msgType(d.contentType);
                }
            }
            , {
                field: 'timeSend', title: '时间', sort: true, width: '10%', templet: function (d) {
                    return UI.getLocalTime(d.timeSend / 1000);
                }
            }
            , {
                field: 'content', title: '内容', sort: true, width: '30%', templet: function (d) {
                    var decodeContent = Msg.decodeContent(d);
                    return decodeContent;
                }
            }, {
                field: 'deleteTime', title: '保留截止时间', width: 170, sort: true, templet: function (d) {
                    if (0 < d.deleteTime)
                        return UI.getLocalTime(d.deleteTime);
                    else return "";
                }
            }
            , {fixed: 'right', title: "操作", align: 'left', width: '10%', toolbar: '#messageListBar'}
        ]]
        , done: function (res, curr, count) {

            //权限判断
            var arr = ['mess-delete'];
            manage.authButton(arr);

            //获取零时保留的值
            var last_value = $("#messageList_limlt").val();
            //获取当前每页大小
            var recodeLimit = tableIns.config.limit;
            //设置零时保留的值
            $("#messageList_limlt").val(recodeLimit);
            //判断是否改变了每页大小数
            if (last_value != recodeLimit) {
                //刷新
                table.reload("message_table", {
                    url: request("/console/chat_logs_all"),
                    page: {
                        curr: 1 //重新从第 1 页开始

                    }
                })
            }

            if (0 == isExecute) {
                setTimeout(function () {
                    Msg.charCount();
                }, 1000);
                isExecute = 1;
            }

            var pageIndex = tableIns.config.page.curr;//获取当前页码
            var resCount = res.count;// 获取table总条数
            currentCount = resCount;
            currentPageIndex = pageIndex;

            Msg.refreshMushDelete();
        }
    });

    //监听表格复选框选择
    table.on('checkbox(message_table)', function (obj) {
        console.log(obj);
        if (obj.type == "one") {
            chooseDeleteCount = obj.checked == true ? chooseDeleteCount + 1 : chooseDeleteCount - 1;
        } else if (obj.type == 'all') {
            chooseDeleteCount = obj.checked == true ? $(".layui-laypage-limits").find("option:selected").val() : 0;
        }
        $(".chooseDeleteCount").html("已选择 " + chooseDeleteCount + " 项");
        if (chooseDeleteCount < 1) {
            $(".mustDelete").hide();
        } else {
            $(".mustDelete").show();
        }
    });

    //列表操作
    table.on('tool(message_table)', function (obj) {
        var layEvent = obj.event,
            data = obj.data;
        if (layEvent === 'delete') { //删除
            Msg.checkDeleteMessageImpl(data._id, 1);
        }
        if (layEvent === 'edit') { //编辑
            console.log(data)
            if (data?.contentType === 1) {
                Msg.editMessage(data, $("#documents").val());
            }
        }
        chooseDeleteCount < 1 ? $(".mustDelete").hide() : $(".mustDelete").show();
    });

    //搜索
    $(".search_message").on("click", function () {

        var sender = Common.getValueForElement("#sender");
        var receiver = Common.getValueForElement("#receiver");

        if (sender.indexOf("*") != -1 || receiver.indexOf("*") != -1) {
            layer.alert("输入不合法");
            return
        }

        if (!Common.isNil(sender) && !/^[0-9]*$/.test(sender)) {
            layer.alert("输入不合法,请输入正确的发送者id");
            return
        }

        if (!Common.isNil(receiver) && !/^[0-9]*$/.test(receiver)) {
            layer.alert("输入不合法,请输入正确的接收者id");
            return
        }

        //关闭超出宽度的弹窗
        $(".layui-layer-content").remove();
        table.reload("message_table", {
            url: request("/console/chat_logs_all"),
            page: {
                curr: 1 //重新从第 1 页开始
            },
            where: {
                sender: Common.getValueForElement("#sender")  //搜索的关键字
                , receiver: Common.getValueForElement("#receiver")
                , keyWord: Common.getValueForElement("#keyWord")
                , documents: Common.getValueForElement("#documents")
            }
        })

        Msg.refreshMushDelete();
    });

})

var Msg = {

    // 查询消息列表
    findMsgList: function (e, pageSize) {
        html = "";
        if (e == undefined) {
            e = 0;
        } else if (pageSize == undefined) {
            pageSize = 10;
        }

        $.ajax({
            type: 'POST',
            url: request('/console/chat_logs_all'),
            data: {
                pageIndex: (e == 0 ? "0" : e - 1),
                pageSize: pageSize
            },
            dataType: 'json',
            async: false,
            success: function (result) {
                checkRequst(result);
                sum = result.data.pageSize;
                if (result.data.pageSize != 0) {
                    $("#pageCount").val(Common.filterHtmlData(result.data.allPageCount));
                    for (var i = 0; i < result.data.pageData.length; i++) {
                        html += "<tr><td>" + Common.filterHtmlData(result.data.pageData[i].sender) + "</td><td>" + Common.filterHtmlData(result.data.pageData[i].sender_nickname)
                            + "</td><td>" + Common.filterHtmlData(result.data.pageData[i].receiver) + "</td><td>" + Common.filterHtmlData(result.data.pageData[i].receiver_nickname)
                            + "</td><td>" + Common.filterHtmlData(result.data.pageData[i].type) + "</td><td>" + UI.getLocalTime(result.data.pageData[i].timeSend) + "</td><td>"
                            + Common.filterHtmlData(result.data.pageData[i].content) + "</td></tr>";
                    }
                    $("#message_table").empty();
                    $("#message_table").append(html);
                }
            }
        });
    },

    // 多选删除
    checkDeleteMessage: function () {
        // 多选操作
        var checkStatus = layui.table.checkStatus('message_table'); //idTest 即为基础参数 id 对应的值

        for (var i = 0; i < checkStatus.data.length; i++) {
            messageIds.push(checkStatus.data[i]._id);
        }
        console.log(messageIds);
        if (0 == checkStatus.data.length) {
            layer.msg("请勾选要删除的行");
            return;
        }
        Msg.checkDeleteMessageImpl(messageIds.join(","), checkStatus.data.length, $("#documents").val());
    },
    checkDeleteMessageImpl: function (messageId, checkLength, collectionName, senderUserId) {
        layer.confirm('确定删除指定单聊聊天记录', {
            icon: 3, title: '提示消息', yes: function () {
                Common.invoke({
                    url: request('/console/deleteChatMsgs'),
                    data: {
                        msgId: messageId,
                        collectionName: collectionName,
                        snederUserId: senderUserId,
                    },
                    success: function (result) {
                        if (result.resultCode == 1) {
                            layer.msg("删除成功", {"icon": 1});
                            messageIds = [];
                            // 刷新table
                            Common.tableReload(currentCount, currentPageIndex, checkLength, "message_table");
                            //layui.table.reload("message_table");
                        }
                    }
                });
                Msg.refreshMushDelete();
            }, btn2: function () {
                messageIds = [];
            }, cancel: function () {
                messageIds = [];
            }
        });
    },
    findTigaseDocuments: function () {
        Common.invoke({
            url: request('/console/find/chat/msg/document'),
            data: {},
            success: function (result) {
                if (result.resultCode == 1) {
                    console.log(result);
                    var html = "";
                    for (var i = 0; i < result.data.length; i++) {
                        html += '<option value="' + result.data[i] + '">' + result.data[i] + '</option>'
                    }
                    $("#documents").append(html);
                    layui.form.render();
                }
            }
        })
    },
    editMessage: (data) => {
        let content = data.content
        // layer.prompt({title: '请输入文本', formType: 2, content: content}, function (value, index, elem) {
        //     if (value === '') return elem.focus();
        //     layer.msg('获得：' + value); // 显示 value
        //     // 关闭 prompt
        //     layer.close(index);
        // });
        layer.prompt({
            formType: 2,
            value: content,
            title: '请输入文本',
            area: ['500px', '100px'] //自定义文本域宽高
        }, function (value, index, elem) {
            // alert(value); //得到value
            // layer.close(index);
            console.log(value);
            Common.invoke({
                url: request('/console/editChatMsg'),
                data: {
                    // messageId: data.messageId,
                    // sender: data.sender,
                    // toUserId: data.receiver,
                    // message: value,
                    type: 1,
                    sender: data.sender,
                    toUserId: data.receiver,
                    messageId: data.messageId,
                    message: value,
                },
                success: function (result) {
                    if (result.resultCode == 1) {
                        layer.msg("更新成功", {"icon": 1});
                        layer.close(index);
                        // 刷新table
                        Common.tableReload(currentCount, currentPageIndex, 0, "message_table");
                        //layui.table.reload("message_table");
                    }
                }
            });
        });
    },
    //解密消息内容
    decodeContent: function (d) {
        if (!Common.isNil(d.content)) {
            if (1 == d.encryptType) {
                //des解密
                var desContent = Common.decryptMsg(d.content, d.messageId, d.timeSend);
                if (desContent.search("https") != -1 || desContent.search("http") != -1) {
                    var link = "<a class='layui-btn layui-btn-xs font_bule' target='_blank' href=\"" + desContent + "\">预览</a>";
                    return link;
                } else {
                    return desContent;
                }
            } else if (2 == d.encryptType) {
                // aes解密
                let testContent = Common.decryptAESMessage(d.messageId, d.content);
                console.log("aes testContent :" + testContent);
                return testContent;
            }
            var text = (Object.prototype.toString.call(d.content) === '[object Object]' ? JSON.stringify(d.content) : d.content)
            try {
                if (text.search("https") != -1 || text.search("http") != -1) {
                    var link = "<a class='layui-btn layui-btn-xs font_bule' target='_blank' href=\"" + text + "\">预览</a>";
                    return link;
                } else {
                    return text;
                }
            } catch (e) {
                return text;
            }
        } else {
            return "";
        }
    },
    //预览文本内容
    showText: function (d) {
        debugger
        var html = '<div class="wrap">';
        html += '<div style="padding: 2%;"><label class="layui-text layui-word-aux" style="padding: 0 19px!important;">' + (Object.prototype.toString.call(d.content) === '[object Object]' ? JSON.stringify(d.content) : d.content) + '</label></div>';
        html += '</div>';

        //弹出层
        layer.open({
            type: 1,
            title: "内容预览",
            offset: ['50px'],
            area: ['700px'],
            shadeClose: true,
            shade: false,
            maxmin: true, //开启最大化最小化按钮
            content: html
        });
    },

    //获取单聊总页数
    charCount: function () {
        if (count > 0) {
            debugger
            layui.use(['laypage', 'layer'], function () {
                var laypage = layui.laypage, layer = layui.layer;
                //完整功能
                laypage.render({
                    elem: 'demo7'
                    , count: count
                    , groups: 3 //只显示 1 个连续页码
                    , first: false //不显示首页
                    , last: false //不显示尾页
                    , limit: Common.limit
                    , limits: Common.limits
                    , layout: ['prev', 'page', 'next', 'limit', 'count', 'skip']
                    , jump: function (obj) {
                    }
                });
            });
        } else {
            $.ajax({
                type: "POST",
                url: request("/console/chat_logs_all/count"),
                data: {
                    sender: Common.getValueForElement("#sender")
                    , receiver: Common.getValueForElement("#receiver")
                    , keyWord: Common.getValueForElement("#keyWord")
                },
                dataType: 'json',
                async: true,
                success: function (result) {
                    count = result.data;
                    layui.use(['laypage', 'layer'], function () {
                        var laypage = layui.laypage, layer = layui.layer;
                        //完整功能
                        laypage.render({
                            elem: 'demo7'
                            , count: count
                            , groups: 3 //只显示 1 个连续页码
                            , first: false //不显示首页
                            , last: false //不显示尾页
                            , limit: Common.limit
                            , limits: Common.limits
                            , layout: ['prev', 'page', 'next', 'limit', 'count', 'skip']
                            , jump: function (obj) {
                                if (isReferTable == 1) {
                                    layui.table.reload("message_table", {
                                        url: request("/console/chat_logs_all"),
                                        limit: obj.limit,
                                        page: {
                                            curr: obj.curr
                                        },
                                        where: {
                                            sender: Common.getValueForElement("#sender")  //搜索的关键字
                                            , receiver: Common.getValueForElement("#receiver")
                                            , keyWord: Common.getValueForElement("#keyWord")
                                        }
                                    });
                                }
                                isReferTable = 1;

                                $(".search_message").removeAttr("disabled");
                                $(".search_message").removeClass("layui-btn-disabled");
                            }
                        });
                    });
                }
            })
        }
    },
    /**
     * 刷新多选删除
     */
    refreshMushDelete: function () {
        chooseDeleteCount = 0;
        $(".mustDelete").hide();
        userIds = [];
    }

}
// 删除一个月前的单聊聊天记录
$(".deleteMonthLogs").on("click", function () {

    layer.confirm('确定删除一个月前的单聊聊天记录？', {icon: 3, title: '提示信息', skin: "layui-ext-motif"}, function (index) {

        Common.invoke({
            url: request('/console/deleteChatMsgs'),
            data: {
                'type': 1
            },
            successMsg: "删除成功",
            errorMsg: "删除失败,请稍后重试",
            success: function (result) {
                if (1 == result.resultCode) {
                    layui.table.reload("message_table");
                }
            },
            error: function (result) {
            }
        });

    });

});
// 删除最近十万条之前的日志
$(".deleteThousandAgoLogs").on("click", function () {

    layer.confirm('确定删除十万条之前的单聊聊天记录？', {icon: 3, title: '提示信息', skin: "layui-ext-motif"}, function (index) {

        Common.invoke({
            url: request('/console/deleteChatMsgs'),
            data: {
                'type': 2
            },
            successMsg: "删除成功",
            errorMsg: "删除失败,请稍后重试",
            success: function (result) {
                if (1 == result.resultCode) {
                    layui.table.reload("message_table");
                }
            },
            error: function (result) {
                layui.layer.alert("数量小于等于100000")
            }
        });

    });

});
$(function () {
    Msg.findTigaseDocuments();
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})

