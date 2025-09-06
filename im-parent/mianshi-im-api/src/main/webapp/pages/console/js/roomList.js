var page = 0;
var lock = 0;
var messageIds = new Array();
var userIds = new Array();
var roomJid;
var roomId;
var roomName;
var consoleAdmin = localStorage.getItem("account");
var currentPageIndex;// 群聊天记录中的当前页码数
var currentCount;// 群聊天记录中的当前总数
var roomControl = new Object();// 群控制消息
var checker_ids = new Array();//缓存勾选合并群组Id
var checker_userId;//缓存勾选合并群主userId
var dead_line;//缓存勾选合并群主userId
layui.config({
    base: "/pages/common/dropdown/"
}).use(['form', 'layer', 'laydate', 'table', 'laytpl', 'dropdown'], function () {
    var form = layui.form, layer = parent.layer === undefined ? layui.layer : top.layer, $ = layui.jquery,
        laydate = layui.laydate, laytpl = layui.laytpl, table = layui.table, dropdown = layui.dropdown;
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
    //群组列表
    var tableInRoom = table.render({
        elem: '#room_table',
        url: request("/console/roomList"),
        id: 'room_table',
        toolbar: '#toolbarDemo',
        page: {
            layout: ['prev', 'page', 'next', 'limit', 'count', 'skip'] //自定义分页布局
            , groups: 3 //只显示 1 个连续页码
            , first: false //不显示首页
            , last: false //不显示尾页
        },
        curr: 0,
        limit: Common.limit,
        limits: Common.limits,
        groups: 7,
        cols: [[ //表头
            {type: 'checkbox'}, {field: 'name', title: '群组名称', width: '13%'}, {
                field: 'desc',
                title: '群组说明',
                width: '13%'
            }, {field: 'userId', title: '创建者Id', width: '12%'}, {
                field: 'nickname',
                title: '创建者昵称',
                width: '12%'
            }, {field: 'userSize', title: '群人数', width: '12%'}, {
                field: 's', title: '状态', width: '12%', templet: function (d) {
                    if (1 == d.s) {
                        return "正常";
                    } else {
                        return "被封锁";
                    }
                }
            }, {
                field: 'isSecretGroup', title: '是否为私密群组', width: '12%', templet: function (d) {
                    if (1 == d.isSecretGroup) {
                        return "私密群组";
                    } else {
                        return "普通群组";
                    }
                }
            }, {
                field: 'needPay', title: '是否为付费群组', width: '12%', templet: function (d) {
                    if (1 == d.needPay) {
                        return "付费群组";
                    } else {
                        return "免费群组";
                    }
                }
            }, {
                field: 'createTime', title: '创建时间', width: '15%', templet: function (d) {
                    return UI.getLocalTime(d.createTime);
                }
            }, {fixed: 'right', width: '25%', title: "操作", align: 'center', toolbar: '#roomListBar'}]],
        done: function (res, curr, count) {
            checkRequst(res);
            //开启下拉菜单
            dropdown.suite();
            //权限判断
            var arr = ['room-delete', 'room-chatRecord', 'room-randUser', 'room-member', 'room-sendMsg', 'room-msgCount', 'room-modifyConf', 'room-locking', 'room-add'];
            manage.authButton(arr);

            //获取零时保留的值
            var last_value = $("#roomList_limlt").val();
            //获取当前每页大小
            var recodeLimit = tableInRoom.config.limit;
            //设置零时保留的值
            $("#roomList_limlt").val(recodeLimit);
            //判断是否改变了每页大小数
            if (last_value != recodeLimit) {
                // 刷新
                table.reload("room_table", {
                    url: request("/console/roomList"), page: {
                        curr: 1 //重新从第 1 页开始
                    }
                })
            }

            $(".group_name").val("");
            $(".leastNumbers").val("");
            $(".keyWord").addClass("keyWord");

            if (count == 0 && lock == 1) {
                layer.msg("暂无数据", {"icon": 2});
                renderTable();
            }
            lock = 0;
            /*if(localStorage.getItem("role")==1 || localStorage.getItem("role")==4){
                $(".btn_addRoom").hide();
                $(".member").hide();
                $(".randUser").hide();
                $(".modifyConf").hide();
                $(".msgCount").hide();
                $(".sendMsg").hide();
                $(".del").hide();
                $(".deleteMonthLogs").hide();
                $(".deleteThousandAgoLogs").hide();
                $(".locking").hide();
                $(".cancelLocking").hide();
                //$(".chatRecord").hide();
            }else if(localStorage.getItem("role")==1 || localStorage.getItem("role")==7){
                $(".btn_addRoom").hide();
                $(".member").hide();
                $(".randUser").hide();
                $(".modifyConf").hide();
                $(".msgCount").hide();
                $(".sendMsg").hide();
                $(".del").hide();
                $(".deleteMonthLogs").hide();
                $(".deleteThousandAgoLogs").hide();
                $(".locking").hide();
                $(".cancelLocking").hide();
                $(".chatRecord").hide();
            }*/
            var pageIndex = tableInRoom.config.page.curr;//获取当前页码
            var resCount = res.count;// 获取table总条数
            currentCount = resCount;
            currentPageIndex = pageIndex;


            //禁止全选按钮操作
            $('th[data-field="0"] .layui-table-cell').html('');

            //设置默认勾选
            for (var i = 0; i < res.data.length; i++) {
                if (checker_ids.includes(res.data[i].id)) {
                    res.data[i]["LAY_CHECKED"] = 'true';
                    var index = res.data[i]['LAY_TABLE_INDEX'];
                    $('.layui-table tr[data-index=' + index + '] input[type="checkbox"]').prop('checked', true);
                    $('.layui-table tr[data-index=' + index + '] input[type="checkbox"]').next().addClass('layui-form-checked');
                }
                ;
            }
        }
    });


    $("#room_table_div").show();
    $("#roomMsgList").hide();
    $("#roomUserList").hide();
    $("#pushToRoom").hide();
    $("#addRandomUser").hide();
    $("#updateRoom").hide();
    $("#addRoom").hide();

    //监听多选框
    table.on('checkbox(room_table)', function (obj) {
        var flag = obj.checked;
        if (flag) {
            checker_userId = checker_ids.length == 0 ? obj.data.userId : checker_userId;
            checker_ids.includes(obj.data.id) == false ? checker_ids.push(obj.data.id) : "";
        } else {
            checker_ids.remove(obj.data.id);
        }
    })

    //群组列表工具栏事件
    table.on('toolbar(room_table)', function (obj) {
        var name = obj.event;

        if (name == "mergeRoom") {
            if (checker_ids.length < 2) {
                layer.msg("请选择两个或两个以上的群组进行合并", {icon: 5});
                return;
            }
            Room.mergeRoom(checker_ids.join(","), checker_userId);
        }
    });

    //列表操作
    table.on('tool(room_table)', function (obj) {
        var layEvent = obj.event, data = obj.data;
        if (layEvent === 'chatRecord') { //聊天记录
            $(".keyWord").css("display", "inline");
            roomJid = data.jid;
            // 查询聊天记录
            Room.baseUIHander();
            $(".keyWord").show();
            $(".keyWord_sender").show();
            $(".keyWord_sender").css("display", "inline");
            $(".search_keyWord").show();
            $(".deleteMonthLogs").show();
            $(".deleteThousandAgoLogs").show();
            var tableInsRoom = table.render({
                elem: '#room_msg',
                toolbar: '#toolbarGroupMessageList',
                url: request("/console/groupchat_logs_all") + "&room_jid_id=" + data.jid,
                id: 'room_msg',
                page: {
                    layout: ['prev', 'page', 'next', 'limit', 'count', 'skip'] //自定义分页布局
                    , groups: 3 //只显示 1 个连续页码
                    , first: false //不显示首页
                    , last: false //不显示尾页
                },
                curr: 0,
                limit: Common.limit,
                limits: Common.limits,
                groups: 7,
                cols: [[ //表头
                    {type: 'checkbox', fixed: 'left'}// 多选
                    , {field: 'room_jid', title: '房间JId', width: 220}, {
                        field: 'sender',
                        title: '发送者Id',
                        width: 100
                    }, {field: 'fromUserName', title: '发送者', width: 220}, {
                        field: 'type', title: '消息类型', width: 220, templet: function (d) {
                            return Common.msgType(d.contentType);
                        }
                    }, {
                        field: 'timeSend', title: '发送时间', width: 220, templet: function (d) {
                            return UI.getLocalTime(d.timeSend / 1000);
                        }
                    }, {
                        field: 'content', title: '内容', width: 350, templet: function (d) {
                            var conetene = Room.decodeConetene(d);
                            return conetene;
                        }
                    }
                    /*,{field: 'deleteTime', title: '保留截止时间',width: 170,templet: function(d){
                        if(0 < d.deleteTime)
                            return UI.getLocalTime(d.deleteTime);
                        else return"";
                    }}*/, {fixed: 'right', width: 150, title: "操作", align: 'left', toolbar: '#roomMessageListBar'}]],
                done: function (res, curr, count) {
                    checkRequst(res);
                    $("#roomMsgList").show();
                    $("#room_table_div").hide();
                    var pageIndex = $(".layui-laypage-limits").find("option:selected").val();//获取当前页码
                    var resCount = res.count;// 获取table总条数
                    currentCount = resCount;
                    currentPageIndex = pageIndex;
                }
            });

        } else if (layEvent === 'member') { //成员管理
            // console.log(JSON.stringify(data))
            Room.baseUIHander();
            roomId = data.id;
            roomName = data.name;
            var tableInsMember = table.render({
                elem: '#room_user',
                toolbar: '#toolbarMembers',
                url: request("/console/roomUserManager") + "&id=" + data.id,
                id: 'room_user',
                page: {
                    layout: ['prev', 'page', 'next', 'limit', 'count', 'skip'] //自定义分页布局
                    , groups: 3 //只显示 1 个连续页码
                    , first: false //不显示首页
                    , last: false //不显示尾页
                },
                curr: 0,
                limit: Common.limit,
                limits: Common.limits,
                groups: 7,
                cols: [[ //表头
                    {type: 'checkbox', fixed: 'left'}// 多选
                    , {field: 'userId', title: '成员UserId', width: 200}, {
                        field: 'nickname',
                        title: '成员昵称',
                        width: 220
                    }, {
                        field: 'role', title: '成员角色', width: 120, templet: function (d) {
                            if (d.role == 1) {
                                return "群主";
                            } else if (d.role == 2) {
                                return "管理员";
                            } else if (d.role == 3) {
                                return "成员";
                            } else if (d.role == 4) {
                                return "隐身人";
                            } else if (d.role == 5) {
                                return "监控人";
                            }
                        }
                    }, {
                        field: 'offlineNoPushMsg', title: '是否屏蔽消息', width: 170, templet: function (d) {
                            return (d.offlineNoPushMsg == 0 ? "否" : "是");
                        }
                    }, {
                        field: 'onlinestate', title: '在线状态', width: 105, templet: function (d) {
                            return (d.onlinestate == 0 ? "离线" : "在线");
                        }
                    }, {field: 'ipAddress', title: '客户端ip地址', width: 170}, {
                        field: 'loginTime', title: '最后上线时间', width: 170, templet: function (d) {
                            if (Common.isNil(d.loginTime)) {
                                return "";
                            } else {
                                return UI.getLocalTime(d.loginTime);
                            }
                        }
                    }, {
                        field: 'createTime', title: '加群时间', width: 170, templet: function (d) {
                            return UI.getLocalTime(d.createTime);
                        }
                    }, {
                        field: 'deadLine', title: '到期时间', width: 170, templet: function (d) {
                            return UI.getLocalTime1(d.deadLine);
                        }
                    }, {fixed: 'right', width: 180, title: "操作", align: 'left', toolbar: '#roomMemberListBar'}]],
                done: function (res, curr, count) {
                    checkRequst(res);
                    $("#roomUserList").show();
                    $("#room_table_div").hide();
                    $(".room_btn_div").hide();
                    $(".visitPathDiv").hide();
                    $("#save_roomId").val(data.id);
                    var pageIndex = tableInsMember.config.page.curr;//获取当前页码
                    var resCount = res.count;// 获取table总条数
                    currentCount = resCount;
                    currentPageIndex = pageIndex;
                }
            });

        } else if (layEvent === 'randUser') { //添加随机用户
            /* Room.baseUIHander();*/
            Room.addRandomUser(data.id);

        } else if (layEvent === 'modifyConf') { //修改配置
            Room.updateRoom(data.id);

        } else if (layEvent === 'msgCount') { //消息统计
            Room.loadGroupMsgCount(data.jid);

        } else if (layEvent === 'sendMsg') { //发送消息
            Room.baseUIHander();
            Room.pushToRoom(data.id, data.jid);
        } else if (layEvent === 'locking') { // 锁定群组
            Room.lockIng(consoleAdmin, data.id, -1);
        } else if (layEvent === 'cancelLocking') {// 解锁
            Room.lockIng(consoleAdmin, data.id, 1);
        } else if (layEvent === 'del') { //删除
            layer.confirm('确认要删除吗？', {
                btn: ['确定', '取消']//按钮
                , skin: "layui-ext-motif"
            }, function (index) {
                layer.close(index);
                Room.deleteRoom(data.id, obj, localStorage.getItem("account"));
                obj.del();
            });
        } else if (layEvent === 'msgRecord') {
            //聊天记录
            //打开聊天面板
            layui.layer.open({
                title: "", skin: 'layui-ext-motif', type: 1, shade: false, area: ['1000px', '800px'], shadeClose: true, //点击遮罩关闭
                content: $("#mp_chatPanel"), cancel: function (index, layero) { //关闭聊天面板后执行
                    layui.layer.close(index)
                    //清空页面上的聊天消息
                    $("#mp_chatPanel #messageContainer").empty();
                }, success: function (layero, index) {  //弹窗打开成功后的回调
                    $(".nano").nanoScroller();

                    chat.createMsg(chat.getChatRoomMessage(data.jid));

                    setTimeout(function () { //将滚动条移动到最下方
                        $(".nano").nanoScroller();//刷新滚动条
                        chat.scrollToEnd(); //滚动到底部
                    }, 400);
                }
            });

        } else if (layEvent == 'destroyRoomMessage') {
            layer.confirm('确定销毁群组聊天记录？', {
                btn: ['确定', '取消']
            }, function () {
                Common.invoke({
                    url: request('/console/destroyRoomMessage'), data: {
                        jids: data.jid,
                    }, success: function (result) {
                        if (result.resultCode == 1) {
                            layer.msg("销毁群组聊天记录成功", {"icon": 1});
                        }
                    }
                })
            });
        }
    });


    table.on('tool(room_user)', function (obj) {
        var layEvent = obj.event, data = obj.data;
        if (layEvent === 'deleteMember') { // 删除群成员
            Room.toolbarMembersImpl($("#save_roomId").val(), data.userId, 1);
        } else if (layEvent === 'setDeadLine') {
            layui.layer.open({
                title: "设置期限",
                type: 1,
                area: ['500px', '230px'],
                shade: 0,
                content: $("#set_dead_line"),
                btn: ['确定', '返回'],
                yes: function (index, latero) {
                    if (!dead_line) {
                        layui.layer.alert("期限不能为空");
                    }
                    console.log("期限:" + dead_line);
                    Room.setDeadLine($("#save_roomId").val(), data.userId, dead_line, function () {
                        layui.layer.close(index);
                    })
                },
                btn1: function (index, layero) {
                    layui.layer.close(index);
                },
                cancel: function (index) {
                    layui.layer.close(index);
                }
            })
        }
    });
    // 删除消息、编辑消息
    table.on('tool(room_msg)', function (obj) {
        var layEvent = obj.event, data = obj.data;

        if (layEvent === 'deleteMessage') { //聊天记录
            layer.confirm('确定删除指定群聊聊天记录', {
                icon: 3, title: '提示消息', yes: function () {
                    Common.invoke({
                        url: request('/console/groupchat_logs_all/del'), data: {
                            msgId: data._id, room_jid_id: data.room_jid
                        }, success: function (result) {
                            if (result.resultCode == 1) {
                                layer.msg("删除成功", {"icon": 1});
                                messageIds = [];
                                obj.del();
                            }
                        }
                    })
                }, btn2: function () {
                    messageIds = [];
                }, cancel: function () {
                    messageIds = [];
                }
            });
        }
        if (layEvent === 'editMessage') { //聊天记录
            let content = data.content
            // layer.prompt({title: '请输入文本', formType: 2, content: content}, function (value, index, elem) {
            //     if (value === '') return elem.focus();
            //     layer.msg('获得：' + value); // 显示 value
            //     // 关闭 prompt
            //     layer.close(index);
            // });
            layer.prompt({
                formType: 2, value: content, title: '请输入文本', area: ['500px', '100px'] //自定义文本域宽高
            }, function (value, index, elem) {
                // alert(value); //得到value
                // layer.close(index);
                console.log(value);
                Common.invoke({
                    url: request('/console/editChatMsg'), data: {
                        // msgId: data.messageId, room_jid_id: data.room_jid, content: value,
                        type: 2,
                        sender: data.sender,
                        messageId: data.messageId,
                        message: value,
                        roomJid: data.room_jid
                    }, success: function (result) {
                        if (result.resultCode == 1) {
                            layer.msg("更新成功", {"icon": 1});
                            layer.close(index);
                            // 刷新table
                            layui.table.reload("room_msg");
                        }
                    }
                });
            });
        }
    });

    //搜索
    $(".search_group").on("click", function () {
        if ($(".group_name").val().indexOf("*") != -1) {
            layer.alert("不支持*号搜索");
            return
        }
        // 关闭超出宽度的弹窗
        $(".layui-layer-content").remove();
        // 校验群人数
        var numbers = $(".leastNumbers").val();
        if (null != numbers && "" != numbers && undefined != numbers) {
            var reg = /^[0-9]\d*$/;
            if (!reg.test(numbers)) {
                layer.alert("请输入有效的群人数");
                return;
            }
        }

        table.reload("room_table", {
            url: request("/console/roomList"), page: {
                curr: 1 //重新从第 1 页开始
            }, where: {
                keyWorld: Common.getValueForElement(".group_name"),  //搜索的关键字
                leastNumbers: Common.getValueForElement(".leastNumbers"),
                isSecretGroup: Common.getValueForElement("#status")
            }
        })
        lock = 1;
    });

    //关键字聊天记录搜索
    $(".search_keyWord").on("click", function () {
        // 关闭超出宽度的弹窗
        $(".layui-layer-content").remove();
        table.reload("room_msg", {
            url: request("/console/groupchat_logs_all") + "&room_jid_id=" + roomJid, page: {
                curr: 1 //重新从第 1 页开始
            }, where: {
                keyWord: Common.getValueForElement(".keyWord"), sender: Common.getValueForElement(".keyWord_sender")
            }
        });
        $(".keyWord").val("");
        lock = 1;
    });
    //日期范围
    laydate.render({
        elem: '.dead_line', type: 'date', lang: 'zh', done: function (value, date, endDate) {  // choose end
            //console.log("date callBack====>>>"+value); //得到日期生成的值，如：2017-08-18
            dead_line = value;
        }, min: 0
    });
})

//重新渲染表单
function renderTable() {
    layui.use('table', function () {
        var table = layui.table;//高版本建议把括号去掉，有的低版本，需要加()
        // table.reload("user_list");
        table.reload("room_table", {
            page: {
                curr: 1 //重新从第 1 页开始
            }, where: {
                keyWorld: Common.getValueForElement(".group_name"),  //搜索的关键字
                leastNumbers: Common.getValueForElement(".leastNumbers")
            }
        })
    });
}

var button = '<button onclick="Room.button_back()" class="layui-btn table_default_btn" style="margin-top: 35px;margin-left: 50px;"><<返回</button>';


var Room = {
    //解密消息内容
    decodeConetene: function (d) {
        if (!Common.isNil(d.content)) {
            if (1 == d.isEncrypt && localStorage.getItem("role") == 6) {
                var desContent = Common.decryptMsg(d.content, d.messageId, d.timeSend);
                if (desContent.search("https") != -1 || desContent.search("http") != -1) {
                    var link = "<a target='_blank' href=\"" + desContent + "\">" + desContent + "</a>";
                    return link;
                } else {
                    return desContent;
                }
            } else {
                var text = (Object.prototype.toString.call(d.content) === '[object Object]' ? JSON.stringify(d.content) : d.content)
                try {
                    if (text.search("https") != -1 || text.search("http") != -1) {
                        var link = "<a target='_blank' href=\"" + text + "\">" + text + "</a>";
                        return link;
                    } else {
                        return text;
                    }
                } catch (e) {
                    return text;
                }
            }
        } else {
            return "";
        }
    }, // 新增群组
    addRoom: function () {
        //弹窗
        layer.open({
            title: "新增群主",
            type: 1,
            btn: ['确定', '返回'],
            area: ['500px', '230px'],
            shade: [0.5, "#393D49"],
            skin: 'layui-ext-motif',
            anim: 2,
            shadeClose: true,
            content: $("#addRoom"),
            yes: function (index, latero) {
                Room.commit_addRoom();
                layui.layer.close(index);
            }
        })
    }, // 提交新增群组
    commit_addRoom: function () {
        if ($("#add_roomName").val() == "") {
            layer.msg("请输入群名称", {"icon": 5});
            return;
        } else if ($("#add_desc").val() == "") {
            layer.msg("请输入群说明", {"icon": 5});
            return;
        }
        Common.invoke({
            url: request('/console/addRoom'), data: {
                userId: localStorage.getItem("account"),// 让当前登录后台管理系统的系统管理员创建房间
                name: Common.getValueForElement("#add_roomName"), desc: Common.getValueForElement("#add_desc")
            }, success: function (result) {
                if (result.resultCode == 1) {
                    layer.msg("新增成功", {"icon": 1});
                    $("#roomList").show();
                    $("#roomMsgList").hide();
                    $("#addRoom").hide();
                    $("#add_roomName").val("");
                    $("#add_desc").val("");
                    // UI.roomList(0);
                    layui.table.reload("room_table", {
                        page: {
                            curr: 1 //重新从第 1 页开始
                        }, where: {}
                    })
                }
            }
        });
    },

    // 删除群组
    deleteRoom: function (id, obj, userId) {
        $.ajax({
            type: 'POST', url: request('/console/deleteRoom'), data: {
                roomId: id, userId: userId
            }, async: false, success: function (result) {
                checkRequst(result);
                if (result.resultCode == 1) {
                    layer.alert("删除成功");
                }
                if (result.resultCode == 0) {
                    layer.alert(result.resultMsg);
                }
            },

        })
    },

    // 群成员管理
    roomUserList: function (e, roomId) {
        html = "";
        if (e == 1) {
            if (page > 0) {
                page--;
            }
        } else if (e == 2) {
            if (sum < 10) {
                layui.layer.alert("已是最后一页");
            } else {
                page++;
            }
        }
        Common.invoke({
            url: request('/console/roomUserManager'), data: {
                id: roomId, pageIndex: page
            }, success: function (result) {
                if (result.data.pageData != null) {
                    sum = result.data.pageData.length;
                    for (var i = 0; i < result.data.pageData.length; i++) {

                        html += "<tr><td>" + result.data.pageData[i].userId + "</td><td>" + result.data.pageData[i].nickname + "</td><td>" + result.data.pageData[i].role + "</td><td>" + (result.data.pageData[i].offlineNoPushMsg == 0 ? "否" : "是") + "</td><td>" + UI.getLocalTime(result.data.pageData[i].createTime) + "</td><td><button onclick='Room.deleteMember(\"" + roomId + "\",\"" + result.data.pageData[i].userId + "\")' class='layui-btn layui-btn-danger layui-btn-xs'>删除</button></td></tr>";
                    }
                    var tab = "<a href='javascript:void(0);' onclick='Room.roomUserList(1,\"" + roomId + "\")' class='layui-laypage-prev layui-disabled' data-page='0'>上一页</a>" + "<a href='javascript:void(0);' onclick='Room.roomUserList(2,\"" + roomId + "\")' class='layui-laypage-next' data-page='2'>下一页</a>";
                    $("#roomUser_table").empty();
                    $("#roomUser_table").append(html);
                    $("#roomUserList_div").empty();
                    $("#roomUserList_div").append(tab);
                    $("#room_table_div").hide();
                    $("#roomMsgList").hide();
                    $("#roomUserList").show();
                    $("#back").empty();
                    $("#back").append(button);

                }
            }
        })
    }, // 删除群成员
    deleteMember: function (roomId, userId, obj) {
        layer.confirm('确定删除该群成员？', {icon: 3, title: '提示信息', skin: "layui-ext-motif"}, function (index) {
            Common.invoke({
                url: request('/console/deleteMember'), data: {
                    userId: userId, roomId: roomId
                }, success: function (result) {
                    if (result.resultCode == 1) {

                        layer.alert("删除成功");
                        obj.del();
                        // Room.roomUserList(0,roomId);
                    }
                }
            })
        })

    }, setDeadLine: function (roomId, userId, deadLine, callback) {
        Common.invoke({
            url: request('/console/setMemberDeadline'), data: {
                roomId: roomId, userId: userId, deadLine: deadLine
            }, success: function (result) {
                if (result.resultCode == 1) {
                    layer.msg("设置成功", {"icon": 1});
                    userIds = [];
                    // renderTable();
                    Common.tableReload(currentCount, currentPageIndex, 1, "room_user");
                    // layui.table.reload("room_user");
                } else if (result.resultCode == 0) {
                    layui.table.reload("room_user");
                    layer.msg(result.resultMsg);
                }
            },

        })
        callback();
    }, // 批量移出群成员
    toolbarMembers: function () {
        // 多选操作
        var checkStatus = layui.table.checkStatus('room_user'); //idTest 即为基础参数 id 对应的值
        console.log("新版：" + checkStatus.data) //获取选中行的数据
        console.log("新版：" + checkStatus.data.length) //获取选中行数量，可作为是否有选中行的条件
        console.log("新版：" + checkStatus.isAll) //表格是否全选
        for (var i = 0; i < checkStatus.data.length; i++) {
            userIds.push(checkStatus.data[i].userId);
        }
        if (0 == checkStatus.data.length) {
            layer.msg("请勾选要移出的行");
            return;
        }
        console.log(userIds);
        Room.toolbarMembersImpl($('#save_roomId').val(), userIds.join(","), checkStatus.data.length);
    },

    toolbarMembersImpl: function (roomId, userId, checkLength) {
        layer.confirm('确定移出指定群成员', {
            icon: 3, title: '提示消息', skin: "layui-ext-motif", yes: function () {
                Common.invoke({
                    url: request('/console/deleteMember'), data: {
                        roomId: roomId, userId: userId, adminUserId: localStorage.getItem("account")
                    }, success: function (result) {
                        if (result.resultCode == 1) {
                            layer.msg("移出成功", {"icon": 1});
                            userIds = [];
                            // renderTable();
                            Common.tableReload(currentCount, currentPageIndex, checkLength, "room_user");
                            // layui.table.reload("room_user");
                        } else if (result.resultCode == 0) {
                            layui.table.reload("room_user");
                            layer.msg(result.resultMsg);
                        }
                    },

                })
            }, btn2: function () {
                userIds = [];
            }, cancel: function () {
                userIds = [];
            }
        });
    },


    // 批量删除群成员（等同于批量删除用户）
    toolbarDeleteMembers: function () {
        // 多选操作
        var checkStatus = layui.table.checkStatus('room_user'); //idTest 即为基础参数 id 对应的值
        console.log("新版：" + checkStatus.data) //获取选中行的数据
        console.log("新版：" + checkStatus.data.length) //获取选中行数量，可作为是否有选中行的条件
        console.log("新版：" + checkStatus.isAll) //表格是否全选
        for (var i = 0; i < checkStatus.data.length; i++) {
            userIds.push(checkStatus.data[i].userId);
        }
        if (0 == checkStatus.data.length) {
            layer.msg("请勾选要删除的行");
            return;
        }
        console.log(userIds);
        Room.toolbarDeleteMembersImpl(userIds.join(","));
    },

    toolbarDeleteMembersImpl: function (userId) {
        layer.confirm('确定删除指定群成员用户,<br>删除后该系统会注销此用户', {
            icon: 3, title: '提示消息', yes: function () {
                Common.invoke({
                    url: request('/console/deleteUser'), data: {
                        userId: userId
                    }, success: function (result) {
                        if (result.resultCode == 1) {
                            layer.msg("删除成功", {"icon": 1});
                            userIds = [];
                            // renderTable();
                            layui.table.reload("room_user");
                        } else if (result.resultCode == 0) {
                            layer.msg(result.resultMsg);
                        }
                    },

                })
            }, btn2: function () {
                userIds = [];
            }, cancel: function () {
                userIds = [];
            }
        });
    },

    // 导出群成员
    exprotExcelByMember: function () {
        var requestUrl = request('/console/exportExcelByGroupMember');
        layui.layer.open({
            title: '数据导出',
            skin: 'layui-ext-motif',
            type: 1,
            offset: 'auto',
            area: ['300px', '140px'],
            btn: ['导出', '取消'],
            content: '<form class="layui-form" style="padding-left: 20px;" method="post" action="' + requestUrl + '">' + '<input type="hidden" name="roomId" value=' + roomId + '>' + '<label>导出群组 "' + roomName + '" 的群成员列表</label>' + '<button id="exportGroupMember_submit"  class="layui-btn" type="submit" lay-submit="" style="display:none">导出</button>' + '</from>',
            success: function (index, layero) {
                layui.form.render();
            },
            yes: function (index, layero) {
                $("#exportGroupMember_submit").click();
                layui.layer.close(index); //关闭弹框
            },
            btn2: function (index, layero) {
                //按钮【取消】的回调

                //return false 开启该代码可禁止点击该按钮关闭
            }

        });
    },

    // 刷新table
    reloadTable: function () {
        // 刷新父级页面
        parent.layui.table.reload("room_user")
    },

    // 邀请用户加入群组 inviteJoinRoom
    inviteJoinRoom: function () {
        console.log("joinRoom :       " + roomId);
        localStorage.setItem("roomId", roomId);
        layer.open({
            title: "", type: 2, skin: 'layui-layer-rim', //加上边框
            area: ['1500px', '780px'], //宽高
            content: 'inviteJoinRoom.html', success: function (index, layero) {

            }
        });
    },

    // 群发消息
    pushToRoom: function (id, jid) {
        var html = "";
        $("#room_table_div").hide();
        $("#pushToRoom").show();
        $("#push_roomJid").val(jid);
        Common.invoke({
            url: request('/console/getRoomMember'), data: {
                roomId: id
            }, success: function (result) {
                debugger
                if (result.data != null) {
                    for (var i = 0; i < result.data.members.length; i++) {
                        if (result.data.members[i].role < 3) {
                            html += "<option value='" + Common.filterHtmlData(result.data.members[i].userId) + "'>" + Common.filterHtmlData(result.data.members[i].nickname) + "</option>";
                        }
                    }
                    $("#push_sender").empty();
                    $("#push_sender").append(html);
                    $("#back").empty();
                    $("#back").append(button);
                    layui.form.render();
                }
            }
        })
    },

    // 群组锁定解锁
    lockIng: function (userId, roomId, status) {
        var confMsg, successMsg = "";
        (status == -1 ? confMsg = '确定封锁该群组？' : confMsg = '确定解封该群组？');
        (status == -1 ? successMsg = "封群成功" : successMsg = "解封群组成功");
        layer.confirm(confMsg, {icon: 3, skin: "layui-ext-motif", title: '提示信息'}, function (index) {

            Common.invoke({
                url: request('/console/updateRoom'), data: {
                    userId: userId, roomId: roomId, s: status
                }, successMsg: successMsg, errorMsg: "加载数据失败，请稍后重试", success: function (result) {
                    layui.table.reload("room_table")
                }, error: function (result) {
                }
            });
        })
    }, // 发送
    commit_push: function () {
        if (null == $("#push_context").val() || "" == $("#push_context").val()) {
            layer.alert("请输入要发送内容");
            return;
        }

        Common.invoke({
            url: request('/console/sendMsg'), data: {
                jidArr: Common.getValueForElement("#push_roomJid"),
                userId: Common.getValueForElement("#push_sender"),
                type: 1,
                content: Common.getValueForElement("#push_context")
            }, success: function (result) {
                layer.alert("发送成功");
                $("#push_context").val("")
            }
        })
    }, // 添加随机用户
    addRandomUser: function (roomId) {
        layui.layer.open({
            title: "添加机器人",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            btn: ["确定", "取消"],
            area: ['500px', '227px'],
            offset: 'auto',
            shadeClose: true,
            content: $("#addRandomUser"),
            success: function (layero, index) {
                $("#roomId").val(Common.filterHtmlData(roomId));
                layui.form.render();
            },
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            },
            yes: function (index, layero) {
                Room.commit_addRandomUser();
                layer.close(index);
                layui.table.reload("room_table", {
                    page: {
                        curr: $(".layui-laypage-em").next().html(), //重新从当前页开始
                    }
                });
            }
        });
    }, commit_addRandomUser: function () {
        Common.invoke({
            url: request('/console/autoCreateUser'), async: false, data: {
                userNum: Common.getValueForElement("#addRandomUserSum"), roomId: $("#roomId").val()
            }, success: function (result) {
                if (result.resultCode == 1) {
                    layer.msg("添加成功", {icon: 1});
                }
            }
        })
    }, // 修改群配置
    updateRoom: function (roomId) {
        $(".room_btn_div").hide();
        Common.invoke({
            url: request('/console/getRoomMember'), data: {
                roomId: roomId
            }, success: function (result) {
                if (result.data != null) {
                    roomControl = result.data;// 群控制参数
                    $("#updateRoom_id").val(result.data.id);
                    $("#update_userId").html(result.data.userId);
                    $("#update_roomId").html(result.data.id);
                    $("#update_roomJid").html(result.data.jid);
                    $("#name").val(result.data.name);
                    $("#desc").val(result.data.desc);
                    $("#chatRecordTimeOut").val(result.data.chatRecordTimeOut);
                    $("#maxUserSize").val(result.data.maxUserSize);
                    $("#isLook").val(result.data.isLook);
                    $("#showRead").val(result.data.showRead);
                    $("#isNeedVerify").val(result.data.isNeedVerify);
                    $("#showMember").val(result.data.showMember);
                    $("#allowSendCard").val(result.data.allowSendCard);
                    // $("#allowHostUpdate").val(result.data.allowHostUpdate);
                    $("#allowInviteFriend").val(result.data.allowInviteFriend);
                    $("#allowUploadFile").val(result.data.allowUploadFile);
                    $("#allowConference").val(result.data.allowConference);
                    $("#allowSpeakCourse").val(result.data.allowSpeakCourse);
                    $("#isAttritionNotice").val(result.data.isAttritionNotice);
                    $("#roomTitleUrl").val(result.data.roomTitleUrl);
                    $("#adminMaxNumber").val(result.data.adminMaxNumber);
                    // 渲染复选框
                    layui.form.render();

                    $("#room_table_div").hide();
                    $("#updateRoom").show();
                    $("#updateRoom1").show();

                    $("#back").empty();
                    $("#back").append(button);
                }

            }
        })
    },

    // 多选删除群组聊天记录
    toolbarGroupMessageList: function () {
        // 多选操作
        var checkStatus = layui.table.checkStatus('room_msg'); //idTest 即为基础参数 id 对应的值
        console.log("新版：" + checkStatus.data) //获取选中行的数据
        console.log("新版：" + checkStatus.data.length) //获取选中行数量，可作为是否有选中行的条件
        console.log("新版：" + checkStatus.isAll) //表格是否全选
        var userId;
        for (var i = 0; i < checkStatus.data.length; i++) {
            messageIds.push(checkStatus.data[i]._id);
            roomJid = checkStatus.data[i].room_jid
        }
        console.log("roomJid" + roomJid + "------" + messageIds);
        if (0 == checkStatus.data.length) {
            layer.msg("请勾选要删除的行");
            return;
        }
        Room.toolbarGroupMessageListImpl(roomJid, messageIds.join(","), checkStatus.data.length);
    },

    toolbarGroupMessageListImpl: function (room_jid_id, messageId, checkLength) {
        layer.confirm('确定删除指定群聊聊天记录', {
            icon: 3, title: '提示消息', skin: "layui-ext-motif", yes: function () {
                Common.invoke({
                    url: request('/console/groupchat_logs_all/del'), data: {
                        msgId: messageId, room_jid_id: room_jid_id
                    }, success: function (result) {
                        if (result.resultCode == 1) {
                            layer.msg("删除成功", {"icon": 1});
                            messageIds = [];
                            // Common.tableReload(currentCount,currentPageIndex,"room_msg")
                            Common.tableReload(currentCount, currentPageIndex, checkLength, "room_msg");
                        }
                    }
                })
            }, btn2: function () {
                messageIds = [];
            }, cancel: function () {
                messageIds = [];
            }
        });
    },

    // 返回
    button_back: function () {
        $(".roomStatus").show();
        $(".group_name").show();
        $(".leastNumbers").show();
        $(".search_group").show();
        $(".btn_addRoom").show();
        $(".deleteMonthLogs").hide();
        $(".deleteThousandAgoLogs").hide();
        $(".keyWord").hide();
        $("#room_table_div").show();
        $("#roomList").show();
        $("#chatGroupType").show();
        $(".search_keyWord").hide();
        $("#roomMsgList").hide();
        $("#roomUserList").hide();
        $(".keyWord_sender").hide();


        $("#addRoom").hide();
        $("#pushToRoom").hide();
        $("#addRandomUser").hide();
        $("#updateRoom").hide();
        $("#updateRoom1").hide();
        $("#back").empty();
        $("#back").append("&nbsp;");
        $(".visitPathDiv").show();
        $(".room_btn_div").show();
        layui.table.reload("room_table");
    }, /**
     * 首界面菜单栏
     */
    baseUIHander: function () {
        $(".group_name").hide();
        $(".leastNumbers").hide();
        $(".group_type").hide();
        $(".search_group").hide();
        $(".btn_addRoom").hide();
        $("#room_table_div").hide();
        $("#chatGroupType").hide();
        $(".roomStatus").hide();
        $(".visitPathDiv").hide();
        $(".room_btn_div").hide();
    }, //加载群聊聊消息统计数据
    loadGroupMsgCount: function (roomJId, startDate, endDate, timeUnit) {

        if (roomJId == null || roomJId == "" || roomJId == undefined) {

            return;
        }
        Common.invoke({
            url: request('/console/groupMsgCount'), data: {
                roomId: roomJId, startDate: startDate, endDate: endDate, timeUnit: timeUnit
            }, successMsg: false, errorMsg: "加载数据失败，请稍后重试", success: function (result) {

                var data = result.data;
                //基于准备好的dom，初始化echarts实例
                var groupMsgSumCount = echarts.init(document.getElementById('groupMsgCount'), 'shine');

                // 使用刚指定的配置项和数据显示图表。
                groupMsgSumCount.setOption(option = {
                    title: {
                        text: '群聊统计图'
                    }, tooltip: {
                        trigger: 'axis'
                    }, xAxis: {
                        data: data.map(function (item) {
                            for (var time in item) {
                                return time;
                            }
                        })
                    }, yAxis: {
                        splitLine: {
                            show: false
                        }
                    }, toolbox: {
                        left: 'center', feature: {
                            dataZoom: {
                                yAxisIndex: 'none'
                            }, restore: {}, saveAsImage: {}
                        }
                    }, dataZoom: [{
                        startValue: '2014-06-01'
                    }, {
                        type: 'inside'
                    }], visualMap: {
                        top: 10, right: 10, pieces: [{
                            gt: 0, lte: 50, color: '#096'
                        }, {
                            gt: 50, lte: 100, color: '#ffde33'
                        }, {
                            gt: 100, lte: 150, color: '#ff9933'
                        }, {
                            gt: 150, lte: 200, color: '#cc0033'
                        }, {
                            gt: 200, lte: 300, color: '#660099'
                        }, {
                            gt: 300, color: '#7e0023'
                        }], outOfRange: {
                            color: '#999'
                        }
                    }, series: {
                        name: '群聊消息数量', type: 'line', data: data.map(function (item) {
                            for (var time in item) {
                                return item[time]
                            }
                        })
                    }
                });


                //群聊消息统计图
                layui.layer.open({
                    title: "",
                    skin: 'layui-ext-motif',
                    type: 1,
                    shade: false,
                    area: ['700px', '450px'],
                    shadeClose: true, //点击遮罩关闭
                    content: $("#groupMsgChart"),
                    cancel: function (index, layero) {
                        //if(confirm('确定要关闭么')){ //只有当点击confirm框的确定时，该层才会关闭
                        layer.close(index)
                        $("#groupMsgChart").hide();
                        // }
                        return false;
                    },
                    success: function (layero, index) {  //弹窗打开成功后的回调
                        //layui.form.render();

                        layui.form.render('select');

                        //日期范围
                        layui.laydate.render({
                            elem: '#groupMsgDate', range: "~", done: function (value, date, endDate) {  // choose end
                                //console.log("date callBack====>>>"+value); //得到日期生成的值，如：2017-08-18
                                var startDate = value.split("~")[0];
                                var endDate = value.split("~")[1];
                                var timeUnit = $(".groupMsg_time_unit").val()

                                Room.loadGroupMsgCount(roomJId, startDate, endDate, timeUnit);

                            }, max: 1
                        });

                        //时间单位切换
                        layui.form.on('select(groupMsg_time_unit)', function (data) {
                            var dateRange = $("#groupMsgDate").val();
                            var startDate = dateRange.split("~")[0];
                            var endDate = dateRange.split("~")[1];

                            Room.loadGroupMsgCount(roomJId, startDate, endDate, data.value);

                        });


                    }


                });


            }, errorCb: function (result) {

            }
        });

    }, /**
     * 合并群组
     * roomIds：群组编号
     * userId：群主编号
     */
    mergeRoom: function () {
        if (checker_ids.length < 2) {
            layer.msg("请选择两个或两个以上的群组进行合并", {icon: 5});
            return;
        }
        var roomIds = checker_ids.join(",");
        var userId = checker_userId;

        //弹窗
        layer.open({
            title: "合并群主",
            type: 1,
            btn: ['确定', '返回'],
            area: ['500px', '230px'],
            shade: [0.5, "#393D49"],
            skin: 'layui-ext-motif',
            anim: 2,
            shadeClose: true,
            content: $("#mergeRoom"),
            yes: function (index, latero) {
                var roomName = $("#merge_roomName").val();
                if (Common.isNil(roomName)) {
                    layer.msg("请输入新群组名称", {"icon": 2});
                    return;
                }
                Room.mergeRoomFromSubmit(roomIds, userId, roomName);
                layui.layer.close(index);
            }
        });
    }, mergeRoomFromSubmit: function (roomIds, userId, roomName) {
        Common.invoke({
            url: request('/console/mergeRoom'), data: {
                'roomIds': roomIds, 'userId': userId, 'roomName': roomName
            }, successMsg: "合并群组成功", errorMsg: "合并群组失败,请稍后重试", success: function (result) {
                if (1 == result.resultCode) {
                    layui.table.reload("room_table");
                    checker_ids.splice(0, checker_ids.length);
                    checker_userId = "";
                }
            }, error: function (result) {
            }
        });
    }
}
// 删除一个月前的日志
$(".deleteMonthLogs").on("click", function () {
    layer.confirm('确定删除一个月前的群聊聊天记录？', {icon: 3, title: '提示信息', skin: "layui-ext-motif"}, function (index) {
        Common.invoke({
            url: request('/console/groupchatMsgDel'), data: {
                'roomJid': roomJid, 'type': 0
            }, successMsg: "删除成功", errorMsg: "删除失败,请稍后重试", success: function (result) {
                if (1 == result.resultCode) {
                    layui.table.reload("room_msg");
                }
            }, error: function (result) {
            }
        });

    });

});
// 删除最近十万条之前的日志
$(".deleteThousandAgoLogs").on("click", function () {
    layer.confirm('确定删除十万条之前的群聊聊天记录？', {icon: 3, skin: "layui-ext-motif", title: '提示信息'}, function (index) {
        Common.invoke({
            url: request('/console/groupchatMsgDel'), data: {
                'roomJid': roomJid, 'type': 1
            }, successMsg: "删除成功", errorMsg: "删除失败,请稍后重试", success: function (result) {
                if (1 == result.resultCode) {
                    layui.table.reload("room_msg");
                }
            }, error: function (result) {
                layui.layer.alert("数量小于等于100000")
            }
        });

    });

});

// 修改群属性
function updateGroupConfig(userId, roomId, roomName, desc, maxUserSize, roomTitleUrl, withdrawTime, adminMaxNumber, callback) {
    console.log("userId：" + userId + "---" + "roomId：" + roomId + "---" + "roomName：" + roomName + "---" + "desc：" + desc + "---" + "maxUserSize：" + maxUserSize + "---roomTitleUrl:" + roomTitleUrl);
    if (!Common.isNil(roomTitleUrl)) {
        try {
            if (!Common.isJSON(roomTitleUrl)) {
                layer.alert("请按照配置实例，配置格式正确的数据");
                return;
            }
            if (4 < Common.JSONLength(JSON.parse(roomTitleUrl))) {
                layer.alert("最多配置四个网站地址");
                return;
            }
        } catch (e) {
            layer.alert("请按照配置实例，配置格式正确的数据");
            return;
        }
    }
    if (!Common.isNil(withdrawTime)) {
        // 输入正整数且大于三十分钟
        if (!(/(^[0-9]\d*$)/.test(withdrawTime))) {
            layui.layer.alert("请输入正确的撤回消息的保留时长！");
            return false;
        } else {
            // 至少半个消息
            if (0 != withdrawTime && withdrawTime < 1800) {
                layui.layer.alert("撤回消息的保留时长最少为半个小时！");
                return false;
            }
        }
    }
    Common.invoke({
        url: request('/console/updateRoom'), data: {
            "userId": userId,
            "roomId": roomId,
            "roomName": (null == roomName ? null : roomName),
            "desc": (null == desc ? null : desc),
            "maxUserSize": (null == maxUserSize ? null : maxUserSize),
            "roomTitleUrl": (null == roomTitleUrl ? null : roomTitleUrl),
            "withdrawTime": (null == withdrawTime ? -1 : withdrawTime),
            "adminMaxNumber": (null == adminMaxNumber ? -1 : adminMaxNumber),

        }, successMsg: "修改成功", errorMsg: "修改失败，请稍后重试", success: function (result) {
            callback();
        }, error: function (result) {

        }
    });
}

// 修改群控制
function updateConfig(userId, roomId, paramName, paramVal, callback) {
    console.log("userId：" + userId + "---" + "roomId：" + roomId + "---" + "paramName：" + paramName + "---" + "paramVal：" + paramVal);
    var newParamName = paramName;
    console.log("参数名称：" + newParamName);
    obj = {
        "userId": userId, "roomId": roomId
    }
    obj[newParamName] = paramVal;
    Common.invoke({
        url: request('/console/updateRoom'),
        data: obj,
        successMsg: "修改成功",
        errorMsg: "修改失败，请稍后重试",
        success: function (result) {
            callback();
        },
        error: function (result) {

        }
    });
}

// 最新的群配置

// 修改群名称
$("#name").mousedown(function () {
    var oldName = Common.getValueForElement("#name");
    $("#name").attr("disabled", "disabled");
    layui.layer.open({
        title: "群组名称修改",
        type: 1,
        btn: ["确定", "取消"],
        area: ['310px'],
        content: '<div id="changePassword" style="padding: 10px 10px;">' + '<input type="text" value="' + oldName + '" required  lay-verify="required" placeholder="新的群组名称" autocomplete="off" class="layui-input changeRoomName">' + '</div>',
        yes: function (index, layero) { //确定按钮的回调
            $('#name').attr("disabled", false);
            var roomId = Common.filterHtmlData($("#update_roomId").html());
            var roomName = Common.getValueForElement(".changeRoomName");
            updateGroupConfig(localStorage.getItem("account"), roomId, roomName, null, null, null, null, null, function () {
                layui.layer.close(index); //关闭弹框
                $("#name").val(roomName);
            })
        },
        btn2: function () {
            $('#name').attr("disabled", false);
        },
        cancel: function (index, layero) {
            $('#name').attr("disabled", false);
            layer.close(index)
        }
    });

    $(".changePassword").focus();
});
// 修改群描述
$("#desc").mousedown(function () {
    $("#desc").attr("disabled", "disabled");
    var oldDesc = Common.getValueForElement("#desc");
    layui.layer.open({
        title: "群组描述修改",
        skin: 'layui-ext-motif',
        type: 1,
        btn: ["确定", "取消"],
        area: ['310px'],
        content: '<div id="changePassword" style="padding: 10px 10px;">' + '<input type="text" value="' + Common.filterHtmlData(oldDesc) + '" required  lay-verify="required" placeholder="新的群组描述" autocomplete="off" class="layui-input changeDesc">' + '</div>',
        yes: function (index, layero) { //确定按钮的回调
            $('#desc').attr("disabled", false);
            var roomId = $("#update_roomId").html();
            var desc = $(".changeDesc").val();
            if (Common.isNil(desc)) {
                layer.alert("群描述不能为空");
                return;
            }
            updateGroupConfig(localStorage.getItem("account"), roomId, null, desc, null, null, null, null, function () {
                layui.layer.close(index); //关闭弹框
                $("#desc").val(desc);
            })
        },
        btn2: function () {
            $('#desc').attr("disabled", false);
        },
        cancel: function (index, layero) {
            $('#desc').attr("disabled", false);
            layer.close(index)
        }
    });
    $(".changeDesc").focus();
});

// 修改群最大人数
$("#maxUserSize").mousedown(function () {
    $("#maxUserSize").attr("disabled", "disabled");
    var oldMaxUserSize = $("#maxUserSize").val();
    layui.layer.open({
        title: "群组最大人数修改",
        skin: 'layui-ext-motif',
        type: 1,
        btn: ["确定", "取消"],
        area: ['310px'],
        content: '<div id="changePassword" style="padding: 10px 10px;">' + '<input type="text" value="' + oldMaxUserSize + '" required lay-verify="required" placeholder="请输入群最大人数" autocomplete="off" class="layui-input changeMaxnum">' + '</div>',
        yes: function (index, layero) { //确定按钮的回调
            $('#maxUserSize').attr("disabled", false);
            var roomId = $("#update_roomId").html();
            var changeMaxnum = $(".changeMaxnum").val();
            if (changeMaxnum > 10000) {
                layui.layer.alert("最高上限10000人")
                return;
            }
            updateGroupConfig(localStorage.getItem("account"), roomId, null, null, changeMaxnum, null, null, null, function () {
                layui.layer.close(index); //关闭弹框
                $("#maxUserSize").val(changeMaxnum);
            })
        },
        btn2: function () {
            $('#maxUserSize').attr("disabled", false);
        },
        cancel: function (index, layero) {
            $('#maxUserSize').attr("disabled", false);
            layer.close(index)
        }
    });
    $("#changeMaxnum").focus();
});

// 修改群最大管理员人数
$("#adminMaxNumber").mousedown(function () {
    $("#adminMaxNumber").attr("disabled", "disabled");
    var oldMaxUserSize = $("#adminMaxNumber").val();
    layui.layer.open({
        title: "群组最大管理员人数修改",
        skin: 'layui-ext-motif',
        type: 1,
        btn: ["确定", "取消"],
        area: ['310px'],
        content: '<div id="changePassword" style="padding: 10px 10px;">' + '<input type="text" value="' + oldMaxUserSize + '" required lay-verify="required" placeholder="请输入群最大管理员人数" autocomplete="off" class="layui-input changeMaxnum">' + '</div>',
        yes: function (index, layero) { //确定按钮的回调
            $('#adminMaxNumber').attr("disabled", false);
            var roomId = $("#update_roomId").html();
            var changeMaxnum = $(".changeMaxnum").val();
            if (changeMaxnum > maxCrowdNumber) {
                layui.layer.alert("管理员人数不能超过群组最大人数")
                return;
            }
            updateGroupConfig(localStorage.getItem("account"), roomId, null, null, null, null, null, changeMaxnum, function () {
                layui.layer.close(index); //关闭弹框
                $("#adminMaxNumber").val(changeMaxnum);
            })
        },
        btn2: function () {
            $('#adminMaxNumber').attr("disabled", false);
        },
        cancel: function (index, layero) {
            $('#adminMaxNumber').attr("disabled", false);
            layer.close(index)
        }
    });
    $("#changeMaxnum").focus();
});

// 修改群组网址
$("#roomTitleUrl").mousedown(function () {
    $("#roomTitleUrl").attr("disabled", "disabled");
    var oldRoomTitleUrl = $("#roomTitleUrl").val().toString();
    layui.layer.open({
        title: "群组网址修改",
        skin: 'layui-ext-motif',
        type: 1,
        btn: ["确定", "取消"],
        area: ['310px'],
        content: '<div id="changePassword" style="padding: 10px 10px;">' + '<input type="text"  value=' + oldRoomTitleUrl + ' required  lay-verify="required" placeholder="新的群组网址" autocomplete="off" class="layui-input changeDesc">' + '</div>',
        yes: function (index, layero) { //确定按钮的回调
            $('#roomTitleUrl').attr("disabled", false);
            var roomId = $("#update_roomId").html();
            var desc = $(".changeDesc").val();
            updateGroupConfig(localStorage.getItem("account"), roomId, null, null, null, desc, null, null, function () {
                layui.layer.close(index); //关闭弹框
                $("#roomTitleUrl").val(desc);
            })
        },
        btn2: function () {
            $('#roomTitleUrl').attr("disabled", false);
        },
        cancel: function (index, layero) {
            $('#roomTitleUrl').attr("disabled", false);
            layer.close(index)
        }
    });
    $(".changeDesc").focus();
});

// 消息撤回的删除时间
$("#withdrawTime").mousedown(function () {
    $("#withdrawTime").attr("disabled", "disabled");
    var oldDesc = Common.getValueForElement("#withdrawTime");
    layui.layer.open({
        title: "群组消息撤回的删除时间",
        skin: 'layui-ext-motif',
        type: 1,
        btn: ["确定", "取消"],
        area: ['310px'],
        content: '<div id="changePassword" style="padding: 10px 10px;">' + '<input type="text" value="' + Common.filterHtmlData(oldDesc) + '" required  lay-verify="required" placeholder="" autocomplete="off" class="layui-input changeDesc">' + '</div>',
        yes: function (index, layero) { //确定按钮的回调
            $('#withdrawTime').attr("disabled", false);
            var roomId = $("#update_roomId").html();
            var desc = $(".changeDesc").val();
            updateGroupConfig(localStorage.getItem("account"), roomId, null, null, null, null, desc, null, function () {
                layui.layer.close(index); //关闭弹框
                $("#withdrawTime").val(desc);
            })
        },
        btn2: function () {
            $('#withdrawTime').attr("disabled", false);
        },
        cancel: function (index, layero) {
            $('#withdrawTime').attr("disabled", false);
            layer.close(index)
        }
    });
    $(".changeDesc").focus();
});


// 群控制消息 lay-filter ： test
layui.form.on('select(test)', function (data) {
    var elemId = data.elem.id;
    var elemVal = data.elem.value;
    var paramValue = roomControl[data.elem.id];
    // 避免重复提交
    if (paramValue == data.elem.value) return; else {
        // 更新内存中的值
        roomControl[data.elem.id] = data.elem.value;
    }
    debugger;
    var roomId = $("#update_roomId").html();
    updateConfig(localStorage.getItem("account"), roomId, elemId, elemVal, function () {
    });
});




