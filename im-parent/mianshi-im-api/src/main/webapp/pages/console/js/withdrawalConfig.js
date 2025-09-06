var page = 0;
var lock = 0;
var userIds = new Array();
var toUserIds = new Array();
var messageIds = new Array();
var nickName;
var userId;
var regeditPhoneOrName;
var currentPageIndex;// 当前页码数
var currentCount;// 当前总数
var tableData;//存放分页数据
var pattern = new RegExp("[`~！@#$^&*()=|{}':;',\\[\\].<>/?~！@#￥……&*（）——|{}【】‘；：”“'。，、？%]");
var chains = /[^\x00-\xff]/ig; //中文符号正则表达式
var update_user = 0;
var datas = {};
var resourceData = {};

layui.use(['jquery', 'form', 'layer', 'laydate', 'table', 'laytpl'], function () {
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;


    //角色列表
    var tableInUser = table.render({
        elem: '#config_list'
        , toolbar: '#toolbarRole'
        , url: request("/console/withdrawalConfig")
        , id: 'config_list'
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
            {field: 'name', title: '提现方式按钮名称', sort: 'true', width: 300}
            , {field: 'type', title: '是否启用', sort: 'true', width: 300,templet : function (d) {
                    if(d.type ==1)
                        return '是'
                    return  '否'
                }}
            , {fixed: 'right', width: 300, title: "操作", align: 'left', toolbar: '#userListBar'}
        ]]
        , done: function (res, curr, count) {
            checkRequst(res);

            //权限判断
            var arr = ['role1-add', 'role1-delete', 'role1-update', 'role1-resource'];
            manage.authButton(arr);

            /*  if(count==0&&lock==1){
                  layer.msg("暂无数据",{"icon":2});
                  renderTable();
              }*/

            var pageIndex = tableInUser.config.page.curr;//获取当前页码
            var resCount = res.count;// 获取table总条数
            currentCount = resCount;
            currentPageIndex = pageIndex;
        }

    });

    //列表操作
    table.on('tool(config_list)', function (obj) {
        var layEvent = obj.event,
            data = obj.data;

        if (layEvent === 'update') {
            //修改角色
            Manage.showRoleDesc(obj.data);
        } else if (layEvent === 'delete') {
            //删除角色
            layer.confirm('确认要删除吗？', {
                btn: ['确定', '取消']
                , skin: "layui-ext-motif"
            }, function (index) {
                $.ajax({
                    type: "POST",
                    url: request("/console/withdrawalDelConfig"),
                    data: {
                        id: obj.data.id
                    },
                    dataType: 'json',
                    async: false,
                    success: function (result) {
                        layer.msg("删除成功！", {icon: 1});
                        obj.del();
                    }
                })
            });
        } else if (layEvent === 'lookAuth') {
            //获取该角色所有资源
            $.ajax({
                type: "POST",
                url: request("/console/queryRoleResourceInfo"),
                data: {
                    roleId: obj.data.roleId
                },
                dataType: 'json',
                async: false,
                success: function (result) {
                    var data = result.data;
                    var html = '<table cellspacing="2" cellpadding="2" border="0" class="layui-table">';
                    html += '<tr><th>资源名称</th><th>资源路径</th><th>资源权限</th></tr>'
                    for (var i = 0; i < data.length; i++) {
                        html += '<tr><td>' + data[i].resourceName + '</td><td>' + data[i].resourceUrl + '</td><td>' + data[i].resourceAuth + '</td></tr>'
                    }
                    html += ' </table>';

                    layui.layer.open({
                        title: '角色权限'
                        , skin: 'layui-ext-motif'
                        , type: 1
                        , offset: 'auto'
                        , area: ['800px', '500px']
                        , content: html
                    });

                }
            })
        } else if (layEvent === 'resource') {
            //查看角色权限
            localStorage.setItem("start_page", "/pages/console/roleManage.html");
            localStorage.setItem("role_add_resource_roleId", data.roleId);
            window.location.href = "/pages/console/roleAddResource.html";
        } else if (layEvent === 'query') {
            //查看该角色下的用户
            var url = "";
            var roleName = obj.data.roleName;
            if (roleName === '机器人') {
                url = "/pages/console/robot.html";
            } else if (roleName === '客服') {
                url = "/pages/console/customer.html";
            } else if (roleName === '财务') {
                url = "/pages/console/finance.html";
            } else if (roleName === '游客') {
                url = "/pages/console/tourist.html";
            } else if (roleName === '普通管理员') {
                url = "/pages/console/admin.html";
            } else if (roleName === '公众号') {
                url = "/pages/console/publicNumber.html";
            } else {
                localStorage.setItem("addNewRoleUser_role", obj.data.role);//角色类型
                localStorage.setItem("addNewRoleUser_roleId", obj.data.roleId);//角色编号
                url = "/pages/console/newRoleList.html";
            }

            var index = layer.open({
                type: 2,
                area: ['90%', '90%'],
                content: url
            });
            /* layer.full(index);*/

        }
    });
});


$(function () {
    $("#roleTable_list").show();
    $("#add_role").hide();
    $("#update_role").hide();
    $("#desc_role").hide();

    //返回按钮
    $(".back").click(function () {
        $('#title').empty();
        $("#title").append("角色列表");
        $("#roleTable_list").show();
        $(".visitPathDiv").show();
        $("#user_table").show();
        $("#update_role").hide();
        $("#add_role").hide();
        $("#desc_role").hide();
        $(".visitPathDiv").show()
        $("#role_table").show()
        $("#title").hide()

    })

    //获取资源信息
    $.ajax({
        type: "POST",
        url: request("/console/querySelectAllResource"),
        data: {},
        dataType: 'json',
        async: false,
        success: function (result) {
            resourceData = result.data;
        }
    })
})


var Manage = {
    //添加角色
    addRole: function () {
        /*$("#title").show();
        $('#title').empty();
        $("#title").append("添加角色");
        $("#roleTable_list").hide();
        $("#add_role").show();
        $("#update_role").hide();
        $("#desc_role").hide();
        $(".visitPathDiv").hide()
        $("#role_table").hide()*/

        layui.layer.open({
            title: "添加提现方式",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            btn: ["确定", "取消"],
            area: ['700px', '300px'],
            offset: 'auto',
            shadeClose: true, //点击遮罩关闭
            content: $("#add_role"),
            yes: function (index, layero) {
                Manage.submitAddRole();
                layer.close(index);
            }
        });

    }
    , descRole: function () {
        $("#title").show();
        $('#title').empty();
        $("#title").append("角色详情");
        $("#roleTable_list").hide();
        $("#add_role").hide();
        $("#update_role").hide();
        $("#desc_role").show();
    }
    , submitAddRole: function () {
        //添加角色表单提交
        var roleName = $("#roleName").val();
        if (roleName === "") {
            layer.msg('提现方式按钮名称不能为空！', {icon: 5});
            return;
        }

        var roleDesc = $(".isSaveRequestLogs").val();
        if (roleDesc === "") {
            layer.msg('是否启用不能为空！', {icon: 5});
            return;
        }




        $.ajax({
            type: "POST",
            url: request("/console/withdrawalAddConfig"),
            data: {
                name: roleName,
                type: roleDesc
            },
            dataType: 'json',
            async: false,
            success: function (result) {
                if (result.resultCode == 1) {
                    layer.msg("添加成功！", {"icon": 1});
                    setTimeout(function () {
                        window.location.href = "/pages/console/withdrawalConfig.html";
                    }, 1500);
                } else {
                    layer.msg(result.resultMsg, {"icon": 5});
                }
            }
        })
    },
    showRoleDesc: function (data) {
        //展示修改数据
        $("#roleName2").val(data.name);
        $(".isSaveRequestLogs2").val(data.type +"");
        $("#update-id").val(data.id);

        layui.layer.open({
            title: "修改提现方式",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            btn: ["确定", "取消"],
            area: ['700px', '320px'],
            offset: 'auto',
            shadeClose: true, //点击遮罩关闭
            content: $("#update_role"),
            yes: function (index, layero) {
                Manage.submitUpdateRole();
                layer.close(index);
            }
        });
    },
    submitUpdateRole: function () {
        //修改角色表单提交
        var roleName = $("#roleName2").val();
        if (roleName === "") {
            layer.msg('提现方式按钮名称不能为空！', {icon: 5});
            return;
        }

        var roleDesc = $(".isSaveRequestLogs2").val();
        if (roleDesc === "") {
            layer.msg('是否启用不能为空！', {icon: 5});
            return;
        }

        var roleId = $("#update-id").val();

        $.ajax({
            type: "POST",
            url: request("/console/withdrawalUpdateConfig"),
            data: {
                id: roleId,
                name: roleName,
                type: roleDesc
            },
            dataType: 'json',
            async: false,
            success: function (result) {
                if (result.resultCode == 1) {
                    layer.msg("修改成功！", {"icon": 1});
                    setTimeout(function () {
                        window.location.href = "/pages/console/withdrawalConfig.html";
                    }, 1500);
                } else {
                    layer.msg("修改失败，名称有重复！", {"icon": 5});
                }
            }
        })
    }
}


//重新渲染表单
function renderForm() {
    //use加载所需模块
    layui.use('form', function () {
        var form = layui.form;//高版本建议把括号去掉，有的低版本，需要加()
        form.render();
    });
}


//重新渲染表单
function renderTable() {
    layui.use('table', function () {
        var table = layui.table;//高版本建议把括号去掉，有的低版本，需要加()
        // table.reload("user_list");
        table.reload("user_list", {
            page: {
                curr: 1 //重新从第 1 页开始
            },
            where: {
                onlinestate: $("#status").val(),// 在线状态
                keyWord: Common.getValueForElement(".nickName")  //搜索的关键字
            }
        })
    });
}

$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})
