var resourceData = {}
var roleResource = {}
$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
    //获取资源信息
    $.ajax({
        type: "POST",
        url: request("/console/query/all/resourceinfo"),
        data: {},
        dataType: 'json',
        async: false,
        success: function (result) {
            resourceData = result.data;
        }
    });

    //权限判断
    var arr = ['resourceInfo-add','resourceInfo-edit', 'resourceInfo-delete'];
    manage.authButton(arr);
});

layui.config({
    base: '/pages/console/'
}).extend({
    treeTable: 'treeTable/treeTable'
}).use(['layer', 'util', 'treeTable'], function () {
    var $ = layui.jquery;
    var layer = layui.layer;
    var util = layui.util;
    var treeTable = layui.treeTable;

    // 渲染表格
    var insTb = treeTable.render({
        elem: '#resource_table',
        data: resourceData,
        tree: {
            iconIndex: 0
        },
        cols: [
            {field: 'resourceName', title: '名称'},
            {field: 'resourceUrl', title: '资源路径'},
            {field: 'resourceAuth', title: '资源权限'},
            {
                field: 'type', title: '类型', templet: function (d) {
                    if (d.type == 0) {
                        return "按钮";
                    } else if (d.type == 1) {
                        return "菜单";
                    } else if (d.type == 2) {
                        return "头部菜单"
                    }
                }
            },
            {
                field: 'status', title: '状态', templet: function (d) {
                    if (d.type == -1) {
                        return "禁用";
                    } else {
                        return "正常";
                    }
                }
            },
            {fixed: 'right', title: "操作", align: 'left', toolbar: '#operationBar'}
        ],
        style: 'margin-top:0;'
    });

    //设置
    insTb.setChecked(roleResource);
    //设置表头为白色
    $(".layui-table thead tr")[0].style.backgroundColor = "#FAFAFA";
});


//返回按钮
$(".back").click(function () {
    $('#title').empty();
    $("#title").append("元素列表");
    $("#roleTable_list").show();
    $("#update_resource").hide();
    $(".bootstrap-table").show();
    $("#add_role_btn").show();
    $("#add_resource").hide();
})

var info = {
    //修改元素
    updateResource: function (id, pid) {
        layui.layer.open({
            title: "修改元素",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            btn: ["确定", "取消"],
            offset: 'auto',
            area: ['700px', '455px'],
            shadeClose: true,
            content: $("#update_resource"),
            success: function () {
                info.showResourceDesc(id);
            },
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            }
            , yes: function (index, layero) {
                info.submitFromUpdate(pid);
                layer.close(index);
            }
        });
    }
    , showResourceDesc: function (id) {
        $.ajax({
            type: "POST",
            url: request("/console/queryResourceInfoById"),
            data: {
                id: id
            },
            dataType: 'json',
            async: false,
            success: function (result) {
                if (result.resultCode == 1) {
                    $("#resourceId").val(result.data.id);
                    $("#resourceNameUp").val(result.data.resourceName);
                    $("#resourceUrlUp").val(result.data.resourceUrl);
                    $("#resourceAuthUp").val(result.data.resourceAuth);
                    $("#resourceTyepUp").find("option[name='" + result.data.type + "']").attr("selected", true);
                    $("#resourceTyepUp").val(result.data.type);
                    $("#resourceStatusUp").val(result.data.status);
                    layui.form.render();
                }
            }
        });
    }
    , submitFromUpdate: function (pid) {
        var resourceName = $("#resourceNameUp").val();
        if (resourceName === "") {
            layer.msg('元素名称不能为空！', {icon: 5});
            return;
        }
        var resourceUrl = $("#resourceUrlUp").val();
        if (resourceUrl === "") {
            layer.msg('元素路径不能为空！', {icon: 5});
            return;
        }
        var resourceAuth = $("#resourceAuthUp").val();

        var resourceType = $("#resourceTyepUp").val();
        if (resourceType === "") {
            layer.msg('请选择元素类型！', {icon: 5});
            return;
        }
        var resourceStatus = $("#resourceStatusUp").val();
        if (resourceStatus === "") {
            layer.msg('请选择元素状态！', {icon: 5});
            return;
        }

        $.ajax({
            type: "POST",
            url: request("/console/updateResourceInfo"),
            data: {
                resourceId: localStorage.getItem("update_respirce_id"),
                resourceName: resourceName,
                resourceUrl: resourceUrl,
                resourceAuth: resourceAuth,
                type: resourceType,
                status: resourceStatus,
                pid: pid
            },
            dataType: 'json',
            async: false,
            success: function (result) {
                localStorage.setItem("add_resource_pid", "");
                if (result.resultCode == 1) {
                    layer.msg("修改成功", {"icon": 1});
                    location.reload();
                } else {
                    layer.msg(result.resultMsg, {"icon": 5});
                }
            }
        })
    }
    //添加元素
    , addResource: function () {
        layui.layer.open({
            title: "添加元素",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            btn: ["确定", "取消"],
            offset: 'auto',
            area: ['700px', '447px'],
            shadeClose: true,
            content: $("#add_resource"),
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            }
            , yes: function (index, layero) {
                info.submitFromAdd("");
                layer.close(index);
            }
        });
    }
    , submitFromAdd: function (pid) {
        //添加元素表单提交
        var resourceName = $("#resourceName").val();
        var resourceAuth = $("#resourceAuth").val();
        var resourceType = $("#resourceType").val();
        var resourceUrl = $("#resourceUrl").val();
        var resourceStatus = $("#resourceStatus").val();


        if (Common.isNil(resourceName)) {
            layer.msg('元素名称不能为空！', {icon: 5});
            return;
        }

        if (Common.isNil(resourceUrl)) {
            layer.msg('元素路径不能为空！', {icon: 5});
            return;
        }

        if (Common.isNil(resourceType)) {
            layer.msg('请选择元素类型！', {icon: 5});
            return;
        }

        if (Common.isNil(resourceStatus)) {
            layer.msg('请选择元素状态！', {icon: 5});
            return;
        }
        $.ajax({
            type: "POST",
            url: request("/console/addResourceInfo"),
            data: {
                resourceName: resourceName,
                resourceUrl: resourceUrl,
                resourceAuth: resourceAuth,
                type: resourceType,
                status: resourceStatus,
                pid: pid
            },
            dataType: 'json',
            async: false,
            success: function (result) {
                localStorage.setItem("add_resource_pid", "");
                if (result.resultCode == 1) {
                    layer.msg("添加成功!", {"icon": 1});
                    location.reload();
                } else {
                    layer.msg(result.resultMsg, {"icon": 5});
                }
            }
        })
    }
    //添加子元素
    , addPidResource: function (id) {
        layui.layer.open({
            title: "添加元素",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            btn: ["确定", "取消"],
            offset: 'auto',
            area: ['700px', '447px'],
            shadeClose: true,
            content: $("#add_resource"),
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            }
            , yes: function (index, layero) {
                info.submitFromAdd(id);
                layer.close(index);
            }
        });
    }
    //删除元素
    , deleteResource: function (id, tablesIndex) {
        layer.confirm('确认要删除吗？', {
            btn: ['确定', '取消']
            , skin: "layui-ext-motif"
        }, function (index) {
            $.ajax({
                type: "POST",
                url: request("/console/delResourceInfo"),
                data: {
                    id: id
                },
                dataType: 'json',
                async: false,
                success: function (result) {
                    if (result.resultCode == 1) {
                        layer.msg("删除成功!", {icon: 1});
                        location.reload();
                    } else {
                        layer.msg(result.resultMsg, {icon: 2});
                    }

                }
            })
        });
    }
}
