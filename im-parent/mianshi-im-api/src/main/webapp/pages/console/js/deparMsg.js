var resourceData = {};
var roleResource = {};
$(function () {
    info.initTemplateUrl();
    //获取资源信息
    $.ajax({
        type: "POST",
        url: request("/console/find/department/list"),
        data: {
            companyId:localStorage.getItem("company_Id")
        },
        dataType: 'json',
        async: false,
        success: function (result) {
            resourceData = result.data;
        }
    });

    //添加公司下的公司部门
    $("#totalDep").click(function () {
        layui.layer.open({
            title: "添加部门",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            btn: ["确定", "取消"],
            area: ['700px', '200px'],
            offset: 'auto',
            shadeClose: true, //点击遮罩关闭
            content: $("#from_bm_add"),
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            }
            , yes: function (index, layero) {
                info.submitAddDeparment();
                layer.close(index);
            }
        });
        /*//设置当前部门编号为公司部门编号
        localStorage.setItem("chat_dep_id", localStorage.getItem("company_Id"));
        //设置操作
        localStorage.setItem("operate", 1);
        window.location.href = "/pages/console/staffMsg.html";*/
    })

    info.initTemplateUrl();
});


layui.config({
    base: '/pages/console/'
}).extend({
    treeTable: 'treeTable/treeTable'
}).use(['layer', 'util', 'treeTable','upload', 'element'], function () {
    var $ = layui.jquery;
    var layer = layui.layer;
    var util = layui.util;
    var upload = layui.upload;
    var element = layui.element;
    var treeTable = layui.treeTable;

    upload.render({
        elem: '#employeeUpload'
        ,url: request("/console/excel/importUserExcelData")
        ,accept: 'file'
        ,done: function(res){
            layer.msg(res.resultMsg,{icon:1});
            console.log(res);
        },
        error:function (error) {
            layer.msg(error.resultMsg,{icon:5});
        }
    });

    upload.render({
        elem: '#departmentUpload'
        ,url: request("/console/excel/importDepartmentExcelData")
        ,accept: 'file'
        ,done: function(res){
            layer.msg(res.resultMsg,{icon:1});
            console.log(res);
        },
        error:function (error) {
            layer.msg(error.resultMsg,{icon:5});
        }
    });

    // 渲染表格
    var insTb = treeTable.render({
        elem: '#demoTreeTable1',
        data: resourceData,
        tree: {
            iconIndex: 1
        },
        cols: [
            {type: 'numbers'},
            {field: 'name', title: '名称'},
            {field: 'role', title: '员工角色', templet: function (d) {
                    if (d.role == 0) {
                        return "普通员工";
                    } else if (d.role == 1) {
                        return "部门管理者";
                    } else if (d.role == 2){
                        return "公司管理员";
                    }else if (d.role == 3){
                        return "公司创建者";
                    }else {
                        return "";
                    }
                }
            },
            {field: 'position', title: '头衔'},
            {field: 'isCustomer', title: '客服',
                templet: function (d) {return d.isCustomer == -1 ? "" : d.isCustomer == 0 ? "否" : "是";}
            }
            ,{fixed: 'right',title:"操作", align:'left', toolbar: '#operationBar'}
        ],
        style: 'margin-top:0;'
    });

    //设置
    insTb.setChecked(roleResource);
    //设置表头为白色
    $(".layui-table thead tr")[0].style.backgroundColor = "white";
});

var info = {
    /*添加部门*/
    addDep: function (data) {
        layui.layer.open({
            title: "添加部门",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            btn: ["确定", "取消"],
            area: ['700px', '200px'],
            offset: 'auto',
            shadeClose: true, //点击遮罩关闭
            content: $("#from_bm_add"),
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            }
            , yes: function (index, layero) {
                info.submitAddDep(data);
                layer.close(index);
            }
        });
    },
    submitAddDep: function (id) {
        if ($("#departName").val() == null || $("#departName").val().length <= 0) {
            layer.msg("请输入部门名称！");
            return;
        }
        $.ajax({
            type: "POST",
            url: request("/console/add/deparment"),
            data: {
                companyId: Common.filterHtmlData(localStorage.getItem("company_Id")),
                parentId: Common.filterHtmlData(id),
                departName: Common.getValueForElement("#departName"),
                createUserId: Common.filterHtmlData(localStorage.getItem("adminId"))
            },
            dataType: 'json',
            async: false,
            success: function (result) {
                if (result.resultCode == 1) {
                    layer.msg("添加成功", {icon: 1})
                    location.reload();
                } else {
                    layer.msg(result.resultMsg);
                }
            },
            error: function (result) {
                layer.msg(result.resultMsg);
            }
        })
    },
    /*修改部门*/
    updateDep: function (data, name) {
        layui.layer.open({
            title: "修改部门信息",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            btn: ["确定", "取消"],
            area: ['700px', '300px'],
            offset: 'auto',
            shadeClose: true, //点击遮罩关闭
            content: $("#from_bm_update"),
            success: function (layero, index) {  //弹窗打开成功后的回调
                info.loadDepMsg(data, name);
                layui.form.render('select');
            },
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            }
            , yes: function (index, layero) {
                info.submitUpadteDep(data, name);
                layer.close(index);
            }
        });

    },
    loadDepMsg: function (id, name) {
        //加载部门页面信息
        $.ajax({
            type: "POST",
            url: request("/console/department/all"),
            data: {
                companyId: Common.filterHtmlData(localStorage.getItem("company_Id"))
            },
            dataType: 'json',
            async: true,
            success: function (result) {
                $("#newDepartName").val(name);
                if (result.data != null) {
                    /*渲染下拉框数据*/
                    for (var i = 0; i < result.data.length; i++) {
                        if (id != result.data[i].id) {
                            $("#moveDep").append("<option value='" + result.data[i].id + "'>" + Common.filterHtmlData(result.data[i].departName) + "</option>");
                        }
                    }
                    layui.form.render();
                }
            }
            , error: function (error) {

            }
        })
    },
    submitUpadteDep: function (id, name) {
        /*修改部门信息*/
        if ($("#newDepartName").val() == null || $("#newDepartName").val().length <= 0) {
            layer.msg("请输入部门名称！");
            return;
        }

        $.ajax({
            type: "POST",
            url: request("/console/update/department"),
            data: {
                //当前部门编号
                departmentId: Common.filterHtmlData(id),
                //修改的部门名称
                newDpartmentName: Common.filterHtmlData($("#newDepartName").val()),
                //移动到那个部门
                newDepId: Common.getValueForElement("#moveDep"),
                //旧部门名称
                oldDpartmentName: Common.filterHtmlData(name)
            },
            dataType: 'json',
            async: false,
            success: function (result) {
                if (result.resultCode == 1) {
                    layer.msg("修改成功", {icon: 1});
                    location.reload();
                } else {
                    layer.msg(result.resultMsg);
                }
            },
            error: function (result) {
                layer.msg(result.resultMsg);
            }
        })
    },
    /*移除部门*/
    deleteDep: function (data) {
        layer.confirm('确定删除该数据？', {
            btn: ['确定','取消'] //按钮
        }, function(){
            $.ajax({
                type: "POST",
                url: request("/console/delete/department"),
                data: {
                    departmentId: data
                },
                dataType: 'json',
                async: false,
                success: function (result) {
                    if (result.resultCode == 1) {
                        layer.msg("删除成功！");
                        location.reload();
                    } else {
                        layer.msg(result.resultMsg);
                    }
                },
                error: function (result) {
                    layer.msg("删除失败！");
                }
            });
        });

    },
    /*添加员工*/
    addEmp: function (data) {
        layui.layer.open({
            title: "添加员工",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            btn: ["确定", "取消"],
            area: ['700px', '300px'],
            offset: 'auto',
            shadeClose: true, //点击遮罩关闭
            content: $("#from_bm_addEmp"),
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            }
            , yes: function (index, layero) {
                info.submitAddEmp(data);
                layer.close(index);
            }
        });
    },
    submitAddEmp: function (id) {
        //不为空操作
        if ($("#bm-userId").val() == null || $("#bm-userId").val().length <= 0) {
            $("#bm-userId").val("")
            layer.msg("请输入用户编号！");
            return;
        }
        //判断是否是数字
        if (isNaN($("#bm-userId").val().trim())) {
            $("#bm-userId").val("")
            layer.msg("请输入正确的用户编号！");
            return;
        }

        $.ajax({
            type: "POST",
            url: request("/console/web/employee/add"),
            data: {
                telephone: Common.getValueForElement("#bm-userId"),
                companyId: Common.filterHtmlData(localStorage.getItem("company_Id")),
                departmentId: Common.filterHtmlData(id),
                role: Common.getValueForElement("#bm_role")
            },
            dataType: 'json',
            async: false,
            success: function (result) {
                if (result.resultCode == 1 && result.data == "101980") {
                    layer.msg("用户已存在！", {icon: 2});
                } else if (result.resultCode == 1) {
                    layer.msg("添加成功", {icon: 1});
                    location.reload();
                } else if (result.resultCode == 100211) {
                    layer.msg("不存在该用户", {icon: 2});
                } else {
                    layer.msg(result.data);
                }
            },
            error: function (result) {
                layer.msg("该用户已存在！");
            }
        })
    },
    /*修改员工信息*/
    updateEmp: function (data) {
        /*//设置当前部门编号
        localStorage.setItem("chat_dep_id", data);
        //设置操作
        localStorage.setItem("operate", 4);
        window.location.href = "/pages/console/staffMsg.html";*/
        layui.layer.open({
            title: "修改员工信息",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            btn: ["确定", "取消"],
            offset: 'auto',
            area: ['700px', '447px'],
            shadeClose: true, //点击遮罩关闭
            content: $("#from_yg_update"),
            success: function () {
                info.loadDepartmentInfo(data);
            },
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            }
            , yes: function (index, layero) {
                info.submitUpdateEmployee(data);
                layer.close(index);
            }
        });
    },
    loadDepartmentInfo: function (id) {
        //加载页面信息
        $.ajax({
            type: "POST",
            url: request("/console/department/all"),
            data: {
                companyId: localStorage.getItem("company_Id")
            },
            dataType: 'json',
            async: true,
            success: function (result) {
                if (result.data != null) {
                    /*渲染下拉框数据*/
                    for (var i = 1; i < result.data.length; i++) {
                        $("#dep").append("<option value='" + result.data[i].id + "'>" + Common.filterHtmlData(result.data[i].departName) + "</option>");
                    }
                    layui.form.render();

                    $.ajax({
                        type: "POST",
                        url: request("/console/employee/msg"),
                        data: {
                            id: id
                        },
                        dataType: 'json',
                        async: false,
                        success: function (result) {
                            if (result.data != null) {
                                $("#chatNum").val(result.data.chatNum);
                                $("#isPause").val(result.data.isPause);
                                $("#companyId").val(result.data.companyId);
                                $("#userId").val(result.data.userId);
                                $("#operationType").val(result.data.operationType);

                                $("#depId").val(result.data.id);
                                $("#nickname").val(result.data.nickname);
                                $("#position").val(result.data.position);
                                $("#isCustomer").val(result.data.isCustomer);
                                $("#role").val(result.data.role);
                                $("#dep").val(result.data.departmentId);
                            }
                            layui.form.render();
                        }
                    })
                }
            }
        })
    },
    submitUpdateEmployee: function () {
        $.ajax({
            type: "POST",
            url: request("/console/update/employee"),
            data: {
                chatNum: $("#chatNum").val(),
                isPause: $("#isPause").val(),
                companyId: $("#companyId").val(),
                userId: $("#userId").val(),
                operationType: $("#operationType").val(),
                id: $("#depId").val(),
                //头衔
                position: $("#position").val(),
                //昵称
                nickname: $("#nickname").val(),
                //是否公众号
                isCustomer: $("#isCustomer").val(),
                //新角色
                role: $("#role").val(),
                //新部门
                departmentId: $("#dep").val()
            },
            dataType: 'json',
            async: false,
            success: function (result) {
                if (result.resultCode == 1) {
                    layer.msg("修改成功", {icon: 1});
                    location.reload();
                }
            }
        })
    },
    delEmp: function (userid, parentId) {
        layer.confirm('确定要删除该条记录？', {
            btn: ['确定', '取消'] //按钮
        }, function (index) {
            $.ajax({
                type: "POST",
                url: request("/console/web/employee/delete"),
                data: {
                    userIds: userid,
                    departmentId: parentId
                },
                dataType: 'json',
                async: true,
                success: function (result) {
                    if (result.resultCode == 1){
                        location.reload();
                    }
                    layer.msg(result.resultMsg, {icon: 2});
                },
                error: function (result) {
                    layer.msg(result.msg, {icon: 2});
                }
            })
            layer.close(index);
        }, function () {
        });
    },
    deleteEmp: function (userid, parentId) {

    },
    submitAddDeparment:function () {
        if (Common.isNil($("#departName").val())){
            layer.msg("请输入部门名称！");
            return;
        }
        $.ajax({
            type:"POST",
            url:request("/console/add/deparment"),
            data:{
                companyId : Common.filterHtmlData(localStorage.getItem("company_Id")),
                parentId : Common.filterHtmlData(localStorage.getItem("company_Id")),
                departName : Common.getValueForElement("#departName"),
                createUserId : Common.filterHtmlData(localStorage.getItem("adminId"))
            },
            dataType:'json',
            async:false,
            success:function(result){
                if(result.resultCode==1){
                    layer.msg("添加成功");
                    location.reload();
                }else{
                    layer.msg(result.resultMsg);
                }
            },
            error:function (result) {
                layer.msg(result.resultMsg);
            }
        })
    },
    //初始化模板地址
    initTemplateUrl:function(){
        Common.invoke({
            url :request('/console/config'),
            data : {},
            success : function(result) {
                $(".employeeUploadUrl").attr("href",result.data.employeeTemplateUrl);
                $(".departmentUploadUrl").attr("href",result.data.departmentTemplateUrl);
            },
            error : function(result) {
                console.log(result);
            }

        });
    },
    //导入员工
    employeeInput:function () {
        layui.layer.open({
            title: "员工",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            offset: 'auto',
            area: ['400px', '150px'],
            shadeClose: true,
            content: $("#employeeInput"),
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            }
        });
    },
    //导入部门
    departmentInput:function () {
        layui.layer.open({
            title: "部门",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            offset: 'auto',
            area: ['400px', '150px'],
            shadeClose: true,
            content: $("#departmentInput"),
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            }
        });
    },

}