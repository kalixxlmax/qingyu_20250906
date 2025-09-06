var adminIds = new Array();
var currentPageIndex;// 当前页码数
var currentCount;// 当前总数
var chooseDeleteCount = 0;
var toUserIds = new Array();
layui.use(['form','layer','laydate','table','laytpl'],function(){
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;

	 //管理员列表
    var tableIns = table.render({
      elem: '#robot_table'
      ,toolbar:'#checkRobot'
      ,url:request("/console/adminList")+"&adminId="+localStorage.getItem("adminId")+"&type="+"3"+"&userId="+localStorage.getItem("account")
      ,id: 'robot_table'
      ,page: true
      ,curr: 0
      ,limit:Common.limit
      ,limits:Common.limits
      ,groups: 7
      ,cols: [[ //表头
           {type:'checkbox',fixed:'left'}// 多选
          ,{field: 'userId', title: 'userId', width:150}
          ,{field: 'phone', title: '账号', width:150}
          ,{field: 'nickName', title: '昵称', width:150}
          ,{field: 'role', title: '角色', width:150,templet: function(d){
          		if(d.role==3){return "机器人"}
          }}
          ,{field: 'createTime', title: '创建时间', width:200,templet: function(d){
          		return UI.getLocalTime(d.createTime);
          }}
          ,{fixed: 'right', width: 300,title:"操作", align:'left', toolbar: '#robotPageListBar'}
        ]]
        ,done:function(res, curr, count){
            checkRequst(res);

            //权限判断
            var arr=['rob-delete','rob-updatePassword','supperAdmin'];
            manage.authButton(arr);

            //获取零时保留的值
            var last_value = $("#robot_limlt").val();
            //获取当前每页大小
            var recodeLimit =  tableIns.config.limit;
            //设置零时保留的值
            $("#robot_limlt").val(recodeLimit);
            //判断是否改变了每页大小数
            if (last_value != recodeLimit){
                // 刷新
                table.reload("robot_table",{
                    url:request("/console/adminList")+"&adminId="+localStorage.getItem("adminId")+"&type="+"3"+"&userId="+localStorage.getItem("account"),
                    page: {
                        curr: 1 //重新从第 1 页开始
                    }
                })
            }


            var pageIndex = tableIns.config.page.curr;//获取当前页码
            var resCount = res.count;// 获取table总条数
            currentCount = resCount;
            currentPageIndex = pageIndex;
        }
    });

    //监听表格复选框选择
    table.on('checkbox(robot_table)', function(obj){
        //当前页总数据量  --> 更换表名
        var thisCount = table.cache.robot_table.length;
        //每页数据量
        var pageCount = $(".layui-laypage-limits").find("option:selected").val();


        if (obj.type == 'one'){
            chooseDeleteCount = obj.checked == true ? chooseDeleteCount + 1 : chooseDeleteCount - 1;
        }else if (obj.type == 'all') {
            chooseDeleteCount = obj.checked == true ? pageCount > thisCount ? thisCount : pageCount : 0;
        }
        $(".chooseDeleteCount").html("已选择 "+chooseDeleteCount+" 项");
        chooseDeleteCount < 1 ? $(".mustDelete").hide() :$(".mustDelete").show();
    });


    //列表操作
    table.on('tool(robot_table)', function(obj){
        var layEvent = obj.event, data = obj.data;
        console.log("delete:"+JSON.stringify(data));
        if(layEvent === 'delete'){ //删除

         	 Robot.checkDeleteRobotImpl(data.userId,data.role,1);

        }else if(layEvent === 'updatePassword'){// 重置密码

            layui.layer.open({
                title:"重置机器人 "+data.phone+" 的密码",
                type: 1,
                skin: 'layui-ext-motif',
                btn:["确定","取消"],
                area: ['310px'],
                content: '<div id="changePassword" class="layui-form" style="margin-top: 15px;">'
                         +   '<div class="layui-form-item">'
                         +        '<input type="password" required  lay-verify="required" placeholder="新的机器人密码" autocomplete="off" class="layui-input admin_passwd">'
                         +    '</div>'
                         +   '<div class="layui-form-item">'
                         +        '<input type="password" required  lay-verify="required" placeholder="确认密码" autocomplete="off" class="layui-input admin_rePasswd">'
                         +    '</div>'
                         +'</div>'

                ,yes: function(index, layero){ //确定按钮的回调

                    var newPasswd = Common.getValueForElement("#changePassword .admin_passwd");
                    var reNewPasswd = Common.getValueForElement("#changePassword .admin_rePasswd");
                    if(newPasswd!=reNewPasswd){
                      layui.layer.msg("两次密码输入不一致",{"icon":2});
                      return;
                    }
                    if (Common.isNil(newPasswd) || Common.isNil(reNewPasswd)) {
                        layui.layer.msg("请输入密码", {"icon": 2});
                        return;
                    }
                    data.password = $.md5(newPasswd);
                    updateRole(data.userId,data.password,function () {
                        layui.layer.close(index); //关闭弹框
                    });
                    /*updateAdmin(localStorage.getItem("adminId"),data,"修改管理员密码", function(){
                        layui.layer.close(index); //关闭弹框
                    });*/
                }


             });

        }else if (layEvent === 'resource'){
            localStorage.setItem("start_page","/pages/console/robot.html");
            localStorage.setItem("user_add_resource_userId",data.id);
            window.location.href="/pages/console/userAddResource.html";
        }

     });

    function updateRole(userId,newPassword,callback){
        console.log("userId"+userId+"---"+"password"+newPassword);
        Common.invoke({
            url : request('/console/updateUserPassword'),
            data : {
                "userId" : userId,
                "password": newPassword
            },
            successMsg : "重置密码成功",
            errorMsg :  "重置密码失败，请稍后重试",
            success : function(result) {
                // layui.layer.close(index); //关闭弹框
                // // location.replace("/pages/console/login.html");
                callback();
            },
            error : function(result) {

            }
        });
    }


    //搜索
    $(".search_live").on("click",function(){
        if($("#toUserName").val().indexOf("*")!=-1){
            layer.alert("不支持*号搜索")
            return
        }
        // 关闭超出宽度的弹窗
        $(".layui-layer-content").remove();
        table.reload("robot_table",{
            url:request("/console/adminList")+"&adminId="+localStorage.getItem("adminId")+"&type="+"3"+"&userId="+localStorage.getItem("account"),
            page: {
                curr: 1 //重新从第 1 页开始
            },
            where: {
                keyWorld : Common.getValueForElement("#toUserName")  //搜索的关键字
            }
        })
        $("#toUserName").val("");
    });


})
var Robot = {
    // 机器人多选删除
    checkDeleteRobot:function(){
        layer.confirm('确定要删除？', {
            btn: ['确定','取消']
        }, function(){
            // 多选操作
            var checkStatus = layui.table.checkStatus('robot_table');

            for (var i = 0; i < checkStatus.data.length; i++){
                adminIds.push(checkStatus.data[i].userId);
            }
            if(0 == checkStatus.data.length){
                layer.msg("请勾选要删除的行");
                return;
            }
            Robot.checkDeleteRobotImpl(adminIds.join(","),checkStatus.data.length);
        });
    },
    /**
     * 刷新多选删除
     */
    refreshMushDelete:function () {
        chooseDeleteCount = 0;
        $(".mustDelete").hide();
        userIds = [];
    },

    checkDeleteRobotImpl:function(adminId,checkLength){
        Common.invoke({
            url : request('/console/delAdmin'),
            data : {
                "adminId" :adminId,
                "type" : 3
            },
            successMsg : "删除成功",
            errorMsg :  "删除失败，请稍后重试",
            success : function(result) {
                if(result.resultCode == 1){
                    Robot.refreshMushDelete();
                    Common.tableReload(currentCount,currentPageIndex,checkLength,"robot_table");
                }
            },
            error : function(result) {
                adminIds = [];
            }
        });
    },
}
$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})



    
 


