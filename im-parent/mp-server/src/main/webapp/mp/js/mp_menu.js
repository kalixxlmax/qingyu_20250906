layui.config({
       base: '/mp/common/'
        }).extend({
            treeTable: 'treeTable/treeTable'
        }).use(['layer', 'util', 'treeTable','table','form'], function () {
            var $ = layui.jquery;
            var layer = layui.layer;
            var util = layui.util;
            var treeTable = layui.treeTable;


        // 渲染表格
        var tableInMenu = treeTable.render({
            elem: '#mpMenu_list',
            toolbar: '#mpMenuTopBar',
            id: "mpMenu_list",
            height: 'full-200',
            tree: {
                iconIndex: 0,
                isPidData: true,
                idName: 'id',
                pidName: 'parentId',
                arrowType: 'arrow1',
                haveChildName: 'haveChild',
                getIcon: function(d) {  // 自定义图标
                    // d是当前行的数据
                   /* if ('0' == d.parentId) {  // 判断是否有子集
                        return '<i class="ew-tree-icon layui-icon">&#xe613;</i>'
                    } else {
                        return '<i class="ew-tree-icon layui-icon">&#xe612;</i>';
                    }*/
                    return '';
                }
            },
            text: {},
            cols: [

                {field: 'id', title:  "菜单Id",sort:'true', width:250}
                // ,{field: 'parentId', title: "parentId",sort:'true', width:200}
                ,{field: 'index', title: "菜单类型",sort:'true', width:120,templet: function (d) {
                    if( (0|"0") == d.parentId ){ return "一级菜单"
                    }else{ return "二级菜单"}
                }}
                ,{field: 'name', title: "菜单名",sort:'true', width:100}
                ,{field: 'index', title: "排序",sort:'true', width:100}
                // ,{field: 'name', title: "子菜单名",sort:'true', width:100}
                ,{field: 'url', title: "访问地址",sort:'true', width:200}
                ,{fixed: 'right', title: mpLanguage.getLanguageName('operation'), align:'center', toolbar: '#mpMenuOptionBar'}

            ],
            reqData: function (data, callback) {

                var parentId = data?data.id:'';
                mpCommon.invoke({
                    url : '/mp/menuList',
                    data : {
                        "parentId":parentId
                    },
                    successMsg : false,
                    errorMsg :  "加载数据失败，请稍后重试",
                    success : function(result) {
                        callback(result.data);
                    },
                    error : function(result) {
                    }
                });

            },
            style: 'margin-top:0;'
        });




        //===============================

        //问题列表
        /*var tableInMenu = treeTable.render({
            elem: '#mpMenu_list'
            ,toolbar: '#mpMenuTopBar'
            ,url: request("/mp/menuList")
            ,id: 'mpMenu_list'
            ,page: true
            ,curr: 0
            ,limit: mpCommon.limit
            ,limits: mpCommon.limits
            ,groups: 7
            ,cols: [[ //表头*/


                /*for(var i=0;i<result.data.length;i++){
                    var url="";
                    if(result.data[i].url!=undefined){
                        url=result.data[i].url;
                    }
                    html+="<tr>
                    <td>"+result.data[i].id+"</td>
                    <td>0</td></td>
                    <td>"+ mpCommon.getLanguageName('index_2') +"</td>
                    <td>"+result.data[i].name+"</td>
                    <td>"+result.data[i].index+"</td>
                    <td></td>
                    <td>"+url+"</td>

                    <td><button onclick='UI.deleteMenu(\""+result.data[i].id+"\")' class='layui-btn' style='height:30px;line-height:30px;border-radius:3px'>"+ mpLanguage.getLanguageName('index_3') +"</button><button onclick='UI.updateMenu(\""+result.data[i].id+"\",null,\""+result.data[i].name+"\",\""+result.data[i].index+"\",\""
                    +((mpCommon.isNil(result.data[i].url))?"":result.data[i].url)+"\",\""+result.data[i].desc+"\",\""+((mpCommon.isNil(result.data[i].menuId))?"":result.data[i].menuId)+"\")' class='layui-btn' style='height:30px;line-height:30px;border-radius:3px'>"+ mpLanguage.getLanguageName('index_4') +"</button></td><tr>";

                    body+="<option value='"+result.data[i].id+"'>"+ mpLanguage.getLanguageName('index_5') + result.data[i].name+"</option>";

                }
                if(result.data[0].menuList.length>0){
                    for(var j=0;j<result.data[0].menuList.length;j++){
                        if(mpCommon.isNil(result.data[0].menuList[j].url)){
                            result.data[0].menuList[j].url="";
                        }
                        html+="<tr>
                        <td>"+result.data[0].menuList[j].id+"</td>
                        <td>"+result.data[0].menuList[j].parentId+"</td>
                        <td>"+ mpCommon.getLanguageName('index_6') +"</td>
                        <td>"+"</td>
                        <td>"+result.data[0].menuList[j].index+"</td>
                        <td>"+result.data[0].menuList[j].name+"</td>
                        <td>"+result.data[0].menuList[j].url+"</td>

                        <td><button onclick='UI.deleteMenu(\""+result.data[0].menuList[j].id+"\")' class='layui-btn' style='height:30px;line-height:30px;border-radius:3px'>"+ mpLanguage.getLanguageName('index_3') +"</button><button onclick='UI.updateMenu(\""+result.data[0].menuList[j].id+"\",\""+result.data[0].menuList[j].parentId+"\",\""+result.data[0].menuList[j].name+"\",\""+result.data[0].menuList[j].index+"\",\""
                        +result.data[0].menuList[j].url+"\",\""+result.data[0].menuList[j].desc+"\",\""+result.data[0].menuList[j].menuId+"\")' class='layui-btn' style='height:30px;line-height:30px;border-radius:3px'>"+ mpLanguage.getLanguageName('index_4') +"</button></td><tr>";
                    }
                }*/

                /*{field: 'id', title:  "菜单Id",sort:'true', width:200}
                // ,{field: 'parentId', title: "parentId",sort:'true', width:200}
                ,{field: 'index', title: "菜单类型",sort:'true', width:120,}
                ,{field: 'name', title: "菜单名",sort:'true', width:100}
                ,{field: 'index', title: "排序",sort:'true', width:100}
                ,{field: 'name', title: "子菜单名",sort:'true', width:100}
                ,{field: 'url', title: "访问地址",sort:'true', width:200}
                ,{fixed: 'right', title: mpLanguage.getLanguageName('operation'), align:'center', toolbar: '#mpMenuOptionBar'}
            ]]
            ,done:function(res, curr, count){
                //checkRequst(res);
                //initLanguage();
                //form.render();
            }

        });*/


        $("#addMenu_btn").on("click",function(){
            MpMenu.addMenu(tableInMenu);
        });


        // 菜单操作项
        treeTable.on('tool(mpMenu_list)', function(obj){
            var layEvent = obj.event,  data = obj.data;

            if(layEvent==='edit_menu'){ //编辑问题

                MpMenu.updateMenu(data);

            }else if(layEvent==='del_menu'){ //删除问题
                layer.confirm('确定删除该菜单吗？', function(index){
                    MpMenu.deleteMenu(data.id,tableInMenu);
                    //obj.del(); //删除对应行（tr）的DOM结构，并更新缓存
                    layer.close(index);
                });

            }else if(layEvent==='add_child_menu'){ //添加子菜单
                MpMenu.addChildMenu(tableInMenu,data);
            }

        });




    var MpMenu = {

        // 添加一级菜单
        addMenu:function(tableInMenu){

            layer.open({
                title : "添加菜单",
                type: 1,
                btn:["确定","取消"],
                btnAlign: 'c',
                area: ['520px','500px'],
                content: $("#update_menu"),
                end: function(){
                    $("#update_menu").hide();
                },
                success : function(){ //弹出成功的回调

                    MpMenu.clearMenuPanel();
                }
                ,yes: function(index, layero){ //确定按钮的回调

                    mpCommon.invoke({
                        url : '/mp/menu/add',
                        data : {
                            parentId : 0,
                            apiName : $("#update_apiName").val(),
                            name : $("#update_name").val(),
                            url : $("#update_urls").val(),
                            index : $("#update_index").val(),
                            desc : $("#update_desc").val()
                        },
                        success : function(result) {

                            MpMenu.clearMenuPanel();

                            tableInMenu.reload({
                                page: {
                                    curr: 1 //重新从第 1 页开始
                                }
                            });

                            layui.layer.msg("添加成功" ,{"icon":1});
                            layui.layer.close(index);
                        },
                        error : function(result) {
                            layui.layer.msg("添加失败 "+result.msg);
                            layui.layer.close(index);
                        }
                    });
                }
            });


        },
        clearMenuPanel:function (){
            //清空菜单面板上填写的数据
            $("#update_apiName").val("");
            $("#update_name").val("");
            $("#update_urls").val("");
            $("#update_index").val("");
            $("#update_desc").val("");
        },
        //修改菜单
        updateMenu : function(data){

            layer.open({
                title : "修改菜单",
                type: 1,
                btn:["确定","取消"],
                btnAlign: 'c',
                area: ['520px','500px'],
                content: $("#update_menu"),
                end: function(){
                    $("#update_menu").hide();
                },
                success : function(){ //弹出成功的回调
                    //$("#update_parentId").attr("disabled",true);

                    //数据回显
                    $("#update_name").val(data.name);
                    $("#update_index").val(data.index);

                    if(mpCommon.isNil(data.url)){
                        $("#update_urls").val("");
                    }else{
                        $("#update_urls").val(data.url);
                    }

                    if(mpCommon.isNil(data.apiName)){
                        $("#update_apiName").val("");
                    }else{
                        $("#update_apiName").val(data.apiName);
                    }

                    if(mpCommon.isNil(data.desc)){
                        $("#update_desc").val("");
                    }else{
                        $("#update_desc").val(data.desc);
                    }

                    layui.form.render(); //刷新表单

                }
                ,yes: function(index, layero){ //确定按钮的回调

                    var reg = new RegExp("^[0-9]*$");
                    var obj = $("#update_index").val();
                    if($("#update_name").val()==""){
                        layui.layer.alert(mpLanguage.getLanguageName('input_menu_name'));
                        return;
                    }else if($("#update_index").val()==""){
                        layui.layer.alert(mpLanguage.getLanguageName('input_sort'));
                        return;
                    }else if(!reg.test(obj)){
                        layui.layer.alert(mpLanguage.getLanguageName('sort_number'));
                        return;
                    }

                    mpCommon.invoke({
                        url : '/mp/menu/saveupdate',
                        data : {
                            id: data.id,
                            apiName:$("#update_apiName").val(),
                            name:$("#update_name").val(),
                            url:$("#update_urls").val(),
                            index:$("#update_index").val(),
                            desc:$("#update_desc").val()
                        },
                        success : function(result) {

                            tableInMenu.reload(); //刷新当前页
                            layui.layer.msg("更新成功" ,{"icon":1});
                            layui.layer.close(index);

                        },
                        error : function(result) {
                            layui.layer.msg( "更新失败" ,{"icon":2});
                            layui.layer.close(index);
                        }
                    });
                }
            });

        },
        // 删除菜单
        deleteMenu:function(menuId,tableInMenu){

            mpCommon.invoke({
                url : '/mp/menu/delete',
                data : {
                    "menuId":menuId
                },
                success : function(result) {
                    if(1==result.resultCode){
                        layui.layer.msg("删除成功",{"icon":1});
                    }else if( 0 == result.resultCode ){
                        layui.layer.msg(""+ result.resultMsg ,{"icon":3});
                    }
                    tableInMenu.reload(); //刷新当前页
                },
                error : function(result) {
                    layer.msg("删除失败",{"icon":2});
                }
            });
        },
        // 添加菜单
        addChildMenu:function(tableInMenu,data){

            layer.open({
                title : "添加子菜单",
                type: 1,
                btn:["确定","取消"],
                btnAlign: 'c',
                area: ['520px','500px'],
                content: $("#update_menu"),
                end: function(){
                    $("#update_menu").hide();
                },
                success : function(){ //弹出成功的回调

                    /*<select id="update_parentId" class="layui-select" disabled="disabled">
                        <option value="0" data-locale="parentId">一级菜单</option>
                    </select>*/
                    // $("#update_parentId").attr("disabled",false);
                }
                ,yes: function(index, layero){ //确定按钮的回调

                    mpCommon.invoke({
                        url : '/mp/menu/add',
                        data : {
                            parentId : data.id,
                            apiName : $("#update_apiName").val(),
                            name : $("#update_name").val(),
                            urls : $("#update_urls").val(),
                            index : $("#update_index").val(),
                            desc : $("#update_desc").val()
                        },
                        success : function(result) {

                           MpMenu.clearMenuPanel();

                            tableInMenu.reload({
                                page: {
                                    curr: 1 //重新从第 1 页开始
                                }
                            });

                            layui.layer.msg("添加成功" ,{"icon":1});
                            layui.layer.close(index);
                        },
                        error : function(result) {
                            layui.layer.msg("添加失败 "+ result.msg);
                            layui.layer.close(index);
                        }
                    });
                }
            });


        },

    }

});

