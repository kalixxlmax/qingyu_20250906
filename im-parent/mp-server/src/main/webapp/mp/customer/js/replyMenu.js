//事件缓存对象
var allEvent = {};

var Menu = {

    addMenu : function(title,parentId,callback){

        $("#addMenu .menuNo").val("");
        $("#addMenu .menuText").val("");

        layer.open({
            title : title,
            type: 1,
            btn:["确定","取消"],
            area: ['350px'],
            content: $("#addMenu"),
            end: function(){ 
                $("#addMenu").hide();
            }
            ,yes: function(index, layero){ //确定按钮的回调
                var menuNo = $("#addMenu .menuNo").val();
                var menuText = $("#addMenu .menuText").val();
                //var answer = $("#addFaq .answer").val();
                if ( "" == menuNo || "" == menuText){
                    layer.msg("请输入菜单描述",{"icon":5});
                    return
                }

                mpCommon.invoke2({
                    url :'/customerService/admin/addMenu',
                    data : {
                        parentId : parentId,
                        menuNo : menuNo,
                        menuText : menuText
                    },
                    success : function(result) {
                        if(result.resultCode == 1){
                            layer.msg("添加成功" ,{"icon":1});
                            callback();
                            layer.close(index); //关闭弹框
                        }else{
                            layer.close(index); //关闭弹框
                            layer.msg(result.resultMsg,{"icon":2});
                        }
                    },
                    error : function(result) {
                       layer.msg(mpLanguage.getLanguageName('failed_load_data'));
                    }
                });
            }
        });

    },
    /** 获取全部的事件列表 **/
    getAllEventList: function(){
        mpCommon.invoke2({
            url : '/customerService/admin/allEventList',
            data : {},
            success : function(result) {
                if(result.resultCode == 1){
                    //layer.msg("添加成功" ,{"icon":1});
                     
                    allEvent = result.data;
                }else{
                    layer.close(index); //关闭弹框
                    layer.msg(result.resultMsg,{"icon":2});
                }
            },
            error : function(result) {
               layer.msg(mpLanguage.getLanguageName('failed_load_data'));
            }
        });
    },
    /** 通过事件标识(value)获取事件名(key)  **/
    getNameByEventSign : function(value, compare = (a, b) => a === b) {

        if (mpCommon.isNil(value)) {
            return "";
        }
        return Object.keys(allEvent).find(k => compare(allEvent[k], value));

    },
    createEventHtml : function(isCancel){

        var eventSelectHtml = '<div class="layui-form-item">'
                            +    '<div class="layui-input-inline">'
                            +        '<select name="interest" id="mnue_event">';
        if(isCancel){
            eventSelectHtml +=           '<option value="cancel">取消事件</option>';             
        }
        for(var key in allEvent){
            eventSelectHtml +=           '<option value="'+allEvent[key]+'">'+key+'</option>'
        }
            eventSelectHtml +=       '</select>'
                            +   '</div>'
                            +'</div>';

        return eventSelectHtml;

    },
    bindEvent : function(menuId,callback){
        layer.open({
            title:"绑定事件",
            type: 1,
            //btn:[mpLanguage.getLanguageName('determine'),mpLanguage.getLanguageName('theCancel')],
            btn:['确定','取消'],
            area: ['300px','300px'],
            content: '<div id="bindEvent_select" class="layui-form" style="margin:20px 40px 10px 40px;;">'
                   + Menu.createEventHtml()
                   +'</div>'
            ,success: function(layero, index){
                layui.form.render('select');
            }  
            ,yes: function(index, layero){ //确定按钮的回调
                var mnue_event_name = $("#bindEvent_select #mnue_event").val();
                if (mnue_event_name == ""){
                    return
                }
                mpCommon.invoke2({
                    url :'/customerService/admin/bindMenuEvent',
                    data : {
                        menuId:menuId,
                        eventName : mnue_event_name
                    },
                    success : function(result) {
                        if(result.resultCode == 1){
                            layer.msg("绑定成功" ,{"icon":1});
                            callback();
                            layer.close(index); //关闭弹框
                        }else{
                            layer.close(index); //关闭弹框
                            layer.msg(result.resultMsg,{"icon":2});
                        }
                    },
                    error : function(result) {
                        layui.layer.msg(mpLanguage.getLanguageName('failed_load_data'));
                    }
                });
            }
        });

    },
    updateMenu : function(title, data, callback){

        layer.open({
            title : title,
            type: 1,
            btn:["确定","取消"],
            area: ['350px'],
            content: $("#addMenu"),
            end: function(){ 
                $("#addMenu").hide();
            }
            ,success : function(){
                //数据回显
                $("#addMenu .menuNo").val(data.menuNo);
                $("#addMenu .menuText").val(data.menuText);

                layui.form.render();
            }
            ,yes: function(index, layero){ //确定按钮的回调
                var menuNo = $("#addMenu .menuNo").val();
                var menuText = $("#addMenu .menuText").val();
                
                if ( "" == menuNo || "" == menuText){
                    layer.msg("请输入菜单描述",{"icon":5});
                    return
                }

                mpCommon.invoke2({
                    url :'/customerService/admin/updateMenu',
                    data : {
                        menuId : data.menuId,
                        menuNo : menuNo,
                        menuText : menuText
                    },
                    success : function(result) {
                        if(result.resultCode == 1){
                            layer.msg("添加成功" ,{"icon":1});
                            callback();
                            layer.close(index); //关闭弹框
                        }else{
                            layer.close(index); //关闭弹框
                            layer.msg(result.resultMsg,{"icon":2});
                        }
                    },
                    error : function(result) {
                       layer.msg(mpLanguage.getLanguageName('failed_load_data'));
                    }
                });
            }
        });

    },
    updateMenuEvent : function(title, data, callback){

        layer.open({
            title : title,
            type: 1,
            btn:["确定","取消"],
            area: ['300px','285px'],
            content: '<div id="bindEvent_select" class="layui-form" style="margin:20px 40px 10px 40px;;">'
                   + Menu.createEventHtml(true)
                   +'</div>'
            ,success: function(layero, index){
                //数据回显
                $("#bindEvent_select #mnue_event").val(data.event);

                layui.form.render('select');
            }
            ,yes: function(index, layero){ //确定按钮的回调
                var event = $("#bindEvent_select #mnue_event").val();

                mpCommon.invoke2({
                    url :'/customerService/admin/updateMenu',
                    data : {
                        menuId : data.menuId,
                        event : event
                    },
                    success : function(result) {
                        if(result.resultCode == 1){
                            layer.msg("更新成功" ,{"icon":1});
                            callback();
                            layer.close(index); //关闭弹框
                        }else{
                            layer.close(index); //关闭弹框
                            layer.msg(result.resultMsg,{"icon":2});
                        }
                    },
                    error : function(result) {
                       layer.msg(mpLanguage.getLanguageName('failed_load_data'));
                    }
                });
            }
        });

    },
    /**  更新自动回复  type 0 : 添加回复  1：修改回复  **/
    updateAutoReply : function(title, type, data,callback){

        layer.open({
            title : title,
            type: 1,
            btn:["确定","取消"],
            area: ['400px','285px'],
            content: $("#addAutoReply")
            ,success: function(layero, index){

                if( 1 == type ){
                    //数据回显
                    $("#addAutoReply .replyType").val(data.autoReply[0].answerType);
                    $("#addAutoReply .replyContent").val(data.autoReply[0].content);
                    layui.form.render('select');
                }

            },
            end : function(){
                $("#addAutoReply").hide();
            }
            ,yes: function(index, layero){ //确定按钮的回调
                var replyType = $("#addAutoReply .replyType").val();
                var replyContent = $("#addAutoReply .replyContent").val();

                mpCommon.invoke2({
                    url :'/customerService/admin/updateMenuAutoReply',
                    data : {
                        menuId : data.menuId,
                        replyType : replyType,
                        replyContent : replyContent
                    },
                    success : function(result) {
                        if(result.resultCode == 1){
                            layer.msg("更新成功" ,{"icon":1});
                            callback();
                            layer.close(index); //关闭弹框
                        }else{
                            layer.close(index); //关闭弹框
                            layer.msg(result.resultMsg,{"icon":2});
                        }
                    },
                    error : function(result) {
                       layer.msg(mpLanguage.getLanguageName('failed_load_data'));
                    }
                });
            }
        });

    },
    deleteMenu : function(data,callback){

        mpCommon.invoke2({
            url :'/customerService/admin/deleteMenu',
            data : {
                menuId : data.menuId
            },
            success : function(result) {
                if(result.resultCode == 1){
                    layer.msg("删除成功" ,{"icon":1});
                    callback();
                } else {
                    layer.msg(result.resultMsg,{"icon":2});
                }
            },
            error : function(result) {
               layer.msg(mpLanguage.getLanguageName('failed_load_data'));
            }
        });

    }
};


layui.config({
        base: '/mp/common/'
    }).extend({
        treeTable: 'treeTable/treeTable'
    }).use(['layer', 'util', 'treeTable','table','form'], function () {
        var $ = layui.jquery;
        var layer = layui.layer;
        var util = layui.util;
        var treeTable = layui.treeTable;
        var table = layui.table;
        var form = layui.form;

        //获取所有事件
        Menu.getAllEventList();

        //添加一级菜单
        $("#add_firstMenu").on("click",function(){
            Menu.addMenu("添加一级菜单","",function(){
                //刷新页面
                tableInReplyMenu.reload({
                    page: { 
                        curr: 1 //重新从第 1 页开始
                    }
                });
            });
        });


        // 渲染表格
        var tableInReplyMenu = treeTable.render({
            elem: '#replyMenu_table',
            toolbar: '#replyMenuTopBar',
            id: "replyMenu_table",
            height: 'full-200',
            tree: {
                iconIndex: 0,
                isPidData: true,
                idName: 'menuId',
                pidName: 'parentId',
                arrowType: 'arrow1',
                getIcon: function(d) {  // 自定义图标
                    // d是当前行的数据
                    /*if (d.haveChild) {  // 判断是否有子集
                        return '<i class="ew-tree-icon layui-icon">&#xe613;</i>'
                    } else {
                        return '<i class="ew-tree-icon layui-icon">&#xe612;</i>';
                    }*/
                    return '';
                }
            },
            text: {},
            cols: [
                //{type: 'numbers'},
                {field: 'menuNo', title: '菜单编号',width: 300},
                {field: 'menuText', title: '菜单内容',width: 200},
                {field: 'event', title: '绑定事件',width: 180, templet: function (d) {
                        return (Menu.getNameByEventSign(d.event));
                }},
                {field: 'autoReply', title: '自动回复',width: 180, templet: function (d) {
                    if(!mpCommon.isNil(d.autoReply)){
                        //return "["+d.autoReply[0].answerType+"]" + " " + d.autoReply[0].content;
                        return  d.autoReply[0].content;
                    }else{ return "----"; }
                }},
                {field: 'createTime', title: '添加时间',width: 180, templet: function (d) {
                        return util.toDateString(d.createTime);
                }},
                {align: 'center', toolbar: '#replyMenuBar', title: '操作', width: 500},
                /*{field: 'content', title: '',templet : function(d){
                    return d.nickname + " ("+d.userId+")";
                }},*/
            ],
            reqData: function (data, callback) {

                var parentId = data?data.menuId:'';
                mpCommon.invoke2({
                    url : '/customerService/admin/menuList',
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


        // 菜单操作项
        treeTable.on('tool(replyMenu_table)', function(obj){
            var layEvent = obj.event,  data = obj.data;
           
            if(layEvent === 'add_subMenu'){ //添加子级菜单

                Menu.addMenu("添加子菜单",data.menuId,function(){
                    tableInReplyMenu.reload(); //刷新当前页
                });

            }else if(layEvent === 'modify_menu'){ //修改菜单
                Menu.updateMenu("修改菜单内容",data,function(){
                    tableInReplyMenu.reload(); //刷新当前页
                });  
                
            }else if(layEvent === 'bind_event'){ //绑定事件
                Menu.bindEvent(data.menuId,function(){
                    tableInReplyMenu.reload(); //刷新当前页
                });
            }else if(layEvent === 'edit_event'){  //编辑事件
                Menu.updateMenuEvent("编辑事件",data,function(){
                    tableInReplyMenu.reload(); //刷新当前页
                });

            }else if(layEvent === 'add_reply'){ //添加自动回复

                Menu.updateAutoReply("添加自动回复", 0, data, function(){
                    tableInReplyMenu.reload(); //刷新当前页
                });

            }else if(layEvent === 'modify_reply'){

                Menu.updateAutoReply("修改自动回复", 1 , data, function(){
                    tableInReplyMenu.reload(); //刷新当前页
                });

            }else if(layEvent === 'del_menu'){ //删除

                if(data.haveChild){
                    layer.msg("该菜单存在子菜单，请先删除子菜单",{"icon":2});
                    return;
                }

                layer.confirm('确定删除该菜单吗？', function(index){
                  //Service.deleteService(data.service_userId);
                  Menu.deleteMenu(data,function(){
                    tableInReplyMenu.reload(); //刷新当前页
                  });
                  //obj.del(); //删除对应行（tr）的DOM结构，并更新缓存
                  layer.close(index);
                });
            }


        });

        
        





    });