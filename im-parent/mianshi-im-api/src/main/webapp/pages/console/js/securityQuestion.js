layui.use(['form','layer','laydate','table','laytpl'],function(){
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;

    //公司列表
    var tableInCompany = table.render({
      elem: '#question_table'
      ,toolbar: '#toolbarUsers'
      ,url:request("/console/list/secret/question")
      ,id: 'question_table'
      ,page: true
      ,curr: 0
      ,limit:Common.limit
      ,limits:Common.limits
      ,groups: 7
      ,cols: [[ //表头
            {field: 'id', title: '编号', width:'15%'},
            {field: 'question', title: '问题', width:'15%'},
            {field: 'status', title: '状态', width:'15%',templet(d){
                   return d.status == 1? "使用" : "禁用";
                }},
            {field: 'createTime', title: '创建时间', width:'15%',templet: function(d){
          		return UI.getLocalTime(d.createTime);
          }}
          ,{fixed: 'right',title:"操作", align:'left', toolbar: '#toolbarCompanys'}
      ]]
	  ,done:function(res, curr, count){
            //checkRequst(res);
            //权限判断
            var arr=['company-query'];
            manage.authButton(arr);

           if(count==0&&lock==1){
             layer.msg("暂无数据",{"icon":2});
             //renderTable();
           }
	  }
    });



     //列表操作
     table.on('tool(question_table)', function(obj){
           var layEvent = obj.event,
               data = obj.data;

           if(layEvent === 'del'){
               //删除
               Common.invoke({
                   url:request('/console/del/secret/question'),
                   data:{
                       id: data.id,
                   },
                   success:function(result){
                       if(result.resultCode==1){
                           layer.msg("删除成功",{"icon":1});
                           obj.del();
                           operation.backBtn();
                       }
                   }
               });
           }else if (layEvent === 'modity'){
                operation.updateBtn();
                $(".questionId").val(data.id);
                $(".question_modity").val(data.question);
                var flag = data.status==1 ? true :false;
                $(".status_modity").prop('checked',flag);
                layui.form.render();
           }
     });


     //搜索公司
    $("#searchCompany_btn").on("click",function(){
        // 关闭超出宽度的弹窗
        $(".layui-layer-content").remove();
        table.reload("question_table",{
            url:request('/console/web/company/list'),
            page: {
                curr: 1 //重新从第 1 页开始
            },
            where: {
                keyword : Common.getValueForElement("#searchCompany_keyword")  //搜索的关键字
            }
        })
    });


    //修改密保问题
    form.on('submit(update)', function(data){
        Common.invoke({
            url:request('/console/create/secret/question'),
            data:{
                question : data.field.question,
                status : data.field.open=="on"?1:-1
            },
            success:function(result){
                if(result.resultCode==1){
                    layer.msg("新增成功！",{"icon":1});
                    operation.backBtn();
                }
            }
        });
        return false;
    });


    //新增密保问题
    form.on('submit(create)', function(data){
        Common.invoke({
            url:request('/console/create/secret/question'),
            data:{
                question : data.field.question,
                status : data.field.open=="on"?1:-1
            },
            success:function(result){
                if(result.resultCode==1){
                    layer.msg("新增成功！",{"icon":1});
                    operation.backBtn();
                }
            }
        });
        return false;
    });
});



var operation = {
    //返回按钮
    backBtn:function () {
        $("#createQuestion").hide();
        $("#updateQuestion").hide();
        $("#listQuestion").show();
        layui.table.reload("question_table",{
            url:request('/console/list/secret/question'),
            page: {
                curr: 1 //重新从第 1 页开始
            },
            where: {
            }
        })
    },
    //新增按钮
    createBtn:function () {
        $("#createQuestion").show();
        $("#listQuestion").hide();
    },

    //修改按钮
    updateBtn:function () {
        $("#updateQuestion").show();
        $("#listQuestion").hide();
    }
}