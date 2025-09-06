layui.use(['form','layer','laydate','table','laytpl','upload'],function(){
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;
        var upload = layui.upload;


    //非管理员登录屏蔽操作按钮
    if(localStorage.getItem("IS_ADMIN")==0){
         $(".appRecharge_div").empty();
    }

    //公司列表
    var tableInCompany = table.render({
      elem: '#companyList_table'
      ,toolbar: '#toolbarUsers'
      ,url:request("/console/web/company/list")
      ,id: 'companyList_table'
      ,page: true
      ,curr: 0
      ,limit:Common.limit
      ,limits:Common.limits
      ,groups: 7
      ,cols: [[ //表头
           //{type:'checkbox',fixed:'left'}// 多选
          {field: 'companyName', title: '公司名称',sort:'true', width:'15%'}
          ,{field: 'createUserId', title: '创建者Id',sort:'true', width:'15%'}
          ,{field: 'empNum', title: '员工数',sort:'true', width:'15%'}
          ,{field: 'createTime', title: '创建时间',sort:'true', width:'15%',templet: function(d){
          		return UI.getLocalTime(d.createTime);
          }}
          ,{field: 'noticeContent', title: '公司公告',sort:'true', width:'15%'}
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
     table.on('tool(companyList_table)', function(obj){
           var layEvent = obj.event,
               data = obj.data;

           if(layEvent === 'companyDetails'){ //删除
             Pro.queryCompanyMessage(data.id , data.companyName);
           }else if(layEvent === 'passChecked'){ //通过审核
               Pro.passCheckedCompany(data.id);
           }
     });


     //搜索公司
    $("#searchCompany_btn").on("click",function(){

        // 关闭超出宽度的弹窗
        $(".layui-layer-content").remove();

        table.reload("companyList_table",{
            url:request('/console/web/company/list'),
            page: {
                curr: 1 //重新从第 1 页开始
            },
            where: {
                keyword : Common.getValueForElement("#searchCompany_keyword")  //搜索的关键字
            }
        })

    });

});



var Pro={
	//查看详情
    queryCompanyMessage:function(id,name){
        localStorage.setItem("company_componyName",name);
        localStorage.setItem("company_Id",id)
		window.location.href="/pages/console/deparMsg.html";
	},
    //新建公司
    createCompany:function () {
        layui.layer.open({
            title: "新建公司",
            skin: 'layui-ext-motif',
            type: 1,
            shade: false,
            offset: 'auto',
            area: ['700px', '200px'],
            shadeClose: true,
            btn:["确认","取消"],
            content: $("#createCompany"),
            cancel: function (index, layero) {
                layer.close(index);
                return false;
            },
            yes:function (index, layero) {
                if (Common.isNil($("#companyName").val())){
                    layui.layer.msg("请输入公司名称",{"icon":2});
                    return;
                }
                Pro.createCompanyFromSubmit($("#companyName").val());
                layer.close(index);
            }
        });
    },
    createCompanyFromSubmit:function(companyName){
        Common.invoke({
            url :request('/console/create/company'),
            data : {
                companyName : companyName
            },
            success : function(result) {
                layui.layer.msg("操作成功",{"icon":1});
                layui.use('table', function(){
                    var table = layui.table;//高版本建议把括号去掉，有的低版本，需要加()
                    table.reload("companyList_table");
                });
            },
            error : function(result) {
                console.log(result);
            }

        });
    },

    passCheckedCompany : function(companyId){

        Common.invoke({
            url:request('/console/web/company/check'),
            data:{
                companyId:companyId,
                isCheck:1
            },
            success:function(result){
                layer.msg("已审核通过", {icon: 1});
            }
        })

    }
}
