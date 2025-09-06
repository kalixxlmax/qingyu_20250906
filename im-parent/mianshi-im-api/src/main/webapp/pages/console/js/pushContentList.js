layui.use(['form','layer','laydate','table','laytpl'],function(){
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;

    //公司列表
    var tableInCompany = table.render({
      elem: '#push_table'
      ,toolbar: '#toolbarUsers'
      ,url:request("/console/get/pushnews/list")
      ,id: 'push_table'
      ,page: true
      ,curr: 0
      ,limit:Common.limit
      ,limits:Common.limits
      ,groups: 7
      ,cols: [[ //表头
           //{type:'checkbox',fixed:'left'}// 多选
          {field: 'id', title: '编号', width:'10%'},
            {field: 'title', title: '标题', width:'10%'},
            {field: 'content', title: '内容', width:'10%'},
            {field: 'type', title: '推送机型', width:'10%',templet(d){
                    var typeName;
                    typeName = d.type === 0 ? "全员通知" :d.type === 2 ? "华为推送" : d.type === 3 ? "VIVO推送" : d.type === 4 ? "OPPO推送" : d.type === 5 ? "小米推送" : d.type === 6 ? "魅族推送" : d.type === 7 ? "apns推送" : d.type ===  8 ? "极光推送" : ""
                    return typeName;
                }},
            {field: 'packageName', title: '推送包名', width:'10%'},
            {field: 'token', title: '推送Token', width:'10%'},
            {field: 'addressURL', title: 'URL地址', width:'10%'},
            {field: 'createTime', title: '发送时间', width:'10%',templet:function(d){
                    return UI.getLocalTime(d.createTime);
                }},
          {fixed: 'right',title:"操作", align:'left', width:'10%', toolbar: '#toolbarCompanys'}
      ]]
	  ,done:function(res, curr, count){
            //checkRequst(res);
            //权限判断
            var arr = ['pushRecord-delete'];
            manage.authButton(arr);

           if(count==0&&lock==1){
             layer.msg("暂无数据",{"icon":2});
             //renderTable();
           }
	  }

    });


     //列表操作
     table.on('tool(push_table)', function(obj){
           var layEvent = obj.event,
               data = obj.data;

           if(layEvent === 'del'){
               layer.confirm('确定要删除该条记录？', {
                   btn: ['确定','取消'] //按钮
               }, function(index){
                   Common.invoke({
                       url : request('/console/delete/pushnews'),
                       data : {
                           "id" : data.id,
                       },
                       success:function(result){
                           checkRequst(result);
                           if(result.resultCode==1){
                               layer.msg("删除成功",{icon: 1});
                           }
                       }
                   });
                   obj.del();
                   layer.close(index);
               }, function(){
               });

           }
     });

    //日期范围
    layui.laydate.render({
        elem: '#msgData'
        ,lang: 'zh'
        ,range: "~"
        ,done: function(value, date, endDate){  // choose end
            //console.log("date callBack====>>>"+value); //得到日期生成的值，如：2017-08-18
            var startTime = value.split("~")[0];
            var endTime = value.split("~")[1];
            // Count.loadGroupMsgCount(roomJId,startDate,endDate,timeUnit);
            table.reload("push_table",{
                page: {
                    curr: 1 //重新从第 1 页开始
                },
                where: {
                    startTime : startTime,
                    endTime : endTime
                }
            })
        }
        ,max: 1
    });

    //点击搜索
    $("#search_btn").click(function () {
        table.reload("push_table",{
            page: {
                curr: 1 //重新从第 1 页开始
            },
            where: {
                content : Common.getValueForElement("#keyword"),
                type : Common.getValueForElement("#type")
            }
        })
    })
});
$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})