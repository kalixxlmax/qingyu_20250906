layui.use(['form','layer','laydate','table','laytpl'],function(){
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;


    //公司列表
    var tableInCompany = table.render({
        elem: '#companyList_table'
        ,toolbar: '#toolbarUsers'
        ,url:request("/console/person/operation/log")
        ,id: 'companyList_table'
        ,page: true
        ,curr: 0
        ,limit:Common.limit
        ,limits:Common.limits
        ,groups: 7
        ,cols: [[ //表头
            //{type:'checkbox',fixed:'left'}// 多选
            {field: 'companyName', title: '公司名称', width:'15%'}
            ,{field: 'createUserId', title: '创建者Id', width:'15%'}
            ,{field: 'empNum', title: '员工数', width:'15%'}
            ,{field: 'createTime', title: '创建时间', width:'15%',templet: function(d){
                    return UI.getLocalTime(d.createTime);
                }}
            ,{field: 'noticeContent', title: '公司公告', width:'15%'}
            ,{fixed: 'right',title:"操作", align:'left', toolbar: '#toolbarCompanys'}
        ]]
        ,done:function(res, curr, count){
            //checkRequst(res);
            //权限判断
            var arr=['company-query'];
            manage.authButton(arr);

            if(count==0&&lock==1){
                layer.msg("暂无数据",{"icon":2});
            }
        }

    });


});