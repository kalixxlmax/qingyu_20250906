layui.use(['form','layer','laydate','table','laytpl','layim'],function(){
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        layim = layui.layim,
        table = layui.table;


    $(".demo").click(function () {
        //打开聊天面板
        layui.layer.open({
            title:"",
            type: 1,
            shade: false,
            area: ['550px', '600px'],
            shadeClose: true, //点击遮罩关闭
            content: $("#mp_chatPanel"),
            cancel: function(index, layero){ //关闭聊天面板后执行
                layer.close(index)
                //清空页面上的聊天消息
                $("#mp_chatPanel #messageContainer").empty();
                return false;
            },
            success : function(layero,index){  //弹窗打开成功后的回调
                $(".nano").nanoScroller();

            }
        });
    })


});