var page=0;
var sum=0;

layui.use(['form','layer','laydate','table','laytpl'],function(){
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;

     
    //非管理员登录屏蔽操作按钮
    if(localStorage.getItem("IS_ADMIN")==0){
    	$(".liveRoom_btn_div").empty();
    	$(".delete").remove();
		$(".chatMsg").remove();
		$(".member").remove();
    }
    /*if(localStorage.getItem("IS_ADMIN")==0){
		$(".btn_addLive").hide();
		$(".delete").hide();
		$(".chatMsg").hide();
		$(".member").hide();
	}*/

	//直播间列表
    var tableIns = table.render({
      elem: '#incomeTotal_table'
      ,url:request("/console/liveRoomList")
      ,toolbar: '#toolbar'
      ,id: 'incomeTotal_table'
      ,page: true
      ,curr: 0
      ,limit:Common.limit
      ,limits:Common.limits
      ,groups: 7
      ,cols: [[ //表头
           {field: 'nickName', title: '主播昵称'}
          // ,{field: 'name', title: '直播间名称',sort: true}
          ,{fixed: 'right', width: 300,title:"操作", align:'left', toolbar: '#incomeListBar'}
        ]]
		,done:function(res, curr, count){

            //权限判断
            var arr=['liveIncome-giftWater'];
            manage.authButton(arr);

            //获取零时保留的值
            var last_value = $("#incomeTotal_limlt").val();
            //获取当前每页大小
            var recodeLimit =  tableIns.config.limit;
            //设置零时保留的值
            $("#incomeTotal_limlt").val(recodeLimit);
            //判断是否改变了每页大小数
            if (last_value != recodeLimit){
                // 刷新
                table.reload("incomeTotal_table",{
                    url:request("/console/userList"),
                    page: {
                        curr: 1 //重新从第 1 页开始
                    }
                })
            }
            $(".timeComponent").hide();

		}
    });

    //列表操作
    table.on('tool(incomeTotal_table)', function(obj){
         var layEvent = obj.event,
            data = obj.data;
        if(layEvent === 'giftWater'){// 直播间礼物流水
            $("#giftTotalMsgDate").val("");
            $("#incomeName").hide();
            $(".search_live").hide();
            $(".visitPathDiv").hide();
            $(".searchIncomeTotal").hide();
        	var tableIns1 = table.render({
			      elem: '#liveRoomGiftWater_table'
			      ,url:request("/console/getGiftList")+"&userId="+data.userId
			      ,id: 'liveRoomGiftWater_table'
                  ,toolbar: '#toolbar'
			      ,page: true
			      ,curr: 0
                  ,limit:Common.limit
                  ,limits:Common.limits
			      ,groups: 7
			      ,cols: [[ //表头
					   {field: 'liveRoomName', title: '直播间名称', width:'15%'}
			       	  ,{field: 'giftId', title: '礼物id', width:'15%'}
			       	  ,{field: 'giftName', title: '礼物名称', width:'15%'}
			          ,{field: 'price', title: '价格', width:'15%'}
                      ,{field: 'actualPrice', title: '实际收入', width:'15%'}
			          ,{field: 'userId', title: '赠送人Id', width:'15%'}
			          ,{field: 'userName', title: '赠送人昵称', width:'15%'}
			          ,{field: 'toUserId', title: '接收人Id', width:'15%'}
			          ,{field: 'toUserName', title: '接收人昵称', width:'15%'}
			          ,{field: 'time', title: '赠送时间', width:'15%',templet: function(d){
			          		return UI.getLocalTime(d.time);
			          }}]]
					,done:function(res, curr, count){
                     checkRequst(res);
			      		// 初始化时间控件
						///layui.form.render('select');
						//日期范围
						layui.laydate.render({
							elem: '#giftTotalMsgDate'
							,range: "~"
							,done: function(value, date, endDate){
								var startDate = value.split("~")[0];
								var endDate = value.split("~")[1];


                                table.reload("liveRoomGiftWater_table",{
                                    url:request("/console/getGiftList"),
                                    page: {
                                        curr: 1 //重新从第 1 页开始
                                    },
                                    where: {
                                        userId : data.userId,  //搜索的关键字
                                   		startDate : startDate,
                                        endDate : endDate
                                    }
                                })
							}
							,max: 1
						});
						$(".current_total").empty().text((0==res.total ? 0: Common.filterHtmlData(res.total) ));
						$(".timeComponent").show();
                    	$("#incomeList").hide();
						$(".liveRoomGiftWater").show();

					}
			    });
        }
      });

    //搜索
    $(".search_live").on("click",function(){
       
            table.reload("incomeTotal_table",{
                url:request("/console/liveRoomList"),
                page: {
                    curr: 1 //重新从第 1 页开始
                },
                where: {
                    nickName : Common.getValueForElement("#incomeName")  //搜索的关键字
                }
            })
        $("#incomeName").val("");
    });
});

var Live={
	btn_back:function(){
        $(".timeComponent").hide();
		$("#incomeList").show();
		$("#liveRoomList").show();
		$("#addLiveRoom").hide();
		$("#liveRoomMsg").hide();
		$("#liveRoomUser").hide();
		$(".liveRoomGiftWater").hide();
        $("#incomeName").show();
        $(".search_live").show();
        $(".searchIncomeTotal").show();
        $(".visitPathDiv").show();
	}

}

$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})