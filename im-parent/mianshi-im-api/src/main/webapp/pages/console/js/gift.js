$(function(){
	//调用父级页面的Js函数
	window.parent.getJointVisitPath();

	//权限判断
	var arr=['gift-add','gift-delete'];
	manage.authButton(arr);
})


layui.config({
	base : "/pages/common/dropdown/"
}).use(['form','layer','laydate','table','laytpl','dropdown'],function() {
	var form = layui.form,
		layer = parent.layer === undefined ? layui.layer : top.layer,
		$ = layui.jquery,
		laydate = layui.laydate,
		laytpl = layui.laytpl,
		table = layui.table,
		dropdown = layui.dropdown;
	//用户列表
	var tableInUser = table.render({
		elem: '#gift_table'
		, url: request("/console/giftList")
		, id: 'gift_table'
		, page: {
			layout: ['prev', 'page', 'next', 'limit', 'count', 'skip'] //自定义分页布局
			, groups: 3 //只显示 1 个连续页码
			, first: false //不显示首页
			, last: false //不显示尾页
		}
		, curr: 0
		, limit: Common.limit
		, limits: Common.limits
		, groups: 7
		, cols: [[ //表头
			 {field: 'name', title: '礼物名称', width: '15%'}
			, {field: 'photo', title: '礼物url', width: '15%'}
			, {field: 'price', title: '礼物价格', width: '15%'}
			/*, {field: 'type', title: '礼物类型', width: '15%'}*/
			, {fixed: 'right', width: '15%', title: "操作", align: 'left', toolbar: '#operationBar'}

		]]
		, done: function (res, curr, count) {
			checkRequst(res);

		}
	});
});

var Gift={
	findGiftList:function(){
		layui.table.reload("gift_table",{
			url:request("/console/giftList"),
			page: {
				curr: 1 //重新从第 1 页开始
			},
			where: {
				name:$("#giftName").val()
			}
		})
		$("#type").val("");

	},
	// 删除礼物
	deleteGift:function(giftId){
		layer.confirm('确定删除该礼物？',{icon:3, title:'提示信息',skin : "layui-ext-motif"},function(index){
			Common.invoke({
			url:request('/console/delete/gift'),
			data:{
				giftId:giftId
			},
			success:function(result){
				if(result.resultCode==1){
					layer.msg("删除礼物成功",{icon:1});
					layui.table.reload("gift_table", {
						page: {
							curr: $(".layui-laypage-em").next().html(), //重新从当前页开始
						}
					})
				}
			}
		})
		});
		
	},
	// 新增礼物
	addGift:function(){
		$(".gift_list").hide();
		$("#addGift").show();
        $("#add_giftName").val("")
        $("#giftUrl").val("")
        $("#giftPrice").val("")
		$(".visitPathDiv").hide();
	},
	back:function(){
		$(".gift_list").show();
		$("#addGift").hide();
	},
	commit_addGift:function(){
		console.log($("#add_giftName").val());
		if($("#add_giftName").val()==""){
			layer.alert("请输入礼物名称");
			return;
		}else if($("#giftUrl").val()==""){
			layer.alert("请输入礼物路径");
			return;

		}else if($("#giftPrice").val()==""){
			layer.alert("请输入礼物价格");
			return;
		}else if($("#giftPrice").val()!=""){
            // 充值金额（正整数）的正则校验
            if(!/^(?!00)(?:[0-9]{1,3}|1000)$/.test($("#giftPrice").val())){
                layer.msg("礼物价格必须为 1-1000 的整数",{"icon":2});
                return;
            }
        }/*else if($("#giftType").val()==""){
			layer.alert("请选择礼物类型");
			return;
		}*/
		Common.invoke({
			url:request('/console/add/gift'),
			data:{
				name: Common.getValueForElement("#add_giftName"),
				photo: Common.getValueForElement("#giftUrl"),
				price: Common.getValueForElement("#giftPrice"),
				/*type: Common.getValueForElement("#giftType")*/
			},
			success:function(result){
				if(result.resultCode==1){
					layer.msg("新增礼物成功",{icon:1});
					Gift.back();
					layui.table.reload("gift_table", {
						page: {
							curr: $(".layui-laypage-em").next().html(), //重新从当前页开始
						}
					})
				}
			}
		})
	}
}