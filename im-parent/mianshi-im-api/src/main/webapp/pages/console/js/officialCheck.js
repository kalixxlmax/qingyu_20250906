layui.use(['form','layer','laydate','table','laytpl'],function(){
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;
		
		//日志列表
	    var tableIns = table.render({

	      elem: '#offilical_table'
	      ,url:request("/console/checkOfficialInfo")
	      ,id: 'offilical_table'
	      ,page: true
	      ,curr: 0
          ,limit:Common.limit
          ,limits:Common.limits
	      ,groups: 9
	      ,cols: [[ //表头
	           {field: 'id', title: '编号',width:'13%'}
	          ,{field: 'telephone', title: '电话',width:150}
	          ,{field: 'companyType', title: '公司类型', width:'11%',templet:function(d){return (d.companyType==1?"企业":d.companyType==0?"个体工商户":"");}}
	          ,{field: 'createTime',title:'操作时间', width:'11%',templet: function(d){return UI.getLocalTime(d.createTime);}}
	          ,{field: 'companyName', title: '公司名称',width:'11%'}
	          ,{field: 'verify', title: '审核结果', width:'11%',templet:function(d){return (d.verify==1?"审核通过":d.verify==2?"审核不通过":d.verify==0?"审核中":"");}}
	          ,{field: 'feedback', title: '审核回馈', width:'11%'}
	          ,{fixed: 'right', width: '20%',title:"操作", align:'left', toolbar: '#openCheckLogListBar'}
	          ]]
			,done:function(res, curr, count){
				checkRequst(res);
				console.log(res);
				//权限判断
				var arr=['offic-delete','offic-detail'];
				manage.authButton(arr);

				//获取零时保留的值
				var last_value = $("#check_limlt").val();
				//获取当前每页大小
				var recodeLimit = tableIns.config.limit;
				$("#check_limlt").val(recodeLimit);
				//判断是否改变了每页大小数
				if (last_value != recodeLimit){
					// 刷新
					table.reload("offilical_table",{
						url:request("/console/checkOfficialInfo"),
						page: {
							curr: 1 //重新从第 1 页开始
						}
					})
				}

			}
	    });
	    // 表格操作
	    table.on('tool(offilical_table)', function(obj){
	        var layEvent = obj.event,
	            data = obj.data;
            if (layEvent === 'detail'){
              /*  sessionStorage.setItem("descId",data.id);
                window.location.href="/pages/console/officialDesc.html";*/
				operation.descBtn();
				operation.officialDesc(data);
            }

	        if(layEvent === 'del'){ //删除日志
	        	layer.confirm('确定删除该日志？',{icon:3, title:'提示信息'},function(index){
                    layer.close(index);
					operation.deleteOfficial(data.id);
	        		obj.del();
	        	})	
	        }


        })
});

var operation={
	backBtn:function(){
		$("#officialDesc").hide();
		$("#tableDiv").show();
		layui.table.reload("offilical_table",{
			url:request("/console/checkOfficialInfo"),
			page: {
				curr: 1 //重新从第 1 页开始
			}
		})
		layui.form.render();
	}
	,deleteOfficial:function(id){
		Common.invoke({
			url:request('/console/delOfficialInfo'),
			data:{
				id:id
			},
			success:function(result){
				layui.layer.alert("删除成功");
			}
		})
	}
	//显示
	,descBtn:function () {
		$("#officialDesc").show();
		$("#tableDiv").hide();
	}
	//审核
	,officialDesc:function (data) {
		$("#telephone").val(Common.filterHtmlData(data.telephone));
		$("#companyName").val(Common.filterHtmlData(data.companyName));
		$("#companyBusinessLicense").val(Common.filterHtmlData(data.companyBusinessLicense));
		$("#adminName").val(Common.filterHtmlData(data.adminName));
		$("#adminID").val(Common.filterHtmlData(data.adminID));
		$("#desc").val(Common.filterHtmlData(data.desc));
		$("#id").val(Common.filterHtmlData(data.id));
		$("#feedback").val("");
		$("#adminTelephone").val(Common.filterHtmlData(data.adminTelephone));
		$("#industryImg").attr('src',data.industryImg);
		$("#verify").val(Common.filterHtmlData(data.verify));
		if (data.companyType === 1){
			$("#companyType").val("企业");
		}else{
			$("#companyType").val("个体工商户");
		}
		$("#country").append("<option value=''>"+Common.filterHtmlData(data.country)+"</option>");
		$("#province").append("<option value=''>"+Common.filterHtmlData(data.province)+"</option>");
		$("#city").append("<option value=''>"+Common.filterHtmlData(data.city)+"</option>");
		layui.form.render();
	}
	//审核提交
	,officialCheckSubmit:function () {
		var verify = Common.getValueForElement("#verify");
		var feedback = Common.getValueForElement("#feedback");
		var id = Common.getValueForElement("#id");
		if (feedback == undefined){
			feedback = "";
		}
		var data1 = {
			"feedback":feedback,
			"verify":verify,
			"id":id
		}
		$.ajax({
			type:"POST",
			url:request('/console/updateOfficialInfo'),
			dataType:"json",
			data:data1,
			async:false,
			success : function(result) {
				if (result.resultCode != 1) {
					layer.msg(result.resultMsg);
					return false;
				}else{
					layer.msg("审核成功！");
					operation.backBtn();
				}
			},
			error : function(result) {
				layer.msg(result.resultMsg);
			}
		})
	}
}