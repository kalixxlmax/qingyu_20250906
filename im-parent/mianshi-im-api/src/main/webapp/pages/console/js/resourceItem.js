layui.use(['form','layer','laydate','table','laytpl'],function(){
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;
		
		//日志列表
	    var tableIns = table.render({
	      elem: '#resourc_table'
	      ,url:request("/console/get/resource/list")
	      ,id: 'resourc_table'
		  ,toolbar: '#toolbar'
	      ,page: true
	      ,curr: 0
          ,limit:Common.limit
          ,limits:Common.limits
	      ,groups: 9
	      ,cols: [[ //表头
	           {field: 'id', title: '编号',width:'13%'}
				,{field: 'fileName', title: '文件名称',width:'19%'}
				,{field: 'url', title: '外网路径',width:'17%'}
				,{field: 'path', title: '上传路径',width:'17%'}
				,{field: 'type', title: '类型',width:'9%',templet(d){
						if (1 == d.type){
							return '本机文件系统';
						}else if(2 == d.type){
							return  "fastDfs";
						}
					}}
				,{field: 'look', title: '预览文件',width:'8%',templet(d){
						var fileExtension = d.url.split('.').pop().toLowerCase();
						if (d.fileType == 'image' || fileExtension == "jpg" || fileExtension == "png" ){
							return "<button style='height: 26px;' class='layui-btn font_bule layui-btn-xs layui-btn-sm' onclick=\"resourceItem.showPicture('"+ d.url +"', '' )\">图片</button>";
						}else if (d.fileType == 'video' || fileExtension == "mp4"){
							return "<button style='height: 26px;' class='layui-btn font_bule layui-btn-xs layui-btn-sm' onclick=\"resourceItem.showVideo('"+ d.url +"', '' )\">视频</button>";
						}else if (d.fileType == 'audio' || fileExtension == "amr"){
							return "<button style='height: 26px;' class='layui-btn font_bule layui-btn-xs layui-btn-sm' onclick=\"resourceItem.showFile('"+ d.url +"', '' )\">语音</button>";
						}else if (d.fileType == 'other' || fileExtension == "zip" || fileExtension == "txt"){
							return "<button style='height: 26px;' class='layui-btn font_bule layui-btn-xs layui-btn-sm' onclick=\"resourceItem.showFile('"+ d.url +"', '' )\">其他</button>";
						}else{
							return "<button style='height: 26px;' class='layui-btn font_bule layui-btn-xs layui-btn-sm' onclick=\"resourceItem.showFile('"+ d.url +"', '' )\">预览文件</button>";
						}
					}}
				,{field: 'citations', title: '引用次数',width:'6%'}
				,{field: 'createTime',title:'创建时间', width:'11%',templet:function (d) {
						return UI.getLocalTime(d.createTime);
					}}
	          ]]
			,done:function(res, curr, count){
				checkRequst(res);
				//获取零时保留的值
				var last_value = $("#check_limlt").val();
				//获取当前每页大小
				var recodeLimit = tableIns.config.limit;
				$("#check_limlt").val(recodeLimit);
				//判断是否改变了每页大小数
				if (last_value != recodeLimit){
					// 刷新
					table.reload("resourc_table",{
						url:request("/console/product/showcase/list"),
						page: {
							curr: 1 //重新从第 1 页开始
						}
					})
				}


				//权限判断
				var arr=['resource-delete'];
				manage.authButton(arr);

				layui.use('form', function() {
					var form = layui.form;
					form.render();
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
						table.reload("resourc_table",{
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
			}
	    });

	//搜索
	$(".search_btn").on("click",function(){
		table.reload("resourc_table",{
			url:request("/console/get/resource/list"),
			page: {
				curr: 1 //重新从第 1 页开始
			},
			where: {
				keyword : Common.getValueForElement(".keyword"),  //搜索的关键字
				fileType : $("#fileType").val()
			}
		})
	});


	table.on('tool(resourc_table)', function(obj){
		var layEvent = obj.event,
			data = obj.data;

		if (layEvent === 'delete'){
			layer.confirm('确认要删除吗 ?', {
				btn : ['确定','取消']//按钮
				,skin : "layui-ext-motif"
			}, function(index) {
				resourceItem.delete(data.id);
				obj.del();
				layer.close(index);
			});
		}
	})
});


var resourceItem = {

	delete:function (date) {
		Common.invoke({
			url : request('/console/delete/resource'),
			data : {
				"id" : date,
			},
			errorMsg :  '删除失败，请稍后重试',
			success : function(result) {
				console.log(result);
				if (result.data){
					layer.msg('删除成功',{"icon":1});
				}
			},
			error : function(result) {
			}
		});
	},

	//预览视频内容
	showVideo:function (url , text) {
		var html="";
		// 视频处理
		html += '<div class="layui-timeline-content layui-text">'
			+'<video class="layui-timeline-title" height="600px" width="400px" controls="controls" src=' + url + '></video>'
			+'</div>';

		html += '<div style="padding: 2%;"><label class="layui-text layui-word-aux" style="padding: 0 19px!important;">描述' + ": "+ text +'</label></div>';
		$("#msgContent").empty();
		$("#msgContent").append(html);

		//弹出层
		layer.open({
			type: 1,
			title: "内容预览",
			offset: ['50px'],
			area: ['700px','800px'],
			shadeClose: true,
			shade: false,
			maxmin: true, //开启最大化最小化按钮
			content: $("#showMsgInfo")
		});
	},

	//预览图片内容
	showPicture:function (pictures,text) {
		var html;
		if (pictures != ""){
			var arr = pictures.split(",");
			for (var i = 0; i < arr.length ; i++){
				if (i == 0){
					html = '<img style="margin: 10px 0px 0px 10px;border: 1px solid red;" src=' + arr[i] + '>';
				}else{
					html += '<img style="margin: 10px 0px 0px 10px;border: 1px solid red;" src=' + arr[i] + '>';
				}
			}

			html += '<div style="padding: 2%;"><label class="layui-text layui-word-aux" style="padding: 0 19px!important;">描述' + ": " + text + '</label></div>';
		}else{
			html = '<div style="padding: 2%;"><label class="layui-text layui-word-aux" style="padding: 0 19px!important;">描述' + ": "  + text + '</label></div>';
		}

		//弹出层
		layer.open({
			type: 1,
			title: "内容预览",
			offset: ['50px'],
			area: ['700px','700px'],
			shadeClose: true,
			shade: false,
			maxmin: true, //开启最大化最小化按钮
			content: html
		});
	},


	//预览文本内容
	showText:function (text) {
		var html = '<div class="wrap">';
		html += '<div style="padding: 2%;"><label class="layui-text layui-word-aux" style="padding: 0 19px!important;">'+ text +'</label></div>';
		html += '</div>';

		//弹出层
		layer.open({
			type: 1,
			title: "内容预览",
			offset: ['50px'],
			area: ['700px'],
			shadeClose: true,
			shade: false,
			maxmin: true, //开启最大化最小化按钮
			content: html
		});
	},

	//预览语言内容 .amr文件
	showFile:function (text) {
		var html;
		//获取后缀
		var index = text.lastIndexOf(".");
		var suffix = text.substr(index+1);

		if ('amr' == suffix){

			html = '<div style="margin-left: 20px;"> <a href="' + text +'" width="68px" height="68px" target="_blank" controls>下载录音</a></div>' +
				"<div style='padding:20px;margin:0 auto;margin-left: 31%';'>" +
				"<img id='playVoice' src='/pages/console/images/voice.png' onclick=\"resourceItem.playVoice('"+ text +"')\">" +
				"<img id='playVoiceGif' src='/pages/console/images/voice.gif' style='display: none;'>" +
				"<button class='layui-btn table_default_btn stopVoiceBtn' style='margin-left: 31px;'>停止暂停</button>" +
				"</div>";

		}else if ('mp3' == suffix){

			html = '<audio controls style="margin-left: 29%;">' +
				'<source src="'+ text +'" type="audio/ogg">' +
				'</audio>';

		}else{
			html = '<div style="padding:20px;margin:0 auto;"> <a href="' + text +'" width="68px" height="68px" target="_blank" controls>点击查看文件' +'</a></div>';
		}

		//弹出层
		layer.open({
			type: 1,
			title: "内容预览",
			offset: ['50px'],
			area: ['700px','150px'],
			shadeClose: true,
			shade: false,
			maxmin: true, //开启最大化最小化按钮
			content: html
		});
	},

	//播放amr语音
	playVoice:function(url){
		var amr = new BenzAMRRecorder();

		//初始化
		amr.initWithUrl(url).then(function() {
			console.log("语言时长（秒）：",amr.getDuration());
			$("#playVoice").hide();
			$("#playVoiceGif").show();
			amr.play();
		});

		amr.onEnded(function() {
			$("#playVoice").show();
			$("#playVoiceGif").hide();
		})

		//暂停播放
		$(".stopVoiceBtn").click(function () {
			amr.stop();
		})
	},
}

var UploadInfo={
	// 上传
	upload:function(){
		$("#uploadSmallFileFrom").ajaxSubmit(function(data){
			var obj = eval("("+data+")");
			console.log(obj.url);
			$("#photoSmallUrl").val(obj.url);
			var imgUrl  =local_data_get("upload_showcase_img");
			$("#" + imgUrl +"").attr("src",obj.url);
		})
	},
	// 选择文件
	selectSmallFile:function(data){
		local_data_sava("upload_showcase_img",data);
		$("#photoSmallUpload").click();
	}
}

$(function(){
	$("#uploadSmallFileFrom").attr("action", Config.getConfig().uploadUrl + "upload/UploadifyServlet");
	//调用父级页面的Js函数
	window.parent.getJointVisitPath();
})