var operationHomePage_ = {
		submitFrom:function () {
			var name = $(".name").val();
			var homeUrl = $(".homeUrl").val();
			var imgUrl = $("#uploadSmall_url").attr("src");
			if (Common.isNil(name)){
				layer.msg("请输入名称！",{"icon":2});
				return;
			}
			if (Common.isNil(homeUrl)){
				layer.msg("请输入地址！",{"icon":2});
				return;
			}
			if (Common.isNil(imgUrl)){
				layer.msg("请选择图片！",{"icon":2});
				return;
			}
			Common.invoke({
				url:request('/console/homepage/config/set'),
				data:{
					name:name,
					homeUrl:homeUrl,
					imgUrl:imgUrl
				},
				success:function(result){
					if(result.resultCode==1){
						layer.msg("保存成功！",{"icon":1});
					}
				}
			})
		},
		// 上传
		upload:function(){
			$("#uploadSmallFileFrom").ajaxSubmit(function(data){
				var obj = eval("("+data+")");
				console.log(obj);
				$("#uploadSmall_url").attr("src",obj.url);
			})
		},
		// 选择文件
		selectSmallFile:function(){
			$("#photoSmallUpload").click();
		},
		submitWebAddress:function(){
		    var name = $(".webAddressName").val();
            var homeUrl = $(".webAddressHomeUrl").val();
            var imgUrl = $("#webAddressUrl").attr("src");
            if (Common.isNil(name)){
                layer.msg("请输入名称！",{"icon":2});
                return;
            }
            if (Common.isNil(homeUrl)){
                layer.msg("请输入地址！",{"icon":2});
                return;
            }
            if (Common.isNil(imgUrl)){
                layer.msg("请选择图片！",{"icon":2});
                return;
            }
            Common.invoke({
                url:request('/console/webAddress/config/set'),
                data:{
                    name:name,
                    homeUrl:homeUrl,
                    imgUrl:imgUrl
                },
                success:function(result){
                    if(result.resultCode==1){
                        layer.msg("保存成功！",{"icon":1});
                    }
                }
            })

		},
		// 上传
        uploadWebAddress:function(){
            $("#uploadWebAddressFileFrom").ajaxSubmit(function(data){
                var obj = eval("("+data+")");
                console.log(obj);
                $("#webAddressUrl").attr("src",obj.url);
            })
        },
        // 选择文件
        selectWebAddressFile:function(){
            $("#photoWebAddressUpload").click();
        },
}


$(function () {
      $("#uploadSmallFileFrom").attr("action", Config.getConfig().uploadUrl + "upload/UploadifyServlet");
      $("#uploadWebAddressFileFrom").attr("action", Config.getConfig().uploadUrl + "upload/UploadifyServlet");
      Common.invoke({
          url:request('/console/clientConfig'),
          data:{
          },
          success:function(result){
              console.log(result);
              if(result.resultCode==1){
                  var homeAddress = result.data.homeAddress;
                  $(".name").val('');
                  $("#homeUrl").attr('');
                  $("#uploadSmall_url").attr("src",result.data.picturn);

                  $(".webAddressName").val('');
                  $(".webAddressHomeUrl").attr('');
                  $("#webAddressUrl").attr("src",result.data.picturn);
                  layui.form.render();
              }
          }
      })
 })