/**
 * 统计报表相关的js
 */
// 基于准备好的dom，初始化echarts实例
function   DataSort(datas){

         var  objectList = new Array();
         var sortMap = new Map();//定义返回数据
         function sersis(mykey,mydata){
             this.mykey=mykey;
             this.mydata=mydata;
         }
        datas.map(function (item) {
			for(var time in item){ 
				//return time; 
				objectList.push(new sersis(time,item[time]));
			}

		})
         //按日期从小到大排序
         objectList.sort(function(a,b){
             return  a.mykey>b.mykey;
         });
        
        for (var j = 0; j < objectList.length; j++) {
             var thisobj = objectList[j];
             //xAxisValues.push(parseInt(thisobj.mykey));
             //obj.data.push(parseFloat(thisobj.mydata[0]));
             console.log("sort test =====>>> " +thisobj.mykey +"   "+thisobj.mydata);
             //--------------------------
             //sortMap[thisobj.mykey] = thisobj.mydata;
        }
         return objectList;
}




$(function(){
	layui.use('laydate', function(){
	  var laydate = layui.laydate,
	          form  = layui.form;
	  form.render('select');

	  //日期范围
	 var time =  laydate.render({
	    elem: '#globalDate'
	    ,range: "~"
	    ,done: function(value, date, endDate){  // choose end
		    //console.log("date callBack====>>>"+value); //得到日期生成的值，如：2017-08-18
		    var startDate = value.split("~")[0];
		    var endDate = value.split("~")[1]; 
		    var timeUnit =  $(".global-time-unit").val()

		    Count.loadUserOnlineStatus(startDate,endDate,timeUnit);
		    Count.loadUserRegisterCount(startDate,endDate,timeUnit);
	    	Count.loadChatMsgCount(startDate,endDate,timeUnit);
	    	Count.loadAddRoomsCount(startDate,endDate,timeUnit);
	    	Count.loadAddFriendsCount(startDate,endDate,timeUnit);

		}
		,max: 1
	  });

	  	Count.getUserRoomFriendMsgTotalNum();

	  	Count.loadUserOnlineStatus();
    	Count.loadUserRegisterCount();
    	Count.loadChatMsgCount();
    	Count.loadAddRoomsCount();
    	Count.loadAddFriendsCount();


   layui.form.on('select(global-time-unit)', function(data){
	  	var dateRange = $("#globalDate").val();


	  	// if(data.value==3){//时间单位切换到小时

	  	//}else
	  	if(data.value==4){ //时间单位切换到分钟

	  		$("#globalDate").val();

	  		$("#globalDate").attr("disabled","");

	  		$(".prompt_info").text("注：时间单位若为分钟，不能选择时间范围,只会显示当前这一天的数据");

	  		dateRange = "";
	  	}else{
	  		$("#globalDate").removeAttr("disabled");
	  		$(".prompt_info").text("");
	  	}

	  	var startDate = dateRange.split("~")[0];
	  	var endDate = dateRange.split("~")[1];

	  	Count.loadUserOnlineStatus(startDate,endDate,data.value);
	  	Count.loadUserRegisterCount(startDate,endDate,data.value);
    	Count.loadChatMsgCount(startDate,endDate,data.value);
    	Count.loadAddRoomsCount(startDate,endDate,data.value);
    	Count.loadAddFriendsCount(startDate,endDate,data.value);
   		 });
	});


	Common.invoke({
		url : request('/console/serven/statistics'),
		data : {},
		successMsg : false,
		errorMsg :  "加载数据失败，请稍后重试",
		success : function(result) {
			var data = [];
			var servenUserActiveSum = 0;
			var servenNewSum = 0;
			for(let i in result.data.servenUserActive) {
				var flag = {
					"product":i,
					"七日用户活跃量":result.data.servenUserActive[i],
					"七日新增用户":result.data.servenNew[i]
				};
				/*var flag = {
					"product":i,
					"七日用户活跃量":3,
					"七日新增用户":10
				};*/
				data.unshift(flag);
				servenUserActiveSum = servenUserActiveSum + result.data.servenUserActive[i];
				servenNewSum = servenNewSum + result.data.servenNew[i];
			}
			$(".sevenStatistics_count_active").html(servenUserActiveSum);
			$(".sevenStatistics_count_new").html(servenNewSum);
			var myChart = echarts.init(document.getElementById('sevenStatistics'));
			// 指定图表的配置项和数据
			var option =  {
				tooltip: {},
				color: ['#FFD8D8','#FF6666'],
				dataset: {
					dimensions: ['product', '七日用户活跃量', '七日新增用户'],
					source: data,
				},
				grid:{
					left: 50,
					right: 16,
					bottom:37,
				},
				xAxis: {type: 'category'},
				yAxis: {},
				series: [
					{type: 'bar', barWidth:'23'},
					{type: 'bar', barWidth:'23',}
				]
			};

			// 使用刚指定的配置项和数据显示图表。
			myChart.setOption(option);

		},
		error : function(result) {
		}
	});


	Common.invoke({
		url : request('/console/online/user/count'),
		data : {},
		successMsg : false,
		errorMsg :  "加载数据失败，请稍后重试",
		success : function(result) {
			var data = [];
			var flag = {
				"name":'最高在线人数',
				"value":result.data.maxOnlineCount
			};
			var flag_ = {
				"name":'当前在线人数',
				"value":result.data.todayLoginNumber
			};
			data.push(flag);
			data.push(flag_);
			$(".onlineUser_count_max").html(result.data.maxOnlineCount);
			$(".onlineUser_count_now").html(result.data.todayLoginNumber);
			// 基于准备好的dom，初始化echarts实例
			var myChart = echarts.init(document.getElementById('onlineUser'));
			// 指定图表的配置项和数据
			var option =  {
				tooltip: {
					trigger: 'item'
				},
				color: ['#FF6666',"#FFD8D8"],
				series: [
					{
						name: '访问来源',
						type: 'pie',
						radius: ['40%', '53%'],
						center: ['107%', '48%'],//设置饼图位置
						width:'47%',
						avoidLabelOverlap: false,
						label: {
							show: false,
							position: 'center'
						},
						emphasis: {
							label: {
								show: false,
								fontSize: '20',
								fontWeight: 'bold'
							}
						},
						labelLine: {
							show: false
						},
						data: data
					}
				]
			};

			// 使用刚指定的配置项和数据显示图表。
			myChart.setOption(option);
		},
		error : function(result) {
		}
	});

});



var  Count = {
	//加载用户注册数据
	loadUserRegisterCount : function (startDate,endDate,timeUnit){

		Common.invoke({
		      url : request('/console/getUserRegisterCount'),
		      data : {
		      	startDate:startDate,
		      	endDate:endDate,
		      	timeUnit:timeUnit
		      },
		      successMsg : false,
		      errorMsg :  "加载数据失败，请稍后重试",
		      success : function(result) {

		        var data = result.data; // DataSort(result.data);
			  // 基于准备好的dom，初始化echarts实例
			  var myChart = echarts.init(document.getElementById('userRegisterCount'));
			  // 指定图表的配置项和数据
			  var option = {
				  tooltip: {
					  trigger: 'axis'
				  },
				  legend: {
					  data: ['用户注册数量'],
					  x:'center',      //可设定图例在左、右、居中
					  y:'bottom',     //可设定图例在上、下、居中
					  left:700
				  },
				  color: ['#FF6666'],
				  toolbox: {
					  show: false,
					  orient: 'vertical',
					  right: 160,
					  feature: {
						  magicType: {type: ['line', 'bar']}
					  }
				  },
				  grid:{
					  right: 170,
					  left: 50
				  },
				  xAxis: {
					  type: 'category',
					  data:data.map(function (item) {
						  for(var time in item){ return time; }
					  })
				  },
				  yAxis: {
					  type: 'value'
				  },
				  series: [
					  {
						  name: '用户注册数量',
						  type: 'bar',
						  data: data.map(function (item) {
							  for( var time in item){ return item[time]}
						  })
					  }
				  ]
			  };

			  // 使用刚指定的配置项和数据显示图表。
			  myChart.setOption(option);

		      },
		      error : function(result) {

		      }
	    });

	},
	/** 获取用户房间好友等总数量 **/
	getUserRoomFriendMsgTotalNum : function(){

			Common.invoke({
			      url : request('/console/countNum'),
			      data : {},
			      successMsg : false,
			      errorMsg :  "加载数据失败，请稍后重试",
			      success : function(result) {
			      		// show view
					  console.log(result)
			      		var totalData = result.data;
			      		$(".user-total").text( Common.filterHtmlData(totalData["userNum"]) );
			      		$(".room-total").text( Common.filterHtmlData(totalData["roomNum"]) );
			      		$(".chat-msg-total").text( Common.filterHtmlData(totalData["msgNum"]) );
			      		$(".friends-total").text( Common.filterHtmlData(totalData["friendsNum"]) );
					    $(".serverActive-total").text( Common.filterHtmlData(totalData["servenActiveCount"]) );
					  	$(".baseHeightOnlineCount-total").text(Common.filterHtmlData(totalData["maxOnlineCount"]) );
					    $(".servenNewUser-total").text( Common.filterHtmlData(totalData["servenNewUser"]) );
					  	$(".todayLoginCount-total").text( Common.filterHtmlData(totalData["todayLoginNumber"]) );
					  	$(".live-total").text(Common.filterHtmlData(totalData["liveNum"]));
			      },
			      error : function(result) {
			      }
	    	});
	},
	//加载单聊消息统计数据
	loadChatMsgCount : function (startDate,endDate,timeUnit){
		if (Common.isNil(startDate) && Common.isNil(endDate)){
			endDate = thisTime();
			startDate = beforeHalfTime();
		}
		Common.invoke({
		      url : request('/console/chatMsgCount'),
		      data : {
		      	startDate:startDate,
		      	endDate:endDate,
		      	timeUnit:timeUnit
		      },
		      successMsg : false,
		      errorMsg :  "加载数据失败，请稍后重试",
		      success : function(result) {

		        var data = result.data;
				  // 基于准备好的dom，初始化echarts实例
				  var myChart = echarts.init(document.getElementById('chatMsgSumCount'));
				  // 指定图表的配置项和数据
				  var option = {
					  tooltip: {
						  trigger: 'axis'
					  },
					  legend: {
						  data: ['单聊消息数量'],
						  x:'center',      //可设定图例在左、右、居中
						  y:'bottom',     //可设定图例在上、下、居中
						  left:700,
					  },
					  color: ['#FF6666'],
					  toolbox: {
						  show: false,
						  orient: 'vertical',
						  right: 160,
						  feature: {
							  magicType: {type: ['line', 'bar']}
						  }
					  },
					  grid:{
						  right: 210,
						  left: 50
					  },
					  xAxis: {
						  type: 'category',
						  boundaryGap: false,
						  data:data.map(function (item) {
							  for(var time in item){ return time; }
						  })
					  },
					  yAxis: {
						  type: 'value'
					  },
					  series: [
						  {
							  name: '单聊消息数量',
							  type: 'line',
							  data: data.map(function (item) {
								  for( var time in item){ return item[time]}
							  })
						  }
					  ]
				  };

				  // 使用刚指定的配置项和数据显示图表。
				  myChart.setOption(option);

		      },
		      error : function(result) {

		      }
	    });

	},
	//加载添加好友统计数据
	loadAddFriendsCount : function (startDate,endDate,timeUnit){

		Common.invoke({
		      url :request('/console/addFriendsCount'),
		      data : {
		      	startDate:startDate,
		      	endDate:endDate,
		      	timeUnit:timeUnit
		      },
		      successMsg : false,
		      errorMsg :  "加载数据失败，请稍后重试",
		      success : function(result) {

		        var data = result.data;
				  // 基于准备好的dom，初始化echarts实例
				  var myChart = echarts.init(document.getElementById('friendsRelationCount'));
				  // 指定图表的配置项和数据
				  var option = {
					  tooltip: {
						  trigger: 'axis'
					  },
					  legend: {
						  data: ['添加好友数量'],
						  x:'center',      //可设定图例在左、右、居中
						  y:'bottom',     //可设定图例在上、下、居中
						  left:700
					  },
					  color: ['#FF6666'],
					  toolbox: {
						  show: false,
						  orient: 'vertical',
						  right: 160,
						  feature: {
							  magicType: {type: ['line', 'bar']}
						  }
					  },
					  grid:{
						  right: 210,
						  left: 50
					  },
					  xAxis: {
						  type: 'category',
						  boundaryGap: false,
						  data:data.map(function (item) {
							  for(var time in item){ return time; }
						  })
					  },
					  yAxis: {
						  type: 'value'
					  },
					  series: [
						  {
							  name: '添加好友数量',
							  type: 'line',
							  data: data.map(function (item) {
								  for( var time in item){ return item[time]}
							  })
						  }
					  ]
				  };

				  // 使用刚指定的配置项和数据显示图表。
				  myChart.setOption(option);
		      },
		      error : function(result) {

		      }
	    });

	},
	loadUserOnlineStatus : function(startDate,endDate,timeUnit){

		var data={
			time:new Array(),
			value:new Array()
		};

		Common.invoke({
		      url : request('/console/getUserStatusCount'),
		      data : {
		      	startDate:startDate,
		      	endDate:endDate,
		      	timeUnit:timeUnit
		      },
		      successMsg : false,
		      errorMsg :  "用户在线状态数据加载失败，请稍后重试",
		      success : function(result) {

				var data = result.data;

		    	/*var list=result.data;
				var time=null;
				for (var index in list) {
					if(3==timeUnit){
						time= new Date(list[index].time*1000).Format("yyyy-MM-dd");
					}else if(2==timeUnit){
						time= new Date(list[index].time*1000).Format("yyyy-MM-dd hh:00");
					}else{ 
						time= new Date(list[index].time*1000).Format("yyyy-MM-dd hh:mm");
					}
					  
					data.time.push(time);
  					data.value.push(list[index].count)
				 }*/



				  // 基于准备好的dom，初始化echarts实例
				  var myChart = echarts.init(document.getElementById('userOnlineNumCount'));
				  // 指定图表的配置项和数据
				  var option = {
					  tooltip: {
						  trigger: 'axis'
					  },
					  grid:{
						  right: 210,
						  left: 50
					  },
					  legend: {
						  data: ['用户在线数量'],
						  x:'center',      //可设定图例在左、右、居中
						  y:'bottom',     //可设定图例在上、下、居中
						  left:700
					  },
					  color: ['#FF6666'],
					  toolbox: {
						  show: false,
						  orient: 'vertical',
						  right: 160,
						  feature: {
							  magicType: {type: ['line','bar']}
						  }
					  },
					  xAxis: {
						  type: 'category',
						  data:data.map(function (item) {
							  for(var time in item){ return time; }
						  })
					  },
					  yAxis: {
						  type: 'value'
					  },
					  series: [
						  {
							  name: '用户在线数量',
							  type: 'bar',
							  data: data.map(function (item) {
								  for( var time in item){ return item[time]}
							  })
						  }
					  ]
				  };

				  // 使用刚指定的配置项和数据显示图表。
				  myChart.setOption(option);
		      

		      },
		      error : function(result) {

		      }
	    });

	},
	//加载群聊聊消息统计数据
	loadGroupMsgCount : function (roomJId,startDate,endDate,timeUnit){

		if(roomJId==null || roomJId=="" || roomJId==undefined){
			
			return;
		}
		Common.invoke({
		      url : request('/console/groupMsgCount'),
		      data : {
		    	roomId:roomJId,
		      	startDate:startDate,
		      	endDate:endDate,
		      	timeUnit:timeUnit
		      },
		      successMsg : false,
		      errorMsg :  "加载数据失败，请稍后重试",
		      success : function(result) {

		        var data = result.data;
		        //基于准备好的dom，初始化echarts实例
				var groupMsgSumCount = echarts.init(document.getElementById('groupMsgCount'),'shine');
				
				// 使用刚指定的配置项和数据显示图表。
				groupMsgSumCount.setOption(option = {
				        title: {
				            text: '群聊统计图'
				        },
				        tooltip: {
				            trigger: 'axis'
				        },
				        xAxis: {
				            data: data.map(function (item) {
				            	for(var time in item){ return time; }
				            })
				        },
				        yAxis: {
				            splitLine: {
				                show: false
				            }
				        },
				        toolbox: {
				            left: 'center',
							orient: 'vertical',
							right: 160,
				            feature: {
				                dataZoom: {
				                    yAxisIndex: 'none'
				                },
				                restore: {},
				                saveAsImage: {}
				            }
				        },
				        dataZoom: [{
				            startValue: '2014-06-01'
				        }, {
				            type: 'inside'
				        }],
				        visualMap: {
				            top: 10,
				            right: 10,
				            pieces: [{
				                gt: 0,
				                lte: 50,
				                color: '#096'
				            }, {
				                gt: 50,
				                lte: 100,
				                color: '#ffde33'
				            }, {
				                gt: 100,
				                lte: 150,
				                color: '#ff9933'
				            }, {
				                gt: 150,
				                lte: 200,
				                color: '#cc0033'
				            }, {
				                gt: 200,
				                lte: 300,
				                color: '#660099'
				            }, {
				                gt: 300,
				                color: '#7e0023'
				            }],
				            outOfRange: {
				                color: '#999'
				            }
				        },
				        series: {
				            name: '群聊消息数量',
				            type: 'line',
				            data: data.map(function (item) {
				            	for( var time in item){ return item[time]}
				            })
				        }
				 });


				//群聊消息统计图
				layui.layer.open({
                    title:"",
					skin: 'layui-ext-motif',
                    type: 1,
                    shade: false,
                    area: ['700px', '450px'],
                    shadeClose: true, //点击遮罩关闭
                    content: $("#groupMsgChart"),
                    cancel: function(index, layero){ 
					  //if(confirm('确定要关闭么')){ //只有当点击confirm框的确定时，该层才会关闭
					    layer.close(index)
					    $("#groupMsgChart").hide();
					 // }
					  return false; 
					},
					success : function(layero,index){  //弹窗打开成功后的回调
                		//layui.form.render();

                		 layui.form.render('select');

	  					  //日期范围
						  layui.laydate.render({
						    elem: '#groupMsgDate'
						    ,range: "~"
						    ,done: function(value, date, endDate){  // choose end
							    //console.log("date callBack====>>>"+value); //得到日期生成的值，如：2017-08-18
							    var startDate = value.split("~")[0];
							    var endDate = value.split("~")[1]; 
							    var timeUnit =  $(".groupMsg_time_unit").val()

							    Count.loadGroupMsgCount(roomJId,startDate,endDate,timeUnit);
							   
							}
							,max: 1
						  });

						 //时间单位切换
						  layui.form.on('select(groupMsg_time_unit)', function(data){
							  var dateRange = $("#groupMsgDate").val(); 
							  var startDate = dateRange.split("~")[0];
							  var endDate = dateRange.split("~")[1]; 

							   Count.loadGroupMsgCount(roomJId,startDate,endDate,data.value);
						    	
						  }); 


                	}


                });
				
				

		      },
		      errorCb : function(result) {

		      }
	    });

	},
	//加载添加群组统计数据
	loadAddRoomsCount : function (startDate,endDate,timeUnit){

		Common.invoke({
		      url : request('/console/addRoomsCount'),
		      data : {
		      	startDate:startDate,
		      	endDate:endDate,
		      	timeUnit:timeUnit
		      },
		      successMsg : false,
		      errorMsg :  "加载数据失败，请稍后重试",
		      success : function(result) {

		        var data = result.data;

				  // 基于准备好的dom，初始化echarts实例
				  var myChart = echarts.init(document.getElementById('addRoomsCount'));
				  // 指定图表的配置项和数据
				  var option = {
					  tooltip: {
						  trigger: 'axis'
					  },
					  legend: {
						  data: ['创建群组数量'],
						  x:'center',      //可设定图例在左、右、居中
						  y:'bottom',     //可设定图例在上、下、居中
						  left:700,
					  },
					  color: ['#FF6666'],
					  toolbox: {
						  show: false,
						  orient: 'vertical',
						  right: 160,
						  feature: {
							  magicType: {type: ['line', 'bar']}
						  }
					  },
					  grid:{
						  right: 210,
						  left: 50
					  },
					  xAxis: {
						  type: 'category',
						  boundaryGap: false,
						  data:data.map(function (item) {
							  for(var time in item){ return time; }
						  })
					  },
					  yAxis: {
						  type: 'value'
					  },
					  series: [
						  {
							  name: '创建群组数量',
							  type: 'line',
							  data: data.map(function (item) {
								  for( var time in item){ return item[time]}
							  })
						  }
					  ]
				  };

				  // 使用刚指定的配置项和数据显示图表。
				  myChart.setOption(option);

		      },
		      error : function(result) {

		      }
	    });

	}
};

//当天 yyyy-mm-dd
function thisTime() {
	var nowDate = new Date();
	return Common.formatDate(nowDate,"yyyy-MM-dd",1);
}

//半年前 yyyy-mm-dd
function beforeHalfTime() {
	var halfTime = 60 * 24 * 3600 * 1000;
	var pastDate = new  Date(new Date().getTime() - halfTime);
	return Common.formatDate(pastDate,"yyyy-MM-dd",1);
}

layui.use(['form','layer','laydate','table','laytpl'],function() {
	var form = layui.form,
		layer = parent.layer === undefined ? layui.layer : top.layer,
		$ = layui.jquery,
		laydate = layui.laydate,
		laytpl = layui.laytpl,
		table = layui.table,
		element = layui.element; //Tab的切换功能，切换事件监听等，需要依赖element模块;


});

