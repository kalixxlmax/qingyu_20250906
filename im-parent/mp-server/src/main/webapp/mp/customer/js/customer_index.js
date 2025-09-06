var customer = {
    //客服管理
    serviceManage:function (num,type,isShow) {
       

        customer.getServiceList(num);
        if (type == 0){
            UI.limit(0,"service_manage")
        }
        if (isShow != -1){
            customer.getServiceVisitorCount("","",2);
        }

    }
     //回复菜单管理
     ,replyMenuManage:function(num, type){


           customer.getReplyMenuList(num);
           if (type == 0){
               UI.limit(0,"replyMenu_manage")
           }

        }
        //获取常见问题列表
        ,getReplyMenuList:function (num) {
            mpCommon.invoke2({
                url : '/customerService/admin/FAQList',
                data : {
                    pageIndex:num,
                    pageSize:10,
                    keyword:$("#replyMenu_keyword").val(),
                },
                success : function(result) {
                    if(result.data.length==0){
                        layui.layer.msg("暂无数据");
                    }else{
                        $("#pageCount").val(result.count);
                        $("#service_manage_Total").empty();
                        $("#service_manage_Total").append("共"+result.total+"条");
                        sum=result.data.length;
                        var html;
                        $("#replyMenu_td").empty();
                        for(var i=0; i< result.data.length;i++){

                           var html="<tr>"
                                +	"<td>"+result.data[i].question +"</td>"
                                +	"<td>"+result.data[i].keyword+"</td>"
                                +	"<td>"+result.data[i].answer+"</td>"
                                +	"<td>"+(new Date(result.data[i].createTime)).format("yyyy-MM-dd hh:mm:ss")+"</td>"
                                +   "<td>"
                                + 		"<button onclick='customer.deleteUseful(\""+result.data[i].id+"\")' class='layui-btn layui-btn-sm layui-btn-danger'>删除</button>"
                                + 		"<button onclick='customer.showUseful(\""+result.data[i].question+"\",\""+result.data[i].id+"\")' class='layui-btn layui-btn-sm layui-btn-primary'>修改</button>"
                                + 	"</td>"
                                +	"</tr>";
                            $("#replyMenu_td").append(html);
                        }}
                },
                error : function(result) {
                    layui.layer.msg("表单提交失败！");
                }
            });
        }

    //常见问题
    ,faqManage:function(num, type){

       customer.getFAQList(num);
       if (type == 0){
           UI.limit(0,"faq_manage")
       }

    }
    //获取常见问题列表
    ,getFAQList:function (num) {
        mpCommon.invoke2({
            url : '/customerService/admin/FAQList',
            data : {
                pageIndex:num,
                pageSize:10,
                keyword:$("#faq_keyword").val(),
            },
            success : function(result) {
                if(result.data.length==0){
                    layui.layer.msg(mpLanguage.getLanguageName('noData'));
                }else{
                    $("#pageCount").val(result.count);
                    $("#service_manage_Total").empty();
                    $("#service_manage_Total").append(mpLanguage.getLanguageName('tota')+result.total+mpLanguage.getLanguageName('article'));
                    sum=result.data.length;
                    var html;
                    $("#faq_td").empty();
                    for(var i=0; i< result.data.length;i++){

                       var html="<tr>"
                            +	"<td>"+result.data[i].question +"</td>"
                            +	"<td>"+result.data[i].keyword+"</td>"
                            +	"<td>"+result.data[i].answer+"</td>"
                            +	"<td>"+(new Date(result.data[i].createTime)).format("yyyy-MM-dd hh:mm:ss")+"</td>"
                            +   "<td>"
                            + 		"<button onclick='customer.deleteUseful(\""+result.data[i].id+"\")' class='layui-btn layui-btn-sm layui-btn-danger'>"+ mpLanguage.getLanguageName('theDelete') +"</button>"
                            + 		"<button onclick='customer.showUseful(\""+result.data[i].question+"\",\""+result.data[i].id+"\")' class='layui-btn layui-btn-sm layui-btn-primary'>"+ mpLanguage.getLanguageName('theModify') +"</button>"
                            + 	"</td>"
                            +	"</tr>";
                        $("#faq_td").append(html);
                    }}
            },
            error : function(result) {
                layui.layer.msg(mpLanguage.getLanguageName('from_submit_failed'));
            }
        });
    }
    //添加常见问题
    ,addFaq:function () {
        layui.layer.open({
            title:mpLanguage.getLanguageName('addFaq'),
            type: 1,
            btn:[mpLanguage.getLanguageName('determine'),mpLanguage.getLanguageName('theCancel')],
            area: ['510px'],
            content: '<div id="addFaq" class="layui-form" style="margin:20px 40px 10px 40px;;">'
                +   '<div class="layui-form-item">'
                +      '<div class="layui-input-block" style="margin: 0 auto;">'
                +        '<input type="text" required  lay-verify="required" placeholder="'+ mpLanguage.getLanguageName('input_problem') +'" autocomplete="off" lay-verify="number" class="layui-input question">'
                +        '<input type="text" required  lay-verify="required" placeholder="'+ mpLanguage.getLanguageName('key_word') +'" autocomplete="off" lay-verify="number" class="layui-input keyword">'
                +        '<textarea name="answer" placeholder="'+ mpLanguage.getLanguageName('input_answer') +'" class="layui-textarea answer"></textarea>'
                +      '</div>'
                +    '</div>'
                +'</div>'
            ,yes: function(index, layero){ //确定按钮的回调
                var question = $("#addFaq .question").val();
                var keyword = $("#addFaq .keyword").val();
                var answer = $("#addFaq .answer").val();
                if (question == ""){
                    layui.layer.msg(mpLanguage.getLanguageName('input_phone_number'),{"icon":5});
                    return
                }
                mpCommon.invoke2({
                    url :'/customerService/admin/addFAQ',
                    data : {
                        question:question,
                        keywordStr:keyword,
                        answer:answer
                    },
                    success : function(result) {
                        if(result.resultCode == 1){
                            layui.layer.msg(mpLanguage.getLanguageName('add_success') ,{"icon":1});
                            layui.layer.close(index); //关闭弹框
                        }else{
                            layui.layer.close(index); //关闭弹框
                            layui.layer.msg(result.resultMsg,{"icon":2});
                        }
                    },
                    error : function(result) {
                        layui.layer.msg(mpLanguage.getLanguageName('failed_load_data'));
                    }
                });
            }
        });
    }
    //获取客服每天接待客服折现统计图
    ,getServiceVisitorCount:function (serviceId) {
        var userRegister = echarts.init(document.getElementById('serviceVisitorCount'),'shine');
        mpCommon.invoke2({
            url : '/customerService/admin/serviceVisitorCount',
            data : {
                startDate:"",
                endDate:"",
                timeUnit:2,
                serviceId:serviceId
            },
            success : function(result) {
                if(result.resultCode==1){
                    var optionchartZhe = {
                        title: {
                            text: mpLanguage.getLanguageName('record_visitors_received')
                        },
                        tooltip: { trigger: 'axis'},
                        /*legend: {
                            data: ['每日访客']
                        },*/
                        xAxis: {
                            // type: 'category',
                            // boundaryGap: false, //从起点开始
                            data: result.data.map(function (item) {
                                for(var time in item){ return time; }
                            })
                        },
                        yAxis: {
                            splitLine: {
                                show: false
                            },
                            type: 'value'
                        },
                        toolbox: {
                            left: 'center',
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
                        series: [{
                            name: mpLanguage.getLanguageName('daily_visitors'),
                            type: 'line', //线性
                            data: result.data.map(function (item) {
                                for(var time in item){return item[time];}
                            }),
                        }]
                    };

                    userRegister.setOption(optionchartZhe, true);
                }
            },
            error : function(result) {
                layer.msg(result.resultMsg,{"icon":5});
            }
        });


        layui.layer.open({
            title:"",
            type: 1,
            shade: false,
            area: ['700px', '450px'],
            shadeClose: true, //点击遮罩关闭
            content: $("#serviceVisitorCountDiv"),
            cancel: function(index, layero){
                layer.close(index)
                $("#serviceVisitorCountDiv").hide();
                return false;
            },
            success : function(layero,index){  //弹窗打开成功后的回调
                layui.form.render('select');
            }
        });
    }

    ,//常用语管理
    usefulManage:function (num,type) {
        


        customer.getUsefulList(num);
        if (type == 0){
            UI.limit(0,"useful_manage")
        }
    }

    //获取企业号下常用语列表
    ,getUsefulList:function (num) {
        mpCommon.invoke2({
            url : '/customerService/admin/usefulTextList',
            data : {
                pageIndex:num,
                pageSize:10,
                keyword:$("#useful_name").val(),
                service_userId:$("#useful_userId").val()
            },
            success : function(result) {
                if(result.data.length==0){
                    $("#useful_td").empty();
                    layui.layer.msg(mpLanguage.getLanguageName('noData'));
                }else{
                    $("#pageCount").val(result.count);
                    $("#service_manage_Total").empty();
                    $("#service_manage_Total").append(mpLanguage.getLanguageName('tota')+result.total+mpLanguage.getLanguageName('article'));
                    sum=result.data.length;
                    var html;
                    for(var i=0;i<result.data.length;i++){
                        var html;
                        html+="<tr>"
                            +	"<td>"+result.data[i].idStr +"</td>"
                            +	"<td>"+result.data[i].service_userId+"</td>"
                            +	"<td>"+result.data[i].service_name+"</td>"
                            +	"<td id='"+result.data[i].idStr +"'>"+result.data[i].content+"</td>"
                            +	"<td>"+(new Date(result.data[i].createTime * 1000)).format("yyyy-MM-dd hh:mm:ss")+"</td>"
                            +   "<td>"
                            + 		"<button onclick='customer.deleteUseful(\""+result.data[i].idStr+"\")' class='layui-btn layui-btn-sm layui-btn-danger'>"+ mpLanguage.getLanguageName('theDelete') +"</button>"
                            + 		"<button onclick='customer.showUseful(\""+result.data[i].content+"\",\""+result.data[i].idStr+"\")' class='layui-btn layui-btn-sm layui-btn-primary'>"+ mpLanguage.getLanguageName('theModify') +"</button>"
                            + 	"</td>"
                            +	"</tr>";
                        $("#useful_td").empty();
                        $("#useful_td").append(html);
                    }}
            },
            error : function(result) {
                layui.layer.msg(mpLanguage.getLanguageName('from_submit_failed'));
            }
        });
    }

    //添加常用语
    ,addCommon:function (serviceId) {
        layui.layer.open({
            title:mpLanguage.getLanguageName('add_phrase'),
            type: 1,
            btn:[mpLanguage.getLanguageName('determine'),mpLanguage.getLanguageName('theCancel')],
            area: ['400px'],
            content: '<div id="changePassword" class="layui-form" style="margin:20px 40px 10px 40px;;">'
                +   '<div class="layui-form-item">'
                +      '<div class="layui-input-block" style="margin: 0 auto;">'
                +        '<textarea name="desc" id="comment" cols="1" placeholder="'+ mpLanguage.getLanguageName('input_phrase') +'" class="layui-textarea"></textarea>'
                +      '</div>'
                +    '</div>'
                +'</div>'

            ,yes: function(index, layero){ //确定按钮的回调
                var comment = $("#comment").val();
                if (comment == ""){
                    layui.layer.msg(mpLanguage.getLanguageName('input_phrase'),{"icon":5});
                    return
                }
                mpCommon.invoke2({
                    url : '/customerService/admin/addUsefulText',
                    data : {
                        context:$("#comment").val(),
                        serviceId:serviceId
                    },
                    success : function(result) {
                        if(result.resultCode == 1){
                            var html;
                            html+="<tr>"
                                +	"<td>"+result.data.idStr +"</td>"
                                +	"<td>"+result.data.service_userId+"</td>"
                                +	"<td>"+result.data.service_name+"</td>"
                                +	"<td id='"+result.data.idStr +"'>"+result.data.content+"</td>"
                                +	"<td>"+(new Date(result.data.createTime * 1000)).format("yyyy-MM-dd hh:mm:ss")+"</td>"
                                +   "<td>"
                                + 		"<button onclick='customer.deleteUseful(\""+result.data.idStr+"\")' class='layui-btn layui-btn-sm layui-btn-danger'>" + mpLanguage.getLanguageName('theDelete') +"</button>"
                                + 		"<button onclick='customer.showUseful(\""+result.data.content+"\",\""+result.data.idStr+"\")' class='layui-btn layui-btn-sm layui-btn-primary'>" + mpLanguage.getLanguageName('theModify') + "</button>"
                                + 	"</td>"
                                +	"</tr>";
                            $("#useful_td").append(html);

                            layui.layer.msg(mpLanguage.getLanguageName('add_success'),{"icon":1});
                            layui.layer.close(index); //关闭弹框
                        }else{
                            layui.layer.close(index); //关闭弹框
                            layui.layer.msg(result.resultMsg,{"icon":2});
                        }
                    },
                    error : function(result) {
                        layui.layer.msg(mpLanguage.getLanguageName('from_submit_failed'));
                    }
                });
            }


        });
    }

    //删除常用语
    ,deleteUseful:function (data) {
        mpCommon.invoke2({
            url : '/customerService/admin/deleteUsefulText',
            data : {
                id:data,
            },
            success : function(result) {
                if(result.resultCode==1){
                    customer.getUsefulList(0);
                    layer.msg(mpLanguage.getLanguageName('delete_success'),{"icon":1});
                }else{
                    layer.msg(result.resultMsg,{"icon":2});
                }
            },
            error : function(result) {
                layer.msg(result.resultMsg,{"icon":5});
            }
        });
    }

    //展示常用语信息
    ,showUseful:function (data,id) {
        layui.layer.open({
            title:mpLanguage.getLanguageName('theModify'),
            type: 1,
            btn:[mpLanguage.getLanguageName('update_data'),mpLanguage.getLanguageName('theCancel')],
            area: ['400px'],
            content: '<div id="changePassword" class="layui-form" style="margin:20px 40px 10px 40px;;">'
                +   '<div class="layui-form-item">'
                +      '<div class="layui-input-block" style="margin: 0 auto;">'
                +        '<textarea name="desc" id="comment" cols="1" placeholder="'+ mpLanguage.getLanguageName('input_phrase') +'" class="layui-textarea">'+data+'</textarea>'
                +      '</div>'
                +    '</div>'
                +'</div>'

            ,yes: function(index, layero){ //确定按钮的回调
                var commonText = $("#commonText").val();
                if (commonText == ""){
                    layui.layer.msg(mpLanguage.getLanguageName('input_phrase'),{"icon":5});
                    return
                }
                mpCommon.invoke2({
                    url : '/customerService/admin/updateUsefulText',
                    data : {
                        idStr:id,
                        content:$("#comment").val()
                    },
                    success : function(result) {
                        if(result.resultCode==1){
                            document.getElementById(id).innerHTML = $("#comment").val();
                            layer.msg(mpLanguage.getLanguageName('update_success'),{"icon":1});
                        }else{
                            layer.msg(result.resultMsg,{"icon":2});
                        }
                    },
                    error : function(result) {
                        layer.msg(result.resultMsg,{"icon":5});
                    }
                });
                layui.layer.close(index);
            }
        });
    }

    //统计图表
    ,countManage:function (num) {
      

        customer.getVisitorCount("","",2);
        customer.getCountCustomVisitrNum();
        customer.getServiceTopTenAndBottomTen();
    }
    //获取接待访客数量最多的前十名
    ,getServiceTopTenAndBottomTen(){
        var serviceTop10_chart = echarts.init(document.getElementById('serviceTop10'));
        var serviceTop10_name = [], serviceBottom10_name = [], serviceTop10_number = [] , serviceBottom10_number = [];
        mpCommon.invoke2({
            url : '/customerService/admin/getServiceTopTenAndBottomTen',
            data : {},
            success : function(result) {
                if (result.resultCode == 1){
                    data = result.data;
                    data.sort(function(a,b){ return a.chatCount-b.chatCount  });
                    for(var i = 1; i<=( data.length >10 ? 10 : data.length) ; i++ ){
                         serviceBottom10_name.push(data[i-1].serviceName);
                         serviceBottom10_number.push(data[i-1].chatCount);

                         serviceTop10_name.push(data[data.length-i].serviceName);
                         serviceTop10_number.push(data[data.length-i].chatCount);
                    }

                }
            },
            error : function(result) {
                layer.msg(result.resultMsg,{"icon":5});
            }
        });

        serviceTop10_chart.setOption( {
            title: {
                text: mpLanguage.getLanguageName('top_ten'),
                subtext: mpLanguage.getLanguageName('ranking')
            },
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                }
            },
            legend: {
                data: [mpLanguage.getLanguageName('numberOf')]
            },
            color: ['#32C5E9', '#67E0E3'],
            grid: {
                containLabel: true
            },
            xAxis: {
                type: 'value',
                boundaryGap: [0, 0.01]
            },
            yAxis: {
                type: 'category',
                data: serviceTop10_name
            },
            series: [
                {
                    name: mpLanguage.getLanguageName('numberOf'),
                    type: 'bar',
                    data: serviceTop10_number
                }
            ]
        });


       var serviceBottom10_chart = echarts.init(document.getElementById('serviceBottom10'));

        serviceBottom10_chart.setOption( {
            title: {
                text: mpLanguage.getLanguageName('bottom_ten'),
                subtext: mpLanguage.getLanguageName('ranking')
            },
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                }
            },
            legend: {
                data: [mpLanguage.getLanguageName('numberOf')]
            },
            color: ['#32C5E9', '#67E0E3'],
            grid: {
                containLabel: true
            },
            xAxis: {
                type: 'value',
                boundaryGap: [0, 0.01]
            },
            yAxis: {
                type: 'category',
                data: serviceBottom10_name
            },
            series: [
                {
                    name: mpLanguage.getLanguageName('numberOf'),
                    type: 'bar',
                    data: serviceBottom10_number
                }
            ]
        });


    }

    /* //获取接待客服数量最少的后十名
    ,getServiceBottom10(){
        var name=[];
        var number=[];
        mpCommon.invoke2({
            url : '/customerService/admin/getServiceBottomTen',
            *//*url : '/customerService/admin/test',*//*
            data : {},
            success : function(result) {
                if (result.resultCode == 1){
                    data = result.data;
                    for (var n in data) {
                        name.push(n);
                        number.push(data[n]);
                    }
                }
            },
            error : function(result) {
                layer.msg(result.resultMsg,{"icon":5});
            }
        });

        var myChart1 = echarts.init(document.getElementById('serviceBottom10'));

        myChart1.setOption( {
            title: {
                text: '客服接待访客最少Bottom10',
                subtext: '排名'
            },
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow'
                }
            },
            legend: {
                data: ['数量']
            },
            color: ['#32C5E9', '#67E0E3'],
            grid: {
                containLabel: true
            },
            xAxis: {
                type: 'value',
                boundaryGap: [0, 0.01]
            },
            yAxis: {
                type: 'category',
                data: name
            },
            series: [
                {
                    name: '数量',
                    type: 'bar',
                    data: number
                }
            ]
        });
    }*/
   
    

    //项目配置
    ,configManage:function (num) {
       

        customer.getSysConfig();
        //设置select下拉框样式
        customer.setSelectCss();


    }

    ,setSelectCss:function () {
        $('#allowPush').siblings("div.layui-form-select").find('dl').addClass("selectBytTwo");
        $('#newInnerTalkHint').siblings("div.layui-form-select").find('dl').addClass("selectByThree");
        $('#newVisitorPush').siblings("div.layui-form-select").find('dl').addClass("selectBytTwo");
        $('#newVisitorMsgPush').siblings("div.layui-form-select").find('dl').addClass("selectBytTwo");
        $('#newVisitorHint').siblings("div.layui-form-select").find('dl').addClass("selectByThree");
        $('#newTalkHint').siblings("div.layui-form-select").find('dl').addClass("selectByThree");
        $('#newVisitorMsgHint').siblings("div.layui-form-select").find('dl').addClass("selectByThree");
        $('#newFowardHint').siblings("div.layui-form-select").find('dl').addClass("selectByThree");
        $('#hideNoTalkVisitor').siblings("div.layui-form-select").find('dl').addClass("selectBytTwo");

        $('#allocationLastService').siblings("div.layui-form-select").find('dl').addClass("selectBytTwo");
        $('#quitWindowsEvaluate').siblings("div.layui-form-select").find('dl').addClass("selectBytTwo");
        $('#newMsgUp').siblings("div.layui-form-select").find('dl').addClass("selectBytTwo");

    }
   
    //留言管理
    ,leaveWordManage:function (num,type) {
        


        customer.getleaveWordList(num);
        if (type == 0){
            UI.limit(0,"leaveWord_manage")
        }
    }

    //留言列表
    ,getleaveWordList:function (num) {
        mpCommon.invoke2({
            url : '/customerService/admin/leaveWordList',
            data : {
                pageIndex:num,
                pageSize:10,
                keyword:$("#leaveWord_name").val(),
                service_userId:$("#leaveWord_serviceId").val()
            },
            success : function(result) {
                if(result.data.length==0){
                    $("#leaveWord_td").empty();
                    layui.layer.msg(mpLanguage.getLanguageName('noData'));
                }else{
                    $("#pageCount").val(result.count);
                    $("#service_manage_Total").empty();
                    $("#service_manage_Total").append(mpLanguage.getLanguageName('tota')+result.total+mpLanguage.getLanguageName('article'));
                    sum=result.data.length;
                    for(var i=0;i<result.data.length;i++){
                        var html;
                        html+="<tr>"
                            +	"<td>"+result.data[i].idStr +"</td>"
                            +	"<td>"+result.data[i].visitor_userId+"</td>"
                            +	"<td>"+(result.data[i].state == 0 ? mpLanguage.getLanguageName('dont_online') : result.data[i].state == 1 ? mpLanguage.getLanguageName('busy') : mpLanguage.getLanguageName('online'))+"</td>"
                            +	"<td>"+result.data[i].service_userId+"</td>"
                            +	"<td>"+result.data[i].service_name+"</td>"
                            +	"<td>"+result.data[i].telephone+"</td>"
                            +	"<td>"+result.data[i].name+"</td>"
                            +	"<td>"+result.data[i].email+"</td>"
                            +	"<td>"+result.data[i].content+"</td>"
                            +	"<td>"+result.data[i].replayContent+"</td>"
                            +	"<td>"+(new Date(result.data[i].createTime * 1000)).format("yyyy-MM-dd hh:mm:ss")+"</td>"
                            +"</tr>";

                        $("#leaveWord_td").empty();
                        $("#leaveWord_td").append(html);
                    }}
            },
            error : function(result) {
                layui.layer.msg(mpLanguage.getLanguageName('from_submit_failed'));
            }
        });
    }



    //评价管理
    ,evaluationManage:function (num,type) {
       

        //设置下拉框高度
        $('#grade').siblings("div.layui-form-select").find('dl').addClass("selectCommon");
        customer.getEvaluationList(num);
        if (type == 0){
            UI.limit(0,"evaluation_manage")
        }
    }

    //查询评价列表
    ,getEvaluationList:function (num) {
        mpCommon.invoke2({
            url : '/customerService/admin/evaluateList',
            data : {
                pageIndex:num,
                pageSize:10,
                keyword:$("#grade").val(),
                service_userId:$("#evaluatio_serviceId").val()
            },
            success : function(result) {
                if(result.data.length==0){
                    $("#evaluation_td").empty();
                    layui.layer.msg(mpLanguage.getLanguageName('noData'));
                }else{
                    $("#pageCount").val(result.count);
                    $("#service_manage_Total").empty();
                    $("#service_manage_Total").append(mpLanguage.getLanguageName('tota')+result.total+mpLanguage.getLanguageName('article'));
                    sum=result.data.length;
                    var html;
                    for(var i=0;i<result.data.length;i++){
                        var html;
                        html+="<tr>"
                            +	"<td>"+result.data[i].idStr +"</td>"
                            +	"<td>"+result.data[i].service_userId+"</td>"
                            +	"<td>"+result.data[i].service_name+"</td>"
                            +	"<td>"+result.data[i].visitor_userId+"</td>"
                            +	"<td>"+result.data[i].visitor_name+"</td>"
                            +	"<td>"+result.data[i].grade+"</td>"
                            +	"<td>"+result.data[i].content+"</td>"
                            +	"<td>"+(new Date(result.data[i].createTime * 1000)).format("yyyy-MM-dd hh:mm:ss")+"</td>"
                            +"</tr>";
                        $("#evaluation_td").empty();
                        $("#evaluation_td").append(html);
                    }}
            },
            error : function(result) {
                layui.layer.msg(mpLanguage.getLanguageName('from_submit_failed'));
            }
        });
    }


}


layui.use(['jquery','form','layer','laydate'],function(){
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate;

    //日期范围
    var time =  laydate.render({
        elem: '#globalDate'
        ,range: "~"
        ,done: function(value, date, endDate){  // choose end
            //console.log("date callBack====>>>"+value); //得到日期生成的值，如：2017-08-18
            var startDate = value.split("~")[0];
            var endDate = value.split("~")[1];
            customer.getVisitorCount(startDate,endDate,2);
        }
        ,max: 0
    });

    mpLanguage.loadProperties(mpLanguage.getLanguage());
})