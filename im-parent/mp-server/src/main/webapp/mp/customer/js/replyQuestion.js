layui.use(['layer', 'util','table'], function () {
        var $ = layui.jquery;
        var layer = layui.layer;
        var util = layui.util;
        var table = layui.table;

        //问题列表
        var tableInQuestion = table.render({
            elem: '#replyQuestion_list'
            ,toolbar: '#replyQuestionTopBar'
            ,url: request(mpCommon.serviceUrl+"/customerService/admin/FAQList")
            ,id: 'replyQuestion_list'
            ,page: true
            ,curr: 0
            ,limit: mpCommon.limit
            ,limits: mpCommon.limits
            ,groups: 7
            ,cols: [[ //表头

                // {field: 'service_userId', title: mpLanguage.getLanguageName('service_id'), width:100}
                {field: 'question', title:  "问题",sort:'true', width:300}
                ,{field: 'description', title: "问题描述",sort:'true', width:200}
                ,{field: 'keywords', title: "关键词",sort:'true', width:200}
                ,{field: 'common', title: "类型",sort:'true', width:100,templet:function(d){
                        return (d.common ? "<span class='common_question'>常见问题</span>": "普通问题");
                    }}
                ,{field: 'createTime', title: mpLanguage.getLanguageName('add_time'),sort:'true', width:200,templet:function(d){
                        return util.toDateString(d.createTime);
                }}
                ,{fixed: 'right', width: 300,title: mpLanguage.getLanguageName('operation'), align:'center', toolbar: '#replyQuestionOptionBar'}
            ]]
            ,done:function(res, curr, count){
                // checkRequst(res);

               //initLanguage();
                //form.render();
            }

        });


        //表头监听
        table.on('toolbar(replyQuestion_list)', function(obj){
            
            var layEvent = obj.event,  data = obj.data;
           
            if(layEvent === 'add_question'){ //添加问题
                Question.addQuestion();
            }else if (layEvent === 'search_question'){
                Question.reloadQuestion();
            }

        });



        // 菜单操作项
        table.on('tool(replyQuestion_list)', function(obj){
            var layEvent = obj.event,  data = obj.data;
           
            if(layEvent==='edit'){ //编辑问题
                Question.editQuestion(data);

            }else if(layEvent==='del'){ //删除问题
                layer.confirm('确定删除该问题吗？', function(index){
                    Question.delQuestion(data.questionId);
                    obj.del(); //删除对应行（tr）的DOM结构，并更新缓存
                    layer.close(index);
                });

            }else if(layEvent==='add_reply'){ //添加自动回复
                

            }

        });

        
        var Question = {

            reloadQuestion : function(){
               
                //重载表格数据
                tableInQuestion.reload({
                  where: { //设定异步数据接口的额外参数，任意设
                    keyword : $(".search_question_keyword").val()
                  }
                  ,page: {
                    curr: 1 //重新从第 1 页开始
                  }
                });
            }
            ,addQuestion : function(){

                //清空表单
                $("#addQuestion .question").val("");
                $("#addQuestion .questionType").val(1);
                $("#addQuestion .keywordStr").val("");
                $("#addQuestion .answerType").val(1);
                $("#addQuestion .answerContent").val("");

                layer.open({
                    title : "添加问题",
                    type: 1,
                    btn:["确定","取消"],
                    area: ['520px','480px'],
                    content: $("#addQuestion"),
                    end: function(){ 
                        $("#addQuestion").hide();
                    }
                    ,yes: function(index, layero){ //确定按钮的回调

                        var question = $("#addQuestion .question").val();
                        var questionType = $("#addQuestion .questionType").val();
                        var keywords = $("#addQuestion .keywordStr").val();
                        var answerType = $("#addQuestion .answerType").val();
                        var answerContent = $("#addQuestion .answerContent").val();
                        
                        if ( "" == question || "" == keywords || "" == answerContent){
                            layer.msg("请填写完整后在提交",{"icon":5});
                            return
                        }

                        mpCommon.invoke2({
                            url :'/customerService/admin/addFaq',
                            data : {
                                question : question,
                                questionType : questionType,
                                keywordStr : keywords,
                                answerType : answerType,
                                answerContent : answerContent
                            },
                            success : function(result) {
                                if(result.resultCode == 1){
                                    layer.msg("添加成功" ,{"icon":1});
                                    layer.close(index); //关闭弹框
                                    tableInQuestion.reload({
                                        page: {
                                            curr: 1 //重新从第 1 页开始
                                        }
                                    });
                                }else{
                                    layer.close(index); //关闭弹框
                                    layer.msg(result.resultMsg,{"icon":2});
                                }
                            },
                            error : function(result) {
                               layer.msg(mpLanguage.getLanguageName('failed_load_data'));
                            }
                        });
                    }
                });

            },
            editQuestion : function(data){

                layer.open({
                    title : "编辑问题",
                    type: 1,
                    btn:["确定","取消"],
                    area: ['520px','480px'],
                    content: $("#addQuestion"),
                    end: function(){ 
                        $("#addQuestion").hide();
                    },
                    success : function(){ //弹出成功的回调

                        //数据回显
                        $("#addQuestion .question").val(data.question);
                        $("#addQuestion .questionType").val((data.common)?2:1);
                        $("#addQuestion .keywordStr").val(data.keywords);
                        $("#addQuestion .answerType").val(data.answers[0].answerType);
                        $("#addQuestion .answerContent").val(data.answers[0].content);

                        layui.form.render(); //刷新表单

                    }
                    ,yes: function(index, layero){ //确定按钮的回调

                        var question = $("#addQuestion .question").val();
                        var questionType = $("#addQuestion .questionType").val();
                        var keywords = $("#addQuestion .keywordStr").val();
                        var answerType = $("#addQuestion .answerType").val();
                        var answerContent = $("#addQuestion .answerContent").val();
                        
                        if ( "" == question || "" == keywords || "" == answerContent){
                            layer.msg("请填写完整后在提交",{"icon":5});
                            return
                        }

                        mpCommon.invoke2({
                            url :'/customerService/admin/updateFaq',
                            data : {
                                questionId : data.questionId,
                                question : question,
                                questionType : questionType,
                                keywordStr : keywords,
                                answerType : answerType,
                                answerContent : answerContent
                            },
                            success : function(result) {
                                if(result.resultCode == 1){
                                    layer.msg("更新成功" ,{"icon":1});
                                    layer.close(index); //关闭弹框
                                    tableInQuestion.reload(); //刷新当前页
                                }else{
                                    layer.close(index); //关闭弹框
                                    layer.msg(result.resultMsg,{"icon":2});
                                }
                            },
                            error : function(result) {
                               layer.msg(mpLanguage.getLanguageName('failed_load_data'));
                            }
                        });
                    }
                });

            },
            delQuestion : function(questionId){
                
                mpCommon.invoke2({
                    url : '/customerService/admin/deleteFaq',
                    data : {
                        questionId : questionId
                    },
                    success : function(result) {
                        if(result.resultCode == 1){
                            //layer.msg(mpLanguage.getLanguageName('delete_success'),{"icon":1});
                           layer.msg('删除成功',{"icon":1});
                        }else{
                            layer.msg(result.resultMsg,{"icon":2});
                        }
                    },
                    error : function(result) {
                    }
                });
            },


        };





    });