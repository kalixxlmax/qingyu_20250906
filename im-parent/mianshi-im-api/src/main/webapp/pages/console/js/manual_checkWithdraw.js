layui.use(['form','layer','laydate','table','laytpl'],function() {
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;

    //非管理员登录屏蔽操作按钮
    if (localStorage.getItem("IS_ADMIN") == 0) {
        $(".bindingSDK_div").empty();
    }

    // 提现申请列表
    var tableIns = table.render({
        elem: '#checkWithdraw_table'
        , url: request("/manualAdmin/getWithdrawList")
        , toolbar: '#toolbar'
        , page: true
        , curr: 0
        , limit: Common.limit
        , limits: Common.limits
        , groups: 7
        , cols: [[ //表头
            {field: 'userId', title: '提现人Id', sort: true, width: 100}
            ,{field: 'nickName', title: '用户昵称',sort: true,width:120}
            ,{field: 'orderNo', title: '订单号',sort: true, width:200}
            , {field: 'money', title: '提现金额', sort: true, width: 120}
            , {field: 'serviceCharge', title: '提现手续费', sort: true, width: 100}
            , {field: 'actualMoney', title: '实际金额', sort: true, width: 120}
            , {field: 'endMoney', title: '提现后余额', sort: true, width: 120}
            , {field: 'type', title: '提现到', sort: true, width: 120,templet : function (d) {
                    return d.typeName;
                }}
            ,{field: 'status', title: '状态',sort: true, width:120,templet:function (d) {
                    return d.status == 1?"申请中":d.status == 2?"已完成":d.status == -1?"已驳回":"已忽略";
                }}
            , {field: 'aliPayName', title: '名称', sort: true, width: 120,templet : function (d) {
                    return Common.isNil(d.withdrawAccount.aliPayName)?"":d.withdrawAccount.aliPayName;
                }}
            , {field: 'aliPayAccount', title: '账户', sort: true, width: 120,templet : function (d) {
                    return Common.isNil(d.withdrawAccount.aliPayAccount)?"":d.withdrawAccount.aliPayAccount;
                }}
            , {field: 'cardName', title: '持卡人姓名', sort: true, width: 120,templet : function (d) {
                    return Common.isNil(d.withdrawAccount.cardName)?"":d.withdrawAccount.cardName;
                }}
            , {field: 'bankCardNo', title: '银行卡号', sort: true, width: 120,templet : function (d) {
                    return Common.isNil(d.withdrawAccount.bankCardNo)?"":d.withdrawAccount.bankCardNo;
                }}
            , {field: 'bankName', title: '银行名称', sort: true, width: 120,templet : function (d) {
                    return Common.isNil(d.withdrawAccount.bankName)?"":d.withdrawAccount.bankName;
                }}
            , {field: 'bankBranchName', title: '支行名称', sort: true, width: 150,templet : function (d) {
                    return Common.isNil(d.withdrawAccount.bankBranchName)?"":d.withdrawAccount.bankBranchName;
                }}
            , {field: 'desc', title: '备注信息', sort: true, width: 120,templet : function (d) {
                    return Common.isNil(d.withdrawAccount.desc)?"":d.withdrawAccount.desc;
                }}
            , {field: 'createTime', title: '时间', sort: true, width: 200,templet : function (d) {
                    return UI.getLocalTime(d.createTime);
                }}
            , {fixed: 'right', width: 350, title: "操作", align: 'left', toolbar: '#checkWithdrawBar'}
        ]]
        , done: function (res, curr, count) {
            checkRequst(res);

            //权限判断
            var arr=['scanRecharge-pass','scanRecharge-dis','scanRecharge-ignore'];
            manage.authButton(arr);

            var totalInfo = JSON.parse(res.totalVo);
            $(".totalWithdraw").empty().text((0 == totalInfo.totalWithdraw ? 0:Common.filterHtmlData(totalInfo.totalWithdraw) ));
            $(".successWithdraw").empty().text((0 == totalInfo.successWithdraw ? 0: Common.filterHtmlData(totalInfo.successWithdraw) ));
            $(".failureWithdraw").empty().text((0 == totalInfo.failureWithdraw ? 0: Common.filterHtmlData(totalInfo.failureWithdraw) ));
            $(".applyWithdraw").empty().text((0 == totalInfo.applyWithdraw ? 0: Common.filterHtmlData(totalInfo.applyWithdraw) ));
            $(".ignoreCount").empty().text((0 == totalInfo.ignoreCount ? 0: Common.filterHtmlData(totalInfo.ignoreCount) ));
            // 初始化时间控件
            layui.laydate.render({
                elem: '#checkWithdrawDate'
                ,range: "~"
                ,done: function(value, date, endDate){  // choose end
                    var startDate = value.split("~")[0];
                    var endDate = value.split("~")[1];

                    table.reload("checkWithdraw_table",{
                        page: {
                            curr: 1 //重新从第 1 页开始
                        },
                        where: {
                            startDate : startDate,
                            endDate : endDate
                        }
                    })
                }
                ,max: 1
            });
        }
    });

    // 表格操作
    table.on('tool(checkWithdraw_table)', function(obj) {
        var layEvent = obj.event,
            data = obj.data;
        if (layEvent === 'delete') {
            Manual_draw.deleteWithdraw(data.id);
        }else if(layEvent === 'approve'){
            Manual_draw.approveWithdraw(data.id);
        }else if(layEvent === 'disallowance'){
            layui.layer.open({
                title:"驳回",
                type: 1,
                skin: 'layui-ext-motif',
                btn:["确定","取消"],
                area: ['400px'],
                content: '<div id="changePassword" class="layui-form" style="padding-top: 15px;">'
                    +        '<textarea id="bhreason" type="text" required  lay-verify="required" placeholder="驳回理由" autocomplete="off" class="layui-input reason" style="height: 100px;"></textarea>'
                    +'</div>'
                ,yes: function(index, layero){ //确定按钮的回调
                    var reason = $('#bhreason').val();
                    if (Common.isNil(reason)){
                        layui.layer.alert("请输入驳回理由");
                        return;
                    }
                    Common.invoke({
                        url:request("/manualAdmin/checkWithdraw"),
                        data:{
                            id:data.id,
                            status:-1,
                            reason:reason
                        },
                        success:function (result) {
                            if(result.resultCode == 1){
                                layui.layer.alert("驳回成功");
                                layui.table.reload("checkWithdraw_table");
                                layui.layer.close(index); //关闭弹框
                            }
                        }
                    })
                }
                ,btn2:function () {
                },
                cancel: function(index, layero){
                    layer.close(index)
                }
            });
        }else if(layEvent === 'ignore'){
            Manual_draw.ignoreWithdraw(data.id);
        }
    });

    // 搜索
    $(".search_checkWithdraw").on("click",function(){
        table.reload("checkWithdraw_table",{
            url:request("/manualAdmin/getWithdrawList"),
            where: {
                keyword :Common.getValueForElement(".checkWithdraw_keyword")
            },
            page: {
                curr: 1 //重新从第 1 页开始
            },
            done: function (res, curr, count) {
                checkRequst(res);

                var totalInfo = JSON.parse(res.totalVo);
                $(".totalWithdraw").empty().text((0 == totalInfo.totalWithdraw ? 0:Common.filterHtmlData(totalInfo.totalWithdraw) ));
                $(".successWithdraw").empty().text((0 == totalInfo.successWithdraw ? 0: Common.filterHtmlData(totalInfo.successWithdraw) ));
                $(".failureWithdraw").empty().text((0 == totalInfo.failureWithdraw ? 0: Common.filterHtmlData(totalInfo.failureWithdraw) ));
                $(".applyWithdraw").empty().text((0 == totalInfo.applyWithdraw ? 0: Common.filterHtmlData(totalInfo.applyWithdraw) ));

                $(".ignoreCount").empty().text((0 == totalInfo.ignoreCount ? 0: Common.filterHtmlData(totalInfo.ignoreCount) ));
            }

        })
    })
})

var Manual_draw={
    approveWithdraw:function (id) {
        layer.confirm('确定通过审核该条提现申请？',{icon:3, title:'提示信息',skin : "layui-ext-motif"},function(index){
            Common.invoke({
                url:request("/manualAdmin/checkWithdraw"),
                data:{
                    id:id,
                    status:2
                },
                success:function (result) {
                    if(result.resultCode == 1){
                        layui.layer.alert("审核成功");
                        layui.table.reload("checkWithdraw_table");
                    }
                }
            })
        })
    },
    disallowanceWithdraw:function (id) {
        layer.confirm('确定驳回该条提现申请？',{icon:3, title:'提示信息',skin : "layui-ext-motif"},function(index){
            Common.invoke({
                url:request("/manualAdmin/checkWithdraw"),
                data:{
                    id:id,
                    status:-1
                },
                success:function (result) {
                    if(result.resultCode == 1){
                        layui.layer.alert("驳回成功");
                        layui.table.reload("checkWithdraw_table");
                    }
                }
            })
        })
    },
    ignoreWithdraw:function(id){
        layer.confirm('确定忽略该条提现申请？',{icon:3, title:'提示信息',skin : "layui-ext-motif"},function(index){
            Common.invoke({
                url:request("/manualAdmin/checkWithdraw"),
                data:{
                    id:id,
                    status:-2
                },
                success:function (result) {
                    if(result.resultCode == 1){
                        layui.layer.alert("忽略成功");
                        layui.table.reload("checkWithdraw_table");
                    }
                }
            })
        })
    },
    deleteWithdraw:function (id) {
        layer.confirm('确定删除该条提现申请？',{icon:3, title:'提示信息',skin : "layui-ext-motif"},function(index){
            Common.invoke({
                url:request("/manualAdmin/deleteWithdraw"),
                data:{
                    id:id
                },
                success:function (result) {
                    if(result.resultCode == 1){
                        layui.layer.alert("删除成功");
                        layui.table.reload("checkWithdraw_table");
                    }
                }
            })
        })
    }
}
$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})