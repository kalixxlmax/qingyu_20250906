var startTime = 0;
var endTime = 0;

$(function () {
    layui.use('laydate', function () {
        var laydate = layui.laydate,
            form = layui.form;
        form.render('select');

        //日期范围
        var time = laydate.render({
            elem: '#globalDate'
            , range: "~"
            , done: function (value, date, endDate) {  // choose end
                //console.log("date callBack====>>>"+value); //得到日期生成的值，如：2017-08-18
                var startDate = value.split("~")[0];
                var endDate = value.split("~")[1];
                if (startDate) {
                    startTime = BillCount.dateToSeconds(new Date(startDate));
                }
                if (endDate) {
                    endTime = BillCount.dateToSeconds(new Date(endDate));
                }
                BillCount.queryBillCount(startTime, endTime);

            }
            , max: 1
        });


        layui.form.on('select(global-time-unit)', function (data) {
            var dateRange = $("#globalDate").val();

            var endDate = new Date();
            endTime = BillCount.dateToSeconds(endDate);
            startDate = new Date(endDate);
            if (data.value == 1) {//时间单位切换到年
                endTime = 0;
                startTime = 0;
            } else if (data.value == 2) {//时间单位切换到年
                startDate.setFullYear(endDate.getFullYear() - 1);
                startTime = BillCount.dateToSeconds(startDate);
            } else if (data.value == 3) {//时间单位切换到月
                startDate.setMonth(endDate.getMonth() - 1);
                startTime = BillCount.dateToSeconds(startDate);
            } else if (data.value == 4) { //时间单位切换到天
                startTime = endTime - 86400;
                $("#globalDate").val();
                //$(".prompt_info").text("注：时间单位若为分钟，不能选择时间范围,只会显示当前这一天的数据");

                dateRange = "";
            } else {
            }
            BillCount.queryBillCount(startTime, endTime);
        });
    });
    BillCount.queryBillCount(0, 0);
});


var BillCount = {

    /** 获取用户房间好友等总数量 **/
    queryBillCount: function (startTime, endTime) {
        startTime = parseInt(startTime);
        endTime = parseInt(endTime);
        Common.invoke({
            url: request('/console/billCount'),
            data: {
                "startTime": startTime,
                "endTime": endTime
            },
            successMsg: false,
            errorMsg: "加载数据失败，请稍后重试",
            success: function (result) {

                var totalData = result.data;
                console.log(totalData);
                var redpacketOverTotal = "0.0";

                var transferOverTotal = "0.0";
                var rechargeTotal = "0.0";
                var cashTotal = "0.0";
                var serviceChargeTotal = "0.0";
                if (totalData.redpacketOverTotal) {
                    redpacketOverTotal = totalData.redpacketOverTotal;
                }
                if (totalData.transferOverTotal) {
                    transferOverTotal = totalData.transferOverTotal;
                }
                if (totalData.rechargeTotal) {
                    rechargeTotal = totalData.rechargeTotal;
                }
                if (totalData.cashTotal) {
                    cashTotal = totalData.cashTotal;
                }
                if (totalData.serviceChargeTotal) {
                    serviceChargeTotal = totalData.serviceChargeTotal;
                }
                $(".redpacketOverTotal").text(redpacketOverTotal);
                $(".transferOverTotal").text(transferOverTotal);
                $(".rechargeTotal").text(rechargeTotal);
                $(".cashTotal").text(cashTotal);
                $(".serviceChargeTotal").text(serviceChargeTotal);

            },
            error: function (result) {
            }
        });
    },
    dateToSeconds: function (date) {
        return date.getTime() / 1000;
    }
}

Date.prototype.Format = function (fmt) { //author: meizz
    var o = {
        "M+": this.getMonth() + 1, //月份
        "d+": this.getDate(), //日
        "h+": this.getHours(), //小时
        "m+": this.getMinutes(), //分
        "s+": this.getSeconds(), //秒
        "q+": Math.floor((this.getMonth() + 3) / 3), //季度
        "S": this.getMilliseconds() //毫秒
    };
    if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (this.getFullYear() + "").substr(4 - RegExp.$1.length));
    for (var k in o)
        if (new RegExp("(" + k + ")").test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (("00" + o[k]).substr(("" + o[k]).length)));
    return fmt;
}

$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})