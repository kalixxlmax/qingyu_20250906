/*响应式操作*/
window.onresize=function(){
    load();
};


function load() {
    var screenWidth  = window.parent.getScreenWidth();
    var xd_screen = 1440;
    var md_screen = 1920;
    var md_screen_ = 1800;
    setResponsive(screenWidth, xd_screen, md_screen, md_screen_);
}

function setResponsive(screenWidth, xd_screen, md_screen, md_screen_) {
    console.log(screenWidth);

    if (screenWidth >= md_screen){
        setSevenActivedata_();
        setOnlineUser_();
        setRegistUserCount_();
        setOnlineCount_();
        setSightMessage_();
        setRoomCount_();
        friendsRelationCount_()
    }

    if (screenWidth < md_screen){
        setSevenActivedata(screenWidth-365);
        setRegistUserCount(screenWidth-290);
        setOnlineCount(screenWidth-290);
        setSightMessage(screenWidth-290);
        setRoomCount(screenWidth-290);
        friendsRelationCount(screenWidth-290);
        setOnlineUser();
    }

    if (screenWidth <= md_screen_){
        setStatistics();
    }

    if (screenWidth > md_screen_){
        setStatistics_();
    }

    if (screenWidth >1140){

    }
}

/*七日数据*/
function setSevenActivedata(setWidth){
    $(".severTest").css('width',setWidth);
    var myChart = echarts.init(document.getElementById('sevenStatistics'));
    myChart.resize({width:setWidth});  // 动态设置容器宽高
}

function setSevenActivedata_(){
    $(".severTest").css('width',"1082px");
    var myChart = echarts.init(document.getElementById('sevenStatistics'));
    myChart.resize({width:1054});
}

/*在线人数*/
function setOnlineUser(){
    $("#onlineUserDiv").css("margin-top","24px");
    $("#onlineUserDiv").css("padding-left","0px");
}

function setOnlineUser_(){
    $("#onlineUserDiv").css("margin-top","4px");
    $("#onlineUserDiv").css("padding-left","17px");
}


/*统计*/
function setStatistics(){
    $(".statistics_body").css("width","202px");
}

function setStatistics_(){
    $(".statistics_body").css("width","292px");
}

/*注册量*/
function setRegistUserCount(setWidth){
    $("#userRegisterCount").css('width',setWidth);
    var myChart = echarts.init(document.getElementById('userRegisterCount'));
    myChart.resize({width:setWidth});
}

function setRegistUserCount_(){
    $("#userRegisterCount").css('width',"1630px");
    var myChart = echarts.init(document.getElementById('userRegisterCount'));
    myChart.resize({width:1630});
}

/*在线量*/
function setOnlineCount(setWidth){
    $("#userOnlineNumCount").css('width',setWidth);
    var myChart = echarts.init(document.getElementById('userOnlineNumCount'));
    myChart.resize({width:setWidth});
}

function setOnlineCount_(){
    $("#userRegisterCount").css('width',"1630px");
    var myChart = echarts.init(document.getElementById('userOnlineNumCount'));
    myChart.resize({width:1630});
}

/*单聊消息*/
function setSightMessage(setWidth){
    $("#chatMsgSumCount").css('width',setWidth);
    var myChart = echarts.init(document.getElementById('chatMsgSumCount'));
    myChart.resize({width:setWidth});
}

function setSightMessage_(){
    $("#chatMsgSumCount").css('width',"1630px");
    var myChart = echarts.init(document.getElementById('chatMsgSumCount'));
    myChart.resize({width:1630});
}

/*创建群组*/
function setRoomCount(setWidth){
    $("#addRoomsCount").css('width',setWidth);
    var myChart = echarts.init(document.getElementById('addRoomsCount'));
    myChart.resize({width:setWidth});
}

function setRoomCount_(){
    $("#addRoomsCount").css('width',"1630px");
    var myChart = echarts.init(document.getElementById('addRoomsCount'));
    myChart.resize({width:1630});
}

/*好友关系*/
function friendsRelationCount(setWidth){
    $("#userOnlineNumCount").css('width',setWidth);
    var myChart = echarts.init(document.getElementById('friendsRelationCount'));
    myChart.resize({width:setWidth});
}

function friendsRelationCount_(){
    $("#userOnlineNumCount").css('width',"1630px");
    var myChart = echarts.init(document.getElementById('userOnlineNumCount'));
    myChart.resize({width:1630});
}

$(function () {
    load();
});

Date.prototype.Format = function (fmt) {
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
};