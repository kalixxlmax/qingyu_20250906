var error = "请求参数验证失败，缺少必填参数或参数错误";
var limit = 15;
var contentId_ = '';
var userId_ = "";
var commentPage = 0;
var fileAddr = "";
var access_token = "";

//获取请求参数
function getURLParameter(name) {
    return decodeURIComponent((new RegExp('[?|&]' + name + '=' + '([^&;]+?)(&|#|;|$)').exec(location.search)||[,""])[1].replace(/\+/g, '%20'))||null;
}

//判空
function isNil(s) {
    return undefined == s || $.trim(s) == "undefined" || null == s || $.trim(s) == "" || $.trim(s) == "null" || JSON.stringify(s) == JSON.stringify({}) || JSON.stringify(s) == JSON.stringify("{}");
}

// 公众号获取头像
function getAvatarUrl(userId,update) {
    if(10000==userId){
        return "./images/im_10000.png";
    }
    var imgUrl = fileAddr+"avatar/o/"+ (parseInt(userId) % 10000) + "/" + userId + ".jpg";
    if(1==update){
        imgUrl+="?x="+Math.random()*10;
    }
    return imgUrl;
}

/**
 * 时间转换
 * 格式1：Y+M+D+h+m+s
 * 格式2：Y+M+D
 */
function getLocalTime(time,format){
    var date = new Date(time * 1000);//时间戳为10位需*1000，时间戳为13位的话不需乘1000
    var Y = date.getFullYear() + '-';
    var M = (date.getMonth()+1 < 10 ? '0'+(date.getMonth()+1) : date.getMonth()+1) + '-';
    var D = date.getDate() + ' ';
    var h = date.getHours() + ':';
    var m = (date.getMinutes()<10?'0'+(date.getMinutes()):date.getMinutes()) + ':';
    var s = (date.getSeconds()<10?'0'+(date.getSeconds()):date.getSeconds());
    if (isNil(format) || format === 1 ){
        return Y+M+D+h+m+s;
    }if (format === 2){
        return Y+M+D;
    }
}

//获取显示内容
function getShowContent(contentId){
    $.ajax({
        type:"POST",
        url : '/mp/find/content',
        data : {
            id:contentId,
            access_token:access_token
        },
        dataType:'json',
        async:false,
        success : function(result) {
            if (result.resultCode == 1) {
                $("#showTitle").append(result.data.title);
                $("#showTime").append(getLocalTime(result.data.createTime, 2));
                $("#showContent").append(result.data.content);
                $("#contentFrom").append(result.data.contentFrom);
                var content = '<span style="float: left;">' +
                                    '<span class="showButtom_read_style">阅读</span>' +
                                    '<span class="showButtom_read_style">'+ result.data.look +'</span>' +
                                '</span>' +
                                '<span style="float: right;">' +
                                    '<img src="'+ (result.data.isPraise == 1 ? "./images/like_success.png" : "./images/like.png") +'" width="20px" height="20px" class="like_'+ result.data.id +'" onclick="giveLikeContent(\''+ result.data.id + '\',0)">' +
                                    '<span class="showButtom_read_style likeCount_'+ result.data.id +'">'+ result.data.praise +'</span>' +
                                '</span>';
                $("#showButtom").append(content);
            }
        },
        error : function(result) {
            $("#showContent").append(result.resultMsg);
        }
    });
}

//获取评论
function getComment(contentId,pageIndex) {
    var data = "";
    $.ajax({
        type:"POST",
        url : '/mp/find/comment',
        data : {
            id:contentId,
            pageIndex:pageIndex,
            access_token:access_token
        },
        dataType:'json',
        async:false,
        success : function(result) {
            if (result.resultCode == 1) {
                data = result.data;
                $("#errorLay").hide();
                $("#showCommentContent").show();
            }else{
                $("#showCommentContent").hide();
                $("#errorLay").show();
                console.log(result.resultMsg)
            }
        },
        error : function(result){
            console.log(result.resultMsg)
        }
    });
    return data;
}

//获取评论数量
function getCommentCount(contentId) {
    $.ajax({
        type:"POST",
        url : '/mp/find/comment/count',
        data : {
            id:contentId,
            access_token:access_token
        },
        dataType:'json',
        async:false,
        success : function(result) {
            console.log(result);
            if (result.resultCode == 1) {
                commentPage = parseInt(result.data/5) + 1;
            }
        },
        error : function(result) {
            commentPage = 0;
        }
    });
}

//加载评论
function loadComment(contentId){
    layui.use('flow', function(){
        var flow = layui.flow;

        flow.load({
            elem: '#LAY_demo2' //流加载容器
            ,scrollElem: '#LAY_demo2' //滚动条所在元素，一般不用填，此处只是演示需要。
            ,isAuto: false
            ,isLazyimg: true
            ,done: function(page, next){ //加载下一页
                //模拟插入
                setTimeout(function(){
                    var lis = [];
                    var data = getComment(contentId,page);
                    for(var i = 0; i < data.length; i++){
                        var content = '<div class="layui-form-item layui-form-text" style="margin-bottom: 6%;">' +
                            '<label class="form-label_">' +
                                '<img src="'+ getAvatarUrl(data[i].userId) +'" onerror="this.src=\'./images/ic_avatar.png\'" class="comment_head layui-circle">' +
                            '</label>' +
                            '<span class="comment_nickname">'+ data[i].userName +'</span>' +
                            '<span style="float: right;">' +
                                '<img src="'+ (data[i].isPraise == 1 ? "./images/praise_success.png" : "./images/praise.png") +'" class="like_'+ data[i].id +'" onclick="giveLike(\''+ data[i].id + '\',1)" width="20px" height="20px">' +
                                '<span class="likeCount_'+ data[i].id +'" style="font-size: 14px;color: rgb(136, 136, 136); margin-left: 2px;">'+ data[i].praise +'</span>' +
                            '</span><br/>' +
                            '<p class="comment_content_font_size">'+ data[i].commentContent +'</p>' +
                            '</div>'
                        lis.push(content);
                    }
                    next(lis.join(''), page  < commentPage)
                }, 500);
            }
        });

    });
}

//点击写留言按钮
var clickNum = 0;
function writeCommentBtn() {
    /*$("#commentFrame").show();*/
    layer.open({
        type: 1,
        title: false,
        closeBtn: 0,
        offset: 'b',
        area: ['100%', '238px'],
        anim: 2,
        shadeClose: false,
        content: $("#showMsgInfo")
    });
    clickNum++;
    document.getElementById("layui-layer"+clickNum).style.borderRadius="20px 20px 0px 0px";
    layui.form.render();
}

//关闭所有弹窗层
function closeDialog() {
    $("#showMsgInfo").hide();
    layer.closeAll();
}

//关闭写留言按钮
function closeCommentBtn() {
    $("#commentFrame").hide();
}

//清空写留言内容
function clearCommentContent() {
    $(".comment_content").val('');
    $(".comment_content_send").addClass("layui-btn-disabled")
}

//发送留言
function sendComment(){
    var commentContent = $(".comment_content").val();
    if (isNil(commentContent)){
        layui.layer.msg("请输入留言内容", {icon: 2});
    }else{
        $(".commentText").val('');
        savaComment(commentContent);
    }
}


//保存留言
function savaComment(commentContent){
    $.ajax({
        type:"POST",
        url : '/mp/sava/comment',
        data : {
            massContentId:contentId_,
            commentContent:commentContent,
            access_token:access_token
        },
        dataType:'json',
        async:false,
        success : function(result) {
            if (result.resultCode == 1) {
                var content = '<div class="layui-form-item layui-form-text" style="margin-bottom: 6%;">' +
                    '<label class="form-label_">' +
                        '<img src="'+ getAvatarUrl(result.data.userId) +'" onerror="this.src=\'./images/ic_avatar.png\'" class="comment_head layui-circle">' +
                    '</label>' +
                    '<span class="comment_nickname">'+ result.data.userName +'</span>' +
                    '<span style="float: right;">' +
                        '<img src="./images/praise.png" class="like_'+ result.data.id +'" onclick="giveLike(\''+ result.data.id + '\',1)" width="20px" height="20px">' +
                        '<span class="likeCount_'+ result.data.id +'" style="font-size: 14px;color: rgb(136, 136, 136);margin-left: 2px;">0</span>' +
                    '</span><br/>' +
                    '<p class="comment_content_font_size">'+ commentContent +'</p>' +
                    '</div>'
                $("#LAY_demo2").prepend(content);
                closeDialog();
                clearCommentContent();
                layui.layer.msg("留言成功", {icon: 1});
                console.log("留言成功")
            }else{
                layui.layer.msg("留言失败", {icon: 2});
                console.log(result.resultMsg);
            }
        },
        error : function(result) {
            console.log("留言失败"+result.resultMsg);
        }
    });
}

//获取配置
function getConfig(){
    $.ajax({
        type:"GET",
        url : '/mp/config',
        data : {},
        dataType:'json',
        async:false,
        success : function(result) {
            if (1 == result.resultCode) {
                fileAddr = result.data.fileAddr;
            }
        },
        error : function(result) {
        }
    });
}

$(function () {
    var contentId = getURLParameter('contentId');
    var userId = getURLParameter('userId');
    var requestAccess_token = getURLParameter('access_token');
    console.log('contentId='+contentId);
    console.log('userId='+userId);
    if (isNil(contentId)){
        $("#showContent").append(error);
        $("#showCommentContent").hide();
    }else{
        contentId_ = contentId;
        userId_ = userId;
        if (isNil(requestAccess_token)){
            $("#showCommentContent").hide();
            $(".writeComment").hide();
        }else{
            access_token = requestAccess_token;
            $("#showCommentContent").show();
            getCommentCount(contentId);
        }
        getShowContent(contentId);
        loadComment(contentId);
        getConfig();
        $(".comment_head_").attr("src",getAvatarUrl(userId));
        console.log(getAvatarUrl(userId));
    }

    $(".comment_test").click(function () {

    })
})

/* 点赞相关 */
//点赞
function giveLike(selector,isComment) {
    var srcUrl = $(".like_" + selector).attr('src');
    if (srcUrl.includes("praise_success")){
        //取消点赞
        $(".like_" + selector).attr('src',"./images/praise.png");
        var count = $(".likeCount_" + selector).html();
        $(".likeCount_" + selector).html(parseInt(count)-1);
        updateGiveLike(selector,-1,isComment);
    }else{
        //点赞
        $(".like_" + selector).attr('src',"./images/praise_success.png");
        var count = $(".likeCount_" + selector).html();
        $(".likeCount_" + selector).html(parseInt(count)+1);
        updateGiveLike(selector,1,isComment);
    }
}

/*文赞点赞*/
function giveLikeContent(selector,isComment) {
    var srcUrl = $(".like_" + selector).attr('src');
    if (srcUrl.includes("like_success")){
        //取消点赞
        $(".like_" + selector).attr('src',"./images/like.png");
        var count = $(".likeCount_" + selector).html();
        $(".likeCount_" + selector).html(parseInt(count)-1);
        updateGiveLike(selector,-1,isComment);
    }else{
        //点赞
        $(".like_" + selector).attr('src',"./images/like_success.png");
        var count = $(".likeCount_" + selector).html();
        $(".likeCount_" + selector).html(parseInt(count)+1);
        updateGiveLike(selector,1,isComment);
    }
}


function updateGiveLike(id,type,isComment) {
    $.ajax({
        type:"POST",
        url : '/mp/give/like',
        data : {
            commentId:id,
            type:type,
            isComment:isComment,
            access_token:access_token
        },
        dataType:'json',
        async:true,
        success : function(result) {
            if (result.resultCode != 1) {
                console.log("点赞失败"+result.resultMsg);
            }
        },
        error : function(result) {
            console.log("点赞失败"+result.resultMsg);
        }
    });
}

//是否输入内容
function isInputContent(obj) {
    var value = obj.value;
    if (isNil(value)){
        $(".comment_content_send").addClass("layui-btn-disabled");
    }else{
        $(".comment_content_send").removeClass("layui-btn-disabled");
    }
}