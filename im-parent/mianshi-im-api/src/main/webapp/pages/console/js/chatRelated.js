/*聊天信息相关操作*/

// bubble_message 气泡消息,群控制
var bubble_message = [83,84,401,402,901,902,903,904,907,906,905,913,915,916,917,918,919,920,921,922,923,924,925,931,944,945];
var chat = {
    //拼接消息
    createMsg:function (contentList) {
        var itemHTML = "";
        console.log("消息内容:",contentList);
        if (Common.isNil(contentList)){
            itemHTML = message.notMsgContent();
            $("#messageContainer").append(itemHTML);
            return "";
        }
        //倒叙循环  时间顺序才是正确的
        for (var i = contentList.length - 1; i >= 0 ; i--) {
            //解密消息内容
            contentList[i].content = Common.decodeContent(contentList[i]);
            if (bubble_message.includes(contentList[i].contentType)){
                itemHTML += message.jointContent(contentList[i]);
            }else{
                itemHTML += '<div class="content_top">' +
                    '<div class="content_time">'+ UI.getLocalTime(contentList[i].timeSend / 1000) +'</div>' +
                    '<div>' +
                    '<div class="content_username_size'+ (Common.isThisUser(contentList[i].sender)?" content_username_right":" content_username_left") +'">'+ JSON.parse(contentList[i].message).fromUserName +'</div>' +
                    '<img onerror=\'this.src=\"/pages/img/ic_avatar.png\"\' class="avatar content_hred_img '+ (Common.isThisUser(contentList[i].sender)?" head_right":"") +'"  src="'+ Common.isThisUser(contentList[i].sender) +'">' +
                        message.jointContent(contentList[i]) +
                    '</div>' +
                    '</div>' +
                    '</div>'
            }
        }
        $("#messageContainer").append(itemHTML);
    }

    //获取群聊信息
    ,getChatRoomMessage:function (jid) {
        var messageContent;
        $.ajax({
            type:"POST",
            url:request("/console/groupchat_logs_all")+"&room_jid_id="+jid,
            data:{
                page: 0,
                limit:9999
            },
            dataType:"json",
            async:false,
            success : function(result) {
                messageContent = result.data;
            },
            error : function(result) {
                layer.msg(result.resultMsg);
                messageContent = "";
            }
        })
        return messageContent;
    }

    //滚动到底部  type ：类型分为发送和接收两种
    ,scrollToEnd : function() {
        setTimeout(function(){ //将滚动条移动到最下方
            $(".nano").nanoScroller();//刷新滚动条
            $(".nano").nanoScroller({ scrollBottom: -100000000000});//滚动到底部
        },400);
    }
    //原图预览
    ,previewImgs : function(src) {
        var maxWidth = (undefined!=document.body.clientWidth)?document.body.clientWidth-100:800;
        var maxHeight = (undefined!=document.body.clientHeight)?document.body.clientHeight-80:700;

        $("#imgZoom_div").append("<img src="+src+" class='mp_imgZoom_img' style='max-width:"+maxWidth+"px; max-height:"+maxHeight+"px;'/>");

        //弹出层
        layer.open({
            type: 1,
            title: false,
            btn:false,
            shade:0.6,
            closeBtn: 0,
            skin: 'layui-layer-nobg',//去掉背景
            shadeClose: true, //点击外围关闭弹窗
            scrollbar: false, //不现实滚动条
            maxWidth: maxWidth,
            maxHeight: maxHeight,
            //area:  ['auto', 'auto'],
            content: $("#imgZoom_div"),//自定义 html 内容，注意这里的整个 html 内容背景肯定是透明的
            success: function(layero, index){

            },
            end: function(){
                $("#imgZoom_div").empty().hide();
                return false;
            },
            cancel: function(index, layero){

                layer.close(index)
                $("#imgZoom_div").empty().hide();
                return false;
            },
        });

    },

    /**
     * [处理消息内容，将表情字符替换为图片]
     * @param  {[type]} content [description]
     * @param  type   不传或者0 表示
     *
     */
    parseContent : function(content,type) {
        var emojlKeys = new Array();
        if(Common.isNil(content))
            return content;
        var s = content;
        var fromIndex = 0;
        while (fromIndex != -1) {
            fromIndex = s.indexOf("[", fromIndex);
            if (fromIndex == -1)
                break;
            else {
                var stop = s.indexOf("]", fromIndex);
                if (stop == -1)
                    break;
                else {
                    var emojlKey = s.substring(fromIndex, stop + 1);
                    emojlKeys.push(emojlKey);
                    fromIndex = fromIndex + 1;
                }
            }
        }
        //表情
        if (emojlKeys.length != 0) {
            var key=null;
            var emojl=null;
            for (var i = 0; i < emojlKeys.length; i++) {
                key = emojlKeys[i];
                try {
                    emojl=_emojl[key];
                }catch (e) {
                }
                if(!Common.isNil(emojl)){
                    if(1==type)
                        s = s.replace(key, "<img src='" + emojl + "' height='20' />");
                    else
                        s = s.replace(key, "<img src='" + emojl + "' height='25' />");
                }
            }
            return s;
        }

        content=chat.hrefEncode(content);

        return content;

    }

    ,hrefEncode:function  (e) {
        var a = e.match(/&lt;a href=(?:'|").*?(?:'|").*?&gt;.*?&lt;\/a&gt;/g);
        if (a) {
            for (var n, i, o = 0, r = a.length; o < r; ++o)
                n = /&lt;a href=(?:'|")(.*?)(?:'|").*?&gt;.*?&lt;\/a&gt;/.exec(a[o]),
                n && n[1] && (i = n[1],this.isUrl(i) && (e = e.replace(n[0], this.htmlDecode(n[0])).replace(n[1], n[1])));
            return e
        }
        return e.replace(new RegExp(this.regText, "ig"), function () {
            return '<a target="_blank" href="' + arguments[0].replace(/^(\s|\n)/, "") + '">' + arguments[0] + "</a> "
        })
    },
    regText :"(\\s|\\n|<br>|^)(http(s)?://.)?(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?(&|&amp;)//=]*)",

    isUrl:function(e) {
        return new RegExp(this.regText, "i").test(e)
    },
    htmlDecode:function (e){
        return e && 0 != e.length ? e.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&") : ""
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
        //播放结束
        amr.onEnded(function() {
            $("#playVoice").show();
            $("#playVoiceGif").hide();
        })

        //暂停播放
        $(".stopVoiceBtn").click(function () {
            amr.stop();
            $("#playVoice").show();
            $("#playVoiceGif").hide();
        })
    },

    //是否群主消息
    isGroupType:function (chatType) {
        return "groupchat"==chatType;
    },

    //是否是 单聊消息
    isChatType:function(chatType){
        return "chat"==chatType;
    },
}





//拼接消息
var message = {
    /**
     * 拼接消息
     * contentType 消息类型
     */
    jointContent:function (contentData) {
        var contentHtml = "";
        switch (contentData.contentType){
            case 1:
                //文字 表情
                contentHtml = message.createTextMsgContent(contentData);
                break;
            case 2:
                //图片
                contentHtml = message.createPicturnContent(contentData);
                break;
            case 3:
                //语音
                contentHtml = message.createVoideContent(contentData);
                break;
            case 4:
                //地图
                contentHtml = message.createMapContent(contentData);
                break;
            case 5:
                //GIF
                contentHtml = message.createGifContent(contentData);
                break;
            case 6:
                //视频
                contentHtml = message.createVideoContent(contentData);
                break;
            case 8:
                //名片
                contentHtml = message.createCardContent(contentData);
                break;
            case 9:
                //文件
                contentHtml = message.createFileContent(contentData);
                break;
            case 83:
                contentHtml = message.createOtherMsgContent(contentData.fromUserName," 领取了你的红包 ","");
                break;
            case 84:
                //戳一戳消息
                contentHtml = message.createOtherMsgContent(contentData.sender_nickname," - 戳了戳你","'");
                break;
            case 401:
                var fileName=contentData.fileName.substring(contentData.fileName.lastIndexOf("/")+1);
                contentHtml = message.createOtherMsgContent(contentData.fromUserName," 上传了群文件 ",fileName)
                break;
            case 402:
                contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 删除了群文件 ");
                break;
            case 901:
                contentHtml = message.createOtherMsgContent(contentData.fromUserName, " 群昵称修改为 ", contentData.content);
                break;
            case 903:
                //禁言/取消禁言
                contentHtml = message.createOtherMsgContent("","删除房间","'");
                break;
            case 904:
                var msg = JSON.parse(contentData.message);
                if(contentData.fromUserId==contentData.toUserId)
                    contentHtml = message.createOtherMsgContent(msg.toUserName," 已退出群组","");
                else{
                    contentHtml = message.createOtherMsgContent(msg.toUserName," 已被移出群组","");
                }
                break;
            case 905:
                //群公告
                contentHtml = message.createGroupNotice(contentData);
                break;
            case 906:
                //禁言/取消禁言
                contentHtml = message.createOtherMsgContent("","群已禁言","'");
                break;
            case 907:
                //增加新成员
                var msg = JSON.parse(contentData.message);
                contentHtml = message.createOtherMsgContent("增加新成员：'",msg.toUserName,"'");
                break;
            case 913:
                if(1==contentData.content||"1"==contentData.content)
                    contentHtml = message.createOtherMsgContent(contentData.toUserName," 被设置管理员 ","");
                else{
                    contentHtml = message.createOtherMsgContent(contentData.toUserName," 被取消管理员 ","");
                }
                break;
            case 915:
                //群已读消息开关
                if(1==contentData.content||"1"==contentData.content){
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 开启了显示消息已读人数");
                }else{
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 关闭了显示消息已读人数");
                }
                break;
            case 916:
                if(Common.isNil(contentData.content)){
                    //邀请好友
                    var inviteObj=eval("(" +contentData.objectId+ ")");
                    if("0"==inviteObj.isInvite||0==inviteObj.isInvite){
                        var count=inviteObj.userIds.split(",").length;
                        contentHtml = message.createOtherMsgContent(contentData.fromUserName," 想邀请 ",count+" 位朋友加入群聊 ");
                    }else{
                        contentHtml =  message.createOtherMsgContent(contentData.fromUserName," 申请加入群聊 ","");
                    }
                }else{
                    if(1==contentData.content||"1"==contentData.content){
                        contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 开启了进群验证");
                    }else{
                        contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 关闭了进群验证");
                    }
                }
                break;
            case 917:
                //群公开状态
                if(1==contentData.content||"1"==contentData.content){
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 修改为隐私群组");
                }else{
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 修改为公开群组");
                }
                break;
            case 918:
                if(1==contentData.content||"1"==contentData.content){
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 开启了显示群成员列表");
                }else
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 关闭了显示群成员列表");
                break;
            case 919:
                if(1==contentData.content||"1"==contentData.content){
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 开启了允许普通群成员私聊");
                }else{
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 关闭了允许普通群成员私聊");
                }
                break;
            case 920:
                if(0==contentData.content||"0"==contentData.content){
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName,"已取消全体禁言");
                }else{
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName,"已开启全体禁言");
                }
                break;
            case 921:
                if(1==contentData.content||"1"==contentData.content){
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 开启了允许普通成员邀请好友");
                }else{
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 关闭了允许普通成员邀请好友");
                }
                break;
            case 922:
                if(1==contentData.content||"1"==contentData.content){
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 开启了允许普通成员上传群共享文件");
                }else{
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 关闭了允许普通成员上传群共享文件");
                }
                break;
            case 923:
                if(1==contentData.content||"1"==contentData.content){
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 开启了允许普通成员召开会议");
                }else{
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 关闭了允许普通成员召开会议");
                }
                break;
            case 924:
                if(1==contentData.content||"1"==contentData.content){
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 开启了允许普通成员讲课");
                }else{
                    contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 关闭了允许普通成员讲课");
                }
                break;
            case 925:
                if(chat.isGroupType(contentData.chatType))
                    return null;
                contentHtml = message.createOtherMsgContent("",contentData.toUserName," 已成为新群主");
                break;
            case 931:  //群锁定、解锁
                contentHtml = message.createOtherMsgContent("","此群已"+(contentData.content==-1?"被锁定":"解除锁定"),"");
                break;
            case 932:
                contentHtml = message.createOtherMsgContent(contentData.fromUserName," 修改群公告 ",contentData.content);
                break;
            case 934:
                contentHtml = message.createTextMsgContent(contentData);
                break;
            case 944:
                contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 禁用群成员修改名片功能");
                break;
            case 945:
                contentHtml = message.createOtherMsgContent("",contentData.fromUserName," 刷新聊天背景水印");
                break;
            default://默认 其他
                contentHtml+="<p class='plain'>";
                contentHtml+= "[web不支持 请在手机上查看]</p>";
        }

        return contentHtml;
    }
    //没有消息
    ,notMsgContent: function () {
        var itemHTML = '<div class="content_top">' +
            '<div style="margin: 0px 20px;text-align: center;color: #b2b2b2;">' +
            '<pre class="js_message_plain">暂时无消息记录</pre>' +
            '</div>' +
            '</div>';
        return itemHTML;
    }
    //文本
    , createTextMsgContent: function (contentData) {
        var parseContent = chat.parseContent(contentData.content);
        var itemHTML = '<div class="bubble js_message_bubble bubble_primary ' + (Common.isThisUser(contentData.sender) ? "msg_right" : "") + '">' +
            '<div style="padding: 4px;">' +
            '<pre class="js_message_plain">' + parseContent + '</pre>' +
            '</div>' +
            '</div>';
        return itemHTML
    }
    //图片
    , createPicturnContent: function (contentData) {
        var itemHTML = '<div class="bubble ' + (Common.isThisUser(contentData.sender) ? "msg_right" : "") + '">' +
            '<img onerror="this.src=\'/pages/img/overdue.png\'" class="msg-img" src="' + contentData.content + '" onclick="chat.previewImgs(\'' + contentData.content + '\')">' +
            '</div>';
        return itemHTML
    }
    //语音
    , createVoideContent: function (contentData) {
        var msg ;
        try {
            msg = JSON.parse(contentData.message);
        }catch (e) {
            console.log("数据解析失败：" + contentData.toString());
            return "";
        }
        var itemHTML = '<div class="bubble js_message_bubble  bubble_primary ' + (Common.isThisUser(contentData.sender) ? "msg_right" : "") + '">' +
            '<div style="padding: 4px;">' +
            '<div class=\'bubble_cont\'>' +
            '<div>' +
            '<img id=\'playVoice\' src=\'./images/voice.png\' style=\'width:25px; height:25px;margin-top:-2.5px\' onclick="chat.playVoice(\'' + contentData.content + '\')">' +
            '<img id=\'playVoiceGif\' class="stopVoiceBtn" src=\'/pages/console/images/voice.gif\' style=\'width:25px; height:25px;margin-top:-2.5px;display: none;\'>' +
            '<span style=\'display:inline; margin-left:15px; margin-right:10px;\'>'+ msg.fileTime +'</span>' +
            '</div>' +
            '</div>' +
            '</div>' +
            '</div>';

        return itemHTML;
    }

    //视频
    , createVideoContent: function (contentData) {
        var itemHTML = '<div class="bubble ' + (Common.isThisUser(contentData.sender) ? "msg_right" : "") + '">' +
            '<div>' +
            '<video class=\'video\' controls style=\'width:240px;height:240px;\'>' +
            '<source src=\'' + contentData.content + '\' type=\'video/mp4\'>' +
            '</video>' +
            '</div>' +
            '</div>';
        return itemHTML;
    }

    //名片
    , createCardContent: function (contentData) {
        var itemHTML = '<div class="bubble js_message_bubble  bubble_primary ' + (Common.isThisUser(contentData.sender) ? "msg_right" : "") + '">' +
            '<div style="padding: 4px;">' +
            '<div class=\'bubble_cont\'>' +
            '<div class="card">' +
            '<div class="card_bd">' +
            '<div class="card_avatar">' +
            '<img class="img"  src=\'/pages/console/images/28.png\'>' +
            '</div>' +
            '<div class="info">' +
            '<h3 class="display_name">' + contentData.content + '</h3>' +
            '</div>' +
            '</div>' +
            '<div class="card_hd">' +
            '<p class="">个人名片</p>' +
            '</div>' +
            '</div>' +
            '</div>' +
            '</div>';
        return itemHTML;
    }

    //文件
    , createFileContent: function (contentData) {
        var itemHTML = '<div class="bubble js_message_bubble  bubble_primary ' + (Common.isThisUser(contentData.sender) ? "msg_right" : "") + '">' +
            '<div style="padding: 4px;">' +
            '<div class=\'bubble_cont\'>' +
            '<div class="card">' +
            '<div class="card_bd">' +
            '<div class="card_avatar">' +
            '<img class="img"  src=\'/pages/console/images/28.png\'>' +
            '</div>' +
            '<div class="info">' +
            '<h3 class="display_name">测试呀</h3>' +
            '</div>' +
            '</div>' +
            '<div class="card_hd">' +
            '<p class="">文件</p>' +
            '</div>' +
            '</div>' +
            '</div>' +
            '</div>' +
            '</div>';
        return itemHTML;
    }
    //地图
    ,createMapContent:function (contentData) {
        var msg = JSON.parse(contentData.body);
        var itemHTML = '<div class="location">'
            +	'<a href="javascript:void(0)" onclick="showToMap(this)"'+' lng="'+msg.location_x+' "lat="'+msg.location_y+'">'
            +		'<img alt="" class="img" src="'+BaiduMap.imgApiUrl+msg.location_y+','+msg.location_x+' ">'
            +		'<p class="desc ng-binding">'+msg.objectId+'</p>'
            +	'</a>'
            + '</div>';
        return itemHTML;
    }
    //群公告
    ,createGroupNotice:function (contentData) {
        var itemHTML = '<div class="content_top">' +
            '<div style="margin: 0px 20px;text-align: center;color: #b2b2b2;">' +
            '<pre class="js_message_plain">'+ contentData.fromUserName  + " 发布群公告："+ contentData.content +'</pre>' +
            '</div>' +
            '</div>';
        return itemHTML;
    }
    //GIF 动画
    ,createGifContent:function (contentData) {
        var itemHTML = '<div class="emoticon">'
            +	'<img  class="custom_emoji msg-img"  src="./gif/'+ contentData.content +'">'
            + '</div>';
        return itemHTML;
    }
    /**
     * 其他消息
     * @param contentData 数据
     * @param fronContent 添加前面内容
     * @param backContent 添加到后面内容
     * @returns {string}
     */
    ,createOtherMsgContent:function (fronContent,contentData,backContent) {
        var itemHTML = '<div class="content_top">' +
            '<div style="margin: 0px 20px;text-align: center;color: #b2b2b2;">' +
            '<pre class="js_message_plain">' + fronContent  + contentData + backContent + '</pre>' +
            '</div>' +
            '</div>';
        return itemHTML;
    }
}