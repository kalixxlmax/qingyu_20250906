$(function () {
    info.getPushConfigModelList();
})

var this_id;//当选项卡的编号  =  PushConfig配置类的 id
layui.use('element', function(){
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        element = layui.element,
        table = layui.table;

    //触发事件
    var active = {
        tabAdd: function(){
            var id = info.addPushConfigModel();
            this_id = id;

            //新增一个Tab项
            element.tabAdd('demo', {
                title: '推送配置<i class="layui-icon layui-unselect layui-tab-close">&#x1006;</i>' //用于演示
                , content: jointHtmlCode()
                , id: id
            });

            //切换选项卡位置
            element.tabChange('demo', id);

            //增加点击关闭事件
            var r = $(".layui-tab-title").find("li");
            //每次新打开tab都是最后一个，所以只对最后一个tab添加点击关闭事件
            r.eq(r.length - 1).children("i").on("click", function () {
                var attr = $(this).parent("li").attr('lay-id');
                layer.open({
                    content:'确定要删除推送配置吗 ?'
                    ,btn: ['确定', '取消']
                    ,yes: function(index, layero){
                        //确定的回调
                        layui.element.tabDelete("demo", attr);
                        layui.element.tabChange("demo", r.length - 1);
                        info.deletePushConfig();
                        layer.close(index);
                    }
                    ,btn2: function(index, layero){
                        //取消回调
                        layer.close(index);
                        return;
                    }
                    ,cancel: function(){
                        //右上角关闭回调
                        layer.close(index);
                        return;
                    }
                });
            });
            element.init();

            layui.form.render();
        }
        ,tabDelete: function(othis){
            //删除指定Tab项
            element.tabDelete('demo', '44'); //删除
            othis.addClass('layui-btn-disabled');
        }
    };


    //切换选项卡监听
    element.on('tab(demo)', function(data){
        if (data.elem.context.attributes == undefined){
         return;
        }
        this_id = data.elem.context.attributes[0].nodeValue;
        info.showPushConfig();
        layui.form.render();
    });


    $('.site-demo-active').on('click', function(){
        var othis = $(this), type = othis.data('type');
        active[type] ? active[type].call(this, othis) : '';
    });


    //表单提交
    form.on("submit(systemConfig)",function(data){
        /*推送配置*/
        var serverAdress = $(".serverAdress_"+ this_id +"").val();
        var packName = $(".packName_"+ this_id +"").val();

        //小米配置
        var xmAppSecret = $(".xmAppSecret_"+ this_id +"").val();

        /*华为推送*/
        var hwAppSecret = $(".hwAppSecret_"+ this_id +"").val();
        var hwAppId = $(".hwAppId_"+ this_id +"").val();
        var hwTokenUrl = $(".hwTokenUrl_"+ this_id +"").val();
        var hwApiUrl = $(".hwApiUrl_"+ this_id +"").val();
        var hwIconUrl = $(".hwIconUrl_"+ this_id +"").val();
        var isOpen = $(".isOpen_"+ this_id +"").val();


        /*百度推送*/
        var bdAppStoreAppId = $(".bdAppStoreAppId_"+ this_id +"").val();
        var bdAppStoreAppKey = $(".bdAppStoreAppKey_"+ this_id +"").val();
        var bdAppStoreSecretKey = $(".bdAppStoreSecretKey_"+ this_id +"").val();
        var bdAppKey = $(".bdAppKey_"+ this_id +"").val();
        var bdRestUrl = $(".bdRest_url_"+ this_id +"").val();
        var bdSecretKey = $(".bdSecretKey_"+ this_id +"").val();


        /*极光推送*/
        var jPushAppKey = $(".jPushAppKey_"+ this_id +"").val();
        var jPushMasterSecret = $(".jPushMasterSecret_"+ this_id +"").val();


        /*google FCM推送*/
        var fcmDataBaseUrl = $(".fcmDataBaseUrl_"+ this_id +"").val();
        var fcmKeyJson = $(".fcmKeyJson_"+ this_id +"").val();


        /*魅族推送*/
        var mzAppSecret = $(".mzAppSecret_"+ this_id +"").val();
        var mzAppId = $(".mzAppId_"+ this_id +"").val();


        /*VIVO推送*/
        var vivoAppId = $(".vivoAppId_"+ this_id +"").val();
        var vivoAppKey = $(".vivoAppKey_"+ this_id +"").val();
        var vivoAppSecret = $(".vivoAppSecret_"+ this_id +"").val();


        /*OPPO推送*/
        var oppoAppKey = $(".oppoAppKey_"+ this_id +"").val();
        var oppoMasterSecret = $(".oppoMasterSecret_"+ this_id +"").val();

        /*推送配置*/
        var betaApnsPk = $(".betaApnsPk_"+ this_id +"").val();
        var appStoreAppId = $(".appStoreAppId_"+ this_id +"").val();
        var appStoreApnsPk = $(".appStoreApnsPk_"+ this_id +"").val();
        var voipPk = $(".voipPk_"+ this_id +"").val();
        var pkPassword = $(".pkPassword_"+ this_id +"").val();
        var isApnsSandbox = $(".isApnsSandbox_"+ this_id +"").val();
        var isDebug = $(".isDebug_"+ this_id +"").val();
        var betaAppId = $(".betaAppId_"+ this_id +"").val();


        Common.invoke({
            url : request('/console/add/pushconfigmodel'),
            data : {
                id:this_id,
                serverAdress:Common.isNull(serverAdress),
                packageName:Common.isNull(packName),
                xmAppSecret:Common.isNull(xmAppSecret),
                hwAppSecret:Common.isNull(hwAppSecret),
                hwAppId:Common.isNull(hwAppId),
                hwTokenUrl:Common.isNull(hwTokenUrl),
                hwApiUrl:Common.isNull(hwApiUrl),
                hwIconUrl:Common.isNull(hwIconUrl),
                isOpen:Common.isNumberNullOne(isOpen),
                bdAppStoreAppId:Common.isNull(bdAppStoreAppId),
                bdAppStoreAppKey:Common.isNull(bdAppStoreAppKey),
                bdAppStoreSecretKey:Common.isNull(bdAppStoreSecretKey),
                bdAppKey:Common.isNull(bdAppKey),
                bdRestUrl:Common.isNull(bdRestUrl),
                bdSecretKey:Common.isNull(bdSecretKey),
                jPushAppKey:Common.isNull(jPushAppKey),
                jPushMasterSecret:Common.isNull(jPushMasterSecret),
                fcmDataBaseUrl:Common.isNull(fcmDataBaseUrl),
                fcmKeyJson:Common.isNull(fcmKeyJson),
                mzAppSecret:Common.isNull(mzAppSecret),
                mzAppId:Common.isNumberNullTwo(mzAppId),
                vivoAppId:Common.isNumberNullTwo(vivoAppId),
                vivoAppKey:Common.isNull(vivoAppKey),
                vivoAppSecret:Common.isNull(vivoAppSecret),
                oppoAppKey:Common.isNull(oppoAppKey),
                oppoMasterSecret:Common.isNull(oppoMasterSecret),
                betaApnsPk:Common.isNull(betaApnsPk),
                appStoreAppId:Common.isNull(appStoreAppId),
                appStoreApnsPk:Common.isNull(appStoreApnsPk),
                voipPk:Common.isNull(voipPk),
                pkPassword:Common.isNull(pkPassword),
                isApnsSandbox:Common.isNumberNullOne(isApnsSandbox),
                isDebug:Common.isNumberNullOne(isDebug),
                betaAppId:Common.isNull(betaAppId)
            },
            successMsg :'应用配置更新成功',
            errorMsg : '应用配置更新失败,请检查网络',
            success : function(result) {
            },
            error : function(result) {
            }
        });

        return false;
    })
});

var info = {
    //查询
    getPushConfigModelList:function () {
        Common.invoke({
            url:request('/console/get/pushconfigmodel/list'),
            data:{},
            success:function(result){
                if(result.resultCode==1){
                    if (result.data.length < 1){
                        info.addTab();
                        layui.element.init();
                        layui.form.render();
                    }else{
                        for (var i = 0; i < result.data.length; i++) {
                            this_id = result.data[i].id;
                            layui.element.tabAdd('demo', {
                                title: '推送配置<i class="layui-icon layui-unselect layui-tab-close">&#x1006;</i>'
                                , content: jointHtmlCode()
                                , id: this_id
                            });
                            //增加点击关闭事件
                            var r = $(".layui-tab-title").find("li");
                            //每次新打开tab都是最后一个，所以只对最后一个tab添加点击关闭事件
                            r.eq(r.length - 1).children("i").on("click", function () {

                                var attr = $(this).parent("li").attr('lay-id');
                                layer.open({
                                    content: '确定要删除推送配置吗 ?'
                                    ,btn: ['确定', '取消']
                                    ,yes: function(index, layero){
                                        //确定的回调
                                        info.deletePushConfig();
                                        layui.element.tabDelete("demo", attr);
                                        layui.element.tabChange("demo", r.length - 1);
                                        layer.close(index);
                                    }
                                    ,btn2: function(index, layero){
                                        //取消回调
                                        layer.close(index);
                                        return;// 开启该代码可禁止点击该按钮关闭
                                    }
                                    ,cancel: function(index){
                                        //右上角关闭回调
                                        layer.close(index);
                                        return; //开启该代码可禁止点击该按钮关闭
                                    }
                                });
                            });
                            layui.element.init();
                            layui.form.render();
                        }
                        info.showPushConfig();
                        layui.element.tabChange('demo', this_id);
                        layui.form.render();
                    }
                }
            }
        });
    }

    //新增推送配置
    ,addPushConfigModel :function () {
        var id ;
        Common.invoke({
            url:request('/console/add/pushconfigmodel'),
            data:{},
            async:false,
            success:function(result){
                if(result.resultCode==1){
                    id = result.data.id;
                }
            }
        });
        return id;
    }

    //添加新推送配置
    ,addTab:function () {
        var id = info.addPushConfigModel();
        this_id = id;
        //新增一个Tab项
        layui.element.tabAdd('demo', {
            title: '推送配置<i class="layui-icon layui-unselect layui-tab-close">&#x1006;</i>' //用于演示
            , content: jointHtmlCode()
            , id: id //实际使用一般是规定好的id，这里以时间戳模拟下
        });

        //切换选项卡位置
        layui.element.tabChange('demo', id);

        //增加点击关闭事件
        var r = $(".layui-tab-title").find("li");
        //每次新打开tab都是最后一个，所以只对最后一个tab添加点击关闭事件
        r.eq(r.length - 1).children("i").on("click", function () {
            var attr = $(this).parent("li").attr('lay-id');
            layer.open({
                content: '确定要删除推送配置吗 ?'
                ,btn: ['确定', '取消']
                ,yes: function(index, layero){
                    //确定的回调
                    info.deletePushConfig();
                    layui.element.tabDelete("demo", attr);
                    layui.element.tabChange("demo", r.length - 1);
                    layer.close(index);
                }
                ,btn2: function(index, layero){
                    //取消回调
                    layer.close(index);
                    return;// 开启该代码可禁止点击该按钮关闭
                }
                ,cancel: function(){
                    //右上角关闭回调
                    layer.close(index);
                    return; //开启该代码可禁止点击该按钮关闭
                }
            });
        });
        layui.element.init();
        layui.form.render();
    }

    //获取选项卡数据  并展现
    ,showPushConfig:function () {
        Common.invoke({
            url:request('/console/get/pushconfigmodel/detail'),
            data:{
                id:this_id
            },
            async:false,
            success:function(result){
                if(result.resultCode==1){
                    if (result.data.androidPush == undefined &&  result.data.iosPush == undefined){
                        return;
                    }else{
                        $(".packName_"+ this_id +"").val(Common.isNull(result.data.packageName));
                        $(".betaAppId_"+ this_id +"").val(Common.isNull(result.data.betaAppId));
                        $(".appStoreAppId_"+ this_id +"").val(Common.isNull(result.data.appStoreAppId));

                        $(".xmAppSecret_"+ this_id +"").val(Common.isNull(result.data.androidPush.xmAppSecret));
                        $(".hwAppSecret_"+ this_id +"").val(Common.isNull(result.data.androidPush.hwAppSecret));
                        $(".hwAppId_"+ this_id +"").val(Common.isNull(result.data.androidPush.hwAppId));
                        $(".hwTokenUrl_"+ this_id +"").val(Common.isNull(result.data.androidPush.hwTokenUrl));
                        $(".hwApiUrl_"+ this_id +"").val(Common.isNull(result.data.androidPush.hwApiUrl));
                        $(".hwIconUrl_"+ this_id +"").val(Common.isNull(result.data.androidPush.hwIconUrl));
                        $(".isOpen_"+ this_id +"").val(Common.isNumberNullOne(result.data.androidPush.isOpen));
                        $(".jPushAppKey_"+ this_id +"").val(Common.isNull(result.data.androidPush.jPushAppKey));
                        $(".jPushMasterSecret_"+ this_id +"").val(Common.isNull(result.data.androidPush.jPushMasterSecret));
                        $(".fcmDataBaseUrl_"+ this_id +"").val(Common.isNull(result.data.androidPush.fcmDataBaseUrl));
                        $(".fcmKeyJson_"+ this_id +"").val(Common.isNull(result.data.androidPush.fcmKeyJson));
                        $(".mzAppSecret_"+ this_id +"").val(Common.isNull(result.data.androidPush.mzAppSecret));
                        $(".mzAppId_"+ this_id +"").val(result.data.androidPush.mzAppId  == -1 ? "" : result.data.androidPush.mzAppId);
                        $(".vivoAppId_"+ this_id +"").val(result.data.androidPush.vivoAppId  == -1 ? "" : result.data.androidPush.vivoAppId);
                        $(".vivoAppKey_"+ this_id +"").val(Common.isNull(result.data.androidPush.vivoAppKey));
                        $(".vivoAppSecret_"+ this_id +"").val(Common.isNull(result.data.androidPush.vivoAppSecret));
                        $(".oppoAppKey_"+ this_id +"").val(Common.isNull(result.data.androidPush.oppoAppKey));
                        $(".oppoMasterSecret_"+ this_id +"").val(Common.isNull(result.data.androidPush.oppoMasterSecret));

                        $(".betaApnsPk_"+ this_id +"").val(Common.isNull(result.data.iosPush.betaApnsPk));
                        $(".appStoreApnsPk_"+ this_id +"").val(Common.isNull(result.data.iosPush.appStoreApnsPk));
                        $(".pkPassword_"+ this_id +"").val(Common.isNull(result.data.iosPush.pkPassword));
                        $(".isApnsSandbox_"+ this_id +"").val(Common.isNumberNullOne(result.data.iosPush.isApnsSandbox));
                        $(".isDebug_"+ this_id +"").val(Common.isNumberNullOne(result.data.iosPush.isDebug));
                        layui.form.render();
                    }
                }
            }
        });
    }

    //删除推送配置
    ,deletePushConfig:function () {
        Common.invoke({
            url:request('/console/delete/pushconfigmodel'),
            data:{
                id:this_id
            },
            async:false,
            success:function(result){
                if(result.data){
                    layer.msg('删除成功 !',{"icon":1});
                }else{
                    layer.msg('删除失败，请稍后重试 !',{"icon":1});
                }
            }
        });
    }
}


//拼接表单代码
function jointHtmlCode() {
    var html = '<div style="width: 100%;">' +
        '<form class="layui-form" style="margin-top: 5px;">' +
        '<table class="layui-table">' +
        '<thead>' +
        '<tr class="sava-btn-float">' +
        '<th style=" border-bottom: 0px;">' +
        '<a class="layui-btn site-demo-active savaBtn motif_button_style" data-type="tabAdd" onclick="info.addTab()">新增推送配置</a>' +
        '<button class="layui-btn save sys-sava pushConfg-sava sava-btn-place motif_button_style" lay-submit="" lay-filter="systemConfig">保存</button>' +
        '</th>' +
        '</tr>' +
        '<tr>' +
        '<th style=" border-bottom: 0px;">' +
        '<b class="pushTitle">消息推送相关配置</b>' +
        '</th>' +
        '</tr>' +
        '</thead>' +
        '</table>' +
        '<table class="layui-table mag0 table-top-interval">' +
        '<thead>' +
        '<tr>' +
        '<th>参数说明</th>' +
        '<th>参数值</th>' +
        '<th>变量名</th>' +
        '<th>参数详细说明</th>' +
        '</tr>' +
        '</thead>' +
        '<tbody>' +
        '<!-- 消息推送相关配置 -->' +
        '<tr>' +
        '<td>编号</td>' +
        '<td><input type="" name="" class="layui-input PushConfigModel_'+ this_id +'"  placeholder="" readonly="readonly" value="'+ this_id +'"></td>' +
        '<td>id</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>安卓包名</td>' +
        '<td><input type="" name="" class="layui-input packName_'+ this_id +'"  placeholder=""></td>' +
        '<td>packName</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>appStore 版本App包名(IOS个人版)</td>' +
        '<td><input type="" name="" class="layui-input appStoreAppId_'+ this_id +'"  placeholder=""></td>' +
        '<td>appStoreAppId</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>企业版 app 包名(IOS企业版)</td>' +
        '<td><input type="" name="" class="layui-input betaAppId_'+ this_id +'"  placeholder=""></td>' +
        '<td>betaAppId</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td colspan="4" data-locale="push_config">' +
        '<b class="pushTitle">小米推送配置</b>' +
        '</td>' +
        '</tr>' +
        '<tr>' +
        '<td>请输入xm_appSecret !</td>' +
        '<td><input type="" name="" class="layui-input xmAppSecret_'+ this_id +'"  placeholder=""></td>' +
        '<td>xmAppSecret</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td colspan="4" data-locale="push_config">' +
        '<b class="pushTitle">华为推送配置</b>' +
        '</td>' +
        '</tr>' +
        '<tr>' +
        '<td>华为 AppSecret</td>' +
        '<td><input type="" name="" class="layui-input hwAppSecret_'+ this_id +'"  placeholder=""></td>' +
        '<td>hwAppSecret</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>华为 AppId</td>' +
        '<td><input type="" name="" class="layui-input hwAppId_'+ this_id +'"  placeholder=""></td>' +
        '<td>hwAppId</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>请输入hw_tokenUrl !</td>' +
        '<td><input type="" name="" class="layui-input hwTokenUrl_'+ this_id +'"  placeholder=""></td>' +
        '<td>hwTokenUrl</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>华为推送请求地址</td>' +
        '<td><input type="" name="" class="layui-input hwApiUrl_'+ this_id +'"  placeholder=""></td>' +
        '<td>hwApiUrl</td>' +
        '<td>华为推送服务的api（请求）地址</td>' +
        '</tr>' +
        '<tr>' +
        '<td>华为推送图标的URL</td>' +
        '<td><input type="" name="" class="layui-input hwIconUrl_'+ this_id +'"  placeholder=""></td>' +
        '<td>hwIconUrl</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>是否打开</td>' +
        '<td>' +
        '<select class="layui-input-inline isOpen_'+ this_id +'">' +
        '<option value="0">否</option>' +
        '<option value="1">是</option>' +
        '</select>' +
        '</td>' +
        '<td>isOpen</td>' +
        '<td></td>' +
        '</tr>' +
        '' +
        '<!-- 极光推送配置 -->' +
        '<tr>' +
        '<td colspan="4" data-locale="push_config">' +
        '<b class="pushTitle">极光推送配置</b>' +
        '</td>' +
        '</tr>' +
        '<tr>' +
        '<td>极光 AppKey</td>' +
        '<td><input type="" name="" class="layui-input jPushAppKey_'+ this_id +'"  placeholder=""></td>' +
        '<td>jPushAppKey</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>极光 MasterSecret</td>' +
        '<td><input type="" name="" class="layui-input jPushMasterSecret_'+ this_id +'"  placeholder=""></td>' +
        '<td>jPushMasterSecret</td>' +
        '<td></td>' +
        '</tr>' +
        '' +
        '<!-- google FCM推送配置 -->' +
        '<tr>' +
        '<td colspan="4" data-locale="push_config">' +
        '<b class="pushTitle">google FCM推送配置</b>' +
        '</td>' +
        '</tr>' +
        '<tr>' +
        '<td>FCM_dataBaseUrl</td>' +
        '<td><input type="" name="" class="layui-input fcmDataBaseUrl_'+ this_id +'"  placeholder=""></td>' +
        '<td>fcmDataBaseUrl</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>FCM_keyJson</td>' +
        '<td><input type="" name="" class="layui-input fcmKeyJson_'+ this_id +'"  placeholder=""></td>' +
        '<td>fcmKeyJson</td>' +
        '<td></td>' +
        '</tr>' +
        '' +
        '<!-- 魅族推送配置 -->' +
        '<tr>' +
        '<td colspan="4" data-locale="push_config">' +
        '<b class="pushTitle">魅族推送配置</b>' +
        '</td>' +
        '</tr>' +
        '<tr>' +
        '<td>魅族 AppSecret</td>' +
        '<td><input type="number" name="" class="layui-input mzAppId_'+ this_id +'"  placeholder=""></td>' +
        '<td>mzAppId</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>魅族 AppId</td>' +
        '<td><input type="" name="" class="layui-input mzAppSecret_'+ this_id +'"  placeholder=""></td>' +
        '<td>mzAppSecret</td>' +
        '<td></td>' +
        '</tr>' +
        '' +
        '<!-- VIVO推送配置 -->' +
        '<tr>' +
        '<td colspan="4" data-locale="push_config">' +
        '<b class="pushTitle">VIVO推送配置</b>' +
        '</td>' +
        '</tr>' +
        '<tr>' +
        '<td>VIVO AppId</td>' +
        '<td><input type="number" name="" class="layui-input vivoAppId_'+ this_id +'"  placeholder=""></td>' +
        '<td>vivoAppId</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>VIVO AppKey</td>' +
        '<td><input type="" name="" class="layui-input vivoAppKey_'+ this_id +'"  placeholder=""></td>' +
        '<td>vivoAppKey</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>VIVO AppSecret</td>' +
        '<td><input type="" name="" class="layui-input vivoAppSecret_'+ this_id +'"  placeholder=""></td>' +
        '<td>vivoAppSecret</td>' +
        '<td></td>' +
        '</tr>' +
        '' +
        '<!-- OPPO推送配置 -->' +
        '<tr>' +
        '<td colspan="4" data-locale="push_config">' +
        '<b class="pushTitle">OPPO推送配置</b>' +
        '</td>' +
        '</tr>' +
        '<tr>' +
        '<td>OPPO AppKey</td>' +
        '<td><input type="" name="" class="layui-input oppoAppKey_'+ this_id +'"  placeholder=""></td>' +
        '<td>oppoAppKey</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>OPPO MasterSecret</td>' +
        '<td><input type="" name="" class="layui-input oppoMasterSecret_'+ this_id +'"  placeholder=""></td>' +
        '<td>oppoMasterSecret</td>' +
        '<td></td>' +
        '</tr>' +
        '' +
        '<!-- IOS推送配置 -->' +
        '<tr>' +
        '<td colspan="4" data-locale="push_config">' +
        '<b class="pushTitle">IOS推送配置</b>' +
        '</td>' +
        '</tr>' +
        '<tr>' +
        '<td>企业版 测试版 apns 推送证书</td>' +
        '<td><input type="" name="" class="layui-input betaApnsPk_'+ this_id +'"  placeholder=""></td>' +
        '<td>betaApnsPk</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>appStore apns 推送证书</td>' +
        '<td><input type="" name="" class="layui-input appStoreApnsPk_'+ this_id +'"  placeholder=""></td>' +
        '<td>appStoreApnsPk</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>voip 证书 密码</td>' +
        '<td><input type="" name="" class="layui-input pkPassword_'+ this_id +'"  placeholder=""></td>' +
        '<td>pkPassword</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>isApnsSandbox</td>' +
        '<td>' +
        '<select class="layui-input-inline isApnsSandbox_'+ this_id +'">' +
        '<option value="0">否</option>' +
        '<option value="1">是</option>' +
        '</select>' +
        '</td>' +
        '<td>isApnsSandbox</td>' +
        '<td></td>' +
        '</tr>' +
        '<tr>' +
        '<td>调试模式  打印 log</td>' +
        '<td>' +
        '<select class="layui-input-inline isDebug_'+ this_id +'">' +
        '<option value="0">否</option>' +
        '<option value="1">是</option>' +
        '</select>' +
        '</td>' +
        '<td>isDebug</td>' +
        '<td></td>' +
        '</tr>' +
        '</tbody>' +
        '</table>' +
        '</form>' +
        '</div>';
    return html;
}
$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})