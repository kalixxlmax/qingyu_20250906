var page = 0;
var sum = 0;
var lock = 0;
var updateId = "";// 修改音乐的Id
var uploadCoverFile = null;
layui.use(['form', 'layer', 'laydate', 'table', 'laytpl'], function () {
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;

    //短视频音乐列表
    var tableIns = table.render({
        elem: '#music_table'
        , url: request("/console/fxList")
        , toolbar: '#toolbar'
        , id: 'music_table'
        , page: true
        , curr: 0
        , limit: Common.limit
        , limits: Common.limits
        , groups: 7
        , cols: [[ //表头
            {
                field: 'icon', title: '图标', sort: true, width: '20%', templet: function (d) {
                    return "<img src='" + d.icon + "' width='40px' height='40px'>";
                }
            }
            , {field: 'name', title: '名称', sort: true, width: '20%'}
            , {field: 'url', title: '跳转地址', sort: true, width: '20%'}
            , {field: 'sort', title: '排序', sort: true, width: '5%'}
            , {fixed: 'right', title: "操作", align: 'left', width: '20%', toolbar: '#musicListBar'}
        ]]
        , done: function (res, curr, count) {
            checkRequst(res);
            //权限判断
            var arr = ['music-delete', 'music-update', 'music-add'];
            manage.authButton(arr);

            //获取当前每页大小
            var recodeLimit = $(".layui-laypage-limits").find("option:selected").val();
            if (undefined != recodeLimit) {
                //获取零时保留的值
                var last_value = $("#music_limlt").val();
                //设置零时保留的值
                $("#music_limlt").val(recodeLimit);
                //判断是否改变了每页大小数
                if (last_value != recodeLimit) {
                    // 刷新
                    table.reload("music_table", {
                        url: request("/console/fxList"),
                        page: {
                            curr: 1 //重新从第 1 页开始
                        }
                    })
                }
            }


            if (count == 0 && lock == 1) {
                // layui.layer.alert("暂无数据",{yes:function(){
                //   renderTable();
                //   layui.layer.closeAll();
                // }});
                layer.msg("暂无数据", {"icon": 2});
                renderTable();
            }
            lock = 0;


            /*if(localStorage.getItem("role")==1 || localStorage.getItem("role")==4 || localStorage.getItem("role")==7){
                $(".btn_addLive").hide();
            }*/
        }
    });


    //列表操作
    table.on('tool(music_table)', function (obj) {
        var layEvent = obj.event,
            data = obj.data;
        console.log(data);
        if (layEvent === 'delete') {// 删除短视频音乐

            Music.deleteMusic(data.id);
        } else if (layEvent === 'update') {// 更新短视频音乐
            Music.updateMusic(data);
        }
    });

    //搜索
    $(".search_live").on("click", function () {
        if ($("#musicName").val().indexOf("*") != -1) {
            layer.alert("不支持*号搜索")
            return;
        }
        table.reload("music_table", {
            url: request("/console/fxList"),
            where: {
                keyword: Common.getValueForElement("#musicName")  //搜索的关键字
            },
            page: {
                curr: 1 //重新从第 1 页开始
            }
        })
        lock = 1;
        $("#musicName").val("");
    });
});

//重新渲染表单
function renderTable() {
    layui.use('table', function () {
        var table = layui.table;//高版本建议把括号去掉，有的低版本，需要加()
        // table.reload("user_list");
        table.reload("music_table", {
            where: {
                keyword: Common.getValueForElement("#musicName")  //搜索的关键字
            },
            page: {
                curr: 1 //重新从第 1 页开始
            }
        })
    });
}

var Music = {
    addMusic: function () {
        $("#music_div").hide();
        $("#addMusic").show();
        $("#musicName_add").val("");
        $("#musicNickName").val("");
        $("#musicCover").val("");
        $("#musicPath").val("");
        $("#uploadMusic").val("");
        $("#uploadCover").val("");
        $(".visitPathDiv").hide();
        $("#uploadMusicCover").attr("action", Config.getConfig().uploadUrl + "/upload/UploadMusicServlet");
        $("#uploadMusicPath").attr("action", Config.getConfig().uploadUrl + "/upload/UploadMusicServlet");
        $("#uploadMusicPath_update").attr("action", Config.getConfig().uploadUrl + "/upload/UploadMusicServlet");
        $("#uploadMusicCover_update").attr("action", Config.getConfig().uploadUrl + "/upload/UploadMusicServlet");
    },
    selectCover: function () {
        $("#uploadCover").click();
    },
    selectMusic: function () {
        $("#uploadMusic").click();
    },
    commit_addMusic: function () {
        if (Common.isNil($("#name").val())) {
            layer.msg("请输入名称", {"icon": 2});
            return;
        } else if (Common.isNil($("#url").val())) {
            layer.msg("请填跳转路径", {"icon": 2});
            return;
        } else if (Common.isNil($("#sort").val())) {
            layer.msg("请填排序", {"icon": 2});
            return;
        } else if (Common.isNil($("#musicPath").html())) {
            layer.msg("请上传图标", {"icon": 2});
            return;
        }
        Common.invoke({
            url: request('/console/addFx'),
            data: {
                name: Common.filterHtmlData($("#name").val()),
                icon: Common.filterHtmlData($("#musicPath").html()),
                url: Common.filterHtmlData($("#url").val()),
                sort: Common.filterHtmlData($("#sort").val())
            },
            success: function (result) {
                if (result.resultCode == 1) {
                    $("#name").val("");
                    $("#url").val("");
                    $("#musicPath").html("");
                    $("#uploadMusic").val("");
                    $("#sort").val("");
                    uploadCoverFile = null;
                    $("#music_div").show();
                    $("#addMusic").hide();
                    layui.layer.alert("新增成功");
                    layui.table.reload("music_table");
                }
            }

        })
    },
    // 删除音乐
    deleteMusic: function (id) {
        layer.confirm('确定删除？', {icon: 3, title: '提示信息', skin: "layui-ext-motif"}, function (index) {
            Common.invoke({
                url: request('/console/deleteFx'),
                data: {
                    musicInfoId: id
                },
                success: function (result) {
                    if (result.resultCode == 1) {
                        layui.layer.alert("删除成功");
                        layui.table.reload("music_table");
                    }
                }
            })
        })

    },
    // 修改音乐
    updateMusic: function (data) {
        $("#musicList").hide();
        $("#updateMusic").show();
        $(".searchMusic").hide();
        $(".visitPathDiv").hide();
        $("#name_update").val(Common.filterHtmlData(data.name));
        $("#sort_update").val(Common.filterHtmlData(data.sort));
        $("#url_update").val(Common.filterHtmlData(data.url));
        $("#musicPath_update").html(Common.filterHtmlData(data.icon));

        $("#uploadMusicPath").attr("action", Config.uploadMusicUrl);
        $("#uploadMusicPath_update").attr("action", Config.uploadMusicUrl);
        updateId = data.id;
    },
    // 提交修改音乐
    commit_updateMusic: function () {
        Common.invoke({
            url: request('/console/updateFx'),
            data: {
                id: updateId,
                url: Common.filterHtmlData($("#url_update").val()),
                sort: Common.filterHtmlData($("#sort_update").val()),
                name: Common.filterHtmlData($("#name_update").val()),
                icon: Common.filterHtmlData($("#musicPath_update").html())
            },
            success: function (result) {
                if (result.resultCode == 1) {
                    layer.msg("修改成功", {icon: 1});
                    $("#musicList").show();
                    $("#updateMusic").hide();
                    layui.table.reload("music_table");
                    Music.btn_back();
                }
            }
        });
    },
    btn_back: function () {
        $("#music_div").show();
        $("#musicList").show();
        $("#addMusic").hide();
        $("#updateMusic").hide();
        $(".searchMusic").show();
        $(".visitPathDiv").show();
    }
}
$(function () {
    //调用父级页面的Js函数
    window.parent.getJointVisitPath();
})