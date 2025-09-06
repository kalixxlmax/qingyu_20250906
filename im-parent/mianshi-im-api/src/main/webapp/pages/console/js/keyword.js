var page = 0;
var sum = 0;
// var button='<button onclick="Key.keyword_list(0)" class="layui-btn layui-btn-primary layui-btn-sm" style="margin-top: 21%;margin-left: 20%;"><<返回</button>';
$(function () {
    Key.keyword_list();
    Key.limit();

    //权限判断
    var arr = ['keyword-add', 'keyword-delete'];
    manage.authButton(arr);
})

layui.use(['form', 'layer', 'laydate', 'table', 'laytpl'], function () {
    var form = layui.form,
        layer = parent.layer === undefined ? layui.layer : top.layer,
        $ = layui.jquery,
        laydate = layui.laydate,
        laytpl = layui.laytpl,
        table = layui.table;


    //监听新增关敏感词提交
    form.on('submit(addKeyword)', function (data) {
        //console.log(data.elem) //被执行事件的元素DOM对象，一般为button对象
        //console.log(data.form) //被执行提交的form对象，一般在存在form标签时才会返回
        //console.log("=============>>>>"+data.field) //当前容器的全部表单字段，名值对形式：{name: value}

        // 提交新增敏感词
        if (Common.isNil(data.field.keywordValue)) {
            layer.msg("敏感词不能为空!", {"icon": 2});
            return;
        }
        $.ajax({
            type: 'POST',
            url: request('/console/addkeyword'),
            data: {
                words: data.field.keywordValue,
                type: data.field.keyword_type
            },
            async: false,
            success: function (result) {
                checkRequst(result);
                if (result.resultCode == 1) {
                    Key.keyword_list();
                    Key.limit();
                    $("#keyWordList").show();
                    $("#addKeyWord").hide();
                    $("#addKeyValue").val("");
                    layer.msg(result.data,{"icon":1,time: 3000});
                }
            }
        })

        return false; //阻止表单跳转。如果需要表单跳转，去掉这段即可。
    });
});

var Key = {
    // 敏感词列表
    keyword_list: function (e, pageSize) {
        html = "";
        if (e == undefined) {
            e = 0;
        } else if (pageSize == undefined) {
            pageSize = Common.limit;
        }
        Common.invoke({
            url: request('/console/keywordfilter'),
            data: {
                pageIndex: (e == 0 ? "0" : e - 1),
                pageSize: pageSize,
                word: $("#keyName").val()
            },
            dataType: 'json',
            async: false,
            success: function (result) {
                console.log("...", result);
                checkRequst(result);
                if (result.data.pageData.length != 0) {
                    $("#pageCount").val(Common.filterHtmlData(result.data.total));
                    for (var i = 0; i < result.data.pageData.length; i++) {
                        html += "<tr id='" + result.data.pageData[i].id + "'>"
                            + "<td>" + Common.filterHtmlData(result.data.pageData[i].word) + "</td>"
                            + "<td>" + (result.data.pageData[i].type == 1 ? '否词' : '普通敏感词') + "</td>"
                            + "<td>" + UI.getLocalTime(result.data.pageData[i].createTime) + "</td>"
                            + "<td>"
                            + "<button onclick='Key.deleteKeyWord(\"" + result.data.pageData[i].id + "\")' class='layui-btn layui-btn-danger layui-btn-xs delete table_default_btn'>删除</button>"
                            + "</td>"
                            + "</tr>";
                    }
                    if ($("#keyName").val() == "" || $("#keyName").val() == undefined) {
                        $("#keywordList_table").empty();
                        $("#keywordList_table").append(html);
                    }
                    $("#keyName").val("");
                    $("#keyWordList").show();
                    $("#addKeyWord").hide();
                    // $("#back").empty();
                    // $("#back").append("&nbsp;");

                }
            }
        })
    },
    // 搜索关键词
    findKeyWord: function () {
        html = "";
        Common.invoke({
            url: request('/console/keywordfilter'),
            data: {
                word: Common.getValueForElement("#keyName")
            },
            success: function (result) {
                if (result.data.pageData != null) {
                    $("#pageCount").val(Common.filterHtmlData(result.data.total));
                    for (var i = 0; i < result.data.pageData.length; i++) {
                        html += "<tr align='left' id='" + result.data.pageData[i].id + "'>"
                            + "<td>" + Common.filterHtmlData(result.data.pageData[i].word) + "</td>"
                            + "<td>" + (result.data.pageData[i].type == 1 ? '否词' : '普通敏感词') + "</td>"
                            + "<td>" + UI.getLocalTime(result.data.pageData[i].createTime) + "</td>"
                            + "<td>"
                            + "<button onclick='Key.deleteKeyWord(\"" + result.data.pageData[i].id + "\")' class='layui-btn layui-btn-danger layui-btn-xs delete'>删除</button>"
                            + "</td>"
                            + "</tr>";
                    }
                    $("#keywordList_table").empty();
                    $("#keywordList_table").append(html);
                    // $("#keyName").val("");
                    Key.limit(1);
                    // $("#back").empty();
                    // $("#back").append("&nbsp;");
                }
            }
        });
    },
    // 新增敏感词
    addKeyWord: function () {
        $("#keyWordList").hide();
        $("#addKeyWord").show();
        $(".visitPathDiv").hide();
        layui.form.render();
    },
    // 删除敏感词
    deleteKeyWord: function (id) {
        layer.confirm('确定删除该敏感词？', {icon: 3, title: '提示信息',skin : "layui-ext-motif"}, function (index) {
            Common.invoke({
                url: request('/console/deletekeyword'),
                data: {
                    id: id
                },
                success: function (result) {
                    if (result.resultCode == 1) {
                        layer.msg("删除成功", {"icon": 1});
                        $("#" + id + "").hide();
                        Key.keyword_list();
                        Key.limit();
                    }
                }
            })
        });

    },
    // 分页
    limit: function (index) {
        layui.use('laypage', function () {
            var laypage = layui.laypage;
            //执行一个laypage实例
            laypage.render({
                elem: 'laypage'
                , count: Common.getValueForElement("#pageCount")
                , limit: Common.limit
                , limits: Common.limits
                , layout: ['count', 'prev', 'page', 'next', 'limit', 'refresh', 'skip']
                , jump: function (obj) {
                    console.log(obj)
                    if (index == 1) {
                        Key.keyword_list(1, obj.limit)
                        index = 0;
                    } else {
                        Key.keyword_list(obj.curr, obj.limit)
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