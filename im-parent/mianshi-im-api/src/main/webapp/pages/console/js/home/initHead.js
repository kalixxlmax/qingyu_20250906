/*
* 初始化头顶部导航栏
*
* */
//图片前缀
var imgPrefix = "/pages/console/images/menu/";
//当前首选项图片地址
var thisMenuUrl = "/pages/console/images/menu/index_img_light.png";
var failPage = "/pages/console/visitFail.html";
var initHead_operation = {
    //读取本地文件 navs_head.json
    readTextFile:function (file, callback) {
        var rawFile = new XMLHttpRequest();
        rawFile.overrideMimeType("application/json");
        rawFile.open("GET", file, true);
        rawFile.onreadystatechange = function() {
            if (rawFile.readyState === 4 && rawFile.status == "200") {
                callback(rawFile.responseText);
            }
        }
        rawFile.send(null);
    },
    //加载导航栏顶部
    initNavigationBarTop:function () {
        initHead_operation.readTextFile("/pages/console/json/navs_head.json", function(text){
            var navsHeadData = JSON.parse(text);
            initHead_operation.jointNavigationBarTop(navsHeadData);
        });
    }
    //拼接导航栏
    ,jointNavigationBarTop:function (jsonData) {
        var htmlContent = "";
        for(var i=0;i<jsonData.length;i++){
            //非超级管理员 并且 没有权限
            if (localStorage.getItem("account") != 1000 && !manage.ifUserAuth(jsonData[i].resourceAuth)){
                continue;
            }

            if  (localStorage.getItem("account") == 1000 || manage.ifUserAuth(jsonData[i].resourceAuthChild)){
                var menu_title_number = "menu_title" + i;
                htmlContent += '<div class="swiper-slide '+ (jsonData[i].isSelect == true ? "selected" : "") +'" onclick="transformMenu(\''+ jsonData[i].title +'\',\''+ jsonData[i].groupName +'\',\''+ jsonData[i].href +'\',\''+ jsonData[i].visitPath +'\',\''+ jsonData[i].defaultId +'\',\''+ menu_title_number +'\'),initHead_operation.savaThisMenuUrl(\''+ jsonData[i].imgUrl +'\')">' +
                    '<img src="'+ jsonData[i].imgUrl +'" class="menu_photo">' +
                    '<p class="menu_title '+ menu_title_number+'">'+ jsonData[i].title +'</p>' +
                    '</div>';
            }else{
                var menu_title_number = "menu_title" + i;
                htmlContent += '<div class="swiper-slide '+ (jsonData[i].isSelect == true ? "selected" : "") +'" onclick="transformMenu(\''+ jsonData[i].title +'\',\''+ jsonData[i].groupName +'\',\''+ failPage +'\',\''+ jsonData[i].visitPath +'\',\''+ jsonData[i].defaultId +'\',\''+ menu_title_number +'\'),initHead_operation.savaThisMenuUrl(\''+ jsonData[i].imgUrl +'\')">' +
                    '<img src="'+ jsonData[i].imgUrl +'" class="menu_photo">' +
                    '<p class="menu_title '+ menu_title_number+'">'+ jsonData[i].title +'</p>' +
                    '</div>';
            }

        }
        $("#navHeadTop").empty();
        $("#navHeadTop").append(htmlContent);

        //初始化滑动
        var swiper1 = new Swiper('.swiper1', {
            //	设置slider容器能够同时显示的slides数量(carousel模式)。
            //	可以设置为number或者 'auto'则自动根据slides的宽度来设定数量。
            //	loop模式下如果设置为'auto'还需要设置另外一个参数loopedSlides。
            slidesPerView: 10,
            paginationClickable: true,//此参数设置为true时，点击分页器的指示点分页器会控制Swiper切换。
            spaceBetween: 10,//slide之间的距离（单位px）。
            freeMode: true,//默认为false，普通模式：slide滑动时只滑动一格，并自动贴合wrapper，设置为true则变为free模式，slide会根据惯性滑动且不会贴合。
            loop: false,//是否可循环
            onTab: function(swiper) {
                var n = swiper1.clickedIndex;
            },
        });
        swiper1.slides.each(function(index, val) {
            var ele = $(this);
            ele.on("click", function() {
                initHead_operation.setCurrentSlide(ele, index, val);
            });
        });

        //切换回首页
        $("#homePage_").click(function () {
            var homePageUrl = "/pages/console/system_report.html";
            var url =initHead_operation.getThisMenuUrl();
            console.log(url == homePageUrl);
            if (url != "" && url!=homePageUrl){
                $("#iframe_body").empty();
                $("#iframe_body").append('<iframe id="iframe" src="/pages/console/system_report.html"></iframe>')
            }
            $("#content_body").css("left","0px");
            $("#left_menu").hide();
        })
    }
    //设置首选项
    ,setCurrentSlide:function (ele, index, val) {
        var flag = ele.context.classList.contains("selected");
        initHead_operation.clearSelected();
        $(".swiper1 .swiper-slide").removeClass("selected");
        ele.addClass("selected");
        if (!flag){
            //更换图片
            var cutPicturnName =  initHead_operation.cutPicturnName( $(ele[0].childNodes).attr("src"));
            $(ele[0].childNodes).attr("src",imgPrefix+ cutPicturnName);
        }
    }
    /**
     * 截取图片名称
     * type 截图类型
     */
    ,cutPicturnName:function (imgName,type) {
        if (Common.isNil(type)){
            type = 0;
        }
        var names = imgName.split("/");
        var length = names.length;
        var name = names[length-1];
        //前缀
        var suffix_img = "."+name.split(".")[1];
        //后缀
        var prefix_img ;
        if (name.includes("light")){
            if (type == 0){
                var imgName = name.split(".")[0];
                prefix_img = imgName.slice(0,imgName.length-6);
            }else{
                prefix_img =  name.split(".")[0];
            }
        }else{
            prefix_img =  name.split(".")[0] + "_light";
        }
        return prefix_img + suffix_img;
    }
    /**
     *  去掉首选项样式图片
     */
    ,clearSelected:function () {
        //获取所有子节点
        var childNodes = $("#navHeadTop")[0].childNodes;
        for (var i = 0; i < childNodes.length ; i++) {
            //找出 selected 的导航
            var flag = childNodes[i].classList.contains("selected");
            if (flag){
                //更换图片
                var childNodes_ = childNodes[i].childNodes;
                var imgUrl = $(childNodes_[0]).attr("src");
                var newImgUrl = initHead_operation.cutPicturnName(imgUrl);
                //是否需要切换首选项图片
                if (imgUrl != thisMenuUrl){
                    $(childNodes_[0]).attr("src",imgPrefix+newImgUrl);
                }
            }
        }
    }
    ,setThisMenuUrl:function (data) {
        thisMenuUrl = data;
    }
    ,getThisMenuUrl:function () {
        return thisMenuUrl;
    }
    //保存当首选项图片路径
    ,savaThisMenuUrl:function (url) {
        //不管当前首选项是否选中  ，  都将此路径变为当前首选项图片（light路径）路径
        var newImgUrl = initHead_operation.cutPicturnName(url,1);
        thisMenuUrl = imgPrefix + newImgUrl;
    }
}

$(function () {
    initHead_operation.initNavigationBarTop();
})