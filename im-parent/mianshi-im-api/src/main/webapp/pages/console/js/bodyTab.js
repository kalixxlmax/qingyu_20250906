/*
	@Author: hsg
	@Time: 2018-8
	@Tittle: bodyTab
	@Description: 点击对应按钮添加新窗口
*/
var tabFilter, currentMenu = "", liIndex, curNav, delMenu,
	changeRefreshStr = window.sessionStorage.getItem("changeRefresh");
layui.define(["element", "jquery"], function (exports) {
	var element = layui.element,
		$ = layui.$,
		layId,
		Tab = function () {
			this.tabConfig = {
				tabFilter: "bodyTab",  //添加窗口的filter
				url: undefined  //获取菜单json地址
			}
		};
	//生成左侧菜单
	Tab.prototype.navBar = function (strData) {
		//加载菜单数据 并保存至本地
		localStorage.setItem("menu_title", JSON.stringify(strData));
		return "";
	}

	//获取二级菜单数据
	Tab.prototype.render = function () {
		//显示左侧菜单
		var _this = this;
		$(".navBar ul").html('<li class="layui-nav-item layui-this"><a data-url="/pages/console/system_report.html"><cite class="left_menu_title">后台首页</cite></a></li>').append(_this.navBar(dataStr)).height($(window).height() - 210);
		element.init();  //初始化页面元素
		$(window).resize(function () {
			$(".navBar").height($(window).height() - 210);
		})
	}

	//是否点击窗口切换刷新页面
	Tab.prototype.changeRegresh = function (index) {
		if (changeRefreshStr == "true") {
			$(".clildFrame .layui-tab-item").eq(index).find("iframe")[0].contentWindow.location.reload();
		}
	}

	//参数设置
	Tab.prototype.set = function (option) {
		var _this = this;
		$.extend(true, _this.tabConfig, option);
		return _this;
	};

	//通过title获取lay-id
	Tab.prototype.getLayId = function (title) {
		$(".layui-tab-title.top_tab li").each(function () {
			if ($(this).find("cite").text() == title) {
				layId = $(this).attr("lay-id");
			}
		})
		return layId;
	}
	//通过title判断tab是否存在
	Tab.prototype.hasTab = function (title) {
		var tabIndex = -1;
		$(".layui-tab-title.top_tab li").each(function () {
			if ($(this).find("cite").text() == title) {
				tabIndex = 1;
			}
		})
		return tabIndex;
	}

	//右侧内容tab操作
	var tabIdIndex = 0;
	Tab.prototype.tabAdd = function (_this) {
		// if(currentMenu==_this.attr("data-url")){ //判断是否重复打开同一页面
		// 	layer.msg("当前页面已打开，请勿重复操作",{'icon':2});
		// 	return;
		// }

		if (window.sessionStorage.getItem("menu")) {
			menu = JSON.parse(window.sessionStorage.getItem("menu"));
		}
		var that = this;
		var openTabNum = that.tabConfig.openTabNum;
		tabFilter = that.tabConfig.tabFilter;
		if (_this.attr("target") == "_blank") { //新窗口打开
			window.open(_this.attr("data-url"));
		} else if (_this.attr("data-url") != undefined) {
			var pageContent = "<iframe src='" + _this.attr("data-url") + "' data-id='" + tabIdIndex + "'></frame>";
			$("." + tabFilter + " .clildFrame .layui-tab-item").empty().append(pageContent);
			currentMenu = _this.attr("data-url");
		}
	}


	//切换后获取当前窗口的内容
	$("body").on("click", ".top_tab li", function () {
		var curmenu = '';
		var menu = JSON.parse(window.sessionStorage.getItem("menu"));
		if (window.sessionStorage.getItem("menu")) {
			curmenu = menu[$(this).index() - 1];
		}
		if ($(this).index() == 0) {
			window.sessionStorage.setItem("curmenu", '');
		} else {
			window.sessionStorage.setItem("curmenu", JSON.stringify(curmenu));
			if (window.sessionStorage.getItem("curmenu") == "undefined") {
				//如果删除的不是当前选中的tab,则将curmenu设置成当前选中的tab
				if (curNav != JSON.stringify(delMenu)) {
					window.sessionStorage.setItem("curmenu", curNav);
				} else {
					window.sessionStorage.setItem("curmenu", JSON.stringify(menu[liIndex - 1]));
				}
			}
		}
		element.tabChange(tabFilter, $(this).attr("lay-id")).init();
		bodyTab.changeRegresh($(this).index());
		setTimeout(function () {
			bodyTab.tabMove();
		}, 100);
	})

	//删除tab
	$("body").on("click", ".top_tab li i.layui-tab-close", function () {
		//删除tab后重置session中的menu和curmenu
		liIndex = $(this).parent("li").index();
		var menu = JSON.parse(window.sessionStorage.getItem("menu"));
		if (menu != null) {
			//获取被删除元素
			delMenu = menu[liIndex - 1];
			var curmenu = window.sessionStorage.getItem("curmenu") == "undefined" ? undefined : window.sessionStorage.getItem("curmenu") == "" ? '' : JSON.parse(window.sessionStorage.getItem("curmenu"));
			if (JSON.stringify(curmenu) != JSON.stringify(menu[liIndex - 1])) {  //如果删除的不是当前选中的tab
				// window.sessionStorage.setItem("curmenu",JSON.stringify(curmenu));
				curNav = JSON.stringify(curmenu);
			} else {
				if ($(this).parent("li").length > liIndex) {
					window.sessionStorage.setItem("curmenu", curmenu);
					curNav = curmenu;
				} else {
					window.sessionStorage.setItem("curmenu", JSON.stringify(menu[liIndex - 1]));
					curNav = JSON.stringify(menu[liIndex - 1]);
				}
			}
			menu.splice((liIndex - 1), 1);
			window.sessionStorage.setItem("menu", JSON.stringify(menu));
		}
		element.tabDelete("bodyTab", $(this).parent("li").attr("lay-id")).init();
		bodyTab.tabMove();
	})

	//刷新当前
	$(".refresh").on("click", function () {  //此处添加禁止连续点击刷新一是为了降低服务器压力，另外一个就是为了防止超快点击造成chrome本身的一些js文件的报错(不过貌似这个问题还是存在，不过概率小了很多)
		if ($(this).hasClass("refreshThis")) {
			$(this).removeClass("refreshThis");
			$(".clildFrame .layui-tab-item.layui-show").find("iframe")[0].contentWindow.location.reload();
			setTimeout(function () {
				$(".refresh").addClass("refreshThis");
			}, 2000)
		} else {
			layer.msg("您点击的速度超过了服务器的响应速度，还是等两秒再刷新吧！");
		}
	})

	//关闭其他
	$(".closePageOther").on("click", function () {
		if ($("#top_tabs li").length > 2 && $("#top_tabs li.layui-this cite").text() != "后台首页") {
			var menu = JSON.parse(window.sessionStorage.getItem("menu"));
			$("#top_tabs li").each(function () {
				if ($(this).attr("lay-id") != '' && !$(this).hasClass("layui-this")) {
					element.tabDelete("bodyTab", $(this).attr("lay-id")).init();
					//此处将当前窗口重新获取放入session，避免一个个删除来回循环造成的不必要工作量
					for (var i = 0; i < menu.length; i++) {
						if ($("#top_tabs li.layui-this cite").text() == menu[i].title) {
							menu.splice(0, menu.length, menu[i]);
							window.sessionStorage.setItem("menu", JSON.stringify(menu));
						}
					}
				}
			})
		} else if ($("#top_tabs li.layui-this cite").text() == "后台首页" && $("#top_tabs li").length > 1) {
			$("#top_tabs li").each(function () {
				if ($(this).attr("lay-id") != '' && !$(this).hasClass("layui-this")) {
					element.tabDelete("bodyTab", $(this).attr("lay-id")).init();
					window.sessionStorage.removeItem("menu");
					menu = [];
					window.sessionStorage.removeItem("curmenu");
				}
			})
		} else {
			layer.msg("没有可以关闭的窗口了@_@");
		}
		//渲染顶部窗口
		tab.tabMove();
	})

	var bodyTab = new Tab();
	exports("bodyTab", function (option) {
		return bodyTab.set(option);
	});

});

//当前顶部导航选中的名称 - 解决点击左侧菜单展示左上方路径
var thisTopHeadTitle;
//当前顶部默认导航路径 - 解决点击顶部菜单展示左上方路径
var defaultTopHeadTitle;

//切换菜单
function transformMenu(title, groupName, href, visitPath, defaultId,titleClassName) {
	showLeftMenu(href, visitPath);
	thisTopHeadTitle = title;
	defaultTopHeadTitle = "";
	if (!Common.isNil(visitPath)) {
		defaultTopHeadTitle = visitPath;
	}
	//显示左侧菜单
	var strData = localStorage.getItem("menu_title");
	var data = JSON.parse(strData);
	$(".navBar ul").css("margin-top","11px");
	$(".navBar ul").html('').append(spliceLeftMenu(data, groupName, defaultId)).height($(window).height() - 210);
	layui.element.init();  //初始化页面元素
	$(window).resize(function () {
		$(".navBar").height($(window).height() - 210);
	});
	setTitleColor(titleClassName);
}

function setTitleColor(titleClassName) {
	for (var i = 0; i < 50; i++) {
		$("." + "menu_title" + i).css("color","rgba(255, 255, 255, 0.7)");
	}
	$("."+titleClassName).css("color","#FF6666");
}

//显示左侧菜单
function showLeftMenu(href, visitPath) {
	console.log(href);
	$("#left_menu").show();
	$("#iframe_body iframe").contents().find('body').css("padding", "0px 24px 0px 32px");
	$("#content_body").css("left", "200px");
	$("#iframe_body iframe").contents().find('#onlineUserDiv').css("margin-left", "116px");
	if (!Common.isNil(href)) {
		$("#iframe_body iframe").attr('src', href);
	}
}

//隐藏左侧菜单
function hideLeftMenu() {
	$("#left_menu").hide();
	$("#iframe_body iframe").contents().find('body').css("padding", "0px 122px 0px 122px");
	$("#content_body").css("left", "0");
	$("#iframe_body iframe").contents().find('#onlineUserDiv').css("margin-left", "0px");
}

//拼接访问路径
function jointVisitPath(path) {
	defaultTopHeadTitle = "";
	thisTopHeadTitle = path;
}

//展示路径
function getJointVisitPath() {
	if (!Common.isNil(defaultTopHeadTitle)) {
		$("#iframe_body iframe").contents().find('.visitPath').html(defaultTopHeadTitle);
	} else {
		$("#iframe_body iframe").contents().find('.visitPath').html(thisTopHeadTitle);
	}
}

//生成左侧菜单
function spliceLeftMenu(strData, groupName, defaultId) {
	var data;
	if (typeof (strData) == "string") {
		data = JSON.parse(strData); //部分用户解析出来的是字符串，转换一下
	} else {
		data = strData;
	}
	var ulHtml = '';
	for (var i = 0; i < data.length; i++) {

		//判断是否同一个分组
		if (data[i].groupName != groupName) {
			continue;
		}
		//超级管理员
		if (localStorage.getItem("account") != 1000) {
			var flag = manage.ifUserAuth(data[i].resourceAuth);
			if (!flag) {
				continue;
			}
		}

		if (data[i].spread || data[i].spread == undefined) {
			ulHtml += '<li class="layui-nav-item layui-nav-itemed">';
		} else {
			ulHtml += data[i].id == defaultId ? '<li class="layui-nav-item layui-this">' : '<li class="layui-nav-item">';
		}
		if (data[i].children != undefined && data[i].children.length > 0) {
			ulHtml += '<a class="tets_14">';
			ulHtml += '<cite class="left_menu_title">' + data[i].title + '</cite>';
			ulHtml += '<span class="layui-nav-more"></span>';
			ulHtml += '</a>';
			ulHtml += '<dl class="layui-nav-child" style="background-color:#F8F8F8!important;">';
			for (var j = 0; j < data[i].children.length; j++) {

				if (localStorage.getItem("account") != 1000) {
					var flag = manage.ifUserAuth(data[i].children[j].resourceAuth);
					if (!flag) {
						continue;
					}
				}

				if (!Common.isNil(data[i].children)) {
					var html = '<option value="' + data[i].children[j].href + '">' + data[i].children[j].title + '</option>';
					$("#serachModules").append(html);
					layui.form.render();
				}

				if (data[i].children[j].target == "_blank") {
					if (data[i].id == defaultId) {
						ulHtml += '<dd class="layui-this"><a onclick="initHead_operation.setThisMenuUrl(\'' + data[i].children[j].href + '\'),jointVisitPath(\'' + thisTopHeadTitle + " -> " + data[i].title + " -> " + data[i].children[j].title + '\')" data-url="' + data[i].children[j].href + '" target="' + data[i].children[j].target + '">';
					} else {
						ulHtml += '<dd><a onclick="initHead_operation.setThisMenuUrl(\'' + data[i].children[j].href + '\'),jointVisitPath(\'' + thisTopHeadTitle + " -> " + data[i].title + " -> " + data[i].children[j].title + '\')" data-url="' + data[i].children[j].href + '" target="' + data[i].children[j].target + '">';
					}
				} else {
					if (data[i].id == defaultId) {
						ulHtml += '<dd class="layui-this"><a onclick="initHead_operation.setThisMenuUrl(\'' + data[i].children[j].href + '\'),jointVisitPath(\'' + thisTopHeadTitle + " -> " + data[i].title + " -> " + data[i].children[j].title + '\')" data-url="' + data[i].children[j].href + '">';

					} else {
						ulHtml += '<dd><a onclick="initHead_operation.setThisMenuUrl(\'' + data[i].children[j].href + '\'),jointVisitPath(\'' + thisTopHeadTitle + " -> " + data[i].title + " -> " + data[i].children[j].title + '\')" data-url="' + data[i].children[j].href + '">';

					}
				}
				ulHtml += '<cite class="left_menu_title">' + data[i].children[j].title + '</cite></a></dd>';
			}
			ulHtml += "</dl>";
		} else {
			if (data[i].target == "_blank") {
				ulHtml += '<a onclick="initHead_operation.setThisMenuUrl(\'' + data[i].href + '\'),jointVisitPath(\'' + thisTopHeadTitle + " -> " + data[i].title + '\')" data-url="' + data[i].href + '" target="' + data[i].target + '">';
			} else {
				ulHtml += '<a onclick="initHead_operation.setThisMenuUrl(\'' + data[i].href + '\'),jointVisitPath(\'' + thisTopHeadTitle + " -> " + data[i].title + '\')" data-url="' + data[i].href + '">';
			}
			ulHtml += '<cite class="left_menu_title">' + data[i].title + '</cite></a>';
		}
		ulHtml += '</li>';
	}
	return ulHtml;
}
