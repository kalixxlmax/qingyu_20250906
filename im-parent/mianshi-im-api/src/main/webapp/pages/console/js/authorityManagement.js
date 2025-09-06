//权限判断
var userAuth = localStorage.getItem("userAuth_resource").split(",");//用户权限
var account =  localStorage.getItem("account");//账号
var role = localStorage.getItem("role");//角色

//登陆菜单判断
var manage={
    //判断是否有该权限
    ifUserAuth:function (data) {
        for (var i = 0; i < userAuth.length; i++) {
            if (userAuth[i] === data) {
                return true;
            }
        }
        return false;
    }
    ,authButton:function (arr) {
        if (account != 1000){
            for(var i = 0; i < arr.length; i++) {
                if (!userAuth.includes(arr[i])){
                    $("."+ arr[i] +"").hide();
                }
            };
        }
    }
    //根据Class选择器隐藏表单
    ,hideButtomByClass:function (name) {
        $("."+ name +"").hide();
    }
    //根据Class选择器显示表单
    ,showButtomByClass:function (name) {
        $("."+ name +"").show();
    }
    //根据Id选择器隐藏表单
    ,hideButtomById:function (name) {
        $("#"+ name +"").hide();
    }
    //根据Id选择器显示表单
    ,showButtomById:function (name) {
        $("#"+ name +"").show();
    }
}