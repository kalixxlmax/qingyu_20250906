$(function () {

    /*加载信息*/
    var data = {
        telephone:sessionStorage.getItem("reviseInfo")
    }

    $.ajax({
        type:"POST",
        url:'/mp/getOfficialInfoByTel',
        dataType:"json",
        data:data,
        contentType : 'application/x-www-form-urlencoded; charset=UTF-8',
        async:false,
        traditional: true,
        success : function(result) {
            console.log(result)
            var data = result.data.officialInfo;
            $("#feedback").val(data.feedback);
            $("#companyName").val(data.companyName);
            $("#companyBusinessLicense").val(data.companyBusinessLicense);
            $("#desc").val(data.desc);
            $("#companyType").val(data.companyType);
            $("#adminName").val(data.adminName);
            $("#adminID").val(data.adminID);
            $("#adminTelephone").val(data.adminTelephone);
            $("#userId").val(result.data.user.userId);

            $("#country").append("<option value='"+data.country+"'>"+data.country+"</option>");
            $("#province").append("<option value='"+data.province+"'>"+data.province+"</option>");
            $("#city").append("<option value='"+data.city+"'>"+data.city+"</option>");

            localStorage.setItem("country",data.country);
            localStorage.setItem("province",data.province);
            localStorage.setItem("city",data.city);
        },
        error : function(result) {
            layer.msg(result.resultMsg);
        }
    })

    for (area in Addr){
        if (area != localStorage.getItem("country")){
            $("#country").append("<option value='"+area+"'>"+area+"</option>");
        }
    }

    layui.form.render();

    /*提交表单*/
    $("#chat-go").click(function () {
        var data = {
            userId:$("#userId").val(),
            telephone:sessionStorage.getItem("reviseInfo"),
            feedback:$("#feedback").val(),
            companyName:$("#companyName").val(),
            companyBusinessLicense:$("#companyBusinessLicense").val(),
            desc:$("#desc").val(),
            companyType:$("#companyType").val(),
            adminName:$("#adminName").val(),
            adminID:$("#adminID").val(),
            adminTelephone:$("#adminTelephone").val(),
            country:$("#country").val(),
            province:$("#province").val(),
            city:$("#city").val()
        }
        console.log(data);

        $.ajax({
            type:"POST",
            url:'/mp/updateOfficialInfoByTel',
            dataType:"json",
            data:data,
            contentType : 'application/x-www-form-urlencoded; charset=UTF-8',
            async:false,
            traditional: true,
            success : function(result) {
                if (result.resultCode == 1){
                    alert("成功提交！");
                    history.back(-1);
                }else{
                    layer.msg(result.resultMsg);
                }
            },
            error : function(result) {
                layer.msg(result.resultMsg);
            }
        })

    })

    /*返回*/
    $("#chat-back").click(function () {
        window.location.href="/mp/login.html";
    })
})

let country='';
layui.form.on('select(country)',function(data){
    console.log(data.value);
    country=data.value;
    layui.form.render();
    for(s in Addr[data.value]){
        if (area != localStorage.getItem("province")) {
            $("#province").append("<option value='" + s + "'>" + s + "</option>");
        }
    }
    layui.form.render();
})

layui.form.on('select(province)',function(data){
    for(c in Addr[country][data.value]){
        if (area != localStorage.getItem("city")) {
            $("#city").append("<option value='" + Addr[country][data.value][c] + "'>" + Addr[country][data.value][c] + "</option>");
        }
    }
    layui.form.render();
})
