package com.basic.im.lable;


import com.basic.im.comm.constants.KConstants;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import io.swagger.annotations.*;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@Api(value="LabelController",tags="标签接口")
@RestController
@RequestMapping(value="/label",method={RequestMethod.GET,RequestMethod.POST})
public class LabelController {


    @Autowired
    LabelManagerImpl labelManager;

    @Autowired
    UserLabelManager userLabelManager;
    
    //创建群标识码

    @ApiOperation("创建群标识码")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="userId" , value="用户编号",dataType="int"),
            @ApiImplicitParam(paramType="query" , name="logo" , value="标识",dataType="String",defaultValue = ""),
            @ApiImplicitParam(paramType="query" , name="name" , value="名称",dataType="String")
    })
    @RequestMapping("/create")
    public JSONMessage create(@RequestParam Integer userId, @RequestParam(defaultValue="") String logo, @RequestParam String name) {
        if(null == userId)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);

        Label newLabel ;

        if(null == name && null == logo)
        {
            newLabel =  labelManager.createLabel(userId);
        }
        else
        {
            Object data = labelManager.queryLabelByName(name);

            if(data != null)
                return JSONMessage.failureByErrCode(KConstants.ResultCode.LabelNameIsUse);
            newLabel =  labelManager.createLabelByParams(userId,logo,name);

        }

        addLabel(userId,newLabel.getCode(),0);
        return JSONMessage.success();
    }

    //获取群标识码
    @ApiOperation("获取群标识码")
    @ApiImplicitParam(paramType="query" , name="labelId" , value="群编号",dataType="ObjectId")
    @RequestMapping("/getlabel")
    public JSONMessage getLabel(@RequestParam ObjectId labelId) {
        if(null == labelId)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);
       Object data =  labelManager.getLabel(labelId);
        return JSONMessage.success(null,data);
    }
    //获取群标识码列表
    @ApiOperation("获取群标识码列表")
    @ApiImplicitParam(paramType="query" , name="userId" , value="用户编号",dataType="int")
    @RequestMapping("/getlabels")
    public JSONMessage getLabelList(@RequestParam Integer userId) {
        if(null == userId)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);
        Object data =labelManager.getLabelList(userId);
        return JSONMessage.success(null,data);
    }

    //跟新群标识码
    @ApiOperation("跟新群标识码")
    @RequestMapping("/update")
    public JSONMessage updateLabel(@ModelAttribute Label label) {
        if(null == label.getCode())
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);

        Object data =labelManager.updateLabel(label);
        if(null == data)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.DataNotExists);
        else
             return JSONMessage.success(null,data);
    }

    //更新群标识码
    @ApiOperation("更新群标识码")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="id" , value="群编号",dataType="ObjectId"),
            @ApiImplicitParam(paramType="query" , name="logo" , value="标识",dataType="String"),
            @ApiImplicitParam(paramType="query" , name="name" , value="名称",dataType="String")
    })
    @RequestMapping("/save")
    public JSONMessage saveLabel(@RequestParam ObjectId id,String name, String logo) {
       if(null == id)
         return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);

        Object data =labelManager.saveLabel(id,name,logo);
        return JSONMessage.success(null,data);
    }

    //添加群标识码
    @ApiOperation("添加群标识码")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="userId" , value="用户编号",dataType="int"),
            @ApiImplicitParam(paramType="query" , name="code" , value="标识",dataType="String"),
            @ApiImplicitParam(paramType="query" , name="date" , value="加密数据",dataType="long")
    })
    @RequestMapping("/add")
    public JSONMessage addLabel(@RequestParam Integer userId,@RequestParam(defaultValue="") String code,@RequestParam long date) {

        if(null == userId)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);

        if(null == code)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);

       Label label =labelManager.getLabelByCode(code);

        if(label == null)//无效群标识码
              return JSONMessage.failureByErrCode(KConstants.ResultCode.LabelNameNotExist);


       Object object =userLabelManager.queryUserLabel(userId,label.getId().toString());
       if(object != null)
           return JSONMessage.failureByErrCode(KConstants.ResultCode.LabelIsExist);

        Object data =userLabelManager.addLabel(userId,label.getId().toString(),label.getName(),label.getLogo(),code,date);

        if(data == null)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.AddFailure);
        else
            return JSONMessage.success(null,data);
    }

    //通过群标识码名称添加
    @ApiOperation("通过群标识码名称添加")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="userId" , value="用户编号",dataType="int"),
            @ApiImplicitParam(paramType="query" , name="name" , value="名称",dataType="String",defaultValue = ""),
            @ApiImplicitParam(paramType="query" , name="date" , value="加密数据",dataType="long",defaultValue = "0")
    })
    @RequestMapping("/addByName")
    public JSONMessage addByName(@RequestParam Integer userId,
    		@RequestParam(defaultValue="") String name,@RequestParam(defaultValue="0") long date) {
        if(null == userId)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);
        if(null == name)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NotLabelName);

        Label label =labelManager.queryLabelByName(name);

        if(label == null)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.LabelNameNotExist);

        Object object =userLabelManager.queryUserLabel(userId,label.getId().toString());
        if(object != null)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.LabelIsExist);

        Object data =userLabelManager.addLabel(userId,label.getId().toString(),label.getName(),label.getLogo(),label.getCode(),date);

        if(data == null)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.AddFailure);
        else
            return JSONMessage.success(null,data);
    }



    //获取群标识码列表
    @ApiOperation("获取群标识码列表")
    @ApiImplicitParam(paramType="query" , name="userId" , value="用户编号",dataType="int")
    @RequestMapping("/getUserLabels")
    public JSONMessage getUserLabels(@RequestParam Integer userId) {
        if (null == userId)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.ParamsLack);
        Object data =userLabelManager.getUserLabels(userId);
        return JSONMessage.success(null,data);
    }


    //查收群标识码
    @ApiOperation("查收群标识码")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="userId" , value="用户编号",dataType="int"),
            @ApiImplicitParam(paramType="query" , name="code" , value="编码",dataType="String")
    })
    @RequestMapping("/open")
    public JSONMessage  openLabel(Integer userId,String code){
       Object data = userLabelManager.queryUserLabelByCode(userId,code);
        return  JSONMessage.success(null,data);
    }

    //查询群标识码名是否已用
    @ApiOperation("查询群标识码名是否已用")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="userId" , value="用户编号",dataType="int"),
            @ApiImplicitParam(paramType="query" , name="name" , value="名称",dataType="String")
    })
    @RequestMapping("/queryLabelByName")
    public JSONMessage  queryLabelByName(Integer userId,String name){
        Object data = labelManager.queryLabelByName(name);
        if(data == null)
            return JSONMessage.success();
        else
            return JSONMessage.failureByErrCode(KConstants.ResultCode.LabelIsExist);
    }

    //判断是否已经添加
    @ApiOperation("判断是否已经添加")
    @ApiImplicitParams({
            @ApiImplicitParam(paramType="query" , name="userId" , value="用户编号",dataType="int"),
            @ApiImplicitParam(paramType="query" , name="name" , value="名称",dataType="String")
    })
    @RequestMapping("/isBuyLabel")
    public JSONMessage  isBuyLabel(Integer userId,String name){
        Label label =labelManager.queryLabelByName(name);

        if(label == null)//无效群标识码
            return JSONMessage.failureByErrCode(KConstants.ResultCode.LabelNameNotExist);

        Object object =userLabelManager.queryUserLabel(userId,label.getId().toString());
        if(object != null)
            return JSONMessage.failureByErrCode(KConstants.ResultCode.LabelIsExist);

        return JSONMessage.success();
    }
}
