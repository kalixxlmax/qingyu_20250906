package com.basic.im.open.vo;

import com.basic.im.comm.utils.BeanUtils;
import com.basic.im.open.opensdk.entity.SkOpenApp;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;

@Data
public class SkOpenAppVO {

	@ApiModelProperty("记录id")
	private  ObjectId gameId;

	@ApiModelProperty("账号ID")
	private String accountId;

	@ApiModelProperty("创建时间")
	private Long createTime;

	@ApiModelProperty("首次时间")
	private Long modifyTime;


	@ApiModelProperty("应用名称")
	private String appName;

	@ApiModelProperty("应用简介")
	private String appIntroduction;

	@ApiModelProperty("应用官网")
	private String appUrl;

	@ApiModelProperty("网站信息扫描件")
	private String webInfoImg;

	@ApiModelProperty("应用小 图片 28*28")
	private String appsmallImg;

	@ApiModelProperty("应用大图片 108*108")
	private String appIcon;

	@ApiModelProperty("appId")
	private String appId;

	@ApiModelProperty("appSecret")
	private String appSecret;

	@ApiModelProperty("分享权限  0 未获得  1 已获得   2 申请中")
	private Byte isAuthShare = 0;

	@ApiModelProperty("登陆权限 0 未获得  1 已获得   2 申请中")
	private Byte isAuthLogin = 0;

	@ApiModelProperty("支付权限 0 未获得  1 已获得   2 申请中")
	private Byte isAuthPay = 0;


	@ApiModelProperty("是否开启群助手  0 未开启  1已开启  2 申请中")
	private Byte isGroupHelper = 0;

	@ApiModelProperty("群助手名称")
	private String helperName;

	@ApiModelProperty("群助手描述")
	private String helperDesc;

	@ApiModelProperty("群助手开发者")
	private String helperDeveloper;

	@ApiModelProperty("支付回调域名")
	private String payCallBackUrl;

	@ApiModelProperty("状态 0 审核中 1正常 -1禁用 下架  2审核失败")
	private Byte status = 0;

	@ApiModelProperty("IOs Bundle ID")
	private String iosAppId;

	@ApiModelProperty("测试版本Bundle ID")
	private String iosBataAppId;

	@ApiModelProperty("Ios 下载地址")
	private String iosDownloadUrl;

	@ApiModelProperty("android应用包名")
	private String androidAppId;

	@ApiModelProperty("android下载地址")
	private String androidDownloadUrl;

	@ApiModelProperty("安卓应用签名")
	private String androidSign;

	@ApiModelProperty("网页应用授权回调域")
	private String callbackUrl;

	@ApiModelProperty("网站类型  1：app  2:网页  3:小游戏")
	private Byte appType = 0;


	public static List<SkOpenAppVO> convertSkOpenAppVO(List<SkOpenApp> skOpenApps){
		List<SkOpenAppVO> skOpenAppVOS = new ArrayList<>();
		skOpenApps.forEach(skOpenApp -> {
			SkOpenAppVO target = new SkOpenAppVO();
			BeanUtils.copyProperties(skOpenApp, target);
			target.setAppIcon(skOpenApp.getAppImg());
			target.setGameId(skOpenApp.getId());
			skOpenAppVOS.add(target);
		});

		return skOpenAppVOS;
	}

}
