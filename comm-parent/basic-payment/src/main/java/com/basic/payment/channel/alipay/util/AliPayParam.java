package com.basic.payment.channel.alipay.util;


import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;


@Setter
@Getter
public class AliPayParam {
	//折扣
	private  ObjectId id;
	private Double discount;
	private int payment_type;//支付类型
	private String subject;//商品名称
	private String notify_time;//通知时间
	private String notify_type;//通知类型
	private String notify_id;//通知校验ID
	private String sign_type;//签名方式
	private String sign;//签名

	private String out_trade_no;//商户网站唯一订单号

	private String trade_no;


	// 交易状态 TRADE_SUCCESS
	private String trade_status;
	//卖家支付宝用户号
	private String seller_id;
	//卖家支付宝账号
	private String seller_email;
	//买家支付宝用户号
	private String buyer_id;
	//买家支付宝账号
	private String buyer_email;
	//交易金额
	private Double total_fee;
	//	购买数量
	private Double quantity;
	//商品单价
	private Double price;
	//商品描述
	private String body;
	//交易创建时间
	private String gmt_create;
	//交易付款时间
	private String gmt_payment;
	//是否调整总价
	private String is_total_fee_adjust;
	//是否使用红包买家
	private String use_coupon;
	//退款状态
	private String refund_status;
	//退款时间
	private String gmt_refund;
	
	

	
	
	
	
}
