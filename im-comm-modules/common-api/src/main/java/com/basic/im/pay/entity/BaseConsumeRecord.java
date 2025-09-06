package com.basic.im.pay.entity;

import com.basic.im.comm.utils.NumberUtil;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.experimental.Accessors;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.text.DecimalFormat;

@Data
@Accessors(chain = true)
@Document(value = "ConsumeRecord")
public class BaseConsumeRecord {

	@Id
	private ObjectId id; //记录id

	private @Indexed
	String tradeNo; //交易单号


	/**
	 * 业务ID
	 */
	private String businessId;

	private @Indexed
	int userId; //用户Id

	/**
	 * 对方用户Id
	 * 接受转账时 为 转账人的ID
	 * <p>
	 * 发送转账时 为 接受放的 ID
	 */
	private @Indexed
	int toUserId;

	private Double money; //金额


	private long time; //时间

	/**
	 * 类型  1:用户充值, 2:用户提现, 3:后台充值, 4:发红包, 5:领取红包,
	 * 6:红包退款  7:转账   8:接受转账   9:转账退回   10:付款码付款
	 *  11:付款码收款   12:二维码付款  13:二维码收款* 14: 直播送礼物,
	 * 15: 直播收到礼物，16：后台手工提现，17：第三方调用支付,18: 扫码手动充值,19: 扫码手动提现
	 * 20
	 */
	private @Indexed
	int type;
	/**
	 * 子业务类型
	 */
	private byte subBusinessType;

	private String desc;  //消费备注

	private int payType;  //支付方式  1：支付宝支付 , 2：微信支付, 3：余额支付, 4:系统支付

	private @Indexed
	int status; //交易状态 0：创建  1：支付完成  2：交易完成  -1：交易关闭

	private int manualPay_status;// 扫码手动充值提现交易状态  1.审核成功 -1.审核失败

	private Double serviceCharge;// 手续费

	private Double currentBalance;// 当前余额

	private Double operationAmount;// 实际操作金额

	private ObjectId redPacketId;// 红包id

	private ObjectId targetId; // 业务实体id

	private int num; // 购买数量

	/**
	 * 1 收入
	 * 2.支出
	 */
	private byte changeType;

	@Transient
	private String userName;// 用户昵称

	@ApiModelProperty("转账说明")
	@Transient
	private String transferRemark;

	@ApiModelProperty("转账状态")
	@Transient
	private int transferStatus = 1;// 1 ：发出  2：已收款  -1：已退款

	public Double getMoney() {
		if (0 < money) {
			money = NumberUtil.format(money);
		}
		return money;
	}

	public void setMoney(Double money) {
		if (0 < money) {
			money = NumberUtil.format(money);
		}

		this.money = money;
	}
}