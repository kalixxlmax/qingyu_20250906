package com.basic.im.pay.controller;

import com.basic.im.api.service.base.AbstractController;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.pay.entity.BaseConsumeRecord;
import com.basic.im.pay.service.impl.ConsumeRecordManagerImpl;
import com.basic.im.room.dao.RoomCoreDao;
import com.basic.im.room.entity.Room;
import com.basic.im.room.service.RoomManager;
import com.basic.im.user.entity.UserMoneyLog;
import com.basic.im.user.constants.MoneyLogConstants.*;
import com.basic.im.user.service.UserCoreService;
import com.wxpay.utils.WXNotify;
import com.wxpay.utils.WXPayUtil;
import com.wxpay.utils.WxPayResult;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * 商务圈接口
 * 
 * @author Administrator
 *
 */
@ApiIgnore
@RestController
@RequestMapping(method = {RequestMethod.GET,RequestMethod.POST})
public class WXController extends AbstractController {

	private static Logger logger = LoggerFactory.getLogger(WXController.class);

	/*@Resource(name = "wxConfig")
	protected WXConfig wxConfig;*/
	
	private static final String TRANSFERS_PAY = "https://api.mch.weixin.qq.com/mmpaymkttransfers/promotion/transfers"; // 企业付款

	
	private static final String TRANSFERS_PAY_QUERY = "https://api.mch.weixin.qq.com/mmpaymkttransfers/gettransferinfo"; // 企业付款查询


	@Autowired
	private ConsumeRecordManagerImpl consumeRecordManager;

	@Autowired
	private UserCoreService userCoreService;
	@Autowired
	private RoomManager roomManager;
	@Autowired
	private RoomCoreDao roomCoreDao;

	@ApiOperation("微信支付回调数据")
	@RequestMapping(value="/user/recharge/wxPayCallBack")
	public void wxPayCallBack(HttpServletRequest request,
							  HttpServletResponse response) throws IOException {
		//把如下代码贴到的你的处理回调的servlet 或者.do 中即可明白回调操作
		logger.info("微信支付回调数据开始");
		BufferedOutputStream out = null;
		String inputLine;
		String notityXml = "";
		String resXml = "";
		try {
			while ((inputLine = request.getReader().readLine()) != null) {
				notityXml += inputLine;
			}
			request.getReader().close();

			Map<String,String> m = WXNotify.parseXmlToList2(notityXml);
			logger.info("接收到的报文：" + m);
			String tradeNo=m.get("out_trade_no");
			BaseConsumeRecord entity=consumeRecordManager.getConsumeRecordByNo(tradeNo);
			if(null==entity) {
                logger.info("交易订单号不存在！-----"+tradeNo);
            } else if(0!=entity.getStatus()) {
                logger.info(tradeNo+"===status==="+entity.getStatus()+"=======交易已处理或已取消!");
            } else if("SUCCESS".equals(m.get("result_code"))){
				boolean flag=Double.valueOf(m.get("cash_fee"))==entity.getMoney()*100;
				if(flag){
					//logger.info("支付金额比较"+m.get("cash_fee")+"=="+entity.getMoney()*100+"=======>"+flag);
					WxPayResult wpr = WXPayUtil.mapToWxPayResult(m);
					//支付成功
					resXml = "<xml>" + "<return_code><![CDATA[SUCCESS]]></return_code>"
							+ "<return_msg><![CDATA[OK]]></return_msg>" + "</xml> ";
					entity.setStatus(KConstants.OrderStatus.END);
					if (entity.getChangeType() == MoenyAddEnum.MOENY_ADD.getType()){
						UserMoneyLog userMoneyLog =new UserMoneyLog(entity.getUserId(),0,tradeNo,entity.getMoney(),
								MoenyAddEnum.MOENY_ADD, MoneyLogEnum.RECHARGE, MoneyLogTypeEnum.NORMAL_PAY);
						userMoneyLog.setExtra("WX_PAY");
						Double balance = userCoreService.rechargeUserMoenyV1(userMoneyLog);
						entity.setCurrentBalance(balance);
					}
					entity.setOperationAmount(entity.getMoney());
					//如果是付费入群业务
					if(entity.getType() == KConstants.ConsumeType.ADD_ROOM){
						//付费入群.直接加入群聊
						int periods = entity.getNum()==0?1:entity.getNum();
						roomManager.paySuccess(entity.getUserId(),entity.getTargetId(), KConstants.Room_Role.MEMBER,
								Room.Member.OperationType.INVITE_PAY,periods);
						//群主增加余额
						String tradeNo1= com.basic.utils.StringUtil.getOutTradeNo();
						Room room = roomCoreDao.getRoomById(entity.getTargetId());
						UserMoneyLog userMoneyLog =new UserMoneyLog(room.getUserId(),0,tradeNo1,entity.getMoney(),
								MoenyAddEnum.MOENY_ADD, MoneyLogEnum.JOIN_ROOM_PAY, MoneyLogTypeEnum.RECEIVE);
						userMoneyLog.setExtra("WX_PAY");
						Double balance = userCoreService.rechargeUserMoenyV1(userMoneyLog,obj ->{
							BaseConsumeRecord record=new BaseConsumeRecord();
							record.setUserId(room.getUserId());
							record.setToUserId(0);
							record.setTradeNo(tradeNo1);
							record.setMoney(entity.getMoney());
							record.setOperationAmount(entity.getMoney());
							record.setCurrentBalance(obj);
							record.setBusinessId(userMoneyLog.getBusinessId());
							record.setStatus(KConstants.OrderStatus.END);
							record.setType(KConstants.ConsumeType.ADD_ROOM);
							record.setChangeType(KConstants.MOENY_ADD);
							record.setPayType(KConstants.PayType.WXPAY); //余额支付
							record.setDesc("付费收益");
							//record.setDesc("Paid income");
							record.setTime(com.basic.utils.DateUtil.currentTimeSeconds());
							consumeRecordManager.saveConsumeRecord(record);
							return null;
						});
						entity.setCurrentBalance(balance);
					}
					consumeRecordManager.getConsumeRecordDao().update(entity.getId(), entity);
					consumeRecordManager.getConsumeRecordDao().saveEntity(wpr);
					logger.info(tradeNo+"========>>微信支付成功!");
				}else{
					logger.info("微信数据返回错误!");
					logger.info("localhost:Money---------"+entity.getMoney()*100);
					logger.info("Wxpay:Cash_fee---------"+m.get("cash_fee"));
				}
			}else{
				logger.info("微信支付失败======"+m.get("return_msg"));
				resXml = "<xml>" + "<return_code><![CDATA[FAIL]]></return_code>"
						+ "<return_msg><![CDATA[报文为空]]></return_msg>" + "</xml> ";
			}
			out = new BufferedOutputStream(response.getOutputStream());
			out.write(resXml.getBytes());
			out.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if (out != null) {
                out.close();
            }
		}

	}

	/**
	 * 企业向个人支付转账
	 * @param request
	 * @param response
	 * @param openid 用户openid
	 * @param callback
	 */
	/*@RequestMapping(value = "/pay", method = RequestMethod.POST)
	public void transferPay(HttpServletRequest request, HttpServletResponse response, String openid, String callback) {
		logger.info("[/transfer/pay]");
		//业务判断 openid是否有收款资格
		
		Map<String, String> restmap = null;
		try {
			Map<String, String> parm = new HashMap<String, String>();
			parm.put("mch_appid", wxConfig.getAppid()); //公众账号appid
			parm.put("mchid", wxConfig.getAppid()); //商户号
			parm.put("nonce_str", PayUtil.getNonceStr()); //随机字符串
			parm.put("partner_trade_no", PayUtil.getTransferNo()); //商户订单号
			parm.put("openid", openid); //用户openid	
			parm.put("check_name", "NO_CHECK"); //校验用户姓名选项 OPTION_CHECK
			//parm.put("re_user_name", "安迪"); //check_name设置为FORCE_CHECK或OPTION_CHECK，则必填
			parm.put("amount", "100"); //转账金额
			parm.put("desc", "测试转账到个人"); //企业付款描述信息
			parm.put("spbill_create_ip", PayUtil.getLocalIp(request)); //Ip地址
			parm.put("sign", PayUtil.getSign(parm, wxConfig.getSecret()));

			String restxml = HttpUtils.posts(TRANSFERS_PAY, XmlUtil.xmlFormat(parm, false));
			restmap = XmlUtil.xmlParse(restxml);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

		if (CollectionUtil.isNotEmpty(restmap) && "SUCCESS".equals(restmap.get("result_code"))) {
			logger.info("转账成功：" + restmap.get("err_code") + ":" + restmap.get("err_code_des"));
			Map<String, String> transferMap = new HashMap<>();
			transferMap.put("partner_trade_no", restmap.get("partner_trade_no"));//商户转账订单号
			transferMap.put("payment_no", restmap.get("payment_no")); //微信订单号
			transferMap.put("payment_time", restmap.get("payment_time")); //微信支付成功时间
			WebUtil.response(response,
					WebUtil.packJsonp(callback,
							JSON.toJSONString(new JsonResult(1, "转账成功", new ResponseData(null, transferMap)),
									SerializerFeatureUtil.FEATURES)));
		}else {
			if (CollectionUtil.isNotEmpty(restmap)) {
				logger.info("转账失败：" + restmap.get("err_code") + ":" + restmap.get("err_code_des"));
			}
			
			WebUtil.response(response, WebUtil.packJsonp(callback, JSON
					.toJSONString(new JsonResult(-1, "转账失败", new ResponseData()), SerializerFeatureUtil.FEATURES)));
		}
	}*/


	

}
