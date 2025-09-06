package com.basic.im.manual.controller;

import com.alibaba.fastjson.JSONObject;
import com.basic.common.model.PageResult;
import com.basic.im.comm.constants.KConstants;
import com.basic.im.comm.model.MessageBean;
import com.basic.im.comm.utils.DateUtil;
import com.basic.im.comm.utils.StringUtil;
import com.basic.im.manual.entity.ReceiveAccount;
import com.basic.im.manual.entity.Recharge;
import com.basic.im.manual.entity.Withdraw;
import com.basic.im.manual.entity.WithdrawAccount;
import com.basic.im.manual.service.impl.ReceiveAccountServiceImpl;
import com.basic.im.manual.service.impl.RechargeServiceImpl;
import com.basic.im.manual.service.impl.WithdrawAccountServiceImpl;
import com.basic.im.manual.service.impl.WithdrawServiceImpl;
import com.basic.im.message.MessageService;
import com.basic.im.message.MessageType;
import com.basic.im.pay.entity.ConsumeRecord;
import com.basic.im.pay.service.impl.ConsumeRecordManagerImpl;
import com.basic.im.security.dto.WithdrawalDTO;
import com.basic.im.user.constants.MoneyLogConstants;
import com.basic.im.user.entity.User;
import com.basic.im.user.entity.UserMoneyLog;
import com.basic.im.user.service.UserCoreService;
import com.basic.im.utils.SKBeanUtils;
import com.basic.im.vo.JSONMessage;
import org.bson.types.ObjectId;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import java.util.List;

/**
 * @author zhm
 * @version V1.0
 * @Description: TODO(todo)
 * @date 2019/12/2 12:25
 */
@ApiIgnore
@RestController
@RequestMapping(value = "/manualAdmin")
public class ManualAdminController {

    @Autowired
    private ReceiveAccountServiceImpl receiveAccountService;
    @Autowired
    private RechargeServiceImpl rechargeService;
    @Autowired
    private UserCoreService userCoreService;
    @Autowired
    private WithdrawServiceImpl withdrawService;
    @Autowired
    private WithdrawAccountServiceImpl withdrawAccountService;
    @Autowired
    private MessageService messageService;
    @Autowired
    private ConsumeRecordManagerImpl consumeRecordManager;
    /**
     * 添加收款账户
     * @param
     * @return
     */
    @RequestMapping(value = "/addReceiveAccount")
    public JSONMessage addReceiveAccount(ReceiveAccount receiveAccount){
        receiveAccountService.addReceiveAccount(receiveAccount);
        return JSONMessage.success();
    }

    /**
     * 获取收款账户列表
     * @param
     * @return
     */
    @RequestMapping(value = "/receiveAccountList")
    public JSONMessage receiveAccountList(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int limit, @RequestParam(defaultValue = "") int type, @RequestParam(defaultValue = "") String keyword){
        try {
            page = page-1;
            PageResult<ReceiveAccount> result = receiveAccountService.getReceiveAccountList(page,limit,type,keyword);
            return JSONMessage.success(result);
        }catch (Exception e){
            return JSONMessage.failureByException(e);
        }

    }

    /**
     * 获取收款账户详情
     * @param
     * @return
     */
    @RequestMapping(value = "/getReceiveAccountInfo")
    public JSONMessage getReceiveAccountInfo(@RequestParam(defaultValue = "") String id){
        Object data = receiveAccountService.getReceiveAccount(new ObjectId(id));
        return JSONMessage.success(data);
    }

    /**
     * 修改收款账户
     * @param
     * @return
     */
    @RequestMapping(value = "/updateReceiveAccount")
    public JSONMessage updateReceiveAccount(@ModelAttribute ReceiveAccount receiveAccount){
        receiveAccountService.updateReceiveAccount(receiveAccount.getId(),receiveAccount);
        return JSONMessage.success();
    }

    /**
     * 删除收款账户
     * @param
     * @return
     */
    @RequestMapping(value = "/deleteReceiveAccount")
    public JSONMessage deleteReceiveAccount(@RequestParam String id){
        receiveAccountService.deleteReceiveAccount(new ObjectId(id));
        return JSONMessage.success();
    }

    /**
     * 获取充值申请记录列表
     * @param
     * @return
     */
    @RequestMapping(value = "/getRechargeList")
    public JSONMessage getRechargeList(@RequestParam(defaultValue = "0") int page,@RequestParam(defaultValue = "10") int limit,@RequestParam(defaultValue = "") String keyword,@RequestParam(defaultValue = "") String startDate,
                                       @RequestParam(defaultValue = "") String endDate){
        page = page-1;
        long startTime = 0; //开始时间（秒）
        long endTime = 0; //结束时间（秒）,默认为当前时间
        if(!StringUtil.isEmpty(startDate) && !StringUtil.isEmpty(endDate)) {
            startTime = StringUtil.isEmpty(startDate) ? 0 : DateUtil.toDate(startDate).getTime()/1000;
            endTime = StringUtil.isEmpty(endDate) ? DateUtil.currentTimeSeconds() : DateUtil.toDate(endDate).getTime()/1000;

        }
        PageResult<Recharge> result = rechargeService.getRechargeList(page,limit,keyword,startTime,endTime);
        for (Recharge recharge : result.getData()) {
            recharge.setNickName(userCoreService.getNickName(recharge.getUserId()));
            recharge.setCurrentMoney(userCoreService.getUserMoenyV1(recharge.getUserId()));
        }
        return JSONMessage.success(result);
    }

    /**
     * 审批充值申请
     * @param
     * @return
     */
    @RequestMapping(value = "/checkRecharge")
    public JSONMessage checkRecharge(@RequestParam(defaultValue = "") String id,@RequestParam(defaultValue = "0") int status){
        Recharge recharge = rechargeService.getRecharge(new ObjectId(id));
        if(recharge.getStatus() == status){
            JSONMessage.failureByErrCode(KConstants.ResultCode.NotRepeatOperation);
        }
        recharge = rechargeService.checkRecharge(new ObjectId(id),status);
        recharge.setStatus(status);
        Double balance = null;
        if(status == 2){
            // 充值成功
            UserMoneyLog userMoneyLog =new UserMoneyLog(recharge.getUserId(),0,id,recharge.getMoney(),
                    MoneyLogConstants.MoenyAddEnum.MOENY_ADD, MoneyLogConstants.MoneyLogEnum.MANUAL_RECHARGE, MoneyLogConstants.MoneyLogTypeEnum.RECEIVE);

            balance = userCoreService.rechargeUserMoenyV1(userMoneyLog);
        }
        String orderNo=StringUtil.getOutTradeNo();
        ConsumeRecord record = new ConsumeRecord();
        record.setMoney(recharge.getMoney());
        record.setUserId(recharge.getUserId());
        record.setTime(DateUtil.currentTimeSeconds());
        record.setType(KConstants.ConsumeType.MANUALPAYRECHARGE);
        record.setChangeType(KConstants.MOENY_ADD);
        record.setDesc("扫码手动充值");
      //  record.setDesc("Manual recharge by scanning code");
        if(status == 2){
            record.setManualPay_status(1);
            record.setCurrentBalance(balance);
        }else{
            record.setManualPay_status(-1);
        }
        record.setStatus(KConstants.OrderStatus.END);
        record.setTradeNo(orderNo);
        int rechargeTpe = recharge.getType();
        int basePayType = (rechargeTpe == 1) ? 2 : (rechargeTpe == 2) ? 1 : (rechargeTpe == 3) ? 5 : 4;
        record.setPayType(basePayType);
        consumeRecordManager.saveConsumeRecord(record);

        User sysUser = userCoreService.getUser(1100);
        MessageBean messageBean = new MessageBean();
        messageBean.setFromUserId(sysUser.getUserId().toString());
        //messageBean.setFromUserName("Payment public account");
        messageBean.setFromUserName("支付公众号");
        messageBean.setType(MessageType.MANUAL_RECHARGE);
        messageBean.setContent(JSONObject.toJSONString(recharge));
        messageBean.setToUserId(String.valueOf(recharge.getUserId()));
        messageBean.setMsgType(0);// 普通单聊消息
        messageBean.setMessageId(StringUtil.randomUUID());
        messageService.send(messageBean);
        return JSONMessage.success();
    }

    /**
     * 删除充值申请
     * @param
     * @return
     */
    @RequestMapping(value = "/deleteRecharge")
    public JSONMessage deleteRecharge(@RequestParam String id){
        rechargeService.deleteRecharge(new ObjectId(id));
        return JSONMessage.success();
    }

    @Autowired
    private RedissonClient redissonClient;

    /**
     * 获取提现申请列表
     * @param
     * @return
     */
    @RequestMapping(value = "/getWithdrawList")
    public JSONMessage getWithdrawList(@RequestParam(defaultValue = "0") int page,@RequestParam(defaultValue = "10") int limit,@RequestParam(defaultValue = "") String keyword,@RequestParam(defaultValue = "") String startDate,
                                       @RequestParam(defaultValue = "") String endDate){
        page = page -1;
        long startTime = 0; //开始时间（秒）
        long endTime = 0; //结束时间（秒）,默认为当前时间
        //时间范围查询
        if(!StringUtil.isEmpty(startDate) && !StringUtil.isEmpty(endDate)) {
            startTime = StringUtil.isEmpty(startDate) ? 0 : DateUtil.toDate(startDate).getTime()/1000;
            endTime = StringUtil.isEmpty(endDate) ? DateUtil.currentTimeSeconds() : DateUtil.toDate(endDate).getTime()/1000;

        }
        PageResult<Withdraw> result = withdrawService.getWithdrawList(page,limit,keyword,startTime,endTime);
        RList<WithdrawalDTO> withdrawalConfig = redissonClient.getList("withdrawalConfig");
        List<WithdrawalDTO> list = withdrawalConfig.range(0, withdrawalConfig.size() - 1);
        result.getData().forEach(withdraw -> {
            WithdrawAccount withdrawAccount = withdrawAccountService.getWithdrawAccount(new ObjectId(withdraw.getWithdrawAccountId()));
            withdraw.setWithdrawAccount(withdrawAccount);
            withdraw.setNickName(userCoreService.getNickName(withdraw.getUserId()));

            list.forEach(o->{
                if(o.getType() == withdrawAccount.getType()){
                    withdraw.setTypeName(o.getName());
                }
            });
        });
        return JSONMessage.success(result);
    }

    /**
     * 审核提现申请
     * @param
     * @return
     */
    @RequestMapping(value = "/checkWithdraw")
    public JSONMessage checkWithdraw(@RequestParam(defaultValue = "") String id,@RequestParam(defaultValue = "0") int status){
        Withdraw withdraw = withdrawService.getWithdraw(new ObjectId(id));
        if(withdraw.getStatus()==status){
            return JSONMessage.failureByErrCode(KConstants.ResultCode.NotRepeatOperation);
        }
        String orderNo=StringUtil.getOutTradeNo();
        ConsumeRecord record = new ConsumeRecord();
        if(status != 2){
            UserMoneyLog userMoneyLog =new UserMoneyLog(withdraw.getUserId(),0,id,withdraw.getMoney(),
                    MoneyLogConstants.MoenyAddEnum.MOENY_ADD, MoneyLogConstants.MoneyLogEnum.MANUAL_RECHARGE, MoneyLogConstants.MoneyLogTypeEnum.UNLOCK_BALANCE);

           Double balanceMoney = userCoreService.rechargeUserMoenyV1(userMoneyLog);
            record.setCurrentBalance(balanceMoney);
        }else{
            Double userMoenyV1 = userCoreService.getUserMoenyV1(withdraw.getUserId());
            record.setCurrentBalance(userMoenyV1);
        }
        withdraw = withdrawService.checkWithdraw(new ObjectId(id),status);
        withdraw.setStatus(status);
        withdraw.setWithdrawAccount(withdrawAccountService.getWithdrawAccount(new ObjectId(withdraw.getWithdrawAccountId())));

        record.setServiceCharge((withdraw.getMoney()* SKBeanUtils.getImCoreService().getPayConfig().getManualPaywithdrawFee()));
        record.setPayType(3);// 余额支付
        record.setOperationAmount(withdraw.getMoney());
        record.setMoney(withdraw.getMoney());
        record.setUserId(withdraw.getUserId());
        record.setTime(DateUtil.currentTimeSeconds());
        record.setType(KConstants.ConsumeType.MANUALPAYWITHDRAW);
        record.setChangeType(KConstants.MOENY_REDUCE);
        record.setBusinessId(id);
        record.setDesc("扫码手动提现");
        //record.setDesc("Manual embodiment of scanning code");
        if(status == 2){
            record.setManualPay_status(1);
        }else{
            record.setManualPay_status(-1);
        }
        record.setStatus(KConstants.OrderStatus.END);
        record.setTradeNo(orderNo);
        consumeRecordManager.saveConsumeRecord(record);

        User sysUser = userCoreService.getUser(1100);
        MessageBean messageBean = new MessageBean();
        messageBean.setFromUserId(sysUser.getUserId().toString());
        messageBean.setFromUserName(sysUser.getUsername());
        messageBean.setType(MessageType.MANUAL_WITHDRAW);
        messageBean.setContent(JSONObject.toJSON(withdraw));
        messageBean.setToUserId(String.valueOf(withdraw.getUserId()));
        messageBean.setMsgType(0);// 普通单聊消息
        messageBean.setMessageId(StringUtil.randomUUID());
        messageService.send(messageBean);
        return JSONMessage.success();
    }

    /**
     * 删除提现申请
     * @param
     * @return
     */
    @RequestMapping(value = "/deleteWithdraw")
    public JSONMessage deleteWithdraw(@RequestParam String id){
        withdrawService.deleteWithdraw(new ObjectId(id));
        return JSONMessage.success();
    }
}
