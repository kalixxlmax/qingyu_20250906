package com.basic.im.admin.controller;

import com.basic.im.common.service.PaymentManager;
import com.basic.im.common.service.RedPacketsManager;
import com.basic.im.common.service.SkTransferManager;
import com.basic.im.pay.dto.BillRecordCountDTO;
import com.basic.im.vo.JSONMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import java.math.BigDecimal;
import java.text.DecimalFormat;

@ApiIgnore
@RestController
@RequestMapping
public class AdminBillController {
    protected Logger logger= LoggerFactory.getLogger(AdminBillController.class);

    @Autowired(required = false)
    private RedPacketsManager redPacketsManager;

    @Autowired(required = false)
    private SkTransferManager transferManager;

    @Autowired(required = false)
    private PaymentManager paymentManager;

    @RequestMapping(value = "/console/billCount")
    public JSONMessage billCount(long startTime, long endTime,@RequestParam(defaultValue ="0") int type) {
        BillRecordCountDTO billRecordCountDTO=null;
        try {
            billRecordCountDTO=paymentManager.queryCashGroupCount(startTime,endTime);
            String transferOverCount = transferManager.queryTransferOverGroupCount(startTime, endTime);
            String rechargeCount = paymentManager.queryRechargeGroupCount(startTime, endTime);
            String redpackOverCount = redPacketsManager.queryRedpackOverGroupCount(startTime, endTime);
            //保留两位小数
            DecimalFormat format = new DecimalFormat("0.00");
            String redpackOverCount_ = format.format(new BigDecimal(redpackOverCount));
            billRecordCountDTO.setTransferOverTotal(transferOverCount);
            billRecordCountDTO.setRedpacketOverTotal(redpackOverCount_);
            billRecordCountDTO.setRechargeTotal(rechargeCount);
        }catch (Exception e){
            logger.error(e.getMessage(),e);
        }
        return JSONMessage.success(billRecordCountDTO);
    }

}
