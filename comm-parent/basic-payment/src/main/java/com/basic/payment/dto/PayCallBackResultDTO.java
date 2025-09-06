package com.basic.payment.dto;

import com.basic.payment.constant.PayConstant;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class PayCallBackResultDTO extends BaseDTO{

    private String result;

    private boolean success;

    private PaySuccessDTO paySuccessDTO;

    public PayCallBackResultDTO(boolean success,String result, PaySuccessDTO paySuccessDTO) {
        this.success=success;
        this.result = result;
        this.paySuccessDTO = paySuccessDTO;
    }

    public static PayCallBackResultDTO fail(){
        return new PayCallBackResultDTO(false,PayConstant.RETURN_VALUE_FAIL,null);
    }

    public static PayCallBackResultDTO success(PaySuccessDTO dto){
        return new PayCallBackResultDTO(true,PayConstant.RETURN_VALUE_SUCCESS,dto);
    }
    public static PayCallBackResultDTO fail(String result){
        return new PayCallBackResultDTO(false,result,null);
    }

    public static PayCallBackResultDTO success(String result,PaySuccessDTO dto){
        return new PayCallBackResultDTO(true,result,dto);
    }
}
