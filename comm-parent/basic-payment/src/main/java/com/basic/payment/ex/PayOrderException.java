package com.basic.payment.ex;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PayOrderException extends RuntimeException {

    private int errCode;

    public PayOrderException(String message) {
        super(message);
    }

    public PayOrderException(int errCode){
        super(errCode+"");
    }
}
