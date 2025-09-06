package com.basic.redisson.ex;

public class LockFailException extends Exception {

    public LockFailException(String message) {
        super(message);
    }
}
