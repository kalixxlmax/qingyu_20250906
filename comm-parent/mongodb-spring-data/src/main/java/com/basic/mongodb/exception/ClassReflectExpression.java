package com.basic.mongodb.exception;

/**
 * 反射异常
 */
public class ClassReflectExpression extends RuntimeException {
    public ClassReflectExpression(String message) {
        super(message);
    }
}
