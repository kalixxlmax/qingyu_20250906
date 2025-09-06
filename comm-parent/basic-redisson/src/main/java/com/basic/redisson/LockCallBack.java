package com.basic.redisson;

@FunctionalInterface
public interface LockCallBack<T> {

    Object execute(T t);
}
