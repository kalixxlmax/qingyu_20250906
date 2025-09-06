package com.basic.mongodb.dynamic;


/**
 * 操作类型，定义方法执行时需要进行的处理方式
 */
public interface OperatorMethod {
    /**
     * TODO 使用冷热分离进行增加时，id不允许在 DAO 层自动生成，必须手动设置
     */
    String INSERT  =   "INSERT";

    String DELETE  =   "DELETE";

    String SELECT  =   "SELECT";

    String UPDATE  =   "UPDATE";

    String COUNT   =   "COUNT";

    String EXISTS  =   "EXISTS";
}
