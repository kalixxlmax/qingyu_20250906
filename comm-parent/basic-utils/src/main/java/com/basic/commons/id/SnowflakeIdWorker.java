package com.basic.commons.id;

import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * Twitter_Snowflake<br>
 * SnowFlake的结构如下(每部分用-分开):<br>
 * 0 - 0000000000 0000000000 0000000000 0000000000 0 - 00000 - 00000 - 000000000000 <br>
 * 1位标识，由于long基本类型在Java中是带符号的，最高位是符号位，正数是0，负数是1，所以id一般是正数，最高位是0<br>
 * 41位时间截(毫秒级)，注意，41位时间截不是存储当前时间的时间截，而是存储时间截的差值（当前时间截 - 开始时间截)
 * 得到的值），这里的的开始时间截，一般是我们的id生成器开始使用的时间，由我们程序来指定的（如下下面程序IdWorker类的startTime属性）。41位的时间截，可以使用69年，年T = (1L << 41) / (1000L * 60 * 60 * 24 * 365) = 69<br>
 * 10位的数据机器位，可以部署在1024个节点，包括5位datacenterId和5位workerId<br>
 * 12位序列，毫秒内的计数，12位的计数顺序号支持每个节点每毫秒(同一机器，同一时间截)产生4096个ID序号<br>
 * 加起来刚好64位，为一个Long型。<br>
 * SnowFlake的优点是，整体上按照时间自增排序，并且整个分布式系统内不会产生ID碰撞(由数据中心ID和机器ID作区分)，并且效率较高，经测试，SnowFlake每秒能够产生26万ID左右。
 */
public class SnowflakeIdWorker {
    /** 开始时间截 (2019-10-01)*/
    private final long twepoch = 1569859200000L;

    /** 机器id所占的位数 */
    private final long workerIdBits = 5L;

    /** 数据标识id所占的位数 */
    private final long datacenterIdBits = 5L;

    /** 支持的最大机器id，结果是31 (这个移位算法可以很快的计算出几位二进制数所能表示的最大十进制数) */
    private final long maxWorkerId = -1L ^ (-1L << workerIdBits);

    /** 支持的最大数据标识id，结果是31 */
    private final long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);

    /** 序列在id中占的位数 */
    private final long sequenceBits = 12L;

    /** 机器ID向左移12位 */
    private final long workerIdShift = sequenceBits;

    /** 数据标识id向左移17位(12+5) */
    private final long datacenterIdShift = sequenceBits + workerIdBits;

    /** 时间截向左移22位(5+5+12) */
    private final long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;

    /** 生成序列的掩码，这里为4095 (0b111111111111=0xfff=4095) */
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);


    /** 步长, 1024 */
    private static long stepSize = 2 << 9;
    /** 基础序列号, 每发生一次时钟回拨, basicSequence += stepSize */
    private long basicSequence = 0L;



    /** 工作机器ID(0~31) */
    private long workerId;

    /** 数据中心ID(0~31) */
    private long datacenterId;


    /** 随机获取数据中心id的参数 32 */
    private   long DATA_RANDOM = maxDatacenterId + 1;
    /** 随机获取机器id的参数 32 */
    private   long WORK_RANDOM = maxWorkerId + 1;


    /** 毫秒内序列(0~4095) */
    private long sequence = 0L;

    /** 上次生成ID的时间截 */
    private long lastTimestamp = -1L;

    //==============================Constructors=====================================

    /**
     * 构造函数
     */
    public SnowflakeIdWorker() {
        this.datacenterId = getDataId();
        this.workerId = getWorkId();

    }

    /**
     * pid 参与运算
     *
     * @param usePid
     */
    public SnowflakeIdWorker(boolean usePid) {
        if(usePid){
            this.datacenterId = getDatacenterId(maxDatacenterId);
            this.workerId = getWorkerId(datacenterId,maxWorkerId);
        }else {
            this.datacenterId = getDataId();
            this.workerId = getWorkId();

        }

    }


    /*public SnowflakeIdWorker(){
        this.datacenterId = getDatacenterId(maxDatacenterId);
        this.workerId = getMaxWorkerId(datacenterId, maxWorkerId);
    }*/

    /**
     * 构造函数
     * @param workerId 工作ID (0~31)
     * @param datacenterId 数据中心ID (0~31)
     */
    public SnowflakeIdWorker(long workerId, long datacenterId) {
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }
        if (datacenterId > maxDatacenterId || datacenterId < 0) {
            throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", maxDatacenterId));
        }
        this.workerId = workerId;
        this.datacenterId = datacenterId;
    }

    // ==============================Methods==========================================
    /**
     * 获得下一个ID (该方法是线程安全的)
     * @return SnowflakeId
     */
    public synchronized long nextId() {
        long timestamp = timeGen();

        //如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过这个时候应当抛出异常
        if (timestamp < lastTimestamp) {
            /**
             * 处理时间回退
             */
            return handleClockBackwards(timestamp);
            /*throw new RuntimeException(
                    String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));*/
        }

        //如果是同一时间生成的，则进行毫秒内序列
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            //毫秒内序列溢出
            if (sequence == 0) {
                //阻塞到下一个毫秒,获得新的时间戳
                timestamp = tilNextMillis(lastTimestamp);
            }
        }
        //时间戳改变，毫秒内序列重置
        else {
            sequence = 0L;
        }

        //上次生成ID的时间截
        lastTimestamp = timestamp;

        //移位并通过或运算拼到一起组成64位的ID
        return ((timestamp - twepoch) << timestampLeftShift)
                | (datacenterId << datacenterIdShift)
                | (workerId << workerIdShift)
                | sequence;
    }
    /**
     * 处理时钟回拨
     *
     * https://github.com/timestatic/snowflake/blob/master/src/test/java/com/timestatic/snowflake/
     *
     */
    private long handleClockBackwards(long currStmp) {
        basicSequence += stepSize;
        if (basicSequence == sequenceMask + 1) {
            basicSequence = 0;
            currStmp = getNextMill();
        }
        sequence = basicSequence;

        lastTimestamp = currStmp;

        return (currStmp - twepoch) << timestampLeftShift  // 时间戳部分
                | workerId << workerIdShift                    // 节点部分
                | sequence;                              // 序列号部分
    }

    private long getNextMill() {
        long mill = getNewstmp();
        while (mill <= lastTimestamp) {
            mill = getNewstmp();
        }
        return mill;
    }

    private long getNewstmp() {
        return System.currentTimeMillis();
    }

    /**
     * 阻塞到下一个毫秒，直到获得新的时间戳
     * @param lastTimestamp 上次生成ID的时间截
     * @return 当前时间戳
     */
    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    /**
     * 返回以毫秒为单位的当前时间
     * @return 当前时间(毫秒)
     */
    protected long timeGen() {
        return System.currentTimeMillis();
    }


    /**
     * 获取字符串s的字节数组，然后将数组的元素相加，对（max+1）取余
     */
    private  long getHostId(String s, long max){
        byte[] bytes = s.getBytes();
        int sums = 0;
        for(int b : bytes){
            sums += b;
        }
        return sums % (max+1);
    }

    /**
     * 根据 host address 取余，发生异常就获取 0到31之间的随机数
     */
    public  long getWorkId(){
        try {
            return getHostId(Inet4Address.getLocalHost().getHostAddress(), maxWorkerId);
        } catch (UnknownHostException e) {
            return new Random().nextInt((int) WORK_RANDOM);
        }
    }

    /**
     * 根据 host name 取余，发生异常就获取 0到31之间的随机数
     */
    public  long getDataId() {
        try {
            return getHostId(Inet4Address.getLocalHost().getHostName(), maxDatacenterId);
        } catch (UnknownHostException e) {
            return new Random().nextInt((int) DATA_RANDOM);
        }
    }



    /**
     * <p>
     * 获取 maxWorkerId
     *
     *根据数据中心ID 和 pid
     *
     * </p>
     */
    protected  long getWorkerId(long datacenterId, long maxWorkerId) {
        StringBuffer mpid = new StringBuffer();
        mpid.append(datacenterId);
        String name = ManagementFactory.getRuntimeMXBean().getName();
        if (!name.isEmpty()) {
            /*
             * GET jvmPid
             */
            mpid.append(name.split("@")[0]);
        }
        /*
         * MAC + PID 的 hashcode 获取16个低位
         */
        return (mpid.toString().hashCode() & 0xffff) % (maxWorkerId + 1);
    }

    /**
     * <p>
     * 数据标识id部分
     * 根据hostName
     * </p>
     */
    protected  long getDatacenterId(long maxDatacenterId) {
        long id = 0L;
        try {
            InetAddress ip = InetAddress.getLocalHost();
            NetworkInterface network = NetworkInterface.getByInetAddress(ip);
            if (network == null) {
                id = 1L;
            } else {
                byte[] mac = network.getHardwareAddress();
                id = ((0x000000FF & (long) mac[mac.length - 1])
                        | (0x0000FF00 & (((long) mac[mac.length - 2]) << 8))) >> 6;
                id = id % (maxDatacenterId + 1);
            }
        } catch (Exception e) {
            System.out.println(" getDatacenterId: " + e.getMessage());
        }
        return id;

    }


    //==============================Test=============================================
    /** 测试 */
    public  void main(String[] args) throws Exception {
        SnowflakeIdWorker idWorker = new SnowflakeIdWorker(0, 1);
        for (int i = 0; i < 1000; i++) {
            long id = idWorker.nextId();
            //System.out.println(Long.toBinaryString(id));
            System.out.println(id);
        }
    }
}