package com.davkas;

/**
 * Created by davkas on 2017/3/10.
 */
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs;
/**
 * 类IdWorker.java的实现描述：Twitter-Snowflake，64位自增ID算法
 *
 * @author hzyangshaokai 2016年10月20日 上午9:43:59
 */
public class IdWorker {
    private Logger                    log                        = Logger.getLogger(this.getClass());
    private static final String       DEFAULT_NAMESPACE          = "netease_idcenter";
    private static final String       DEFAULT_CHARSETNAME        = "UTF-8";
    private static final int          DEFAULT_SESSION_TIMEOUT_MS = 3 * 1000;
    private volatile long             datacenterId;                                                                // 机器ID，可以理解为一个独立实体一个ID
    private final long                idepoch                    = 946656000000L;
    private static final long         datacenterIdBits           = 10L;                                            // 机器标识ID占用位数
    private static final long         maxDatacenterId            = -1L ^ (-1L << datacenterIdBits);                // 最大支持机器数量1023
    private static final long         sequenceBits               = 12L;                                            // 12位递增ID
    private static final long         sequenceMask               = -1L ^ (-1L << sequenceBits);                    // 一毫秒最大递增序号4095
    private static final long         datacenterIdShift          = sequenceBits;
    private static final long         timestampLeftShift         = sequenceBits + datacenterIdBits;
    private long                      lastTimestamp              = -1L;
    private long                      sequence                   = 0;                                              // 初始虚列值
    private static final Random       r                          = new Random();
    private SimpleDateFormat          dateFormat                 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
    private CuratorFramework          zkClient;
    private String                    zkConnectString;
    private volatile boolean          started                    = false;
    private ZkConnectionStateListener connectionStateListener;
    private Executor                  connectionStateListenerExecutor;
    public IdWorker(String zkConnectString){
        this.zkConnectString = zkConnectString;
        this.connectionStateListenerExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable target) {
                return new Thread(target, "ConnectionStateListener_Thread");
            }
        });
    }
    /**
     * 初始化
     *
     * @throws Exception
     */
    public void init() throws Exception {
        // 初始化监听器
        this.connectionStateListener = new ZkConnectionStateListener(this);
        // 启动ZK连接
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.builder().connectString(zkConnectString).sessionTimeoutMs(DEFAULT_SESSION_TIMEOUT_MS).retryPolicy(retryPolicy).namespace(DEFAULT_NAMESPACE).build();
        // 添加状态监控
        zkClient.getConnectionStateListenable().addListener(this.connectionStateListener,
                this.connectionStateListenerExecutor);
        zkClient.start();
        // 超过3秒连接不上报错
        try {
            if (!zkClient.blockUntilConnected(3, TimeUnit.SECONDS)) {
                throw new RuntimeException("zookeeper client can't connected");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("zookeeper client connecte interrupted", e);
        }
        // 获取一个集群ID
        datacenterId = buildDatacenterId();
        started = true;
        log.info("IdWorker is start");
    }
    /**
     * 重新分配一个机器ID
     *
     * @throws Exception
     */
    protected void rebuildDatacenterId() throws Exception {
        datacenterId = buildDatacenterId();
        log.info("datacenterId rebuild,current datacenterId:" + datacenterId);
    }
    /**
     * 关闭
     */
    public void shutdown() {
        if (!started) {
            return;
        }
        started = false;
        // 关闭zk连接
        zkClient.close();
        log.info("IdWorker is shutdown");
    }
    /**
     * 生成一个唯一的集群ID
     *
     * @return
     * @throws Exception
     */
    private long buildDatacenterId() throws Exception {
        try {
            // 获取所有已经分配的集群ID
            List<String> usedIds = zkClient.getChildren().forPath("/datacenter_id");
            if (usedIds.size() == maxDatacenterId) {
                throw new RuntimeException("datacenterId is full!");
            }
            // 获取可以分配的集群ID
            List<Integer> unusedIds = new ArrayList<>(1024);
            for (int i = 1; i <= maxDatacenterId; i++) {
                if (!usedIds.contains(Integer.toString(i))) {
                    unusedIds.add(i);
                }
            }
            // 可分配ID中随机分配个创建节点，以免冲突
            int centerId = unusedIds.get(r.nextInt(unusedIds.size()));
            if (tryToCreateDatacenterPath(centerId)) {
                return centerId;
            } else {
                log.info("datacenter id exist,retry...");
                return buildDatacenterId();
            }
        } catch (NoNodeException e) {
            // 第一次启动，试图创建第一个节点
            int centerId = 1;
            if (tryToCreateDatacenterPath(centerId)) {
                return centerId;
            } else {
                log.info("datacenter id exist,retry...");
                return buildDatacenterId();
            }
        }
    }
    /**
     * 试图创建集群节点
     *
     * @param centerId
     * @return
     * @throws Exception
     */
    private boolean tryToCreateDatacenterPath(int centerId) throws Exception {
        String data = String.format("host:%s", getHost());
        String path = String.format("/datacenter_id/%d", centerId);
        try {
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(path,
                    data.getBytes(DEFAULT_CHARSETNAME));
            return true;
        } catch (NodeExistsException e) {
            log.info("node exist:" + path);
            return false;
        }
    }
    private static String getHost() {
        String host;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            host = addr.getHostAddress();
        } catch (Exception ex) {
            host = "127.0.0.1";
        }
        return host;
    }
    /**
     * 获取ID
     *
     * @return
     */
    public long getId() {
        if (!started) {
            throw new RuntimeException("IdWorker not start!");
        }
        long id = nextId();
        return id;
    }
    /**
     * 获取下一个ID
     *
     * @return
     */
    private synchronized long nextId() {
        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {
            throw new IllegalStateException("Clock moved backwards.");
        }
        // 序列号支持1毫秒产生4095个自增序列id
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);// 若同一毫秒把序列号用完了，则等待至下一毫秒
            }
        } else {
            sequence = 0;
        }
        lastTimestamp = timestamp;
        long id = ((timestamp - idepoch) << timestampLeftShift)//
                | (datacenterId << datacenterIdShift)//
                | sequence;
        return id;
    }
    /**
     * 获取ID的时间戳
     *
     * @param id
     * @return
     */
    public long getIdTimestamp(long id) {
        return idepoch + (id >> timestampLeftShift);
    }
    /**
     * 获取ID的时间戳格式日期 <br />
     * yyyy-MM-dd HH:mm:ss SSS
     *
     * @param id
     * @return
     */
    public String getIdTimestampFormat(long id) {
        Date date = new Date(getIdTimestamp(id));
        return dateFormat.format(date);
    }
    /**
     * 获取下一秒
     *
     * @param lastTimestamp
     * @return
     */
    private long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }
    /**
     * 获取当前的毫秒
     *
     * @return
     */
    private long timeGen() {
        return System.currentTimeMillis();
    }
    public long getDatacenterId() {
        return datacenterId;
    }
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IdWorker{");
        sb.append(", datacenterId=").append(datacenterId);
        sb.append(", idepoch=").append(idepoch);
        sb.append(", lastTimestamp=").append(lastTimestamp);
        sb.append(", sequence=").append(sequence);
        sb.append('}');
        return sb.toString();
    }
    /**
     * 关闭
     */
    protected void close() {
        this.started = false;
    }
    /**
     * 启动
     */
    protected void start() {
        this.started = true;
    }
}
