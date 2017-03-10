package com.davkas;

/**
 * Created by davkas on 2017/3/10.
 */
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.log4j.Logger;
/**
 * 类ZkConnectionStateListener.java的实现描述：zk连接状态监听
 *
 * @author hzyangshaokai 2016年10月20日 下午4:07:19
 */
public class ZkConnectionStateListener implements ConnectionStateListener {
    private Logger           log  = Logger.getLogger(this.getClass());
    private IdWorker         worker;
    private volatile boolean lost = false;
    public ZkConnectionStateListener(IdWorker worker){
        this.worker = worker;
    }
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (newState.equals(ConnectionState.LOST)) {
            // 会话超时(可能本机网络问题或zookeeper服务集群整体宕机)，关闭IdWorker，防止该机器ID被其他机器申请去，导致生成重复的ID
            log.error("zookeeper session lost,IdWorker will stop!");
            lost = true;
            worker.close();
        } else if (newState.equals(ConnectionState.RECONNECTED)) {
            if (lost) {
                log.info("zookeeper connection reconnected,IdWorker will restart!");
                // 如果是zookeeper服务集群整体宕机造成的本地会话超时事件触发，那么实际上以前的状态可以全部恢复，会导致一批集群ID浪费（zookeeper服务集群整体宕机概率极低）
                // 丢失重写建立连接后重新分配机器ID
                try {
                    worker.rebuildDatacenterId();
                    lost = false;
                    worker.start();
                    log.info("IdWorker restart success!");
                } catch (Exception e) {
                    log.error("IdWorker restart fail!", e);
                }
            }
        }
    }
}
