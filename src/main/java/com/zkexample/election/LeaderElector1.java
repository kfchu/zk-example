package com.zkexample.election;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.zkexample.queue.DistributedQueue;

/**
 * The class is try to nominate the client himself as the leader.
 * 
 * @author kfchu
 * 
 */
public class LeaderElector1 implements Watcher {

    private static final Logger logger = Logger.getLogger(DistributedQueue.class);
    private static final String LEADER_NODE_PATH = "/leader";

    private ZooKeeper zooKeeper;

    private String clientName;

    public LeaderElector1(String clientName) {
        this.clientName = clientName;
    }

    /**
     * Check if i am the leader.
     * 
     * @return true if the thread is the leader
     * @throws InterruptedException
     * @throws KeeperException
     */
    public boolean nominateSelfForLeader() {
        logger.debug(clientName + " try to become leader ...");
        try {
            byte[] data = zooKeeper.getData(LEADER_NODE_PATH, this, null);

            String leaderName = new String(data);
            if (clientName.equals(leaderName)) {
                logger.info(clientName + " is still the leader!");
                return true;
            } else {
                logger.info(clientName + " try to become leader fail. Current leader is: " + new String(data));
                return false;
            }
        } catch (KeeperException.NoNodeException e) {
            // when the node not exist, then try to become the leader
            return createLeaderNode();
        } catch (Exception e) {
            logger.error(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Try to create the leader znode.
     * 
     * @return true if the leader node can be created successfully
     * @throws KeeperException
     * @throws InterruptedException
     */
    private boolean createLeaderNode() {
        try {
            zooKeeper.create(LEADER_NODE_PATH, clientName.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            logger.info(clientName + " become the leader now!");
            return true;
        } catch (KeeperException.NodeExistsException e) {
            logger.info(clientName + " said: shit, i am late ...");
            return false;
        } catch (Exception e) {
            logger.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // try to nominate itself again.
        logger.debug("watch trigger!");
        nominateSelfForLeader();
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

}
