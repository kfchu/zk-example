package com.zkexample.queue;

import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Distributed queue implementation.
 * 
 * @author kfchu
 * 
 */
public class DistributedQueue {

    private static final Logger logger = Logger.getLogger(DistributedQueue.class);
    private static final String CHILD_NODE_PREFIX = "qnode";

    private ZooKeeper zooKeeper;

    private String queuePath;

    @SuppressWarnings("unused")
    private DistributedQueue() {
        // will not be used
    }

    public DistributedQueue(String queue) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(queuePath, false);
        if (stat == null) {
            logger.error("No such queue:" + queue);
            throw new KeeperException.NoNodeException();
        }
    }

    public boolean offer(byte[] data) {
        try {
            String path = zooKeeper.create(queuePath + "/" + CHILD_NODE_PREFIX, data, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            logger.info("New member " + path + " is offered");
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    public byte[] take() throws KeeperException, InterruptedException {
        if (size() == 0) {
            throw new NoSuchElementException("No element in queue");
        }

        // get the head member
        String headPath = queuePath + "/" + getSmallestChildName();
        // fetch data
        byte[] result = zooKeeper.getData(headPath, false, null);
        // remove the head member
        zooKeeper.delete(headPath, -1);

        logger.info("Member " + headPath + " is taken");

        return result;
    }

    public int size() throws KeeperException, InterruptedException {
        List<String> childerns = zooKeeper.getChildren(queuePath, null);
        return childerns == null ? 0 : childerns.size();
    }

    /**
     * Get the child name with the smallest subfix.
     * 
     * @return smallest child name.
     * @throws KeeperException
     * @throws InterruptedException
     */
    private String getSmallestChildName() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(queuePath, null);

        Long smallestIndex = Long.MAX_VALUE;
        String smallestChildName = "";

        for (String child : children) {
            Long id = Long.parseLong(child.substring(CHILD_NODE_PREFIX.length()));
            if (id < smallestIndex) {
                smallestIndex = id;
                smallestChildName = child;
            }
        }

        return smallestChildName;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }
}
