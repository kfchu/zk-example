package com.zkexample.config;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zkexample.ZooKeeperBaseTest;

public class ConfigHelperTest extends ZooKeeperBaseTest {

    private ZooKeeper zk;

    private ConfigHelper configHelper;

    @Before
    public void before() throws IOException, KeeperException, InterruptedException {
        zk = createClient();
        zk.create(ConfigHelper.CONFIG_NODE_PATH, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        configHelper = new ConfigHelper();
        configHelper.setZooKeeper(zk);
    }

    @After
    public void after() throws IOException, InterruptedException, KeeperException {
        List<String> childrenPath = zk.getChildren(ConfigHelper.CONFIG_NODE_PATH, false);
        for (String childPath : childrenPath) {
            zk.delete(ConfigHelper.CONFIG_NODE_PATH + "/" + childPath, -1);
        }
        // delete the parent node
        zk.delete(ConfigHelper.CONFIG_NODE_PATH, -1);
    }

    @Test
    public void testConfigDynamicUpdate() throws KeeperException, InterruptedException {
        // prepare
        String configKey = "hostname";
        zk.create(ConfigHelper.CONFIG_NODE_PATH + "/" + configKey, "http://www.kfchu.com".getBytes(),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // get data once
        MockServer mockServer = new MockServer(configHelper);
        assertEquals("http://www.kfchu.com", mockServer.getHostname());

        // change the hostname
        zk.setData(ConfigHelper.CONFIG_NODE_PATH + "/" + configKey, "http://www.jeffzhu.com".getBytes(), -1);
        Thread.sleep(100);
        assertEquals("http://www.jeffzhu.com", mockServer.getHostname());
    }

    public class MockServer {

        private ConfigHelper configHelper;

        private String hostname;

        public MockServer(ConfigHelper configHelper) {
            this.configHelper = configHelper;
        }

        public String getHostname() {
            if (hostname == null) {
                hostname = configHelper.getString("hostname", new ConfigUpdateCallback() {

                    @Override
                    public void onChange(Object newValue) {
                        setHostname(String.valueOf(newValue));
                    }
                });
            }

            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

    }

}
