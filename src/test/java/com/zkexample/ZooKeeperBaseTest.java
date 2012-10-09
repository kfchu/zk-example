package com.zkexample;

import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Ignore;

@Ignore
public class ZooKeeperBaseTest {

    // setup the client
    protected ZooKeeper createClient() throws IOException {
        return new ZooKeeper("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183", 2000, null);
    }
}
