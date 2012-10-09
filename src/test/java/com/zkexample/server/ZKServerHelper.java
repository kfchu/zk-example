package com.zkexample.server;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

public class ZKServerHelper {

    /**
     * Run as standalone mode.
     */
    public static void startAsStandalone(String configPath) {
        ZooKeeperServerMain standaloneMain = new ZooKeeperServerMain();
        ServerConfig config = new ServerConfig();
        try {
            config.parse(configPath);
            standaloneMain.runFromConfig(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Run as replicated mode.
     */
    public static void startAsReplicated(String configPath) {
        QuorumPeerMain quorumPeerMain = new QuorumPeerMain();
        QuorumPeerConfig config = new QuorumPeerConfig();
        try {
            config.parse(configPath);
            quorumPeerMain.runFromConfig(config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
