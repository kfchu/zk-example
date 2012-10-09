package com.zkexample.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

/**
 * The class is used to read the configuration from ZooKeeper.<br>
 * If the config value is changed, then it will be dynamic call the
 * callback method.
 * 
 * @author kfchu
 * 
 */
public class ConfigHelper implements Watcher {

    public static final String CONFIG_NODE_PATH = "/config";

    private ZooKeeper zooKeeper;

    private Map<String, ConfigUpdateCallback> key2CallbackMap = new HashMap<String, ConfigUpdateCallback>();

    /**
     * Get the config value as String.
     * 
     */
    public String getString(String key, ConfigUpdateCallback callback) {
        try {
            // register the callback if not exsit
            if (!key2CallbackMap.containsKey(key) && callback != null) {
                key2CallbackMap.put(key, callback);
            }
            // getData by key and set the watcher.
            byte[] data = zooKeeper.getData(CONFIG_NODE_PATH + "/" + key, this, null);
            return new String(data);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the config value as Integer.
     * 
     */
    public Integer getInteger(String key, ConfigUpdateCallback callback) {
        String valueStr = getString(key, callback);
        return Integer.parseInt(valueStr);
    }

    /**
     * Get the config value as Boolean.
     * 
     */
    public Boolean getBoolean(String key, ConfigUpdateCallback callback) {
        String valueStr = getString(key, callback);
        return Boolean.parseBoolean(valueStr);
    }

    /**
     * When the config value is changed, then it will call the
     * callback interface for updating the value.
     * 
     */
    @Override
    public void process(WatchedEvent event) {
        // only detect the data change event
        if (event.getType() != EventType.NodeDataChanged) {
            return;
        }

        // parse the config key from the path
        String key = parseKeyFromPath(event.getPath());

        // if key is empty or null, then skip
        if (key == null || key.equals("")) {
            return;
        }

        // if the callback is not set, then do nothing
        if (!key2CallbackMap.containsKey(key)) {
            return;
        }

        // execute the callback and set the watcher again.
        ConfigUpdateCallback callback = key2CallbackMap.get(key);
        callback.onChange(getString(key, callback));
    }

    private String parseKeyFromPath(String path) {
        return path.startsWith(CONFIG_NODE_PATH) ? path.substring(CONFIG_NODE_PATH.length() + 1) : null;
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

}
