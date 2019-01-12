package com.acc.zookeeper.handlers;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZookeeperConnectionHandler {
    private static final Logger LOGGER = Logger.getLogger(ZookeeperConnectionHandler.class);
    private static final int SESSION_TIMEOUT = 3000; //session timeout set at 3s
    private ZooKeeper zooKeeper;
    private String hostname;

    public ZookeeperConnectionHandler(String hostname) {
        this.hostname = hostname;
        LOGGER.info(this.getClass().getSimpleName()+" created.");
    }

    public void connect() throws IOException {
        zooKeeper = new ZooKeeper(hostname, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}
