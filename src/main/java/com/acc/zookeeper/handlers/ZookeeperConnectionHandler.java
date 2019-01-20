package com.acc.zookeeper.handlers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import org.apache.zookeeper.server.WatchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ZookeeperConnectionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperConnectionHandler.class);
    private static final int SESSION_TIMEOUT = 3000; //session timeout set at 3s
    private static final int CONNECT_TIMEOUT = 5000; //wait for connecting to remote zookeeper server for 5s
    private ZooKeeper zooKeeper;
    @Value("${app.zookeeper.hostname}")
    private String hostname;
    private boolean isConnected;

    public ZookeeperConnectionHandler() {
        LOGGER.info(this.getClass().getSimpleName()+" created.");
    }

    public void connect() {
        CountDownLatch cdl = new CountDownLatch(1);
        try {
            zooKeeper = new ZooKeeper(hostname, SESSION_TIMEOUT, ev -> {
                    if (ev.getState() == Watcher.Event.KeeperState.SyncConnected) {
                        isConnected = true;
                    } else {
                        isConnected = false;
                    }
                    cdl.countDown();
                    LOGGER.info("Server connection status: "+(isConnected ? "connected" : "disconnected" ));
                }
            );
            cdl.await(CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (IOException | InterruptedException ex) {
            isConnected = false;
        }
    }

    public void disconnect() throws InterruptedException {
        if(isConnected) zooKeeper.close();
        isConnected = false;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public ZooKeeper getZooKeeper() {
        if(!isConnected) {
            connect();
        }
        return zooKeeper;
    }
}
