package com.acc.zookeeper.handlers;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Author: Cristi Ando Ciupav
 * Date: 14/01/2019
 * Email: accexpert@gmail.com
 */
@Component
public class DataChangeWatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataChangeWatcher.class);
    private ZookeeperConnectionHandler connectionHandler;

    @Autowired
    public DataChangeWatcher(ZookeeperConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
    }

    private void setWatcher() {
        try {
            List<String> children = connectionHandler.getZooKeeper().getChildren("/", false);

        } catch (Exception e) {

        }
    }

    private void handleWatchedData() {

    }
}
