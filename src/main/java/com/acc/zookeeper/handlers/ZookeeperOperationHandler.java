package com.acc.zookeeper.handlers;

import com.acc.zookeeper.models.BaseZookeeperModel;
import org.apache.log4j.Logger;

public class ZookeeperOperationHandler {
    private static final Logger LOGGER = Logger.getLogger(ZookeeperOperationHandler.class);
    private ZookeeperConnectionHandler connection;

    public ZookeeperOperationHandler(ZookeeperConnectionHandler connection) {
        this.connection = connection;
        LOGGER.info(this.getClass().getSimpleName()+" created.");
    }

    public boolean createNewNode(NodeTypes nodeType, String name, BaseZookeeperModel data) {

        return false;
    }

    public BaseZookeeperModel getNode(String name) {

        return null;
    }


}
