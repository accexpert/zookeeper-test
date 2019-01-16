package com.acc.zookeeper.handlers;

import com.acc.zookeeper.models.BaseZookeeperModel;
import com.acc.zookeeper.models.NodeTypes;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
//@EnableScheduling
public class ZookeeperOperationHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperOperationHandler.class);
    private ZookeeperConnectionHandler connection;
    private ZookeeperDataSerializer dataSerializer;
    private DataChangeWatcher dataChangeWatcher;

    @Autowired
    public ZookeeperOperationHandler(ZookeeperConnectionHandler connection, ZookeeperDataSerializer dataSerializer, DataChangeWatcher dataChangeWatcher) {
        this.connection = connection;
        this.dataSerializer = dataSerializer;
        this.dataSerializer = dataSerializer;
        LOGGER.info(this.getClass().getSimpleName()+" created.");
    }

    public String createNewNode(NodeTypes nodeType, String nodeName, BaseZookeeperModel data) throws KeeperException, InterruptedException {
        LOGGER.info("Create node: path: "+nodeName+"; type: "+nodeType+"; data: "+data);
        String dataSerial = dataSerializer.serializeData(data);
        //return the node path
        return connection.getZooKeeper().create(nodeName, dataSerial.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, getCreateMode(nodeType));
    }

    public void deleteNode(String nodeName) throws KeeperException, InterruptedException {
        connection.getZooKeeper().delete(nodeName, 0);
    }

    public boolean setWatch() {
        try {
            connection.getZooKeeper().exists("/*", new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    LOGGER.info("--- "+watchedEvent.getPath());
                }
            });
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    public Stat checkIfNodeExists(String nodeName) throws KeeperException, InterruptedException {
        return connection.getZooKeeper().exists(nodeName, false);
    }

    public BaseZookeeperModel getData(String nodeName) throws KeeperException, InterruptedException {
        BaseZookeeperModel data = dataSerializer.deserialize(
                new String(connection.getZooKeeper().getData(
                        nodeName,
                        false,
                        connection.getZooKeeper().exists(nodeName, false))
                ), BaseZookeeperModel.class);
        return data;
    }

    public Stat setData(String nodeName, BaseZookeeperModel data) throws KeeperException, InterruptedException {
        return connection.getZooKeeper().setData(nodeName, dataSerializer.serializeData(data).getBytes(), 0);
    }

//    @Scheduled(fixedDelay = 1000*60*60)
    public void empty() {}

    private CreateMode getCreateMode(NodeTypes nodeType) {
        switch (nodeType) {
            case persistent:
                return CreateMode.PERSISTENT;
            case ephemeral:
                return CreateMode.EPHEMERAL;
            case persistent_seqeuencial:
                return CreateMode.PERSISTENT_SEQUENTIAL;
            case ephemeral_sequencial:
                return CreateMode.EPHEMERAL_SEQUENTIAL;
            default:
                return CreateMode.PERSISTENT;
        }
    }
}
