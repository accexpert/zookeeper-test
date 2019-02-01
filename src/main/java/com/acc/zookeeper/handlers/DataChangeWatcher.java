package com.acc.zookeeper.handlers;

import java.util.List;

import com.acc.zookeeper.utils.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Author: Cristi Ando Ciupav
 * Date: 14/01/2019
 * Email: accexpert@gmail.com
 */
public class DataChangeWatcher implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataChangeWatcher.class);
    /**
     * The thread and program will stop when zNode /stop is created.
     * After this node is created, the program will delete this node and end the program.
     */
    private static final String STOP_ZNODE = "/stop";
    private static boolean threadIsRunning = true;
    private ZookeeperConnectionHandler connectionHandler;

    @Autowired
    public DataChangeWatcher(ZookeeperConnectionHandler connectionHandler) {
        this.connectionHandler = connectionHandler;
        LOGGER.info(this.getClass().getSimpleName()+" created.");
    }

    private void setWatcherOnChildrens(String zNode) throws InterruptedException, KeeperException {
        Stat stopNodeExists = connectionHandler.getZooKeeper().exists(STOP_ZNODE, false);
        if(null!=stopNodeExists) {
            connectionHandler.getZooKeeper().delete(STOP_ZNODE, connectionHandler.getZooKeeper().exists(STOP_ZNODE, false).getVersion());
            LOGGER.info("Detecting thread stop received.");
            threadIsRunning = false;
            return;
        }
        setChildrenCreatedWatcherOnNode(zNode);
        List<String> children = connectionHandler.getZooKeeper().getChildren(zNode, false);
        for(String child: children) {
            String path = "/".equals(zNode)? zNode+child : zNode+"/"+child;
            Stat statistics = connectionHandler.getZooKeeper().exists(path, false);
            LOGGER.info("Children: "+path+"; stat: "+(null!=statistics));
            if(null != statistics) {
                setUpdateWatcherOnNode(path);
                setChildrenCreatedWatcherOnNode(path);
            } else {
                setExistsWatcherOnNode(path);
            }
        }
    }

    private void setChildrenCreatedWatcherOnNode(String zNode) throws InterruptedException, KeeperException {
        Watcher watcher = createWatcher(zNode, Watcher.Event.EventType.NodeChildrenChanged);
        connectionHandler.getZooKeeper().getChildren(zNode, watcher);
    }

    private void setUpdateWatcherOnNode(String zNode) throws InterruptedException, KeeperException {
        Watcher watcher = createWatcher(zNode, Watcher.Event.EventType.NodeDataChanged);
        connectionHandler.getZooKeeper().getData(zNode, watcher, connectionHandler.getZooKeeper().exists(zNode, false));
    }

    private void setExistsWatcherOnNode(String zNode) throws InterruptedException, KeeperException {
        Watcher watcher = createWatcher(zNode, Watcher.Event.EventType.NodeCreated);
        connectionHandler.getZooKeeper().exists(zNode, watcher);
    }

    private Watcher createWatcher(String zNode, Watcher.Event.EventType type) {
        LOGGER.info("Create watcher type "+type+ " for znode "+zNode);
        return new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    String path = watchedEvent.getPath();
                    LOGGER.info("Change: " + watchedEvent.getPath()+"; event type: "+watchedEvent.getType());
//                    List<String> children = connectionHandler.getZooKeeper().getChildren(path, false);
//                    for(String a: children) {
//                        Stat stat = connectionHandler.getZooKeeper().exists("/"+a, false);
//                        Utils.printStatisticsStructure(stat, "/"+a);
//                    }
                    if(watchedEvent.getType().equals(Event.EventType.NodeChildrenChanged)) {
                        setWatcherOnChildrens(path);
                    } else if (watchedEvent.getType().equals(Event.EventType.NodeDataChanged)) {
                        setUpdateWatcherOnNode(path);
                    }
                } catch (KeeperException | InterruptedException e) {
                    LOGGER.error("Error: "+e.getMessage());
                }
            }
        };
    }

    @Override
    public void run() {
        try {
            setWatcherOnChildrens("/");
            while (threadIsRunning) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException| KeeperException e) {
            LOGGER.error(e.getMessage());
        }
    }
}
