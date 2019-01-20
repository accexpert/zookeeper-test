package com.acc.zookeeper;

import com.acc.zookeeper.config.ApplicationConfig;
import com.acc.zookeeper.handlers.ZookeeperConnectionHandler;
import com.acc.zookeeper.models.NodeTypes;
import com.acc.zookeeper.handlers.ZookeeperOperationHandler;
import com.acc.zookeeper.models.BaseZookeeperModel;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import com.acc.zookeeper.utils.Utils;

import static com.acc.zookeeper.utils.Constants.ROOT_NODE_NAME;

public class StartMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartMain.class);

    public static void main(String[] args) throws KeeperException, InterruptedException {
//        new StartMain().testRemoveNodeWithVersionFailed();
        new StartMain().testExistsNode();
    }

    private void testCreatePersistentNode() throws KeeperException, InterruptedException {
        ApplicationContext context = startSpring();
        connectZookeeperServer(context);
        ZookeeperOperationHandler operationHandler = context.getBean("zookeeperOperationHandler", ZookeeperOperationHandler.class);
        LOGGER.info("Node created: "+operationHandler.createNewNode(NodeTypes.persistent, ROOT_NODE_NAME, createModel()));
        disconnectZookeeperServer(context);
    }

    private void testRemoveNodeNoVersion() throws KeeperException, InterruptedException {
        ApplicationContext context = startSpring();
        connectZookeeperServer(context);
        ZookeeperOperationHandler operationHandler = context.getBean("zookeeperOperationHandler", ZookeeperOperationHandler.class);
        operationHandler.deleteNode(ROOT_NODE_NAME);
        LOGGER.info("Node deleted");
        Stat stat = operationHandler.checkIfNodeExists(ROOT_NODE_NAME);
        Utils.printStatisticsStructure(stat);
        disconnectZookeeperServer(context);
    }

    private void testRemoveNodeWithVersion() throws KeeperException, InterruptedException {
        ApplicationContext context = startSpring();
        connectZookeeperServer(context);
        ZookeeperOperationHandler operationHandler = context.getBean("zookeeperOperationHandler", ZookeeperOperationHandler.class);
        Stat statistic = operationHandler.checkIfNodeExists(ROOT_NODE_NAME);
        operationHandler.deleteNode(ROOT_NODE_NAME, statistic.getVersion());
        LOGGER.info("Node deleted");
        Stat stat = operationHandler.checkIfNodeExists(ROOT_NODE_NAME);
        Utils.printStatisticsStructure(stat);
        disconnectZookeeperServer(context);
    }

    private void testRemoveNodeWithVersionFailed() throws KeeperException, InterruptedException {
        ApplicationContext context = startSpring();
        connectZookeeperServer(context);
        ZookeeperOperationHandler operationHandler = context.getBean("zookeeperOperationHandler", ZookeeperOperationHandler.class);
        Stat statistic = operationHandler.checkIfNodeExists(ROOT_NODE_NAME);
        BaseZookeeperModel model = new BaseZookeeperModel();
        model.setName("Updated node");
        try {
            operationHandler.setData(ROOT_NODE_NAME, model, statistic.getVersion());
            operationHandler.deleteNode(ROOT_NODE_NAME, statistic.getVersion());
            LOGGER.info("Node deleted");
        } catch (KeeperException e) {
            LOGGER.error("Keeper exception: "+e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted exception: "+e.getMessage());
        }
        Stat stat = operationHandler.checkIfNodeExists(ROOT_NODE_NAME);
        Utils.printStatisticsStructure(stat);
        disconnectZookeeperServer(context);
    }

    private void testExistsNode() throws KeeperException, InterruptedException {
        ApplicationContext context = startSpring();
        connectZookeeperServer(context);
        ZookeeperOperationHandler operationHandler = context.getBean("zookeeperOperationHandler", ZookeeperOperationHandler.class);
        Stat stat = operationHandler.checkIfNodeExists(ROOT_NODE_NAME);
        Utils.printStatisticsStructure(stat);
//        operationHandler.setWatch();
//        disconnectZookeeperServer(context);
    }

    private void testEphefemeralNode() throws KeeperException, InterruptedException {
        ApplicationContext context = startSpring();
        connectZookeeperServer(context);
        ZookeeperOperationHandler operationHandler = context.getBean("zookeeperOperationHandler", ZookeeperOperationHandler.class);
        LOGGER.info("Node created: "+operationHandler.createNewNode(NodeTypes.ephemeral, ROOT_NODE_NAME, createModel()));
        Stat stat = operationHandler.checkIfNodeExists(ROOT_NODE_NAME);
        Utils.printStatisticsStructure(stat);
        LOGGER.info("Waiting...");
        //sleep the thread for 10s in order to check from another session of ZK Cli if the node exists and then removed.
        Thread.sleep(30000);
        LOGGER.info("Done waiting. Close session");
        disconnectZookeeperServer(context);
    }

    /**
     * This method test an edge case for which a developer should be aware when using Zookeeper.
     * When an ephemeral znode is created in a client session and the client process is terminated
     * without closing explicitly the session with Zookeeper, the ephemeral znode remains until
     * Zookeeper notice that the client is no longer available. If the client disconnects and
     * reconnects quickly, the ephemeral znode exists set by the old session.
     */
    private void testEphefemeralNodeIssue1() throws KeeperException, InterruptedException {
        ApplicationContext context = startSpring();
        connectZookeeperServer(context);
        ZookeeperOperationHandler operationHandler = context.getBean("zookeeperOperationHandler", ZookeeperOperationHandler.class);
        LOGGER.info("Node created: "+operationHandler.createNewNode(NodeTypes.ephemeral, ROOT_NODE_NAME, createModel()));
        Stat stat = operationHandler.checkIfNodeExists(ROOT_NODE_NAME);
        Utils.printStatisticsStructure(stat);
        LOGGER.info("Waiting...");
        //sleep the thread for 10s in order to check from another session of ZK Cli if the node exists and then removed.
        Thread.sleep(10000);
        LOGGER.info("Done waiting. Close session");
        //now the node can be seen for a limited period of time using ZK Cli of running the program again
        //and using testExistsNode() method
        //the node will be deleted by ZK when the server will notice that the client is no longer online
    }

    // UTILS METHODS
    private ApplicationContext startSpring() {
        SpringApplication application = new SpringApplication(ApplicationConfig.class);
        ApplicationContext context = application.run();
        return context;
    }

    private BaseZookeeperModel createModel() {
        BaseZookeeperModel data = new BaseZookeeperModel();
        data.setName("Cristi");
        return data;
    }

    private void connectZookeeperServer(ApplicationContext context) {
        ZookeeperConnectionHandler connectionHandler = context.getBean("zookeeperConnectionHandler", ZookeeperConnectionHandler.class);
        connectionHandler.connect();
    }

    private void disconnectZookeeperServer(ApplicationContext context) {
        ZookeeperConnectionHandler connectionHandler = context.getBean("zookeeperConnectionHandler", ZookeeperConnectionHandler.class);
        try {
            connectionHandler.disconnect();
        } catch (InterruptedException e) {
            LOGGER.error("Cannot disconnect zookeeper. "+e.getLocalizedMessage());
        }
    }


}
