package com.acc.zookeeper;

import com.acc.zookeeper.config.ApplicationConfig;
import com.acc.zookeeper.handlers.ZookeeperConnectionHandler;
import com.acc.zookeeper.models.NodeTypes;
import com.acc.zookeeper.handlers.ZookeeperOperationHandler;
import com.acc.zookeeper.models.BaseZookeeperModel;
import com.acc.zookeeper.utils.Constants;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

public class StartMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartMain.class);

    public static void main(String[] args) throws KeeperException, InterruptedException {
        new StartMain().testExistsNode();
//        new StartMain().testEphefemeralNode();
//        new StartMain().testCreateNode();
    }

    private void testCreateNode() throws KeeperException, InterruptedException {
        ApplicationContext context = startSpring();
        connectZookeeperServer(context);
        ZookeeperOperationHandler operationHandler = context.getBean("zookeeperOperationHandler", ZookeeperOperationHandler.class);
        LOGGER.info("Node created: "+operationHandler.createNewNode(NodeTypes.persistent, Constants.ROOT_NODE_NAME, createModel()));
        disconnectZookeeperServer(context);
    }

    private void testRemoveNode() throws KeeperException, InterruptedException {
        ApplicationContext context = startSpring();
        connectZookeeperServer(context);
        ZookeeperOperationHandler operationHandler = context.getBean("zookeeperOperationHandler", ZookeeperOperationHandler.class);
        operationHandler.deleteNode(Constants.ROOT_NODE_NAME);
        LOGGER.info("Node deleted");
        Stat stat = operationHandler.checkIfNodeExists(Constants.ROOT_NODE_NAME);
        printStatisticsStructure(stat);
        disconnectZookeeperServer(context);
    }

    private void testExistsNode() throws KeeperException, InterruptedException {
        ApplicationContext context = startSpring();
        connectZookeeperServer(context);
        ZookeeperOperationHandler operationHandler = context.getBean("zookeeperOperationHandler", ZookeeperOperationHandler.class);
        Stat stat = operationHandler.checkIfNodeExists(Constants.ROOT_NODE_NAME);
        printStatisticsStructure(stat);
        disconnectZookeeperServer(context);
    }

    private void testEphefemeralNode() throws KeeperException, InterruptedException {
        ApplicationContext context = startSpring();
        connectZookeeperServer(context);
        ZookeeperOperationHandler operationHandler = context.getBean("zookeeperOperationHandler", ZookeeperOperationHandler.class);
        LOGGER.info("Node created: "+operationHandler.createNewNode(NodeTypes.ephemeral, Constants.ROOT_NODE_NAME, createModel()));
        Stat stat = operationHandler.checkIfNodeExists(Constants.ROOT_NODE_NAME);
        printStatisticsStructure(stat);
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
        LOGGER.info("Node created: "+operationHandler.createNewNode(NodeTypes.ephemeral, Constants.ROOT_NODE_NAME, createModel()));
        Stat stat = operationHandler.checkIfNodeExists(Constants.ROOT_NODE_NAME);
        printStatisticsStructure(stat);
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

    private void printStatisticsStructure(Stat stat) {
        if(null != stat) {
            LOGGER.info("\n------------------------------------------------------------------------------------------------------\n"
                    + "Statistics info:\n"
                    + "Creation transaction id: " + stat.getCzxid()+"\n"
                    + "Creation time: " + new SimpleDateFormat("dd-MM-yyyy hh:mm:ss").format(new Date(stat.getCtime()))+"\n"
                    + "Modify transaction id: " + stat.getMzxid()+"\n"
                    + "Modify time: " + new SimpleDateFormat("dd-MM-yyyy hh:mm:ss").format(new Date(stat.getMtime()))+"\n"
                    + "Number of changes to the data of this znode: "+stat.getVersion()+"\n"
                    + "Number of changes to the children of this znode: "+stat.getCversion()+"\n"
                    + "Number of changes to the ACL of this znode: "+stat.getAversion()+"\n"
                    + "Is an ephemeral node: "+(stat.getEphemeralOwner()!=0)+"\n"
                    + (stat.getEphemeralOwner()!=0 ? "Session id of this ephemeral znode: "+stat.getEphemeralOwner()+"\n" : "")
                    + "Data length stored in this znode: "+stat.getDataLength()+"\n"
                    + "Number of children znodes for this znode: "+stat.getNumChildren()
                    + "\n------------------------------------------------------------------------------------------------------\n"
            );
        } else {
            LOGGER.info("\n------------------------------------------------------------------------------------------------------\n"
                    + "Statistics for node is null"
                    + "\n------------------------------------------------------------------------------------------------------\n");
        }
    }
}
