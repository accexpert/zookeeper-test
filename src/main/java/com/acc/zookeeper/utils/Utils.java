package com.acc.zookeeper.utils;

import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static void printStatisticsStructure(Stat stat) {
        printStatisticsStructure(stat, null);
    }

    public static void printStatisticsStructure(Stat stat, String path) {
        if(null != stat) {
            LOGGER.info("\n------------------------------------------------------------------------------------------------------\n"
                    + "Statistics info:\n"
                    + (null!=path ? "Path: " + path + "\n" : "")
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
