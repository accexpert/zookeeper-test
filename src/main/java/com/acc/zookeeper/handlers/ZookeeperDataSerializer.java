package com.acc.zookeeper.handlers;

import com.acc.zookeeper.models.BaseZookeeperModel;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Author: Cristi Ando Ciupav
 * Date: 14/01/2019
 * Email: accexpert@gmail.com
 */
@Component
public class ZookeeperDataSerializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperDataSerializer.class);
    private Gson gson;

    public ZookeeperDataSerializer() {
        gson = new Gson();
        LOGGER.info(this.getClass().getSimpleName()+" created.");
    }

    public String serializeData(BaseZookeeperModel data) {
        return gson.toJson(data);
    }

    public BaseZookeeperModel deserialize(String data, Class<? extends BaseZookeeperModel> type) {
        return gson.fromJson(data, type);
    }
}
