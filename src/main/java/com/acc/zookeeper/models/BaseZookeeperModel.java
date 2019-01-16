package com.acc.zookeeper.models;

public class BaseZookeeperModel {
    private String name;

    public BaseZookeeperModel() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "BaseZookeeperModel{" +
                "name='" + name + '\'' +
                '}';
    }
}
