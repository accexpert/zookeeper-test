package com.acc.zookeeper.config;

import com.acc.zookeeper.handlers.DataChangeWatcher;
import com.acc.zookeeper.handlers.ZookeeperConnectionHandler;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Author: Cristi Ando Ciupav
 * Date: 14/01/2019
 * Email: accexpert@gmail.com
 */
@Configuration
@SpringBootApplication
@ComponentScan(value = "com.acc.zookeeper")
public class ApplicationConfig {

    @Bean
    public TaskExecutor threadTaskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public CommandLineRunner dataChangeWatcherThread(TaskExecutor threadTaskExecutor, final ZookeeperConnectionHandler zookeeperConnectionHandler) {
        return strings -> threadTaskExecutor.execute(new DataChangeWatcher(zookeeperConnectionHandler));
    }
}
