package com.example.roseDB_Example.config;

import jakarta.annotation.PreDestroy;
import main.java.com.rosedb.db.DB;
import main.java.com.rosedb.options.Options;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
public class RoseDBConfig {

    private DB db;

    @Bean
    public DB roseDB() {
        try {
            // 定义数据库文件夹路径
            Path dirPath = Paths.get("rosedb_data");

            // 检查路径是否存在，如果不存在则创建
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }

            System.out.println("Database directory path: " + dirPath.toString());

            // 配置 RoseDB 的选项
            Options options = new Options(
                    dirPath.toString(),          // dirPath: 数据库文件路径
                    1024 * 1024 * 100,           // segmentSize: 每个数据段的大小 (100MB)
                    true,                        // sync: 是否同步写入磁盘
                    1024,                        // bytesPerSync: 每字节同步的数量
                    1000,                        // watchQueueSize: 事件监听队列的大小
                    "0 0 * * *");                // autoMergeCronExpr: 数据合并的Cron表达式 (每天凌晨)

            System.out.println("Options configured.");

            // 打开并返回 RoseDB 实例
            db = DB.open(options);

            System.out.println("DB instance opened successfully.");

            return db;
        } catch (Exception e) {
            e.printStackTrace(); // 打印异常信息
            throw new RuntimeException("Failed to initialize RoseDB", e); // 抛出运行时异常
        }
    }

}

