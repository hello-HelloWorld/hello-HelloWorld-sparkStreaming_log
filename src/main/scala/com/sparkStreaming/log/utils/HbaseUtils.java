package com.sparkStreaming.log.utils;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/7/10 15:54
 */

import javafx.scene.control.TableColumnBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * Hbase工具类，用于：
 * 连接HBase，获得表实例
 */
public class HbaseUtils {

    private Configuration configuration = null;
    private Connection connection = null;
    private static HbaseUtils instance = null;

    /*
     * 在私有构造方法中舒适化属性
     * */
    private HbaseUtils() {
        try {
            configuration = new Configuration();
            //指定要访问的zk服务器
            configuration.set("hbase.zookeeper.quorum", "hadoop102:2181");
            //得到hbase的连接
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
     * 获得hbase连接实例
     * */
    public static synchronized HbaseUtils getInstance() {
        if (instance==null){
            instance = new HbaseUtils();
        }
        return instance;
    }

    /*
    * 由表名得到一个表的实例
    * */
    public HTable getHtable(String tableName){
        HTable table = null;
        try {
            table = (HTable) connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }
}
