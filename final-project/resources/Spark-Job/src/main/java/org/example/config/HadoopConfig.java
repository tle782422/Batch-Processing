package org.example.config;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

public class HadoopConfig {

    public FileSystem initFileSystem() throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.client.use.datanode.hostname", true);
        conf.setBoolean("dfs.datanode.use.datanode.hostname", true);
        conf.set("fs.defaultFS", "hdfs://namenode:9000");
        conf.set("dfs.replication", "3");
        conf.set("dfs.blocksize", "128m");
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("root"));
        return FileSystem.get(conf);
    }

    // Làm việc với RDD
    public static Configuration configureHadoop(JavaSparkContext sc) {
        Configuration hadoopConf = sc.hadoopConfiguration();
        hadoopConf.setInt(FileSystem.FS_DEFAULT_NAME_KEY + ".min.replication", 1);
        hadoopConf.setBoolean("dfs.client.use.datanode.hostname", true);
        hadoopConf.setBoolean("dfs.datanode.use.datanode.hostname", true);
        hadoopConf.set("fs.defaultFS", "hdfs://namenode:9000");
        hadoopConf.set("dfs.replication", "1");
        hadoopConf.set("dfs.blocksize", "128m");
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("TLe"));
        return hadoopConf;
    }

    // Làm việc với DataFrame hoặc SPARK SQL
    public static Configuration configureHadoop() {
        Configuration hadoopConf = new Configuration();
        hadoopConf.setInt(FileSystem.FS_DEFAULT_NAME_KEY + ".min.replication", 1);
        hadoopConf.setBoolean("dfs.client.use.datanode.hostname", true);
        hadoopConf.setBoolean("dfs.datanode.use.datanode.hostname", true);
        hadoopConf.set("fs.defaultFS", "hdfs://namenode:9000");
        hadoopConf.set("dfs.replication", "1");
        hadoopConf.set("dfs.blocksize", "128m");
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("TLe"));
        return hadoopConf;
    }

}
