package com.xt.hadoop.hdfs.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by XT on 2018/5/6.
 */
public class SequenceFileUtilTest {
    Configuration conf ;
    @Before
    public void init(){
         conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://myserver");
        conf.set("dfs.nameservices","myserver");
        conf.set("dfs.ha.namenodes.myserver","nn1,nn2");
        conf.set("dfs.namenode.rpc-address.myserver.nn1","hadoop1:9000");
        conf.set("dfs.namenode.rpc-address.myserver.nn2","hadoop2:9000");
        conf.set("dfs.client.failover.proxy.provider.myserver","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("username","hadoop");
    }

    public void initKrb() throws IOException {
        System.setProperty("java.security.krb5.conf","/etc/krb5.conf");
        Configuration conf = new Configuration();
        conf.set("dfs.nameservices","myserver");
        conf.set("dfs.ha.namenodes.myserver","nn1,nn2");
        conf.set("dfs.namenode.rpc-address.myserver.nn1","hadoop1:9000");
        conf.set("dfs.namenode.rpc-address.myserver.nn2","hadoop2:9000");
        conf.set("dfs.client.failover.proxy.provider.myserver","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("username","hadoop");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("hdfs/hadoop1@HD.POWERRICH.COM","/HTTP.keytab");
    }

    @Test
    public void testUploadFile() throws Exception{
        Map<String,InputStream> map = new HashMap();
        map.put("1",new FileInputStream(new File("D:\\11.txt")));
        SequenceFileUtil.upLoadFile(conf,map,"hdfs://myserver/11.file");
    }

    @Test
    public void testDownload() throws Exception{
        SequenceFileUtil.downLoadFile(conf,"hdfs://myserver/11.file","D:\\122.txt","1");
    }

}
