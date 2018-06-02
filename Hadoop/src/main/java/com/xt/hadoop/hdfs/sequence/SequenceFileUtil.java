package com.xt.hadoop.hdfs.sequence;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by XT on 2018/5/5.
 */
public class SequenceFileUtil {

    /**
     * 上传序列化文件
     * @param conf
     * @param fileStream
     * @param hdfsPath
     * @return
     * @throws Exception
     */
    public static boolean upLoadFile(Configuration conf, Map<String, InputStream> fileStream, String hdfsPath) throws Exception {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf,"root");
        Path path = new Path(hdfsPath);
        Writer writer = null;
        if (fs.exists(path)) {
            //文件存在
            return false;
        }
        try {
            writer = new Writer(fs, conf, path, Text.class, BytesWritable.class);
            WriterAppend(fileStream, writer);
            return true;
        } catch (Exception e) {
            throw e;
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    /**
     * 下载一个文件
     * @param conf
     * @param hdfsPath
     * @param localFilePath
     * @param fileName
     * @return
     * @throws Exception
     */
    public static boolean downLoadFile(Configuration conf,String hdfsPath,String localFilePath,String fileName) throws Exception{
        Path path = new Path(hdfsPath);
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = null;
        FileOutputStream fout = null;
        try {
            if (StringUtils.isEmpty(fileName)){
                throw new Exception("文件名称不能为空！");
            }
            if (!fs.exists(path)){
                throw new Exception("文件不存在！");
            }

            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            BytesWritable value = (BytesWritable)ReflectionUtils.newInstance(reader.getValueClass(),conf);
             fout = new FileOutputStream(localFilePath);
             while (reader.next(key,value)){
                 if (key.toString().equals(fileName)){
                     fout.write(value.getBytes(),0,value.getLength());
                 }
             }
             return true;
        }catch (Exception e){
            throw e;
        }finally {
            IOUtils.closeStream(fout);
            IOUtils.closeStream(reader);
        }
    }

    /**
     * 修改序列化的文件(新增/修改)
     * @param conf
     * @param fileStreamMap
     * @param hdfsPath
     * @param isAppend
     * @return
     * @throws Exception
     */
    public static boolean updateSequenceFile(Configuration conf,Map<String,InputStream> fileStreamMap,String hdfsPath,boolean isAppend) throws Exception{
        SequenceFile.Reader reader = null;
        Writer writer = null;
        try {
            FileSystem fs = FileSystem.get(conf);

            String trashPath = moveFileToTrash(conf, hdfsPath);
            reader = new SequenceFile.Reader(fs,new Path(trashPath), conf);
            writer = new Writer(fs,conf,new Path(hdfsPath),Text.class,BytesWritable.class);

            //读取旧的数据加到新的流中
            Writable oldKey = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            BytesWritable oldValue = (BytesWritable)ReflectionUtils.newInstance(reader.getValueClass(),conf);

            while (reader.next(oldKey,oldValue)){
                if (!isAppend){
                    if (fileStreamMap.containsKey(oldKey.toString())){
                        //如果循环写入时遇到要修改的文件，则跳过写入
                        continue;
                    }
                }
                writer.append(oldKey,oldValue);
            }
            //追加新的文件数据
            WriterAppend(fileStreamMap, writer);
            return true;
        }catch (Exception e){
            throw e;
        }finally {
            IOUtils.closeStream(reader);
            IOUtils.closeStream(writer);
        }
    }

    //填充writer
    private static void WriterAppend(Map<String, InputStream> fileStreamMap, Writer writer) throws IOException {
        Set<String> keySet = fileStreamMap.keySet();
        Iterator<String> iterator = keySet.iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            Text textKey = new Text(key);
            InputStream is = fileStreamMap.get(key);
            BytesWritable bw = new BytesWritable(org.apache.commons.io.IOUtils.toByteArray(is));
            writer.append(textKey, bw);
        }
    }

    /**
     * 移动文件到回收站
     * @param conf
     * @param hdfsAddress
     * @return
     * @throws Exception
     */
    public static String moveFileToTrash(Configuration conf, String hdfsAddress) throws Exception {
        String username = conf.get("username");
        String hdfsPath = conf.get("dfs.nameservices");
        String pathUrl = hdfsAddress.replace("hdfs://" + hdfsPath, "");
        String dirUrl = pathUrl.substring(0, hdfsAddress.lastIndexOf("/"));
        String trashDirPath = "hdfs://" + hdfsPath + "/user/" + username + "/.Trash/Current" + dirUrl;
        String trashPath = "hdfs://" + hdfsPath + "/user/" + username + "/.Trash/Current" + pathUrl + new SimpleDateFormat("-yyyyMMddhhmmss").format(new Date());

        Path path = new Path(trashDirPath);
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
        fs.rename(new Path(hdfsAddress), new Path(trashPath));
        return trashPath;
    }
}
