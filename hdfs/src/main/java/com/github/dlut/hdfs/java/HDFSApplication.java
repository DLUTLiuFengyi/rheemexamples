package com.github.dlut.hdfs.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class HDFSApplication {

    //获取配置对象
    private FileSystem getFiledSystem() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem;
    }

    //读取文件
    private void readHDFSFile(String filePath) {
        FSDataInputStream fsDataInputStream = null;
        try{
            Path path = new Path(filePath);
            fsDataInputStream = this.getFiledSystem().open(path);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(fsDataInputStream != null) {
                IOUtils.closeStream(fsDataInputStream);
            }
        }
    }

    //上传文件
    private void writeHDFSFile(String localPath, String hdfsPath) {
        FSDataOutputStream outputStream = null;
        FileInputStream fileInputStream = null;

        try{
            Path path = new Path(hdfsPath);
            outputStream = this.getFiledSystem().create(path);
            fileInputStream = new FileInputStream(new File(localPath));
            //输入流，输出流，缓冲区大小，是否关闭数据流，如果false就在finally里关闭
            IOUtils.copyBytes(fileInputStream, outputStream, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(fileInputStream != null) {
                IOUtils.closeStream(fileInputStream);
            }
            if (outputStream != null) {
                IOUtils.closeStream(outputStream);
            }
        }
    }

    public static void main(String[] args) {
        HDFSApplication hdfsApplication = new HDFSApplication();

        String localPath = "D:/rheem/tmp.txt";
        String hdfsPath = "hdfs://10.141.221.217:9000/firstin/tmpfile.txt";
        hdfsApplication.writeHDFSFile(localPath, hdfsPath);
    }
}
