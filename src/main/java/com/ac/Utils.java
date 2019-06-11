package com.ac;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author aochong
 * @create 2019-06-07 9:17
 **/
public class Utils {
    public static void deleteFileIfexists(String file,Configuration configuration){
        Path path = new Path(file);
        try{
            FileSystem fs = FileSystem.get(configuration);
            if(fs.exists(path)){
                fs.delete(path, true);
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}
