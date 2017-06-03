package io.openmessaging.demo;


import io.openmessaging.KeyValue;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lee on 5/31/17.
 */
public class MessageBroker {
    private KeyValue properties;
    private static volatile MessageBroker INSTANCE = null;
    List<String> producerList = new ArrayList<>();
    public MessageBroker(KeyValue properties)  {
        this.properties = properties;
        getFileSet();
    }

    private void getFileSet() {
        String absPath = properties.getString("STORE_PATH");
        File dir = new File(absPath);
        File[] producerFiles = null;

        producerFiles = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("Thread-");
            }
        });
        for(File producerFile: producerFiles)
            producerList.add(producerFile.getName());

    }





    public static MessageBroker getInstance(KeyValue properties) {
        if (INSTANCE == null) {
            synchronized (MessageBroker.class) {
                if (INSTANCE == null)
                    INSTANCE  = new MessageBroker(properties);
            }
        }
        return INSTANCE;
    }




}
