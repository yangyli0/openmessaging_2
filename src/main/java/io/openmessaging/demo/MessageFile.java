package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lee on 5/16/17.
 */

public class MessageFile {
    private KeyValue properties;
    private String fileName;

    private final int BUFFER_SIZE = 512 * 1024 * 1024;
    MappedByteBuffer mappedByteBuffer = null;

    public MessageFile(KeyValue properties, String fileName) {
        this.properties = properties;
        this.fileName = fileName;
        loadFile();
    }

    public void loadFile() {
        String absPath = properties.getString("STORE_PATH")+"/" + fileName;
        RandomAccessFile raf = null;
        //BufferedRandomAccessFile raf = null;
        try {
            //raf = new BufferedRandomAccessFile(absPath, "r");
            raf = new RandomAccessFile(absPath, "r");
            FileChannel fc = raf.getChannel();
            mappedByteBuffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, BUFFER_SIZE);
        } catch (IOException e) { e.printStackTrace();}
    }
}
/*
public class MessageFile {
    private KeyValue properties;
    private String fileName;

    private final int BUFFER_SIZE =  512 * 1024 * 1024;
    private List<MappedByteBuffer> mapBufList = new ArrayList<>();
    //private MappedByteBuffer mapBuf = null;

    public MessageFile(KeyValue properties, String fileName) {
        this.properties = properties;
        this.fileName = fileName;
        loadFile(); // 将文件分成MappedByteBuffer
    }

    public List<MappedByteBuffer> getMapBufList() {
        return this.mapBufList;
    }

    public void loadFile() {
        String absPath = properties.getString("STORE_PATH")+ "/" +fileName;
        //RandomAccessFile raf = null;
        BufferedRandomAccessFile raf = null;
        try {
            //raf = new RandomAccessFile(absPath, "r");
            raf = new BufferedRandomAccessFile(absPath, "r");
            FileChannel fc = raf.getChannel();

            for (long i = 0; i < fc.size(); i += BUFFER_SIZE) { // i需要是long型，i 最后等于文件大小
                MappedByteBuffer mapBuf = null;
                if ( i + BUFFER_SIZE > fc.size())
                    mapBuf = fc.map(FileChannel.MapMode.READ_ONLY, i, fc.size() - i);
                else
                    mapBuf = fc.map(FileChannel.MapMode.READ_ONLY, i, BUFFER_SIZE);

                mapBufList.add(mapBuf);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
*/















