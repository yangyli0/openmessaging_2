package io.openmessaging.demo;


import io.openmessaging.KeyValue;



import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lee on 5/29/17.
 */
public class MessageStore {
    //ConcurrentHashMap<String, Set<String>> bucketTable;
    Map<String, Set<String>> bucketTable;

    private static AtomicInteger numOfProducer = new AtomicInteger(0);
    private static AtomicInteger finishCnt = new AtomicInteger(0);

    //private FileChannel indexFileChannel;
    //private MappedByteBuffer indexMapBuf;
    //private final String indexFileName = "index";
    //private final int INDEX_BUFFER_SIZE = 1024 * 1024;

    private volatile  static MessageStore INSTANCE = null;
    private KeyValue properties;
    private MessageStore(KeyValue properties) {
        this.properties = properties;
        //bucketTable = new ConcurrentHashMap<>();
        bucketTable = new HashMap<>();
    }


    public static MessageStore getInstance(KeyValue properties) {
        numOfProducer.incrementAndGet();
        if (INSTANCE == null) {
            synchronized (MessageStore.class) {
                if (INSTANCE == null) {
                    INSTANCE = new MessageStore(properties);
                }
            }
        }
        return INSTANCE;
    }


    /*
    public void addProducer(String bucket, String producer) {
        if (!bucketTable.containsKey(bucket))
            bucketTable.put(bucket, new HashSet<>());
        Set<String> producerSet = bucketTable.get(bucket);
        if (!producerSet.contains(producer))
            producerSet.add(producer);
    }
    */

    // topic->生产者对应关系写盘
    public void writeIndexFile() {
        int cnt = finishCnt.incrementAndGet();
        int totalProducer = numOfProducer.get();
        if (cnt == totalProducer) {
            synchronized (finishCnt) {
                if (cnt == totalProducer) {
                    writeKvs();
                }
            }
        }
    }

    public void writeKvs() {
        String absPath = properties.getString("STORE_PATH")+"/"+ "index";
        RandomAccessFile raf = null;
        byte[] indexBytes = getListMapBytes();
        try {
            raf = new RandomAccessFile(absPath, "rw");
            FileChannel fc = raf.getChannel();
            MappedByteBuffer mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, indexBytes.length);
            mapBuf.put(indexBytes);

        } catch (IOException e) { e.printStackTrace();}
    }

    public byte[] getListMapBytes() {
        StringBuilder  sb = new StringBuilder();
        for (Map.Entry<String, Set<String>> entry : bucketTable.entrySet()) {
            sb.append(entry.getKey());
            sb.append(':');
            for (String pro: entry.getValue()) {
                sb.append(pro);
                sb.append(',');
            }
            sb.deleteCharAt(sb.length()-1);
            sb.append('\n');
        }
        return sb.toString().getBytes();
    }





}
