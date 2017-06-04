package io.openmessaging.demo;

import io.openmessaging.*;


import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by lee on 5/16/17.
 */


public class MessageWriter implements Runnable {
    private KeyValue properties;

    private final int MQ_CAPACITY = 10000;
    private BlockingQueue<Message> mq;

    private final int BUFFER_SIZE = 512 * 1024 * 1024;  // TODO: 尝试256
    private FileChannel fc;
    private MappedByteBuffer mapBuf;
    private String producerId;

    //private Map<String, Integer> slotSeat;    //记录下一个要填的槽的位置
    //private Map<String, MetaInfo> firstMsgMeta; //记录第一条消息的位置和长度，　落盘用

    private int msgLen; // 记录当前消息的长度


    public MessageWriter(KeyValue properties) {
        this.properties = properties;
        mq = new LinkedBlockingQueue<>(MQ_CAPACITY);
        //slotSeat = new HashMap<>();
        //firstMsgMeta = new HashMap<>();
    }

    public void run() {
        producerId = Thread.currentThread().getName();
        String absPath = properties.getString("STORE_PATH") + "/" + producerId;
        RandomAccessFile raf = null;
        //BufferedRandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(absPath, "rw");
            //raf = new BufferedRandomAccessFile(absPath, "rw");
            fc = raf.getChannel();
            mapBuf = fc.map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);    // TODO:保证只映射一次
        } catch (IOException e) { e.printStackTrace();}

        while (true) {
            try {
                DefaultBytesMessage message = (DefaultBytesMessage) mq.take();
                if (new String(message.getBody()).equals(""))   break;

                KeyValue property = message.properties();
                KeyValue header = message.headers();

                // 写消息
                byte[] propertyBytes = getKvsBytes(property);
                byte[] headerBytes =getKvsBytes(header);
                byte[] body = message.getBody();
                msgLen = propertyBytes.length + headerBytes.length + body.length;

                String queueOrTopic;
                if (header.containsKey(MessageHeader.TOPIC))
                    queueOrTopic = header.getString(MessageHeader.TOPIC);
                else
                    queueOrTopic = header.getString(MessageHeader.QUEUE);

                queueOrTopic = queueOrTopic.substring(1);

                mapBuf.put((queueOrTopic+",").getBytes());
                mapBuf.putInt(msgLen);

                //mapBuf.put((queueOrTopic+","+msgLen+",").getBytes());

                /*
                StringBuilder sb = new StringBuilder();
                sb.append(queueOrTopic);
                sb.append(",");
                sb.append(msgLen);
                sb.append(",");
                mapBuf.put(sb.toString().getBytes());
                */

                mapBuf.put(propertyBytes);
                mapBuf.put(headerBytes);
                mapBuf.put(body);










            } catch (InterruptedException e) { e.printStackTrace();}
        }


    }

    /*
    public void addTag(String queueOrTopic) {
        int index = queueOrTopic.lastIndexOf('_');
        int offset = Integer.parseInt(queueOrTopic.substring(index+1));
        if (queueOrTopic.startsWith("Q"))
            mapBuf.putChar('Q');
        else
            mapBuf.putChar('T');
        mapBuf.putInt(offset);

        mapBuf.putInt(msgLen);
    }
    */

    public void addMessage(Message message) {
        try {
            mq.put(message);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    public byte[] getKvsBytes(KeyValue kvs) {
        return ((DefaultKeyValue)kvs).getBytes();
    }



}























