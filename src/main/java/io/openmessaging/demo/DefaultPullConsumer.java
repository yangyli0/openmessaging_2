package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;


import java.nio.MappedByteBuffer;
import java.util.*;

/**
 * Created by lee on 5/16/17.
 */
public class DefaultPullConsumer implements PullConsumer{
    private KeyValue properties;
    private String queue;
    private List<String> bucketList = new ArrayList<>();

    private int curProducer = 0;    // producer下标

    //private int msgCnt = 0;
    private MappedByteBuffer mapBuf = null;


    private Map<String, MessageFile> messageFileMap = null;
    private MessageBroker messageBroker;




    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;

        messageBroker = MessageBroker.getInstance(properties);
        messageFileMap = new HashMap<>(messageBroker.producerList.size());
    }



    @Override public KeyValue properties() { return properties; }

    @Override public  Message poll() {
        Message message = null;
        while (curProducer < messageBroker.producerList.size()) {
            String producerId = messageBroker.producerList.get(curProducer);
            message = pullMessage(producerId);
            if (message != null) {
                return message;
            }
            curProducer++;
        }

        return message;
    }




    public Message pullMessage(String producerId) {
        if (!messageFileMap.containsKey(producerId))
            messageFileMap.put(producerId, new MessageFile(properties, producerId));

        mapBuf = messageFileMap.get(producerId).mappedByteBuffer;
        while (true) {
            int i = mapBuf.position();
            if (i < mapBuf.capacity() && mapBuf.get(i) == 0)    break;

            for (; i < mapBuf.capacity() && mapBuf.get(i) != 44; i++);  // 找到分隔符
            byte[] tag = new byte[i-mapBuf.position()];
            mapBuf.get(tag, 0, tag.length);
            mapBuf.get();   // 跳过','
            int msgLen =mapBuf.getInt();

            /*
            i = mapBuf.position();
            for (; i < mapBuf.capacity() && mapBuf.get(i) !=44; i++);
            byte[] lenBytes = new byte[i - mapBuf.position()];
            mapBuf.get(lenBytes, 0, lenBytes.length);
            mapBuf.get();   //跳过','
            int msgLen = Integer.parseInt(new String(lenBytes));
            */


            if (bucketList.contains(new String(tag))) { // 消息topic是当前消费者订阅的topic
                byte[] msgBytes = new byte[msgLen];
                mapBuf.get(msgBytes, 0, msgLen);
                return assemble(msgBytes);
            }

            int cursor = mapBuf.position(); // 读完消息的长度后的位置
            mapBuf.position(cursor + msgLen);   // 跳过当前消息
        }
        return null;
    }




    public Message assemble(byte[] msgBytes) {
        int i, j;
        // 获取property

        DefaultKeyValue property = new DefaultKeyValue();
        for (i = 0; i < msgBytes.length && msgBytes[i] != ','; i++);
        byte[] propertyBytes = Arrays.copyOfRange(msgBytes, 0, i);  // [start, end)
        insertKVs(propertyBytes, property);
        j = ++i; // 跳过","

        // 获取headers
        DefaultKeyValue header = new DefaultKeyValue();
        for (; i < msgBytes.length && msgBytes[i] != ','; i++);
        byte[] headerBytes = Arrays.copyOfRange(msgBytes, j, i);
        insertKVs(headerBytes, header);
        j = ++i; // 跳过","

        // 获取body
        for (; i < msgBytes.length && msgBytes[i] != '\n'; i++);
        byte[] body = Arrays.copyOfRange(msgBytes, j, i);

        String queueOrTopic = header.getString(MessageHeader.TOPIC);
        DefaultBytesMessage message = null;
        DefaultMessageFactory messageFactory = new DefaultMessageFactory();
        if (queueOrTopic != null)
            message = (DefaultBytesMessage) messageFactory.createBytesMessageToTopic(queueOrTopic, body);
        else
            message = (DefaultBytesMessage) messageFactory.createBytesMessageToQueue(queueOrTopic, body);

        message.setHeaders(header);
        message.setProperties(property);

        return message;

    }



    public void insertKVs(byte[] kvBytes, KeyValue map) {
        String kvStr = new String(kvBytes);
        String[] kvPairs = kvStr.split("\\|");
        for (String kv: kvPairs) {

            String[] tuple = kv.split("#");

            if(tuple[1].startsWith("i"))
                map.put(tuple[0], Integer.parseInt(tuple[1].substring(1)));
            else if(tuple[1].startsWith("d"))
                map.put(tuple[0], Double.parseDouble(tuple[1].substring(1)));
            else if (tuple[1].startsWith("l"))
                map.put(tuple[0], Long.parseLong(tuple[1].substring(1)));
            else
                map.put(tuple[0], tuple[1].substring(1));

        }

    }


    @Override public Message poll(KeyValue properties) { throw new UnsupportedOperationException("Unsupported"); }

    @Override public  void attachQueue(String queueName, Collection<String> topics) {   //TODO:同步关键字可去
        if (queue != null && !queue.equals(queueName))
            throw new ClientOMSException("You have already attached to a queue: " + queue);
        queue = queueName;

        bucketList.addAll(topics);
        bucketList.add(queueName);

    }

    @Override public void ack(String messageId) { throw new UnsupportedOperationException("Unsupported"); }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

}
