package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by lee on 5/16/17.
 */
public class DefaultKeyValue implements KeyValue {
    private final Map<String, Object> kvs = new HashMap<>();


    @Override
    public KeyValue put(String key, int value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        kvs.put(key, value);
        return this;
    }



    @Override
    public int getInt(String key) {
        return (Integer)kvs.getOrDefault(key, 0);
    }

    @Override
    public long getLong(String key) {
        return (Long)kvs.getOrDefault(key, 0L);
    }

    @Override
    public double getDouble(String key) {
        return (Double)kvs.getOrDefault(key, 0.0d);
    }

    @Override
    public String getString(String key) {
        return (String)kvs.getOrDefault(key, null);
    }



    @Override
    public Set<String> keySet() {
        return kvs.keySet();
    }

    @Override
    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }


    public boolean isInt(String key) {
        Object obj = kvs.get(key);
        return (obj instanceof  Integer);
    }

    public boolean isDouble(String key) {
        Object obj = kvs.get(key);
        return (obj instanceof  Double);
    }

    public boolean isLong(String key) {
        Object obj = kvs.get(key);
        return (obj instanceof  Long);
    }

    public boolean isString(String key) {
        Object obj = kvs.get(key);
        return (obj instanceof String);
    }

    public String getValue(String key) {
        return (String)kvs.get(key);
    }

    public byte[] getBytes() {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<String, Object>entry: kvs.entrySet()) {
            sb.append(entry.getKey());
            sb.append("#");
            sb.append(entry.getValue());
            sb.append("|");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(",");
        return sb.toString().getBytes();
    }
}
