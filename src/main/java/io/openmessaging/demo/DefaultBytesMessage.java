package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

/**
 * Created by lee on 5/16/17.
 */
public class DefaultBytesMessage implements BytesMessage {

    private KeyValue headers = new DefaultKeyValue();
    private byte[] body;
    private KeyValue properties;



    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }
    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        headers.put(key, "i"+value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, "l"+value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        headers.put(key, "d"+value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, "s"+value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, "i"+value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, "l"+value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, "d"+value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, "s"+value);
        return this;
    }

    public void setHeaders(DefaultKeyValue headers) {
        this.headers = headers;
    }

    public void setProperties(DefaultKeyValue properties) {
        this.properties = properties;
    }
}
