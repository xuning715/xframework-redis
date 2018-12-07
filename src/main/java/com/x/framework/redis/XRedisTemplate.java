package com.x.framework.redis;

import com.alibaba.fastjson.JSON;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.concurrent.TimeUnit;

public class XRedisTemplate {

    private final static String star = "*";
    private final static String colon = ":";
    private final static String blank = "";
    private final static String SERVICE_KEY = "SERVICE_KEY";

    private RedisTemplate redisTemplate;

    public XRedisTemplate() {
        System.out.println("====================XRedisTemplate has been inited=======================");
    }

    public void setRedisTemplate(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public String get(String key) {
        key = key.replaceAll(colon, blank);
        ValueOperations<String, String> keyValue = redisTemplate.opsForValue();
        return keyValue.get(key);
    }

    public String get(String key, int expireSeconds) {
        key = key.replaceAll(colon, blank);
        ValueOperations<String, String> keyValue = redisTemplate.opsForValue();
        String json = keyValue.get(key);
        if (json != null && json.length() > 0) {
            redisTemplate.expire(key, expireSeconds, TimeUnit.SECONDS);
        }
        return json;
    }

    public Object getAopObject(String key, int expireSeconds) {
        key = key.replaceAll(colon, blank);
        HashOperations keyValue = redisTemplate.opsForHash();
        Object value = keyValue.get(key, key);
        if (value != null) {
            redisTemplate.expire(key, expireSeconds, TimeUnit.SECONDS);
        }
        return value;
    }

    public String set(String key, Object value) {
        String json = null;
        try {
            key = key.replaceAll(colon, blank);
            if (value != null) {
                json = JSON.toJSONString(value);
            }
            if (json != null && json.length() > 0) {
                ValueOperations<String, String> keyValue = redisTemplate.opsForValue();
                keyValue.set(key, json);
            }
        } finally {
            return json;
        }
    }

    public String set(String key, Object value, int expireSeconds) {
        String json = null;
        try {
            key = key.replaceAll(colon, blank);
            if (value != null) {
                json = JSON.toJSONString(value);
            }
            if (json != null && json.length() > 0) {
                ValueOperations<String, String> keyValue = redisTemplate.opsForValue();
                keyValue.set(key, json, expireSeconds, TimeUnit.SECONDS);
            }
        } finally {
            return json;
        }
    }

    public void setAopObject(String key, Object value, int expireSeconds) {
        key = key.replaceAll(colon, blank);
        if (value != null) {
            HashOperations keyValue = redisTemplate.opsForHash();
            keyValue.put(key, key, value);
            redisTemplate.expire(key, expireSeconds, TimeUnit.SECONDS);
        }
    }

    public void delete(String key) {
        redisTemplate.delete(key);
    }

    public void deleteAopObject(String key) {
        redisTemplate.delete(redisTemplate.keys(key + star));
    }

    public void lpush(String key, Object value) {
        key = key.replaceAll(colon, blank);
        String json = JSON.toJSONString(value);
        ListOperations<String, String> list = redisTemplate.opsForList();
        if (json != null && json.length() > 0) {
            list.leftPush(key, json);
        }
    }

    public <T> T rpop(String key, final Class<T> clazz) {
        T value = null;
        try {
            key = key.replaceAll(colon, blank);
            ListOperations<String, String> list = redisTemplate.opsForList();
            String json = list.rightPop(key);
            if (json != null && json.length() > 0) {
                value = JSON.parseObject(json, clazz);
            }
        } finally {
            return value;
        }
    }

}
