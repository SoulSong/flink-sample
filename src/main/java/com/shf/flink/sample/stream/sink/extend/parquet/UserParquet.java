package com.shf.flink.sample.stream.sink.extend.parquet;


import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import java.io.Serializable;

/**
 * @author songhaifeng
 */
public class UserParquet implements IndexedRecord, Serializable {
    private String name;
    private Integer age;
    private String sex;
    private String address;

    public UserParquet() {
    }

    public UserParquet(String name, Integer age, String sex, String address) {
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.address = address;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public void put(int i, Object o) {
    }

    /**
     * index value depends on keys order in user.avsc file
     *
     * @param i parameter index
     * @return value
     */
    @Override
    public Object get(int i) {
        switch (i) {
            case 0:
                return name;
            case 1:
                return age;
            case 2:
                return sex;
            case 3:
                return address;
            default:
                return null;
        }
    }

    @Override
    public Schema getSchema() {
        return null;
    }

    @Override
    public String toString() {
        return "UserParquet{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", sex='" + sex + '\'' +
                ", address='" + address + '\'' +
                '}';
    }
}