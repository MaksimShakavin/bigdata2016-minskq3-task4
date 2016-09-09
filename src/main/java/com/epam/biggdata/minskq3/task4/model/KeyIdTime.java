package com.epam.biggdata.minskq3.task4.model;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyIdTime implements WritableComparable<KeyIdTime>{
    private String iPinionID;
    private long timestamp;

    public KeyIdTime(String iPinionID, long timestamp) {
        this.iPinionID = iPinionID;
        this.timestamp = timestamp;
    }

    public KeyIdTime() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(iPinionID);
        dataOutput.writeLong(timestamp);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        iPinionID = dataInput.readUTF();
        timestamp = dataInput.readLong();
    }

    @Override
    public int compareTo(KeyIdTime o) {
        int idResult = iPinionID.compareToIgnoreCase(o.iPinionID);
        if (idResult != 0) {
            return idResult;
        }
        return Long.compare(timestamp, o.timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyIdTime keyIdTime = (KeyIdTime) o;

        if (timestamp != keyIdTime.timestamp) return false;
        return iPinionID.equals(keyIdTime.iPinionID);

    }

    @Override
    public int hashCode() {
        int result = iPinionID.hashCode();
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }


    //GETTERS AND SETTERS

    public String getiPinionID() {
        return iPinionID;
    }

    public void setiPinionID(String iPinionID) {
        this.iPinionID = iPinionID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
