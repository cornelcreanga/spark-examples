package com.ccreanga.spark.examples.iceberg;

import com.ccreanga.spark.examples.util.UuidUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.Instant;

public class SensorPing implements Serializable {

    byte[] customerId;
    byte[] sensorId;
    int sensorType;
    double latitude;
    double longitude;
    Instant createdTimestamp;
    Instant receivedTimestamp;

    SensorPing() {
    }

    public SensorPing(byte[] customerId, byte[] sensorId, int sensorType, double latitude, double longitude, long createdTimestamp, long receivedTimestamp) {
        this.customerId = customerId;
        this.sensorId = sensorId;
        this.sensorType = sensorType;
        this.latitude = latitude;
        this.longitude = longitude;
        this.createdTimestamp = Instant.ofEpochMilli(createdTimestamp);
        this.receivedTimestamp = Instant.ofEpochMilli(receivedTimestamp);
    }

    public byte[] getCustomerId() {
        return customerId;
    }

    public void setCustomerId(byte[] customerId) {
        this.customerId = customerId;
    }

    public byte[] getSensorId() {
        return sensorId;
    }

    public void setSensorId(byte[] sensorId) {
        this.sensorId = sensorId;
    }

    public int getSensorType() {
        return sensorType;
    }

    public void setSensorType(int sensorType) {
        this.sensorType = sensorType;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public Instant getCreatedTimestamp() {
        return createdTimestamp;
    }

    public void setCreatedTimestamp(Instant createdTimestamp) {
        this.createdTimestamp = createdTimestamp;
    }

    public Instant getReceivedTimestamp() {
        return receivedTimestamp;
    }

    public void setReceivedTimestamp(Instant receivedTimestamp) {
        this.receivedTimestamp = receivedTimestamp;
    }

    @Override
    public String toString() {
        return "SensorPing{" +
                "customerId=" + UuidUtils.asUuid(customerId) +
                ", sensorId=" + UuidUtils.asUuid(sensorId) +
                ", sensorType=" + sensorType +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", createdTimestamp=" + createdTimestamp +
                ", receivedTimestamp=" + receivedTimestamp +
                '}';
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.write(customerId);
        out.write(sensorId);
        out.write(sensorType);
        out.writeDouble(latitude);
        out.writeDouble(longitude);
        out.writeLong(createdTimestamp.toEpochMilli());
        out.writeLong(receivedTimestamp.toEpochMilli());
    }

    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
        customerId = new byte[16];
        in.readFully(customerId, 0, 16);
        sensorId = new byte[16];
        in.readFully(sensorId, 0, 16);
        sensorType = in.readInt();
        latitude = in.readDouble();
        longitude = in.readDouble();
        createdTimestamp = Instant.ofEpochMilli(in.readLong());
        receivedTimestamp = Instant.ofEpochMilli(in.readLong());
    }
}