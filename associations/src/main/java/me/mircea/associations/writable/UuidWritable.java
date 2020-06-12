package me.mircea.associations.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

// Credits to https://gist.github.com/stucchio/858014
public class UuidWritable implements WritableComparable<UuidWritable> {
    private UUID value;

    public UuidWritable(long mostSignificantBits, long leastSignificantBits) {
        value = new UUID(mostSignificantBits, leastSignificantBits);
    }

    public UuidWritable(String stringRep) {
        value = UUID.fromString(stringRep);
    }

    public UuidWritable(Text textRep) {
        value = UUID.fromString(textRep.toString());
    }

    public UuidWritable(UUID uuid) {
        value = uuid;
    }

    public UuidWritable() {
        value = UUID.randomUUID();
    }

    public String toString() {
        return value.toString();
    }

    public Text toText() {
        return new Text(toString());
    }

    public boolean equals(Object obj) {
        UuidWritable other = (UuidWritable)obj;
        return (value.getMostSignificantBits() == other.value.getMostSignificantBits()) && (value.getLeastSignificantBits() == other.value.getLeastSignificantBits());
    }

    public int hashCode() {
        return value.hashCode();
    }

    //Implementation of WritableComparable
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        long mostSignificantBits = in.readLong();
        long leastSignificantBits = in.readLong();
        value = new UUID(mostSignificantBits, leastSignificantBits);
    }

    public static UuidWritable read(DataInput in) throws IOException {
        UuidWritable result = new UuidWritable();
        result.readFields(in);
        return result;
    }

    @Override
    public int compareTo(UuidWritable w) {
        return value.compareTo(w.value);
    }

}
