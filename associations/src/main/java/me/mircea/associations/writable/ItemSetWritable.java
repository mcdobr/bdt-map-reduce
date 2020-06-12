package me.mircea.associations.writable;

import org.apache.hadoop.io.ArrayPrimitiveWritable;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ItemSetWritable extends ArrayPrimitiveWritable implements Comparable<ItemSetWritable> {
    public ItemSetWritable(Object value) {
        super(value);
    }

    @Override
    public int[] get() {
        return (int[]) super.get();
    }

    @Override
    public String toString() {
        int[] value = get();
        return Arrays.stream(value)
                .boxed()
                .sorted()
                .map(Object::toString)
                .collect(Collectors.joining(","));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ItemSetWritable)) {
            return false;
        } else {
            return this == obj || Arrays.equals(this.get(), ((ItemSetWritable) obj).get());
        }
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(get());
    }

    @Override
    public int compareTo(ItemSetWritable itemSetWritable) {
        return this.toString().compareTo(itemSetWritable.toString());
    }
}
