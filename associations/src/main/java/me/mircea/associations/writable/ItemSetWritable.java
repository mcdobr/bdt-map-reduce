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
        return this == obj || (obj != null && this.toString().equals(obj.toString()));
    }

    @Override
    public int compareTo(ItemSetWritable itemSetWritable) {
        return this.toString().compareTo(itemSetWritable.toString());
    }
}
