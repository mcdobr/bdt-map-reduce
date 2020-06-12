package me.mircea.associations;

import org.apache.hadoop.io.ArrayPrimitiveWritable;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ItemSetWritable extends ArrayPrimitiveWritable {
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
                .map(Object::toString)
                .collect(Collectors.joining(","));
    }
}
