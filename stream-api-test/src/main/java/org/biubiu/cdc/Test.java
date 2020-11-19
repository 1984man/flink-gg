package org.biubiu.cdc;

import static org.apache.flink.core.memory.MemoryUtils.UNSAFE;

public class Test {
    protected static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    public static void main(String[] args) {

        System.out.println(BYTE_ARRAY_BASE_OFFSET);
    }
}
