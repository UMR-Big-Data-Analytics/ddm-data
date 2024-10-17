package de.ddm.utils;

import org.openjdk.jol.info.GraphLayout;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

public class MemoryUtils {

    public static long byteSizeOf(Object object) {
        return GraphLayout.parseInstance(object).totalSize();
    }

    public static long bytesMax() {
        MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        return heapMemoryUsage.getMax(); // Max memory as specified by the jvm -Xmx flag (-1 if not specified)
    }

    public static long bytesCommitted() {
        MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        return heapMemoryUsage.getCommitted(); // Currently allocated memory to the JVM by the OS (can be smaller than max!)
    }

    public static long bytesUsed() {
        MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        return heapMemoryUsage.getUsed(); // Currently used memory by the application
    }

    public static long bytesFree() {
        final long max = bytesMax();
        final long committed = bytesCommitted();
        final long used = bytesUsed();

        return (max < 0) ? committed - used : max - used;
    }
}
