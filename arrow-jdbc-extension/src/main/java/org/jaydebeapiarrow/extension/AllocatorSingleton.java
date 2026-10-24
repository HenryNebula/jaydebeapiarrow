package org.jaydebeapiarrow.extension;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public enum AllocatorSingleton {
    INSTANCE;

    private static RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
    private static final AtomicInteger childNumber = new AtomicInteger(0);
    // Shared allocator for C Data Interface exports: the ArrowArray /
    // ArrowSchema structs are tiny, and their memory is freed back to this
    // allocator by the release callbacks Python invokes. A dedicated
    // long-lived allocator (instead of one child per export) keeps the
    // memory pooled and reusable across batches.
    private static final BufferAllocator exportAllocator =
            rootAllocator.newChildAllocator("c-data-export", 0, Long.MAX_VALUE);

    public static BufferAllocator getChildAllocator() {
        return rootAllocator.newChildAllocator(nextChildName(), 0, Long.MAX_VALUE);
    }

    public static BufferAllocator getExportAllocator() {
        return exportAllocator;
    }

    private static String nextChildName() {
        return "Allocator-Child-" + childNumber.incrementAndGet();
    }

}