package marcbp.trino.s3file.list;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ListingSplitTest {

    @Test
    void listingSplitExposesRetainedSizeForFaultTolerantExecution() {
        assertTrue(ListingSplit.INSTANCE.getRetainedSizeInBytes() > 0);
        assertEquals(16L, ListingSplit.INSTANCE.getRetainedSizeInBytes());
    }
}
