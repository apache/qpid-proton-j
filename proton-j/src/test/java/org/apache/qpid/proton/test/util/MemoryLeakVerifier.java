package org.apache.qpid.proton.test.util;

import static org.junit.Assert.assertNull;
import java.lang.ref.WeakReference;

/*
 * Based on http://stackoverflow.com/questions/6749948/automated-memory-leak-detection-in-java
 */
public class MemoryLeakVerifier {
    private static final int MAX_GC_ITERATIONS = 20;
    private static final int GC_SLEEP_TIME = 10;

    private final WeakReference<?> reference;

    public MemoryLeakVerifier(Object object) {
        this.reference = new WeakReference<Object>(object);
    }

    public Object getObject() {
        return reference.get();
    }

    /**
     * Attempts to perform a full garbage collection so that all weak references will be removed. Usually only a single
     * GC is required, but there have been situations where some unused memory is not cleared up on the first pass. This
     * method performs a full garbage collection and then validates that the weak reference now has been cleared. If it
     * hasn't then the thread will sleep and then retry more times. If after this the
     * object still has not been collected then the assertion will fail. Based upon the method described in:
     * http://www.javaworld.com/javaworld/javatips/jw-javatip130.html
     */
    public void assertGarbageCollected(String name) {
        Runtime runtime = Runtime.getRuntime();
        for (int i = 0; i < MAX_GC_ITERATIONS; i++) {
            runtime.runFinalization();
            runtime.gc();
            if (getObject() == null)
                break;

            // Pause for a while and then go back around the loop to try again...
            try {
                Thread.sleep(GC_SLEEP_TIME);
            }
            catch (InterruptedException e) {
            }
        }
        assertNull(name + ": object should not exist after " + MAX_GC_ITERATIONS + " collections", getObject());
    }
}
