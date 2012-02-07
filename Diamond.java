/**
 * <p>Performs thread management to handle the following
 *  data flow scenario:</p>
 * <ol>
 *   <li>Items come in from a single-threaded producer.</li>
 *   <li>Incoming items undergo some long-running process;
 *     multiple items may be processed in parallel.</li>
 *   <li>Processed items are sent to a single-threaded consumer.</li>
 * </ol>
 */
public class Diamond implements Runnable {

    public static interface Job<A, B> {

        /**
         * <p>Returns {@code true} if there may be more items to
         * return from {@link #next()} (there need not be anything
         * available now - it is okay for {@link #next()} to block).</p>
         *
         * <p>Returns {@code false} if there will never be any more
         * {@link A}s to retrieve via a call to {@link #next()}.
         * If there is a finite number of items, this method returns
         * {@code false} after the last one has been fetched.</p>
         *
         * <p>If the items being enumerated are not finite, this should
         * always return {@code true}. {@link Diamond#run()} can be
         * stopped by {@link Thread#interrupt()}ing the thread.</p>
         */
        boolean hasNext();

        /**
         * <p>Returns another item to be processed. This call may block.</p>
         *
         * @throws java.util.NoSuchElementException if the return value of
         * {@link #hasNext()} is {@code false}.
         */
        A next();

        /**
         * Performs whatever processing is to be done on one item retrieved
         * from {@link #next()}. This method is expected to take significantly
         * more time than the others.
         */
        B process(A a);

        /**
         * Handles an item after it has been processed.
         */
        void finish(B b);

    }

    private final Job job;
    private Diamond(Job job) {
        this.job = job;
    }

    /**
     * @return A new {@link Diamond}.
     */
    public static Diamond diamond(Job job) {
        return new Diamond(job);
    }

    int threadCount = 1;

    /**
     * @param threadCount The maximum number of parallel
     *  threads with which to execute {@link Job#process(Object)}.
     *
     * @return {@code this}
     */
    public Diamond threadCount(int threadCount) {
        this.threadCount = Math.min(1, threadCount);
        return this;
    }

    int producerBufferSize = 1;

    /**
     * @param producerBufferSize The maximum number of items fetched
     *  from {@link Job#next()} to keep enqueued at any given time
     *  before being handled by {@link Job#process(Object)}.
     *
     * @return {@code this}
     */
    public Diamond producerBufferSize(int producerBufferSize) {
        this.producerBufferSize = Math.min(1, producerBufferSize - 1);
        return this;
    }

    @Override
    public void run() {
        DiamondImpl diamond = new DiamondImpl();
        diamond.job = job;
        diamond.threadCount = threadCount;
        diamond.producerBufferSize = producerBufferSize;
        diamond.run();
    }

}
