import java.util.List;
import java.util.concurrent.*;

import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newFixedThreadPool;

class DiamondImpl<A, B> {

    Diamond.Job<A, B> job;
    int threadCount;
    int producerBufferSize;

    private BlockingQueue<Value<A>> aq;
    private BlockingQueue<Value<B>> bq;

    void run() {
        aq = new ArrayBlockingQueue<Value<A>>(producerBufferSize);
        bq = new LinkedBlockingQueue<Value<B>>();
        List<Thread> threads = asList(new Producer(), new Processor(), new Consumer());
        try {
            for (Thread thread : threads) thread.join();
        } catch (InterruptedException e) {
            for (Thread thread : threads) thread.interrupt();
        }
    }

    private class Producer extends Thread {
        @Override
        public void run() {
            while (!interrupted()) {
                if (!job.hasNext()) {
                    try {
                        Value.poison(aq);
                    } catch (InterruptedException e) {
                        break;
                    }
                    break;
                }
                A a = job.next();
                try {
                    aq.put(Value.value(a));
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    private class Processor extends Thread {
        @Override
        public void run() {
            ExecutorService executorService = newFixedThreadPool(threadCount);
            while (!interrupted()) {
                final Value<A> a;
                try {
                    a = aq.take();
                    if (a.isPoison()) {
                        Value.poison(aq);
                        Value.poison(bq);
                        break;
                    }
                } catch (InterruptedException e) {
                    break;
                }
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        B b = job.process(a.get());
                        bq.add(Value.value(b));
                    }
                });
            }
            executorService.shutdownNow();
        }
    }

    private class Consumer extends Thread {
        @Override
        public void run() {
            while (!interrupted()) {
                Value<B> b;
                try {
                    b = bq.take();
                    if (b.isPoison()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    break;
                }
                job.finish(b.get());
            }
        }
    }

}

class Value<T> {

    static <T> Value<T> value(T value) {
        return new Value<T>(value);
    }

    T get() {
        return value;
    }

    @SuppressWarnings("unchecked")
    static <T> void poison(BlockingQueue<Value<T>> q) throws InterruptedException {
        q.put(POISON);
    }

    boolean isPoison() {
        return this == POISON;
    }

    private static final Value POISON = new Value<Object>(null);

    private final T value;

    private Value(T value) {
        this.value = value;
    }

}
