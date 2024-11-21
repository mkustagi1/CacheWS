package cache.dataimportes.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConsumerService implements Runnable {

    private final BlockingQueue<Callable> consumerQueue;
    private final ExecutorService es;

    public ConsumerService(BlockingQueue<Callable> sharedQueue) {
        this.consumerQueue = sharedQueue;
        es = Executors.newFixedThreadPool(100, (Runnable r) -> {
            Thread t = new Thread(r);
            t.setPriority(Thread.MAX_PRIORITY);
            return t;
        });
    }

    @Override
    public void run() {
        while (true) {
            try {
                Future f1 = es.submit(consumerQueue.take());
                f1.get();
            } catch (InterruptedException | ExecutionException ex) {
                Logger.getLogger(ConsumerService.class.getName()).log(Level.SEVERE, null, ex);
                es.shutdown();
            }
        }
    }
}
