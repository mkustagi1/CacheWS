package cache.interfaces;

/**
 * @author Manjunath Kustagi
 */
public interface Runnable {
    
    enum Status {DONE, ACTIVE, ERROR, PAUSED};
    
    boolean start();
    boolean stop();
    boolean pause();
//    Status status();
    double progress();
    
}
