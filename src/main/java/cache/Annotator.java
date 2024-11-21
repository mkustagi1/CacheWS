package cache;

import javax.jws.WebMethod;
import cache.interfaces.Runnable;

/**
 *
 * @author Manjunath Kustagi
 */

//@WebService
public class Annotator implements Runnable{

    public Annotator(){
        
    }

    @WebMethod()
    public boolean upload(String name) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @WebMethod()
    public boolean fastqToFasta(String name) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean start() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean stop() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean pause() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

//    @Override
//    public Status status() {
//        throw new UnsupportedOperationException("Not supported yet.");
//    }

    @Override
    public double progress() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
