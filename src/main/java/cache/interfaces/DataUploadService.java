package cache.interfaces;

import cache.dataimportes.formbeans.DataUploadBean;
import cache.dataimportes.holders.ExperimentResult;

/**
 * @author Manjunath Kustagi
 */
public interface DataUploadService extends cache.interfaces.Runnable {

    long createExperiment(int paramId, DataUploadBean bean);

    long updateExperiment(int paramId, DataUploadBean bean);

    long uploadSequence(long expID, long count, String sequence);

    void uploadExperiment(Long id, String filename);

    ExperimentResult getExperiment(int rowId, int page);

    ExperimentResult getExperimentById(Long eid);
    
    long getExperimentCount();

    int getPageSize();

    int getExperimentStatus(long expId);
}
