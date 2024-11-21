package cache.dataimportes.holders;

import java.io.Serializable;

/**
 * @author Manjunath Kustagi
 */
public class ExperimentResult implements Comparable<ExperimentResult>, Serializable {

    /**
     * Column definition
     */
    public static final String[] headerName = {"Alignment ID", "Exon ID", "Read ID", "Experiment ID", "Score"};
    public long experimentID;
    public int distance;
    public String library;
    public String cellLine;
    public String protein;
    public String investigator;
    public String method;
    public String mappingParameters;
    public String annotation;
    public int key;
//    public DataUploadBean dataUploadBean;
    
    @Override
    public int compareTo(ExperimentResult t) {
        return new Long(experimentID).compareTo(t.experimentID);
    }
}
