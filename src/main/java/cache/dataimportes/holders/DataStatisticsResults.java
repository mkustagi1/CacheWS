package cache.dataimportes.holders;

import java.io.Serializable;

/**
 * @author Manjunath Kustagi
 */
public class DataStatisticsResults implements Comparable<DataStatisticsResults>, Serializable {

    /**
     * Experiment statistics
     */
    public long totalMappedReadsNonRedundant;
    public long totalMappedReads;
    public long totalReadsNonRedundant;
    public long totalReads;

    @Override
    public int compareTo(DataStatisticsResults t) {
        return new Long(totalReads).compareTo(t.totalReads);
    }
}
