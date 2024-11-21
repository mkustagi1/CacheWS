package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.Map;

/**
 * Experiment class corresponding to <code>experiment</code> cassandra table
 */
@Table(keyspace = "cache", name = "experimentkey",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Experimentkey implements Serializable {

    @PartitionKey(0)
    @Column(name = "experimentid")
    private long experimentid;

    @ClusteringColumn(0)
    @Column(name = "distance")
    private int distance;

    @Column(name = "key")
    private int key;

    public Experimentkey() {
    }

    public Experimentkey(long id, int distance, int key) {
        this.key = key;
        this.experimentid = id;
        this.distance = distance;
    }

    public int getKey() {
        return this.key;
    }

    public void setKey(int k) {
        this.key = k;
    }            
    
    public long getExperimentid() {
        return this.experimentid;
    }

    public void setExperimentid(long id) {
        this.experimentid = id;
    }

    public int getDistance() {
        return this.distance;
    }

    public void setDistance(int d) {
        this.distance = d;
    }        
    
    @Override
    public boolean equals(Object other) {
        if ((this == other)) {
            return true;
        }
        if ((other == null)) {
            return false;
        }
        if (!(other instanceof Experimentkey)) {
            return false;
        }
        Experimentkey castOther = (Experimentkey) other;

        return (this.getExperimentid() == castOther.getExperimentid())
                && (this.getDistance() == castOther.getDistance())
                && (this.getKey() == castOther.getKey());
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + (int) this.getExperimentid();
        result = 37 * result + (int) this.getDistance();
        result = 37 * result + (int) this.getKey();
        return result;
    }

}
