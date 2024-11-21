package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.UUID;

/**
 * Reads class corresponding to <code>unmappedreads</code> cassandra table
 */
@Table(keyspace = "cache", name = "unmappedreads",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Unmappedreads implements Serializable {

    @PartitionKey
    @Column(name = "experimentid")
    private long experimentid;

    @ClusteringColumn
    @Column(name = "readid")
    private UUID readid;

    public Unmappedreads() {
    }

    public Unmappedreads(UUID id) {
        this.readid = id;
    }

    public Unmappedreads(long experimentid, UUID id) {
        this.experimentid = experimentid;
        this.readid = id;
    }

    public long getExperimentid() {
        return this.experimentid;
    }

    public void setExperimentid(long experimentid) {
        this.experimentid = experimentid;
    }

    public UUID getReadid() {
        return this.readid;
    }

    public void setReadid(UUID id) {
        this.readid = id;
    }
    
    @Override
    public boolean equals(Object other) {
        if ((this == other)) {
            return true;
        }
        if ((other == null)) {
            return false;
        }
        if (!(other instanceof Reads)) {
            return false;
        }
        Unmappedreads castOther = (Unmappedreads) other;

        return (this.getExperimentid() == castOther.getExperimentid())
                && ((this.getReadid() == null ? castOther.getReadid() == null : this.getReadid().equals(castOther.getReadid())) 
                || (this.getReadid() != null && castOther.getReadid() != null && this.getReadid().equals(castOther.getReadid())));
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + (int) this.getExperimentid();
        result = 37 * result + (getReadid() == null ? 0 : this.getReadid().hashCode());
        return result;
    }    
}
