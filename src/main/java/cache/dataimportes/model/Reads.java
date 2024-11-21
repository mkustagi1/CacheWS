package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.UUID;

/**
 * Reads class corresponding to <code>reads</code> cassandra table
 */
@Table(keyspace = "cache", name = "reads",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Reads implements Serializable {

    @PartitionKey
    @Column(name = "experimentid")
    private long experimentid;

    @ClusteringColumn
    @Column(name = "id")
    private UUID id;

    @Column(name = "sequence")
    private String sequence;

    @Column(name = "count")
    private Long count;

    public Reads() {
    }

    public Reads(UUID id) {
        this.id = id;
    }

    public Reads(long experimentid, UUID id, String sequence, long count) {
        this.experimentid = experimentid;
        this.id = id;
        this.sequence = sequence;
        this.count = count;
    }

    public long getExperimentid() {
        return this.experimentid;
    }

    public void setExperimentid(long experimentid) {
        this.experimentid = experimentid;
    }

    public UUID getId() {
        return this.id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Long getCount() {
        return this.count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getSequence() {
        return this.sequence;
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
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
        Reads castOther = (Reads) other;

        return (this.getExperimentid() == castOther.getExperimentid())
                && ((this.getId() == null ? castOther.getId() == null : this.getId().equals(castOther.getId())) || (this.getId() != null && castOther.getId() != null && this.getId().equals(castOther.getId())));
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + (int) this.getExperimentid();
        result = 37 * result + (getId() == null ? 0 : this.getId().hashCode());
        return result;
    }    
}
