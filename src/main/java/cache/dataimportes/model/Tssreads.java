package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.UUID;

/**
 * Tssreads class corresponding to <code>tssreads</code> cassandra table
 */
@Table(keyspace = "cache", name = "tssreads",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Tssreads implements Serializable {

    @PartitionKey
    @Column(name = "key")
    private int key;

    @ClusteringColumn
    @Column(name = "id")
    private UUID id;

    @Column(name = "sequence")
    private String sequence;

    @Column(name = "count")
    private Long count;

    @Column(name = "removeda")
    private Integer removeda;

    public Tssreads() {
    }

    public Tssreads(UUID id) {
        this.id = id;
    }

    public Tssreads(int key, UUID id, String sequence, long count, int ra) {
        this.key = key;
        this.id = id;
        this.sequence = sequence;
        this.count = count;
        this.removeda = ra;
    }

    public int getKey() {
        return this.key;
    }

    public void setKey(int key) {
        this.key = key;
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
    
    public Integer getRemoveda() {
        return this.removeda;
    }

    public void setRemoveda(Integer ra) {
        this.removeda = ra;
    }

    @Override
    public boolean equals(Object other) {
        if ((this == other)) {
            return true;
        }
        if ((other == null)) {
            return false;
        }
        if (!(other instanceof Tssreads)) {
            return false;
        }
        Tssreads castOther = (Tssreads) other;

        return (this.getKey() == castOther.getKey())
                && ((this.getId() == null ? castOther.getId() == null : 
                this.getId().equals(castOther.getId())) || (this.getId() != null 
                && castOther.getId() != null && this.getId().equals(castOther.getId())));
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + (int) this.getKey();
        result = 37 * result + (getId() == null ? 0 : this.getId().hashCode());
        return result;
    }    
}
