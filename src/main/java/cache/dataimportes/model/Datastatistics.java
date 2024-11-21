package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 * Datastatistics class corresponding to <code>datastatistics</code> cassandra table
 */
@Table(keyspace = "cache", name = "datastatistics",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Datastatistics implements Serializable {

    @PartitionKey(0)
    @Column(name = "key")
    private int key;

    @PartitionKey(1)
    @Column(name = "experimentid")
    private long experimentid;

    @PartitionKey(2)
    @Column(name = "distance")
    private int distance;

    @ClusteringColumn(0)
    @Column(name = "id")
    private UUID id;

    @Column(name = "annotations")
    private Map<String, Long> annotations;
    
    @Column(name = "totalmappedreads")
    private Long totalmappedreads;

    @Column(name = "totalmappedreadsnonredundant")
    private Long totalmappedreadsnonredundant;

    @Column(name = "totalreads")
    private Long totalreads;

    @Column(name = "totalreadsnonredundant")
    private Long totalreadsnonredundant;

    @Column(name = "totalmappedreadsRC")
    private Long totalmappedreadsRC;

    @Column(name = "totalmappedreadsnonredundantRC")
    private Long totalmappedreadsnonredundantRC;

    public Datastatistics() {
    }

    public Datastatistics(long experimentid) {
        this.experimentid = experimentid;
    }

    public Datastatistics(long experimentid, UUID id, 
            Long totalmappedreads, Long totalmappedreadsnonredundant, 
            Long totalreads, Long totalreadsnonredundant) {
        this.experimentid = experimentid;
        this.id = id;
        this.totalmappedreads = totalmappedreads;
        this.totalmappedreadsnonredundant = totalmappedreadsnonredundant;
        this.totalreads = totalreads;
        this.totalreadsnonredundant = totalreadsnonredundant;        
    }

    public int getKey() {
        return this.key;
    }

    public void setKey(int k) {
        this.key = k;
    }            
    
    public int getDistance() {
        return this.distance;
    }

    public void setDistance(int d) {
        this.distance = d;
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

    public Long getTotalmappedreads() {
        return this.totalmappedreads;
    }

    public void setTotalmappedreads(Long totalmappedreads) {
        this.totalmappedreads = totalmappedreads;
    }

    public Long getTotalmappedreadsnonredundant() {
        return this.totalmappedreadsnonredundant;
    }

    public void setTotalmappedreadsnonredundant(Long totalmappedreadsnonredundant) {
        this.totalmappedreadsnonredundant = totalmappedreadsnonredundant;
    }

    public Long getTotalreads() {
        return this.totalreads;
    }

    public void setTotalreads(Long totalreads) {
        this.totalreads = totalreads;
    }

    public Long getTotalreadsnonredundant() {
        return this.totalreadsnonredundant;
    }

    public void setTotalreadsnonredundant(Long totalreadsnonredundant) {
        this.totalreadsnonredundant = totalreadsnonredundant;
    }

    public Long getTotalmappedreadsRC() {
        return this.totalmappedreadsRC;
    }

    public void setTotalmappedreadsRC(Long totalmappedreadsRC) {
        this.totalmappedreadsRC = totalmappedreadsRC;
    }

    public Long getTotalmappedreadsnonredundantRC() {
        return this.totalmappedreadsnonredundantRC;
    }

    public void setTotalmappedreadsnonredundantRC(Long totalmappedreadsnonredundantRC) {
        this.totalmappedreadsnonredundantRC = totalmappedreadsnonredundantRC;
    }

    public Map<String, Long> getAnnotations() {
        return this.annotations;
    }
    
    public void setAnnotations(Map<String, Long> at) {
        this.annotations = at;
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
        Datastatistics castOther = (Datastatistics) other;

        return (this.getId() == castOther.getId())
                && (this.getExperimentid() == castOther.getExperimentid())
                && (this.getDistance() == castOther.getDistance());
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + (int) this.getExperimentid();
        result = 37 * result + (int) this.getDistance();
        return result;
    }    
}
