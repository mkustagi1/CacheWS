package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.UUID;

/**
 * Transcriptalignmentpolyatranspose class corresponding to <code>transcriptalignmentpolyatranspose</code> cassandra table
 */
@Table(keyspace = "cache", name = "transcriptalignmentpolyatranspose",
        readConsistency = "QUORUM",
        writeConsistency = "ANY",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Transcriptalignmentpolyatranspose implements Serializable {

    @PartitionKey(0)
    @Column(name = "key")
    private long key;

    @ClusteringColumn(0)
    @Column(name = "readid")
    private UUID readid;

    @ClusteringColumn(1)
    @Column(name = "transcriptid")
    private long transcriptid;

    public Transcriptalignmentpolyatranspose() {
    }

    public Transcriptalignmentpolyatranspose(long key, long transcriptid, UUID readid) {
        this.key = key;
        this.transcriptid = transcriptid;
        this.readid = readid;
    }

    public long getKey() {
        return this.key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public UUID getReadid() {
        return this.readid;
    }

    public void setReadid(UUID readid) {
        this.readid = readid;
    }

    public long getTranscriptid() {
        return this.transcriptid;
    }

    public void setTranscriptid(long transcriptid) {
        this.transcriptid = transcriptid;
    }

    @Override
    public boolean equals(Object other) {
        if ((this == other)) {
            return true;
        }
        if ((other == null)) {
            return false;
        }
        if (!(other instanceof Transcriptalignmentpolyatranspose)) {
            return false;
        }
        Transcriptalignmentpolyatranspose castOther = (Transcriptalignmentpolyatranspose) other;

        return (this.getKey() == castOther.getKey())
                && ((this.getReadid() == null ? castOther.getReadid() == null : this.getReadid().equals(castOther.getReadid())) || (this.getReadid() != null && castOther.getReadid() != null && this.getReadid().equals(castOther.getReadid())))
                && (this.getTranscriptid() == castOther.getTranscriptid());
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + (int) this.getKey();
        result = 37 * result + (getReadid() == null ? 0 : this.getReadid().hashCode());
        result = 37 * result + (int) this.getTranscriptid();
        return result;
    }

}