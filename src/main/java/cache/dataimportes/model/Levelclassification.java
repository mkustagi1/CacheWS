package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.Objects;

/**
 * Levelclassification class corresponding to <code>levelclassification</code> cassandra table
 */
@Table(keyspace = "cache", name = "levelclassification",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Levelclassification implements Serializable {

    @PartitionKey(0)
    @Column(name = "category")
    private String category;

    @ClusteringColumn(0)
    @Column(name = "level")
    private String level;

    @Column(name = "transcriptid")
    private long transcriptid;

    public Levelclassification() {
    }

    public Levelclassification(String c, String l, long t) {
        this.category = c;
        this.level = l;
        this.transcriptid = t;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public long getTranscriptid() {
        return transcriptid;
    }

    public void setTranscriptid(long transcriptid) {
        this.transcriptid = transcriptid;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.category);
        hash = 97 * hash + Objects.hashCode(this.level);
        hash = 97 * hash + (int) (this.transcriptid ^ (this.transcriptid >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Levelclassification other = (Levelclassification) obj;
        if (this.transcriptid != other.transcriptid) {
            return false;
        }
        if (!Objects.equals(this.category, other.category)) {
            return false;
        }
        return Objects.equals(this.level, other.level);
    }    
}