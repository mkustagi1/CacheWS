package cache.dataimportes.model;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import java.io.Serializable;
import java.util.Objects;

/**
 * Indices class corresponding to <code>indices</code> cassandra table
 */
@com.datastax.driver.mapping.annotations.Table(keyspace = "cache", name = "indices",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false,
        caseSensitiveTable = false)
public class Indices implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey(0)
    @Column(name = "key")
    private int key;

    @ClusteringColumn(0)
    @Column(name = "experimentid")
    private long experimentid;

    @Column(name = "primaryindex")
    private String primaryindex;

    @Column(name = "secondaryindex")
    private String secondaryindex;

    @Column(name = "tertiaryindex")
    private String tertiaryindex;

    public Indices() {
    }


    public Indices(int key, long experimentid) {
        this.key = key;
        this.experimentid = experimentid;
    }

    public Indices(int key, long experimentid, String pi, String si, String ti) {
        this.key = key;
        this.experimentid = experimentid;
        this.primaryindex = pi;
        this.secondaryindex = si;
        this.tertiaryindex = ti;
    }

    public String getPrimaryindex() {
        return primaryindex;
    }

    public void setPrimaryindex(String primaryindex) {
        this.primaryindex = primaryindex;
    }

    public String getSecondaryindex() {
        return secondaryindex;
    }

    public void setSecondaryindex(String secondaryindex) {
        this.secondaryindex = secondaryindex;
    }

    public String getTertiaryindex() {
        return tertiaryindex;
    }

    public void setTertiaryindex(String tertiaryindex) {
        this.tertiaryindex = tertiaryindex;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 13 * hash + this.key;
        hash = 13 * hash + Objects.hashCode(this.primaryindex);
        hash = 13 * hash + Objects.hashCode(this.secondaryindex);
        hash = 13 * hash + Objects.hashCode(this.tertiaryindex);
        hash = 13 * hash + (int) (this.experimentid ^ (this.experimentid >>> 32));
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
        final Indices other = (Indices) obj;
        if (this.key != other.key) {
            return false;
        }
        if (this.experimentid != other.experimentid) {
            return false;
        }
        if (!this.primaryindex.equals(other.primaryindex)) {
            return false;
        }
        if (!this.secondaryindex.equals(other.secondaryindex)) {
            return false;
        }
        return this.tertiaryindex.equals(other.tertiaryindex);
    }
}
