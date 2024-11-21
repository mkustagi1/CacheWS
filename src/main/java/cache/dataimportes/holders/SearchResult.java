package cache.dataimportes.holders;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Manjunath Kustagi
 */
public class SearchResult implements Comparable<SearchResult>, Serializable {

    public enum SearchType {TRANSCRIPTS, READS};
    /**
     * Column definition
     */
    public static final String[] HEADER_NAME = {"Experiment ID", "Read ID", "Sequence", "Read Count", "Score"};

    public String sequence = "";
    public String name = "";
    public long readCount = 0l;
    public UUID readID;
    public long experimentID = 0;
    public long transcriptID = 0;
    public double score = 0d;
    public int searchLength = 101;
    public SearchType type = SearchType.READS;
    public Strand strand = Strand.BOTH;
    
    public SearchResult(){
        
    }
    
    public SearchResult(long eid, long tid, UUID rid, long rc, String s, String n, double sc, int sl, SearchType t, Strand st) {
        experimentID = eid;
        transcriptID = tid;
        readID = rid;
        readCount = rc;
        sequence = s;
        name = n;
        score = sc;
        searchLength = sl;
        type = t;
        strand = st;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.sequence);
        hash = 97 * hash + Objects.hashCode(this.name);
        hash = 97 * hash + (int) (this.readCount ^ (this.readCount >>> 32));
        hash = 97 * hash + Objects.hashCode(this.readID);
        hash = 97 * hash + (int)this.experimentID;
        hash = 97 * hash + (int)this.transcriptID;
        hash = 97 * hash + (int) (Double.doubleToLongBits(this.score) ^ (Double.doubleToLongBits(this.score) >>> 32));
        hash = 97 * hash + (int)this.searchLength;
        hash = 97 * hash + Objects.hashCode(this.type);
        hash = 97 * hash + Objects.hashCode(this.strand);
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
        final SearchResult other = (SearchResult) obj;
        if (this.readCount != other.readCount) {
            return false;
        }
        if (this.experimentID != other.experimentID) {
            return false;
        }
        if (this.transcriptID != other.transcriptID) {
            return false;
        }
        if (Double.doubleToLongBits(this.score) != Double.doubleToLongBits(other.score)) {
            return false;
        }
        if (!Objects.equals(this.sequence, other.sequence)) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.readID, other.readID)) {
            return false;
        }
        if (this.searchLength != other.searchLength) {
            return false;
        }
        if (this.type != other.type) {
            return false;
        }
        return this.strand == other.strand;
    }
    
    
    @Override
    public int compareTo(SearchResult t) {
        return new Long(t.readCount).compareTo(readCount);
    }        
}