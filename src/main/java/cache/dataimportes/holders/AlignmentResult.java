package cache.dataimportes.holders;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author Manjunath Kustagi
 */
public class AlignmentResult implements Comparable<AlignmentResult>, Serializable {

    /**
     * Column definition
     */
    public static final String[] headerName = {"Alignment ID", "Exon ID", "Read ID", "Experiment ID", "Score"};

    public long alignmentID;
    public long exonID;
    public UUID readID;
    public int experimentID;
    public double score;

    @Override
    public int compareTo(AlignmentResult t) {
        return new Double(score).compareTo(t.score);
    }        
}
