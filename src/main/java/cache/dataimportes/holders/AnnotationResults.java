package cache.dataimportes.holders;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author Manjunath Kustagi
 */
public class AnnotationResults implements Comparable<AnnotationResults>, Serializable {

    /**
     * Column definition
     */
    public static final String[] headerName = {"ID", "Transcript ID", "Start Coordinate", "Stop Coordinate", "Sequence", "Annotation", "Variant", "Reverse Complement"};

    public String sequence;
    public String annotation;
    public String variant;
    public UUID id;
    public long transcriptId;
    public long startCoordinate;
    public long stopCoordinate;
    public Strand strand;
    public boolean predicted;
    
    public AnnotationResults() {
        
    }
    
    public AnnotationResults(UUID id, long tid, String seq, String ann, String var, long startC, long stopC, Strand st, boolean p) {
        this.id = id;
        transcriptId = tid;
        sequence = seq;
        annotation = ann;
        variant = var;
        startCoordinate = startC;
        stopCoordinate = stopC;
        strand = st;
        predicted = p;
    }

    @Override
    public boolean equals(Object o) {
        AnnotationResults other = (AnnotationResults)o;
        return (this.startCoordinate == other.startCoordinate) 
                && (this.stopCoordinate == other.stopCoordinate)
                && (this.transcriptId == other.transcriptId);
    }
    
    @Override
    public int compareTo(AnnotationResults t) {
        return new Integer(t.sequence.length()).compareTo(sequence.length());
    }        
    
    @Override
    public String toString() {
        return sequence + ":" + annotation + ":" + variant + ":" + id.toString() + ":" 
                + Long.toString(transcriptId) + ":" + Long.toString(startCoordinate) + ":" + Long.toString(stopCoordinate) + ":"
                + strand.name() + ":" + Boolean.toString(predicted);
    }
    
    public void parseValues(String encrypted) {
        String[] tokens = encrypted.split(":");
        sequence = tokens[0];
        annotation = tokens[1];
        variant = tokens[2];
        id = UUID.fromString(tokens[3]);
        transcriptId = Integer.parseInt(tokens[4]);
        startCoordinate = Integer.parseInt(tokens[5]);
        stopCoordinate = Integer.parseInt(tokens[6]);
        strand = Strand.valueOf(tokens[7]);
        predicted = Boolean.parseBoolean(tokens[8]);
    }
}