package cache.interfaces;

import cache.dataimportes.holders.AlignmentResult;
import cache.dataimportes.holders.AnnotationResults;
import cache.dataimportes.holders.Strand;
import cache.dataimportes.holders.TranscriptMappingResults;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author Manjunath Kustagi
 */
public interface AlignmentService {
    
    Long runAlignment(int key, long experimentId, int distance);

    Long runAlignmentWithList(List<Long> transcripts);

    Long runAlignmentWithMismatches(List<Long> transcripts);

    Long runAlignmentWithLists(List<Long> transcripts, List<Long> experimentIds);

    void stopAlignment(long token);

    AlignmentResult getAlignment(int alignmentId, int key, long experimentId, int distance);

    String getAlignedSequences(int alignmentId);

    long getAlignmentCount(int key, long experimentId, int distance);

    Map<Long, List<Long>> loadMappedTranscripts(int key, long experimentId, int distance);

    Map<Long, List<Long>> loadMappedGenes(int key, long experimentId, int distance);

    long getUniqueReadCount(int key, long experimentId, int distance);
    
    TranscriptMappingResults getTranscriptMapping(int index, int key, long experimentId, int distance, Map<Long, List<Long>> mappedTranscripts);

    List<TranscriptMappingResults> getTranscriptMappingByName(String transcriptName, int key, long experimentId, int distance);

    TranscriptMappingResults populateAlignmentDisplay(TranscriptMappingResults tmr, int key, long experimentId, int distance);
    
    long getTranscriptMappingCount(int key, long experimentId, int distance);

    Long updateAlignment(int key, long experimentId, int distance);

    int preProcessAlignments(int key, long experimentId, int distance);

    int checkAlignments(int key, long experimentId, int distance);

    int createCoverageForExperiment(int key, long experimentId, int distance);
    
    void getTranscriptMappingCountAndAnnotations(int key, long experimentId, int distance, Map<Long, List<Long>> mappedTranscripts, int min, int max, Strand strand);

    List<TranscriptMappingResults> getMappedTranscripts(int key, long experimentId, int distance, UUID readId);

    List search(String r, boolean rc);
    
    int getTranscriptCountForGene(String gene);
    
    List<String> getGenesByPartialSymbol(String partialGene);
    
    List<String> getDistinctBiotypes();
    
    List<String> getGenesForBiotypes(List<String> biotypes);
    
    void collateUniqueMappingsForGene(int key, long experimentId, int distance);
    
    void createOrDeleteAnnotation(List<AnnotationResults> ar, boolean create);
    
    List<AnnotationResults> getAnnotationsForTranscript(long transcriptId);
    
    void deleteTranscript(long transcriptId, String authToken);

    void editTranscriptName(long transcriptId, String newName, String authToken);

    void beginEditTranscript(long transcriptId, String authToken);
    
    TranscriptMappingResults previewEditTranscript(TranscriptMappingResults tmr, int key, long experimentId, int distance, String authToken);

    void persistEditTranscript(TranscriptMappingResults tmr, List<AnnotationResults> arList, int key, long experimentId, int distance, boolean all, String authToken, String email);
}
