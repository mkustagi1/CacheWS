package cache.interfaces;

import cache.dataimportes.formbeans.DataUploadBean;
import java.util.List;

/**
 *
 * @author Manjunath Kustagi
 */
public interface ParameterService extends cache.interfaces.Runnable{
    DataUploadBean getAdapterPruningParameters(int id);
    
    DataUploadBean populateChoices(DataUploadBean bean, int id);

    List<String> getAntibodies();
    List<String> getCrosslinkers();
    List<String> getOrganism();
    List<String> getCellines();
    List<String> getInvestigators();
    List<String> getMethods();
    List<String> getMappingParameters();
    List<String> getCollaborators();
    List<String> getNucleosideConcentrations();
    List<String> getNucleosideIncubations();
    List<String> getProteinInductions();
    List<String> getExcitationWavelengths();
    List<String> getCrosslinkEnergys();
    List<String> getProteinTags();
    List<String> getAntibodyAmounts();
    List<String> getRnaseUseds();
    List<String> getWashStepss();
    List<String> getBarcodes();
    List<String> getSequencers();
    List<String> getBackgroundTreatments();
    List<String> getAnnotations();
    
    int saveAdapterPruningParameters(int oldId, DataUploadBean bean);
}