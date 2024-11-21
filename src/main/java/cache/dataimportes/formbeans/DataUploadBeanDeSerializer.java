package cache.dataimportes.formbeans;

import com.caucho.hessian.io.AbstractDeserializer;
import com.caucho.hessian.io.AbstractHessianInput;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author Manjunath Kustagi
 */
public class DataUploadBeanDeSerializer extends AbstractDeserializer {
    
    @Override
    public Class getType() {
        return DataUploadBean.class;
    }

    @Override
    public Object readObject(AbstractHessianInput ois) throws IOException {
        DataUploadBean bean = new DataUploadBean();
        bean.setProtein((String) ois.readObject());
        bean.setProteinInductionTime((String) ois.readObject());
        bean.setProteinInductionTimes((List<String>) ois.readObject());
        bean.setInvestigator((String) ois.readObject());
        bean.setInvestigators((List<String>) ois.readObject());
        bean.setCollaborator((String) ois.readObject());
        bean.setCollaborators((List<String>) ois.readObject());
        bean.setOrganism((String) ois.readObject());
        bean.setOrganisms((List<String>) ois.readObject());
        bean.setCellLine((String) ois.readObject());
        bean.setCellLines((List<String>) ois.readObject());
        bean.setNucleosideConcentration((String) ois.readObject());
        bean.setNucleosideConcentrations((List<String>) ois.readObject());
        bean.setNucleosideIncubation((String) ois.readObject());
        bean.setNucleosideIncubations((List<String>) ois.readObject());
        bean.setSampleVolume((String) ois.readObject());
        bean.setSampleMass((String) ois.readObject());
        bean.setExcitationWavelength((String) ois.readObject());
        bean.setExcitationWavelengths((List<String>) ois.readObject());
        bean.setCrosslinkEnergy((String) ois.readObject());
        bean.setCrosslinkEnergys((List<String>) ois.readObject());
        bean.setAntibody((String) ois.readObject());
        bean.setAntibodys((List<String>) ois.readObject());
        bean.setProteinTag((String) ois.readObject());
        bean.setProteinTags((List<String>) ois.readObject());
        bean.setAntibodyAmount((String) ois.readObject());
        bean.setAntibodyAmounts((List<String>) ois.readObject());
        bean.setRnaseUsed((String) ois.readObject());
        bean.setRnaseUseds((List<String>) ois.readObject());
        bean.setRnaseTreatmentNumber((String) ois.readObject());
        bean.setWashSteps((String) ois.readObject());
        bean.setWashStepss((List<String>) ois.readObject());
        bean.setBarcode((String) ois.readObject());
        bean.setBarcodes((List<String>) ois.readObject());
        bean.setPcrCycles((String) ois.readObject());
        bean.setMinReadLength((String) ois.readObject());
        bean.setFivePrimeAdapter((String) ois.readObject());
        bean.setFivePrimeAdapters((List<String>) ois.readObject());
        bean.setThreePrimeAdapter((String) ois.readObject());
        bean.setThreePrimeAdapters((List<String>) ois.readObject());
        bean.setSequencer((String) ois.readObject());
        bean.setSequencers((List<String>) ois.readObject());
        bean.setBackgroundTreatment((String) ois.readObject());
        bean.setBackgroundTreatments((List<String>) ois.readObject());
        bean.setPercentData((Integer) ois.readObject());
        return bean;
    }
}
