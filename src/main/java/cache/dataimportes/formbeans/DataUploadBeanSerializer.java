package cache.dataimportes.formbeans;

import com.caucho.hessian.io.AbstractHessianOutput;
import com.caucho.hessian.io.AbstractSerializer;
import java.io.IOException;

/**
 *
 * @author Manjunath Kustagi
 */
public class DataUploadBeanSerializer extends AbstractSerializer {

    
    @Override
    public void writeObject(Object obj, AbstractHessianOutput oos) throws IOException {
        DataUploadBean bean = (DataUploadBean) obj;
        oos.writeObject(bean.getProtein());
        oos.writeObject(bean.getProteinInductionTime());
        oos.writeObject(bean.getProteinInductionTimes());
        oos.writeObject(bean.getInvestigator());
        oos.writeObject(bean.getInvestigators());
        oos.writeObject(bean.getCollaborator());
        oos.writeObject(bean.getCollaborators());
        oos.writeObject(bean.getOrganism());
        oos.writeObject(bean.getOrganisms());
        oos.writeObject(bean.getCellLine());
        oos.writeObject(bean.getCellLines());
        oos.writeObject(bean.getNucleosideConcentration());
        oos.writeObject(bean.getNucleosideConcentrations());
        oos.writeObject(bean.getNucleosideIncubation());
        oos.writeObject(bean.getNucleosideIncubations());
        oos.writeObject(bean.getSampleVolume());
        oos.writeObject(bean.getSampleMass());
        oos.writeObject(bean.getExcitationWavelength());
        oos.writeObject(bean.getExcitationWavelengths());
        oos.writeObject(bean.getCrosslinkEnergy());
        oos.writeObject(bean.getCrosslinkEnergys());
        oos.writeObject(bean.getAntibody());
        oos.writeObject(bean.getAntibodys());
        oos.writeObject(bean.getProteinTag());
        oos.writeObject(bean.getProteinTags());
        oos.writeObject(bean.getAntibodyAmount());
        oos.writeObject(bean.getAntibodyAmounts());
        oos.writeObject(bean.getRnaseUsed());
        oos.writeObject(bean.getRnaseUseds());
        oos.writeObject(bean.getRnaseTreatmentNumber());
        oos.writeObject(bean.getWashSteps());
        oos.writeObject(bean.getWashStepss());
        oos.writeObject(bean.getBarcode());
        oos.writeObject(bean.getBarcodes());
        oos.writeObject(bean.getPcrCycles());
        oos.writeObject(bean.getMinReadLength());
        oos.writeObject(bean.getFivePrimeAdapter());
        oos.writeObject(bean.getThreePrimeAdapter());
        oos.writeObject(bean.getThreePrimeAdapters());
        oos.writeObject(bean.getSequencer());
        oos.writeObject(bean.getSequencers());
        oos.writeObject(bean.getBackgroundTreatment());
        oos.writeObject(bean.getBackgroundTreatments());
        oos.writeObject(bean.getPercentData());
    }
}