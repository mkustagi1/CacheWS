package cache.dataimportes.formbeans;

import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import org.jdesktop.observablecollections.ObservableCollections;
import org.jdesktop.observablecollections.ObservableList;

/**
 *
 * @author Manjunath Kustagi
 */
public class DataUploadBean implements Serializable {

    public static enum PROGRESS {STARTED, UPLOADING, ALIGNING, DONE};
    
    private String protein;
    private String library;
    private String proteinInductionTime;
    private ObservableList<String> proteinInductionTimes;
    private String investigator;
    private ObservableList<String> investigators;
    private String collaborator;
    private ObservableList<String> collaborators;
    private String annotation;
    private ObservableList<String> annotations;
    private String organism;
    private ObservableList<String> organisms;
    private String cellLine;
    private ObservableList<String> cellLines;
    private String nucleosideConcentration;
    private ObservableList<String> nucleosideConcentrations;
    private String nucleosideIncubation;
    private ObservableList<String> nucleosideIncubations;
    private String sampleVolume;
    private String sampleMass;
    private String excitationWavelength;
    private ObservableList<String> excitationWavelengths;
    private String crosslinkEnergy;
    private ObservableList<String> crosslinkEnergys;
    private String antibody;
    private ObservableList<String> antibodys;
    private String proteinTag;
    private ObservableList<String> proteinTags;
    private String antibodyAmount;
    private ObservableList<String> antibodyAmounts;
    private String rnaseUsed;
    private ObservableList<String> rnaseUseds;
    private String rnaseTreatmentNumber;
    private String washSteps;
    private ObservableList<String> washStepss;
    private String barcode;
    private ObservableList<String> barcodes;
    private String pcrCycles;
    private String minReadLength;
    private String fivePrimeAdapter;
    private ObservableList<String> fivePrimeAdapters;
    private String threePrimeAdapter;
    private ObservableList<String> threePrimeAdapters;
    private String sequencer;
    private ObservableList<String> sequencers;
    private String backgroundTreatment;
    private ObservableList<String> backgroundTreatments;
    private int percentData = 100;
    private String email;
    private Integer preparationDateDay;
    private Integer preparationDateMonth;
    private Integer preparationDateYear;
    private String method;
    private ObservableList<String> methods;
    private String mappingParameter;
    private ObservableList<String> mappingParameters;
    
    private PROGRESS progress = PROGRESS.DONE;
    
    private PropertyChangeSupport propertySupport;

    public DataUploadBean() {
        propertySupport = new PropertyChangeSupport(this);
    }

    public void makeObservable() {
        proteinInductionTimes = ObservableCollections.observableList(proteinInductionTimes);
        investigators = ObservableCollections.observableList(investigators);
        annotations = ObservableCollections.observableList(annotations);
        organisms = ObservableCollections.observableList(organisms);
        cellLines = ObservableCollections.observableList(cellLines);
        nucleosideConcentrations = ObservableCollections.observableList(nucleosideConcentrations);
        nucleosideIncubations = ObservableCollections.observableList(nucleosideIncubations);
        excitationWavelengths = ObservableCollections.observableList(excitationWavelengths);
        if (crosslinkEnergys != null) {
            crosslinkEnergys = ObservableCollections.observableList(crosslinkEnergys);
        }
        antibodys = ObservableCollections.observableList(antibodys);
        proteinTags = ObservableCollections.observableList(proteinTags);
        antibodyAmounts = ObservableCollections.observableList(antibodyAmounts);
        rnaseUseds = ObservableCollections.observableList(rnaseUseds);
        washStepss = ObservableCollections.observableList(washStepss);
        barcodes = ObservableCollections.observableList(barcodes);
        if (fivePrimeAdapters != null) {
            fivePrimeAdapters = ObservableCollections.observableList(fivePrimeAdapters);
        }
        if (threePrimeAdapters != null) {
            threePrimeAdapters = ObservableCollections.observableList(threePrimeAdapters);
        }
        sequencers = ObservableCollections.observableList(sequencers);
        backgroundTreatments = ObservableCollections.observableList(backgroundTreatments);
        methods = ObservableCollections.observableList(methods);
        mappingParameters = ObservableCollections.observableList(mappingParameters);
    }

    public ObservableList<String> getInvestigators() {
        return investigators;
    }

    public void setInvestigators(List<String> value) {
        List<String> oldValue = investigators;
        investigators = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("investigators", oldValue, investigators);
    }

    public ObservableList<String> getMethods() {
        return methods;
    }

    public void setMethods(List<String> value) {
        List<String> oldValue = methods;
        methods = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("methods", oldValue, methods);
    }

    public ObservableList<String> getMappingParameters() {
        return mappingParameters;
    }

    public void setMappingParameters(List<String> value) {
        List<String> oldValue = mappingParameters;
        mappingParameters = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("mappingParameters", oldValue, mappingParameters);
    }

    public ObservableList<String> getCollaborators() {
        return collaborators;
    }

    public void setCollaborators(List<String> value) {
        List<String> oldValue = collaborators;
        collaborators = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("collaborators", oldValue, collaborators);
    }

    public ObservableList<String> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(List<String> value) {
        List<String> oldValue = annotations;
        annotations = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("annotations", oldValue, annotations);
    }

    public ObservableList<String> getOrganisms() {
        return organisms;
    }

    public void setOrganisms(List<String> value) {
        List<String> oldValue = organisms;
        organisms = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("organisms", oldValue, organisms);
    }

    public ObservableList<String> getCellLines() {
        return cellLines;
    }

    public void setCellLines(List<String> value) {
        List<String> oldValue = cellLines;
        cellLines = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("cellLines", oldValue, cellLines);
    }

    public ObservableList<String> getNucleosideConcentrations() {
        return nucleosideConcentrations;
    }

    public void setNucleosideConcentrations(List<String> value) {
        List<String> oldValue = nucleosideConcentrations;
        nucleosideConcentrations = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("nucleosideConcentrations", oldValue, nucleosideConcentrations);
    }

    public ObservableList<String> getNucleosideIncubations() {
        return nucleosideIncubations;
    }

    public void setNucleosideIncubations(List<String> value) {
        List<String> oldValue = nucleosideIncubations;
        nucleosideIncubations = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("nucleosideIncubations", oldValue, nucleosideIncubations);
    }

    public ObservableList<String> getExcitationWavelengths() {
        return excitationWavelengths;
    }

    public void setExcitationWavelengths(List<String> value) {
        List<String> oldValue = excitationWavelengths;
        excitationWavelengths = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("excitationWavelengths", oldValue, excitationWavelengths);
    }

    public ObservableList<String> getCrosslinkEnergys() {
        return crosslinkEnergys;
    }

    public void setCrosslinkEnergys(List<String> value) {
        List<String> oldValue = crosslinkEnergys;
        crosslinkEnergys = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("crosslinkEnergys", oldValue, crosslinkEnergys);
    }

    public ObservableList<String> getAntibodys() {
        return antibodys;
    }

    public void setAntibodys(List<String> value) {
        List<String> oldValue = antibodys;
        antibodys = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("antibodys", oldValue, antibodys);
    }

    public ObservableList<String> getProteinTags() {
        return proteinTags;
    }

    public void setProteinTags(List<String> value) {
        List<String> oldValue = proteinTags;
        proteinTags = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("proteinTags", oldValue, proteinTags);
    }

    public ObservableList<String> getAntibodyAmounts() {
        return antibodyAmounts;
    }

    public void setAntibodyAmounts(List<String> value) {
        List<String> oldValue = antibodyAmounts;
        antibodyAmounts = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("antibodyAmounts", oldValue, antibodyAmounts);
    }

    public ObservableList<String> getRnaseUseds() {
        return rnaseUseds;
    }

    public void setRnaseUseds(List<String> value) {
        List<String> oldValue = rnaseUseds;
        rnaseUseds = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("rnaseUseds", oldValue, rnaseUseds);
    }

    public ObservableList<String> getWashStepss() {
        return washStepss;
    }

    public void setWashStepss(List<String> value) {
        List<String> oldValue = washStepss;
        washStepss = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("washStepss", oldValue, washStepss);
    }

    public ObservableList<String> getBarcodes() {
        return barcodes;
    }

    public void setBarcodes(List<String> value) {
        List<String> oldValue = barcodes;
        barcodes = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("barcodes", oldValue, barcodes);
    }

    public ObservableList<String> getFivePrimeAdapters() {
        return fivePrimeAdapters;
    }

    public void setFivePrimeAdapters(List<String> value) {
        List<String> oldValue = fivePrimeAdapters;
        fivePrimeAdapters = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("fivePrimeAdapters", oldValue, fivePrimeAdapters);
    }

    public ObservableList<String> getThreePrimeAdapters() {
        return threePrimeAdapters;
    }

    public void setThreePrimeAdapters(List<String> value) {
        List<String> oldValue = threePrimeAdapters;
        threePrimeAdapters = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("threePrimeAdapters", oldValue, threePrimeAdapters);
    }

    public ObservableList<String> getSequencers() {
        return sequencers;
    }

    public void setSequencers(List<String> value) {
        List<String> oldValue = sequencers;
        sequencers = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("sequencers", oldValue, sequencers);
    }

    public ObservableList<String> getBackgroundTreatments() {
        return backgroundTreatments;
    }

    public void setBackgroundTreatments(List<String> value) {
        List<String> oldValue = backgroundTreatments;
        backgroundTreatments = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("backgroundTreatments", oldValue, backgroundTreatments);
    }

    public ObservableList<String> getProteinInductionTimes() {
        return proteinInductionTimes;
    }

    public void setProteinInductionTimes(List<String> value) {
        List<String> oldValue = proteinInductionTimes;
        proteinInductionTimes = ObservableCollections.observableList(value);
        propertySupport.firePropertyChange("proteinInductionTimes", oldValue, proteinInductionTimes);
    }

    public String getProteinInductionTime() {
        return proteinInductionTime;
    }

    public void setProteinInductionTime(String value) {
        String oldValue = proteinInductionTime;
        proteinInductionTime = value;
        propertySupport.firePropertyChange("proteinInductionTime", oldValue, proteinInductionTime);
    }

    public String getBarcode() {
        return barcode;
    }

    public void setBarcode(String value) {
        String oldValue = barcode;
        barcode = value;
        propertySupport.firePropertyChange("barcode", oldValue, barcode);
    }

    public String getInvestigator() {
        return investigator;
    }

    public void setInvestigator(String value) {
        String oldValue = investigator;
        investigator = value;
        propertySupport.firePropertyChange("investigator", oldValue, investigator);
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String value) {
        String oldValue = method;
        method = value;
        propertySupport.firePropertyChange("method", oldValue, method);
    }
    public String getMappingParameter() {
        return mappingParameter;
    }

    public void setMappingParameter(String value) {
        String oldValue = mappingParameter;
        mappingParameter = value;
        propertySupport.firePropertyChange("mappingParameter", oldValue, mappingParameter);
    }

    public String getCollaborator() {
        return collaborator;
    }

    public void setCollaborator(String value) {
        String oldValue = collaborator;
        collaborator = value;
        propertySupport.firePropertyChange("collaborator", oldValue, collaborator);
    }

    public String getAnnotation() {
        return annotation;
    }

    public void setAnnotation(String value) {
        String oldValue = annotation;
        annotation = value;
        propertySupport.firePropertyChange("annotation", oldValue, annotation);
    }

    public String getNucleosideConcentration() {
        return nucleosideConcentration;
    }

    public void setNucleosideConcentration(String value) {
        String oldValue = nucleosideConcentration;
        nucleosideConcentration = value;
        propertySupport.firePropertyChange("nucleosideConcentration", oldValue, nucleosideConcentration);
    }

    public String getNucleosideIncubation() {
        return nucleosideIncubation;
    }

    public void setNucleosideIncubation(String value) {
        String oldValue = nucleosideIncubation;
        nucleosideIncubation = value;
        propertySupport.firePropertyChange("nucleosideIncubation", oldValue, nucleosideIncubation);
    }

    public String getSampleVolume() {
        return sampleVolume;
    }

    public void setSampleVolume(String value) {
        String oldValue = sampleVolume;
        sampleVolume = value;
        propertySupport.firePropertyChange("sampleVolume", oldValue, sampleVolume);
    }

    public String getSampleMass() {
        return sampleMass;
    }

    public void setSampleMass(String value) {
        String oldValue = sampleMass;
        sampleMass = value;
        propertySupport.firePropertyChange("sampleMass", oldValue, sampleMass);
    }

    public String getExcitationWavelength() {
        return excitationWavelength;
    }

    public void setExcitationWavelength(String value) {
        String oldValue = excitationWavelength;
        excitationWavelength = value;
        propertySupport.firePropertyChange("excitationWavelength", oldValue, excitationWavelength);
    }

    public String getPcrCycles() {
        return pcrCycles;
    }

    public void setPcrCycles(String value) {
        String oldValue = pcrCycles;
        pcrCycles = value;
        propertySupport.firePropertyChange("pcrCycles", oldValue, pcrCycles);
    }

    public String getCrosslinkEnergy() {
        return crosslinkEnergy;
    }

    public void setCrosslinkEnergy(String value) {
        String oldValue = crosslinkEnergy;
        crosslinkEnergy = value;
        propertySupport.firePropertyChange("crosslinkEnergy", oldValue, crosslinkEnergy);
    }

    public String getRnaseUsed() {
        return rnaseUsed;
    }

    public void setRnaseUsed(String value) {
        String oldValue = rnaseUsed;
        rnaseUsed = value;
        propertySupport.firePropertyChange("rnaseUsed", oldValue, rnaseUsed);
    }

    public String getRnaseTreatmentNumber() {
        return rnaseTreatmentNumber;
    }

    public void setRnaseTreatmentNumber(String value) {
        String oldValue = rnaseTreatmentNumber;
        rnaseTreatmentNumber = value;
        propertySupport.firePropertyChange("rnaseTreatmentNumber", oldValue, rnaseTreatmentNumber);
    }

    public String getSequencer() {
        return sequencer;
    }

    public void setSequencer(String value) {
        String oldValue = sequencer;
        sequencer = value;
        propertySupport.firePropertyChange("sequencer", oldValue, sequencer);
    }

    public String getBackgroundTreatment() {
        return backgroundTreatment;
    }

    public void setBackgroundTreatment(String value) {
        String oldValue = backgroundTreatment;
        backgroundTreatment = value;
        propertySupport.firePropertyChange("backgroundTreatment", oldValue, backgroundTreatment);
    }

    public String getProteinTag() {
        return proteinTag;
    }

    public void setProteinTag(String value) {
        String oldValue = proteinTag;
        proteinTag = value;
        propertySupport.firePropertyChange("proteinTag", oldValue, proteinTag);
    }

    public String getProtein() {
        return protein;
    }

    public void setProtein(String value) {
        String oldValue = protein;
        protein = value;
        propertySupport.firePropertyChange("protein", oldValue, protein);
    }

    public String getLibrary() {
        return library;
    }

    public void setLibrary(String value) {
        String oldValue = library;
        library = value;
        propertySupport.firePropertyChange("library", oldValue, library);
    }

    public String getWashSteps() {
        return washSteps;
    }

    public void setWashSteps(String value) {
        String oldValue = washSteps;
        washSteps = value;
        propertySupport.firePropertyChange("washSteps", oldValue, washSteps);
    }

    public String getOrganism() {
        return organism;
    }

    public void setOrganism(String value) {
        String oldValue = organism;
        organism = value;
        propertySupport.firePropertyChange("organism", oldValue, organism);
    }

    public String getCellLine() {
        return cellLine;
    }

    public void setCellLine(String value) {
        String oldValue = cellLine;
        cellLine = value;
        propertySupport.firePropertyChange("cellLine", oldValue, cellLine);
    }

    public String getAntibodyAmount() {
        return antibodyAmount;
    }

    public void setAntibodyAmount(String value) {
        String oldValue = antibodyAmount;
        antibodyAmount = value;
        propertySupport.firePropertyChange("antibodyAmount", oldValue, antibodyAmount);
    }

    public String getAntibody() {
        return antibody;
    }

    public void setAntibody(String value) {
        String oldValue = antibody;
        antibody = value;
        propertySupport.firePropertyChange("antibody", oldValue, antibody);
    }

    public String getMinReadLength() {
        return minReadLength;
    }

    public void setMinReadLength(String value) {
        String oldValue = minReadLength;
        minReadLength = value;
        propertySupport.firePropertyChange("minReadLength", oldValue, minReadLength);
    }

    public int getPercentData() {
        return percentData;
    }

    public void setPercentData(int value) {
        int oldValue = percentData;
        percentData = value;
        propertySupport.firePropertyChange("percentData", oldValue, percentData);
    }

    public String getFivePrimeAdapter() {
        return fivePrimeAdapter;
    }

    public void setFivePrimeAdapter(String value) {
        String oldValue = fivePrimeAdapter;
        fivePrimeAdapter = value;
        propertySupport.firePropertyChange("fivePrimeAdapter", oldValue, fivePrimeAdapter);
    }

    public String getThreePrimeAdapter() {
        return threePrimeAdapter;
    }

    public void setThreePrimeAdapter(String value) {
        String oldValue = threePrimeAdapter;
        threePrimeAdapter = value;
        propertySupport.firePropertyChange("threePrimeAdapter", oldValue, threePrimeAdapter);
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String value) {
        String oldValue = email;
        email = value;
        propertySupport.firePropertyChange("email", oldValue, email);
    }

    public Integer getPreparationDateDay() {
        return preparationDateDay;
    }

    public void setPreparationDateDay(Integer value) {
        Integer oldValue = preparationDateDay;
        preparationDateDay = value;
        propertySupport.firePropertyChange("preparationDateDay", oldValue, preparationDateDay);
    }

    public Integer getPreparationDateMonth() {
        return preparationDateMonth;
    }

    public void setPreparationDateMonth(Integer value) {
        Integer oldValue = preparationDateMonth;
        preparationDateMonth = value;
        propertySupport.firePropertyChange("preparationDateMonth", oldValue, preparationDateMonth);
    }

    public Integer getPreparationDateYear() {
        return preparationDateYear;
    }

    public void setPreparationDateYear(Integer value) {
        Integer oldValue = preparationDateYear;
        preparationDateYear = value;
        propertySupport.firePropertyChange("preparationDateYear", oldValue, preparationDateYear);
    }
    
    public PROGRESS getProgress() {
        return progress;
    }

    public void setProgress(PROGRESS p) {
        PROGRESS oldValue = progress;
        progress = p;
        propertySupport.firePropertyChange("progress", oldValue, progress);
    }

    public void addPropertyChangeListener(PropertyChangeListener listener) {
        propertySupport.addPropertyChangeListener(listener);
    }

    public void removePropertyChangeListener(PropertyChangeListener listener) {
        propertySupport.removePropertyChangeListener(listener);
    }

    public PropertyChangeListener[] getPropertyChangeListeners() {
        return propertySupport.getPropertyChangeListeners();
    }

    private void writeObject(ObjectOutputStream oos)
            throws IOException {
        oos.writeObject(getProtein());
        oos.writeObject(getLibrary());
        oos.writeObject(getProteinInductionTime());
        oos.writeObject(getProteinInductionTimes());
        oos.writeObject(getInvestigator());
        oos.writeObject(getInvestigators());
        oos.writeObject(getMethod());
        oos.writeObject(getMethods());
        oos.writeObject(getMappingParameter());
        oos.writeObject(getMappingParameters());
        oos.writeObject(getAnnotation());
        oos.writeObject(getAnnotations());
        oos.writeObject(getOrganism());
        oos.writeObject(getOrganisms());
        oos.writeObject(getCellLine());
        oos.writeObject(getCellLines());
        oos.writeObject(getNucleosideConcentration());
        oos.writeObject(getNucleosideConcentrations());
        oos.writeObject(getNucleosideIncubation());
        oos.writeObject(getNucleosideIncubations());
        oos.writeObject(getSampleVolume());
        oos.writeObject(getSampleMass());
        oos.writeObject(getExcitationWavelength());
        oos.writeObject(getExcitationWavelengths());
        oos.writeObject(getCrosslinkEnergy());
        oos.writeObject(getCrosslinkEnergys());
        oos.writeObject(getAntibody());
        oos.writeObject(getAntibodys());
        oos.writeObject(getProteinTag());
        oos.writeObject(getProteinTags());
        oos.writeObject(getAntibodyAmount());
        oos.writeObject(getAntibodyAmounts());
        oos.writeObject(getRnaseUsed());
        oos.writeObject(getRnaseUseds());
        oos.writeObject(getRnaseTreatmentNumber());
        oos.writeObject(getWashSteps());
        oos.writeObject(getWashStepss());
        oos.writeObject(getBarcode());
        oos.writeObject(getBarcodes());
        oos.writeObject(getPcrCycles());
        oos.writeObject(getMinReadLength());
        oos.writeObject(getFivePrimeAdapter());
        oos.writeObject(getFivePrimeAdapters());
        oos.writeObject(getThreePrimeAdapter());
        oos.writeObject(getThreePrimeAdapters());
        oos.writeObject(getSequencer());
        oos.writeObject(getSequencers());
        oos.writeObject(getBackgroundTreatment());
        oos.writeObject(getBackgroundTreatments());
        oos.writeObject(getPercentData());
        oos.writeObject(getEmail());
        oos.writeObject(getPreparationDateDay());
        oos.writeObject(getPreparationDateMonth());
        oos.writeObject(getPreparationDateYear());
    }

    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {
        setProtein((String) ois.readObject());
        setLibrary((String) ois.readObject());
        setProteinInductionTime((String) ois.readObject());
        setProteinInductionTimes((List<String>) ois.readObject());
        setInvestigator((String) ois.readObject());
        setInvestigators((List<String>) ois.readObject());
        setMethod((String) ois.readObject());
        setMethods((List<String>) ois.readObject());
        setMappingParameter((String) ois.readObject());
        setMappingParameters((List<String>) ois.readObject());
        setAnnotation((String) ois.readObject());
        setAnnotations((List<String>) ois.readObject());
        setOrganism((String) ois.readObject());
        setOrganisms((List<String>) ois.readObject());
        setCellLine((String) ois.readObject());
        setCellLines((List<String>) ois.readObject());
        setNucleosideConcentration((String) ois.readObject());
        setNucleosideConcentrations((List<String>) ois.readObject());
        setNucleosideIncubation((String) ois.readObject());
        setNucleosideIncubations((List<String>) ois.readObject());
        setSampleVolume((String) ois.readObject());
        setSampleMass((String) ois.readObject());
        setExcitationWavelength((String) ois.readObject());
        setExcitationWavelengths((List<String>) ois.readObject());
        setCrosslinkEnergy((String) ois.readObject());
        setCrosslinkEnergys((List<String>) ois.readObject());
        setAntibody((String) ois.readObject());
        setAntibodys((List<String>) ois.readObject());
        setProteinTag((String) ois.readObject());
        setProteinTags((List<String>) ois.readObject());
        setAntibodyAmount((String) ois.readObject());
        setAntibodyAmounts((List<String>) ois.readObject());
        setRnaseUsed((String) ois.readObject());
        setRnaseUseds((List<String>) ois.readObject());
        setRnaseTreatmentNumber((String) ois.readObject());
        setWashSteps((String) ois.readObject());
        setWashStepss((List<String>) ois.readObject());
        setBarcode((String) ois.readObject());
        setBarcodes((List<String>) ois.readObject());
        setPcrCycles((String) ois.readObject());
        setMinReadLength((String) ois.readObject());
        setFivePrimeAdapter((String) ois.readObject());
        setFivePrimeAdapters((List<String>) ois.readObject());
        setThreePrimeAdapter((String) ois.readObject());
        setThreePrimeAdapters((List<String>) ois.readObject());
        setSequencer((String) ois.readObject());
        setSequencers((List<String>) ois.readObject());
        setBackgroundTreatment((String) ois.readObject());
        setBackgroundTreatments((List<String>) ois.readObject());
        setPercentData((Integer) ois.readObject());
        setEmail((String) ois.readObject());
        setPreparationDateDay((Integer)ois.readObject());
        setPreparationDateMonth((Integer)ois.readObject());
        setPreparationDateYear((Integer)ois.readObject());
    }
}
