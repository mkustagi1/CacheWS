package cache.dataimportes.util;

import cache.dataimportes.model.Bartelpolyareads;
import cache.dataimportes.model.Biotypes;
import cache.dataimportes.model.Datastatistics;
import cache.dataimportes.model.Experiment;
import cache.dataimportes.model.Experimentkey;
import cache.dataimportes.model.Experimentsummary;
import cache.dataimportes.model.Indices;
import cache.dataimportes.model.Levelclassification;
import cache.dataimportes.model.Method;
import cache.dataimportes.model.Polyareads;
import cache.dataimportes.model.Reads;
import cache.dataimportes.model.Transcript;
import cache.dataimportes.model.Transcriptalignment;
import cache.dataimportes.model.Transcriptalignment1mismatch;
import cache.dataimportes.model.Transcriptalignment1mismatchtranspose;
import cache.dataimportes.model.Transcriptalignment2mismatch;
import cache.dataimportes.model.Transcriptalignment2mismatchtranspose;
import cache.dataimportes.model.Transcriptalignmentbartelpolya;
import cache.dataimportes.model.Transcriptalignmentbartelpolyatranspose;
import cache.dataimportes.model.Transcriptalignmentpolya;
import cache.dataimportes.model.Transcriptalignmentpolyatranspose;
import cache.dataimportes.model.Transcriptalignmentsummary;
import cache.dataimportes.model.Transcriptalignmenttranspose;
import cache.dataimportes.model.Transcriptalignmenttss;
import cache.dataimportes.model.Transcriptalignmenttsstranspose;
import cache.dataimportes.model.Transcriptannotations;
import cache.dataimportes.model.Tssreads;
import cache.dataimportes.model.Unmappedreads;
import cache.dataimportes.model.Users;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

/**
 *
 * @author Manjunath Kustagi
 */
public class DataAccess {

    public static final Cluster CLUSTER = Cluster.builder()
            .withClusterName("cache")
            .addContactPoint("127.0.0.1")
            .withSocketOptions(
                    new SocketOptions()
                    .setConnectTimeoutMillis(3000)
                    .setReadTimeoutMillis(Integer.MAX_VALUE))
            .build();

    public static final Session SESSION = CLUSTER.connect();

    public static final MappingManager MANAGER = new MappingManager(SESSION);

    public static final Mapper<Reads> READSMAPPER = MANAGER.mapper(Reads.class);

    public static final Mapper<Experiment> EXPERIMENTMAPPER = MANAGER.mapper(Experiment.class);

    public static final Mapper<Experimentkey> EXPERIMENTKEYMAPPER = MANAGER.mapper(Experimentkey.class);

    public static final Mapper<Unmappedreads> UNMAPPEDREADSMAPPER = MANAGER.mapper(Unmappedreads.class);

    public static final Mapper<Transcript> TRANSCRIPTMAPPER = MANAGER.mapper(Transcript.class);

    public static final Mapper<Transcriptalignment> TRANSCRIPTALIGNMENTMAPPER = MANAGER.mapper(Transcriptalignment.class);

    public static final Mapper<Transcriptalignmentpolya> TRANSCRIPTALIGNMENTPOLYAMAPPER = MANAGER.mapper(Transcriptalignmentpolya.class);

    public static final Mapper<Transcriptalignmentbartelpolya> TRANSCRIPTALIGNMENTBARTELPOLYAMAPPER = MANAGER.mapper(Transcriptalignmentbartelpolya.class);

    public static final Mapper<Transcriptalignmenttss> TRANSCRIPTALIGNMENTTSSMAPPER = MANAGER.mapper(Transcriptalignmenttss.class);

    public static final Mapper<Transcriptalignment1mismatch> TRANSCRIPTALIGNMENT1MISMATCHMAPPER = MANAGER.mapper(Transcriptalignment1mismatch.class);

    public static final Mapper<Transcriptalignment2mismatch> TRANSCRIPTALIGNMENT2MISMATCHMAPPER = MANAGER.mapper(Transcriptalignment2mismatch.class);

    public static final Mapper<Transcriptalignmenttranspose> TRANSCRIPTALIGNMENTTRANSPOSEMAPPER = MANAGER.mapper(Transcriptalignmenttranspose.class);

    public static final Mapper<Transcriptalignment1mismatchtranspose> TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEMAPPER = MANAGER.mapper(Transcriptalignment1mismatchtranspose.class);

    public static final Mapper<Transcriptalignment2mismatchtranspose> TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEMAPPER = MANAGER.mapper(Transcriptalignment2mismatchtranspose.class);

    public static final Mapper<Datastatistics> DATASTATISTICSMAPPER = MANAGER.mapper(Datastatistics.class);

    public static final Mapper<Polyareads> POLYAREADSMAPPER = MANAGER.mapper(Polyareads.class);

    public static final Mapper<Bartelpolyareads> BARTELPOLYAREADSMAPPER = MANAGER.mapper(Bartelpolyareads.class);

    public static final Mapper<Tssreads> TSSREADSMAPPER = MANAGER.mapper(Tssreads.class);

    public static final Mapper<Transcriptalignmentsummary> TRANSCRIPTALIGNMENTSUMMARYMAPPER = MANAGER.mapper(Transcriptalignmentsummary.class);

    public static final Mapper<Experimentsummary> EXPERIMENTSUMMARYMAPPER = MANAGER.mapper(Experimentsummary.class);

    public static final Mapper<Transcriptalignmenttsstranspose> TRANSCRIPTALIGNMENTTSSTRANSPOSEMAPPER = MANAGER.mapper(Transcriptalignmenttsstranspose.class);

    public static final Mapper<Transcriptalignmentpolyatranspose> TRANSCRIPTALIGNMENTPOLYATRANSPOSEMAPPER = MANAGER.mapper(Transcriptalignmentpolyatranspose.class);

    public static final Mapper<Transcriptalignmentbartelpolyatranspose> TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEMAPPER = MANAGER.mapper(Transcriptalignmentbartelpolyatranspose.class);

    public static final Mapper<Biotypes> BIOTYPESMAPPER = MANAGER.mapper(Biotypes.class);

    public static final Mapper<Transcriptannotations> TRANSCRIPTANNOTATIONSMAPPER = MANAGER.mapper(Transcriptannotations.class);

    public static final Mapper<Users> USERSMAPPER = MANAGER.mapper(Users.class);

    public static final Mapper<Levelclassification> LEVELCLASSIFICATIONMAPPER = MANAGER.mapper(Levelclassification.class);

    public static final Mapper<Method> METHODMAPPER = MANAGER.mapper(Method.class);

    public static final Mapper<Indices> INDICESMAPPER = MANAGER.mapper(Indices.class);

    public static final PreparedStatement EXPERIMENTCOUNTQUERY = SESSION.prepare("select count(*) "
            + "from cache.experiment");

    public static final PreparedStatement EXPERIMENTKEYQUERY = SESSION.prepare("select experimentid, distance, key "
            + "from cache.experimentkey");

    public static final PreparedStatement EXPERIMENTQUERY = SESSION.prepare("select key, id, distance, library, investigator, annotation, method, readlength, attributes "
            + "from cache.experiment where key=:ky and id=:id and distance=:d");

    public static final PreparedStatement EXPERIMENTREADLENGTHQUERY = SESSION.prepare("select readlength from cache.experiment where key=:ky and id=:id and distance=:d");

    public static final PreparedStatement EXPERIMENTSUMMARYQUERY = SESSION.prepare("select * "
            + "from cache.experimentsummary where experimentid=:eid and distance=:d and transcriptid=:ti");

    public static final PreparedStatement EXPERIMENTSUMMARYCOUNTQUERY = SESSION.prepare("select count(*) "
            + "from cache.experimentsummary where experimentid=:eid and distance=:d and transcriptid=:ti");

    public static final PreparedStatement DATASTATISTICSQUERY = SESSION.prepare("select key, experimentid, distance, id, annotations, "
            + "totalmappedreads, totalmappedreadsnonredundant, totalreads, totalreadsnonredundant,totalmappedreadsRC, totalmappedreadsnonredundantRC "
            + "from cache.datastatistics where key=:ky and experimentid=:eid and distance=:d");

    public static final PreparedStatement DATASTATISTICSCOUNTQUERY = SESSION.prepare("select count(*) "
            + "from cache.datastatistics where key=:ky and experimentid=:eid and distance=:d");

    public static final PreparedStatement TRANSCRIPTQUERY = SESSION.prepare("select key, transcriptid, name, sequence"
            + " from cache.transcript where key=1 and transcriptid=:ti");

    public static final PreparedStatement TRANSCRIPTALIGNMENTQUERY = SESSION.prepare("select experimentid, transcriptid, "
            + "reversecomplement, id, readid, score, startcoordinate, stopcoordinate, readcount from cache.transcriptalignment where experimentid=:ei and transcriptid=:ti");
    
    public static final PreparedStatement TRANSCRIPTALIGNMENTREVERSECOMPLEMENTQUERY = SESSION.prepare("select experimentid, transcriptid, "
            + "reversecomplement, id, readid, score, startcoordinate, stopcoordinate from cache.transcriptalignment where experimentid=:ei and transcriptid=:ti and reversecomplement=:rc");

    public static final PreparedStatement TRANSCRIPTALIGNMENT1MISMATCHQUERY = SESSION.prepare("select experimentid, transcriptid, "
            + "reversecomplement, mismatchtype, id, mismatchcoordinate, readid, score, startcoordinate, stopcoordinate, readcount, variant from cache.transcriptalignment1mismatch where experimentid=:ei and transcriptid=:ti");

    public static final PreparedStatement TRANSCRIPTALIGNMENT1MISMATCHREVERSECOMPLEMENTQUERY = SESSION.prepare("select experimentid, transcriptid, "
            + "reversecomplement, mismatchtype, id, mismatchcoordinate, readid, score, startcoordinate, stopcoordinate, variant from cache.transcriptalignment1mismatch where experimentid=:ei and transcriptid=:ti and reversecomplement=:rc");

    public static final PreparedStatement TRANSCRIPTALIGNMENT2MISMATCHQUERY = SESSION.prepare("select experimentid, transcriptid, "
            + "reversecomplement, mismatch1type, mismatch2type, id, mismatch1coordinate, mismatch2coordinate, readid, score, startcoordinate, stopcoordinate, readcount, variant1, variant2 from cache.transcriptalignment2mismatch where experimentid=:ei and transcriptid=:ti");

    public static final PreparedStatement TRANSCRIPTALIGNMENT2MISMATCHREVERSECOMPLEMENTQUERY = SESSION.prepare("select experimentid, transcriptid, "
            + "reversecomplement, mismatch1type, mismatch2type, id, mismatch1coordinate, mismatch2coordinate, readid, score, startcoordinate, stopcoordinate, variant1, variant2 from cache.transcriptalignment2mismatch where experimentid=:ei and transcriptid=:ti and reversecomplement=:rc");

    public static final PreparedStatement TRANSCRIPTALIGNMENTPOLYAQUERY = SESSION.prepare("select * from cache.transcriptalignmentpolya where key=1 and transcriptid=:ti");

    public static final PreparedStatement TRANSCRIPTALIGNMENTBARTELPOLYAQUERY = SESSION.prepare("select * from cache.transcriptalignmentbartelpolya where key=1 and transcriptid=:ti");

    public static final PreparedStatement TRANSCRIPTALIGNMENTTSSQUERY = SESSION.prepare("select * from cache.transcriptalignmenttss where key=1 and transcriptid=:ti");

    public static final PreparedStatement TRANSCRIPTALIGNMENTTRANSPOSEQUERY = SESSION.prepare("select transcriptid, count from cache.transcriptalignmenttranspose where experimentid=:ei and readid=:ri");

    public static final PreparedStatement TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEQUERY = SESSION.prepare("select transcriptid, count from cache.transcriptalignment1mismatchtranspose where experimentid=:ei and readid=:ri");

    public static final PreparedStatement TRANSCRIPTALIGNMENT2MISMATCHTRANSPOSEQUERY = SESSION.prepare("select transcriptid, count from cache.transcriptalignment2mismatchtranspose where experimentid=:ei and readid=:ri");

    public static final PreparedStatement TRANSCRIPTALIGNMENTTSSTRANSPOSEQUERY = SESSION.prepare("select transcriptid from cache.transcriptalignmenttsstranspose where key=1 and readid=:ri");

    public static final PreparedStatement TRANSCRIPTALIGNMENTPOLYATRANSPOSEQUERY = SESSION.prepare("select transcriptid from cache.transcriptalignmentpolyatranspose where key=1 and readid=:ri");

    public static final PreparedStatement TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEQUERY = SESSION.prepare("select transcriptid from cache.transcriptalignmentbartelpolyatranspose where key=1 and readid=:ri");

    public static final PreparedStatement READSCOUNTQUERY = SESSION.prepare("select count from cache.reads where experimentid=:ei and id=:ri");

    public static final PreparedStatement READSQUERY = SESSION.prepare("select sequence, count from cache.reads where experimentid=:ei and id=:ri");

    public static final PreparedStatement UPDATETRANSCRIPTALIGNMENTQUERY = SESSION.prepare("update cache.transcriptalignment set readcount=:rdc where experimentid=:ei and transcriptid=:ti and reversecomplement=:rc and id=:id");

    public static final PreparedStatement UPDATETRANSCRIPTALIGNMENT1MISMATCHQUERY = SESSION.prepare("update cache.transcriptalignment1mismatch set readcount=:rdc where experimentid=:ei and transcriptid=:ti and reversecomplement=:rc and mismatchtype=:mt and id=:id");

    public static final PreparedStatement UPDATETRANSCRIPTALIGNMENT2MISMATCHQUERY = SESSION.prepare("update cache.transcriptalignment2mismatch set readcount=:rdc where experimentid=:ei and transcriptid=:ti and reversecomplement=:rc and mismatch1type=:mt1 and mismatch2type=:mt2 and id=:id");

    public static final PreparedStatement ALLBIOTYPEQUERY = SESSION.prepare("select biotype, leveliclassification, levelifunction, leveligenestructure, leveliiclassification, leveliiiclassification, "
            + "levelivclassification from cache.biotypes where key=1");

//    public static final PreparedStatement ALLBIOTYPESLIKEQUERY = SESSION.prepare("select biotype, leveliclassification, levelifunction, leveligenestructure, leveliiclassification, leveliiiclassification, "
//            + "levelivclassification from cache.biotypes where key=1"
//            + " and (biotype like :bt or leveliclassification like :bt or levelifunction like :bt"
//            + " or leveligenestructure like :bt or leveliiclassification like :bt"
//            + " or leveliiiclassification like :bt or levelivclassification like :bt)");

    public static final PreparedStatement BIOTYPESQUERY = SESSION.prepare("select * from cache.biotypes where key=1 and transcriptid=:ti");

    public static final PreparedStatement USERSQUERY = SESSION.prepare("select * from cache.users where key=1 and user=:us");

    public static final PreparedStatement TRANSCRIPTANNOTATIONSQUERY = SESSION.prepare("select * from cache.transcriptannotations where key=1 and transcriptid=:ti");

    public static final PreparedStatement MAXTRANSCRIPTIDQUERY = SESSION.prepare("select max(transcriptid) from cache.transcript where key=1;");

    public static final PreparedStatement LEVELCLASSIFICATIONQUERY = SESSION.prepare("select * from cache.levelclassification where category=:c and level=:l");
}
