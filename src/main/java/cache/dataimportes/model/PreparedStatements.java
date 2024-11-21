package cache.dataimportes.model;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author Manjunath Kustagi
 */
public class PreparedStatements {
        final static Cluster CLUSTER = Cluster.builder()
            .withClusterName("cache")
            .addContactPoint("127.0.0.1")
            .build();

    public final static Session SESSION = CLUSTER.connect();

    static final String KEYSPACE = "cache";
    static final String TA_TABLE = "transcriptalignment";
    static final String TAT_TABLE = "transcriptalignmenttranspose";
    static final String TA1_TABLE = "transcriptalignment1mismatch";
    static final String TAT1_TABLE = "transcriptalignment1mismatchtranspose";
    static final String TA2_TABLE = "transcriptalignment2mismatch";
    static final String TAT2_TABLE = "transcriptalignment2mismatchtranspose";
    static final String UMR_TABLE = "unmappedreads";

    final static PreparedStatement TA_INSERT_STMT = SESSION.prepare(String.format("INSERT INTO %s.%s ("
            + "experimentid, transcriptid, reversecomplement, id, readid, score, startcoordinate, stopcoordinate"
            + ") VALUES ("
            + ":eid, :tid, :rc, :id, :rid, :sc, :stc, :stoc"
            + ")", KEYSPACE, TA_TABLE));
        
    final static PreparedStatement TAT_INSERT_STMT = SESSION.prepare(String.format("INSERT INTO %s.%s ("
            + "experimentid, readid, transcriptid"
            + ") VALUES ("
            + ":eid, :rid, :tid"
            + ")", KEYSPACE, TAT_TABLE));

    final static PreparedStatement TA1_INSERT_STMT = SESSION.prepare(String.format("INSERT INTO %s.%s ("
            + "experimentid, transcriptid, reversecomplement, mismatchtype, id, mismatchcoordinate, readid, score, startcoordinate, stopcoordinate, variant"
            + ") VALUES ("
            + ":eid, :tid, :rc, :mt, :id, :mc, :rid, :sc, :stc, :stoc, :v"
            + ")", KEYSPACE, TA1_TABLE));

    final static PreparedStatement TAT1_INSERT_STMT = SESSION.prepare(String.format("INSERT INTO %s.%s ("
            + "experimentid, readid, transcriptid"
            + ") VALUES ("
            + ":eid, :rid, :tid"
            + ")", KEYSPACE, TAT1_TABLE));

    final static PreparedStatement TA2_INSERT_STMT = SESSION.prepare(String.format("INSERT INTO %s.%s ("
            + "experimentid, transcriptid, reversecomplement, mismatch1type, mismatch2type, id, mismatch1coordinate, "
            + "mismatch2coordinate, readid, score, startcoordinate, stopcoordinate, variant1, variant2"
            + ") VALUES ("
            + ":eid, :tid, :rc, :m1t, :m2t, :id, :m1c, :m2c, :rid, :sc, :stc, :stoc, :v1, :v2"
            + ")", KEYSPACE, TA2_TABLE));

    final static PreparedStatement TAT2_INSERT_STMT = SESSION.prepare(String.format("INSERT INTO %s.%s ("
            + "experimentid, readid, transcriptid"
            + ") VALUES ("
            + ":eid, :rid, :tid"
            + ")", KEYSPACE, TAT2_TABLE));

    final static PreparedStatement UMR_INSERT_STMT = SESSION.prepare(String.format("INSERT INTO %s.%s ("
            + "experimentid, readid"
            + ") VALUES ("
            + ":eid, :rid"
            + ")", KEYSPACE, UMR_TABLE));

    final static PreparedStatement UMR_DELETE_STMT = SESSION.prepare(String.format("DELETE FROM %s.%s where"
            + "experimentid=:eid and readid=:rid", KEYSPACE, UMR_TABLE));

    public static BoundStatement bindTranscriptalignmentInsert(Map<String, Object> values) {
        BoundStatement bs = TA_INSERT_STMT.bind()
                .setLong("eid", (Long)values.get("eid"))
                .setLong("tid", (Long)values.get("tid"))
                .setBool("rc", (Boolean)values.get("rc"))
                .setUUID("id", (UUID)values.get("id"))
                .setUUID("rid", (UUID)values.get("rid"))
                .setDouble("sc", (Double)values.get("sc"))
                .setLong("stc", (Long)values.get("stc"))
                .setLong("stoc", (Long)values.get("stoc"));
        return bs;
    }

    public static BoundStatement bindTranscriptalignmentTransposeInsert(Map<String, Object> values) {
        BoundStatement bs = TAT_INSERT_STMT.bind().setLong("eid", (Long)values.get("eid"))
                .setUUID("rid", (UUID)values.get("rid")).setLong("tid", (Long)values.get("tid"));
        return bs;
    }

    public static BoundStatement bindTranscriptalignment1MismatchInsert(Map<String, Object> values) {
        BoundStatement bs = TA1_INSERT_STMT.bind()
                .setLong("eid", (Long)values.get("eid"))
                .setLong("tid", (Long)values.get("tid"))
                .setBool("rc", (Boolean)values.get("rc"))
                .setString("mt", (String)values.get("mt"))
                .setUUID("id", (UUID)values.get("id"))
                .setInt("mc", (Integer)values.get("mc"))
                .setUUID("rid", (UUID)values.get("rid"))
                .setDouble("sc", (Double)values.get("sc"))
                .setLong("stc", (Long)values.get("stc"))
                .setLong("stoc", (Long)values.get("stoc"))
                .setString("v", (String)values.get("v"));
        return bs;
    }

    public static BoundStatement bindTranscriptalignment1MismatchTransposeInsert(Map<String, Object> values) {
        BoundStatement bs = TAT1_INSERT_STMT.bind().setLong("eid", (Long)values.get("eid"))
                .setUUID("rid", (UUID)values.get("rid")).setLong("tid", (Long)values.get("tid"));
        return bs;
    }

    public static BoundStatement bindTranscriptalignment2MismatchInsert(Map<String, Object> values) {
        BoundStatement bs = TA2_INSERT_STMT.bind()
                .setLong("eid", (Long)values.get("eid"))
                .setLong("tid", (Long)values.get("tid"))
                .setBool("rc", (Boolean)values.get("rc"))
                .setString("m1t", (String)values.get("m1t"))
                .setString("m2t", (String)values.get("m2t"))
                .setUUID("id", (UUID)values.get("id"))
                .setInt("m1c", (Integer)values.get("m1c"))
                .setInt("m2c", (Integer)values.get("m2c"))
                .setUUID("rid", (UUID)values.get("rid"))
                .setDouble("sc", (Double)values.get("sc"))
                .setLong("stc", (Long)values.get("stc"))
                .setLong("stoc", (Long)values.get("stoc"))
                .setString("v1", (String)values.get("v1"))
                .setString("v2", (String)values.get("v2"));
        return bs;
    }

    public static BoundStatement bindTranscriptalignment2MismatchTransposeInsert(Map<String, Object> values) {
        BoundStatement bs = TAT2_INSERT_STMT.bind().setLong("eid", (Long)values.get("eid"))
                .setUUID("rid", (UUID)values.get("rid")).setLong("tid", (Long)values.get("tid"));
        return bs;
    }
    
    public static BoundStatement bindUnmappedReadsInsert(Map<String, Object> values) {
        BoundStatement bs = UMR_INSERT_STMT.bind().setLong("eid", (Long)values.get("eid")).setUUID("rid", (UUID)values.get("rid"));
        return bs;
    }

    public static BoundStatement bindUnmappedReadsDelete(Map<String, Object> values) {
        BoundStatement bs = UMR_DELETE_STMT.bind().setLong("eid", (Long)values.get("eid")).setUUID("rid", (UUID)values.get("rid"));
        return bs;
    }
}
