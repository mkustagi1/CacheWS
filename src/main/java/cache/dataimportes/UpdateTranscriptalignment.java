package cache.dataimportes;

import cache.dataimportes.model.Reads;
import cache.dataimportes.model.Transcriptalignment1mismatch;
import cache.dataimportes.model.Transcriptalignment2mismatch;
import cache.dataimportes.util.ConsumerService;
import cache.dataimportes.util.DamerauLevenshteinEditDistance;
import static cache.dataimportes.util.DataAccess.READSMAPPER;
import static cache.dataimportes.util.DataAccess.SESSION;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT1MISMATCHMAPPER;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT2MISMATCHMAPPER;
import cache.dataimportes.util.Fuzzy;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.lang3.StringUtils;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.views.ComplementSequenceView;
import org.biojava.nbio.core.sequence.views.ReversedSequenceView;

public class UpdateTranscriptalignment {

    static final BlockingQueue<Callable> CONSUMER_QUEUE = new LinkedBlockingQueue<>();
    static final Map<Long, String> TRANSCRIPTS = new HashMap<>();

    static {
//        LogManager.getLogManager().reset();
//        Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
//        globalLogger.setLevel(java.util.logging.Level.WARNING);
        Thread consumerService = new Thread(new ConsumerService(CONSUMER_QUEUE));
        consumerService.setPriority(Thread.MAX_PRIORITY);
        consumerService.start();
    }

    public void runAlignment1Mismatch(Long experimentId, Integer index) {
        try {
            Statement stmt = QueryBuilder
                    .select()
                    .all()
                    .from("cache", "transcriptalignment1mismatch")
                    .where(eq("experimentid", experimentId))
                    .setFetchSize(1000);
            ResultSet rs = SESSION.execute(stmt);

            List<Transcriptalignment1mismatch> mismatches = TRANSCRIPTALIGNMENT1MISMATCHMAPPER.map(rs).all();

            Integer rowCount = mismatches.size();
            System.out.println("rowCount: " + rowCount);

            Integer numThreads = 80;
            Long batchSize = (long) Math.ceil(rowCount.doubleValue() / numThreads.doubleValue());
            System.out.println("batchSize: " + batchSize);

            int tCount = 0;
            Map<Integer, Integer[]> bounds = new HashMap<>();
            Integer min = null, max = null;
            for (int i = 0; i < mismatches.size(); i++) {
                if (i == 0) {
                    min = i;
                } else if (i == (tCount + 1) * batchSize) {
                    max = i;
                    bounds.put(tCount, new Integer[]{min, max});
                    tCount++;
                    min = max;
                } else if ((i == rowCount - 1) && (bounds.get(numThreads - 1) == null)) {
                    max = i;
                    bounds.put((numThreads - 1), new Integer[]{min, max});
                }
            }

            ExecutorService ex = Executors.newFixedThreadPool(numThreads, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });
            System.out.println("ExecutorService created..");

            List<Callable<Object>> callables = new ArrayList<>();
            for (int j = 0; j < numThreads; j++) {
                Integer[] range = bounds.get(j);
                System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                CallableImplTranscripts t = new CallableImplTranscripts(experimentId, j, range[0], range[1], index, mismatches);
                callables.add(t);
            }

            System.out.println("Invoking callables");
            ex.invokeAll(callables);
            ex.shutdown();
        } catch (NumberFormatException | InterruptedException e) {
            Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public void runAlignment2Mismatch(Long experimentId, Integer index) {
        try {
            Statement stmt = QueryBuilder
                    .select()
                    .all()
                    .from("cache", "transcriptalignment2mismatch")
                    .where(eq("experimentid", experimentId))
                    .setFetchSize(1000);
            ResultSet rs = SESSION.execute(stmt);

            List<Transcriptalignment2mismatch> mismatches = TRANSCRIPTALIGNMENT2MISMATCHMAPPER.map(rs).all();

            Integer rowCount = mismatches.size();
            System.out.println("rowCount: " + rowCount);

            Integer numThreads = 80;
            Long batchSize = (long) Math.ceil(rowCount.doubleValue() / numThreads.doubleValue());
            System.out.println("batchSize: " + batchSize);

            int tCount = 0;
            Map<Integer, Integer[]> bounds = new HashMap<>();
            Integer min = null, max = null;
            for (int i = 0; i < mismatches.size(); i++) {
                if (i == 0) {
                    min = i;
                } else if (i == (tCount + 1) * batchSize) {
                    max = i;
                    bounds.put(tCount, new Integer[]{min, max});
                    tCount++;
                    min = max;
                } else if ((i == rowCount - 1) && (bounds.get(numThreads - 1) == null)) {
                    max = i;
                    bounds.put((numThreads - 1), new Integer[]{min, max});
                }
            }

            ExecutorService ex = Executors.newFixedThreadPool(numThreads, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });
            System.out.println("ExecutorService created..");

            List<Callable<Object>> callables = new ArrayList<>();
            for (int j = 0; j < numThreads; j++) {
                Integer[] range = bounds.get(j);
                System.out.println(j + ", min: " + range[0].toString() + ", max: " + range[1].toString());
                CallableImpl2MismatchTranscripts t = new CallableImpl2MismatchTranscripts(experimentId, j, range[0], range[1], index, mismatches);
                callables.add(t);
            }

            System.out.println("Invoking callables");
            ex.invokeAll(callables);
            ex.shutdown();
        } catch (NumberFormatException | InterruptedException e) {
            Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    class CallableImplTranscripts implements Callable {

        Long experimentId;
        Integer min;
        Integer max;
        Integer index;
        int threadId;
        List<Transcriptalignment1mismatch> mismatches;

        public CallableImplTranscripts(long experimentId, int threadId, Integer min, Integer max, Integer index, List<Transcriptalignment1mismatch> mismatches) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
                this.index = index;
                this.mismatches = mismatches;
            } catch (Exception ex) {
                Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        public Object call() {
            System.out.println("called threadId: " + threadId);
            final Object lock = new Object();

            long start = System.nanoTime();

            int count = 0;
            BatchStatement batch = new BatchStatement();
            for (int i = min; i < max; i++) {
                try {
                    Transcriptalignment1mismatch ta1m = mismatches.get(i);
                    boolean rc = ta1m.getReversecomplement();
                    UUID rid = ta1m.getReadid();
                    Long tid = ta1m.getTranscriptid();
                    Reads r = READSMAPPER.get(experimentId, rid);
                    String s = r.getSequence();
                    if (rc) {
                        DNASequence sequence = new DNASequence(s);
                        org.biojava.nbio.core.sequence.template.Sequence _rc
                                = new ReversedSequenceView<>(
                                        new ComplementSequenceView<>(sequence));
                        String revComp = _rc.getSequenceAsString();
                        Integer m1c = computeMismatch1Coordinate(TRANSCRIPTS.get(tid), revComp, rc);
                        ta1m.setMismatchcoordinate(m1c);
                        batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta1m));
                    } else {
                        Integer m1c = computeMismatch1Coordinate(TRANSCRIPTS.get(tid), s, rc);
                        ta1m.setMismatchcoordinate(m1c);
                        batch.add(TRANSCRIPTALIGNMENT1MISMATCHMAPPER.saveQuery(ta1m));
                    }
                    if (batch.size() > 10000) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                    if (count % 10000 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("UpdateTranscriptalignment: Aligned " + count + " alignments from experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (Exception ex) {
                    Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("All alignments in experiment " + experimentId + " on thread " + threadId + " are updated, it took " + seconds + " seconds");

            return threadId;
        }

        Integer computeMismatch1Coordinate(String s, String r, boolean rc) {
            Integer m1c = 0;
            Fuzzy fuzzy = new Fuzzy();
            double distance = fuzzy.containability(s, r) * r.length();
            if (Math.rint(distance) == 1) {
                int st = fuzzy.getResultStart() - 1;
                int end = fuzzy.getResultEnd();
                if (end <= s.length()) {
                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                            = DamerauLevenshteinEditDistance.compute(s.substring(st, end), r);
                    if (ledr.getDistance() == 1.0) {
                        String editLine = ledr.getEditSequence();
                        String eString = ledr.getTopAlignmentRow();
                        String rString = ledr.getBottomAlignmentRow();
                        String mt = "";
                        String v = "";
                        if (editLine.contains("S")) {
                            m1c = editLine.indexOf("S");
                            mt = "S";
                            v = r.substring(m1c, m1c + 1);
                        } else if (editLine.contains("TT")) {
                            m1c = editLine.indexOf("TT");
                            mt = "TT";
                            v = r.substring(m1c, m1c + 2);
                        } else if (editLine.contains("I")) {
                            m1c = eString.indexOf("-");
                            mt = "I";
                            v = r.substring(m1c, m1c + 1);
                        } else if (editLine.contains("D")) {
                            m1c = rString.indexOf("-");
                            mt = "D";
                            v = s.substring(m1c, m1c + 1);
                        }
                    }
                }
            }
            return m1c;
        }
    }

    class CallableImpl2MismatchTranscripts implements Callable {

        Long experimentId;
        int min;
        int max;
        Integer index;
        int threadId;
        List<Transcriptalignment2mismatch> mismatches;

        public CallableImpl2MismatchTranscripts(long experimentId, int threadId, int min, int max, Integer index, List<Transcriptalignment2mismatch> mismatches) {
            try {
                this.experimentId = experimentId;
                this.threadId = threadId;
                this.min = min;
                this.max = max;
                this.index = index;
                this.mismatches = mismatches;
            } catch (Exception ex) {
                Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        @Override
        public Object call() {
            System.out.println("called threadId: " + threadId);
            final Object lock = new Object();

            long start = System.nanoTime();

            int count = 0;
            BatchStatement batch = new BatchStatement();
            for (int i = min; i < max; i++) {
                try {
                    Transcriptalignment2mismatch ta2m = mismatches.get(i);
                    boolean rc = ta2m.getReversecomplement();
                    UUID rid = ta2m.getReadid();
                    Long tid = ta2m.getTranscriptid();
                    String mt1 = ta2m.getMismatch1type();
                    String mt2 = ta2m.getMismatch2type();
                    Reads r = READSMAPPER.get(experimentId, rid);
                    String s = r.getSequence();
                    Triplet[] mm;
                    if (rc) {
                        DNASequence sequence = new DNASequence(s);
                        org.biojava.nbio.core.sequence.template.Sequence _rc
                                = new ReversedSequenceView<>(
                                        new ComplementSequenceView<>(sequence));
                        String revComp = _rc.getSequenceAsString();
                        mm = computeMismatch2Coordinate(TRANSCRIPTS.get(tid), revComp, true);
                    } else {
                        mm = computeMismatch2Coordinate(TRANSCRIPTS.get(tid), s, false);
                    }
                    if (mt1.equals(mt2)) {
                        if (ta2m.getVariant1().equals(mm[0].v)) {
                            ta2m.setMismatch1coordinate(mm[0].p);
                            ta2m.setMismatch2coordinate(mm[1].p);
                        } else {
                            ta2m.setMismatch1coordinate(mm[1].p);
                            ta2m.setMismatch2coordinate(mm[0].p);
                        }
                    } else if (mt1.equals(mm[0].m) && ta2m.getVariant1().equals(mm[0].v)) {
                        ta2m.setMismatch1coordinate(mm[0].p);
                        ta2m.setMismatch2coordinate(mm[1].p);
                    } else {
                        ta2m.setMismatch1coordinate(mm[1].p);
                        ta2m.setMismatch2coordinate(mm[0].p);
                    }
                    batch.add(TRANSCRIPTALIGNMENT2MISMATCHMAPPER.saveQuery(ta2m));

                    if (batch.size() > 10000) {
                        SESSION.execute(batch);
                        batch.clear();
                    }
                    if (count % 10000 == 0) {
                        long end1 = System.nanoTime();
                        long seconds1 = TimeUnit.NANOSECONDS.toSeconds(end1 - start);
                        System.out.println("UpdateTranscriptalignment: Aligned " + count + " alignments from experiment "
                                + experimentId + " on thread: " + threadId + " in " + seconds1 + " seconds");
                    }
                    count++;
                } catch (Exception ex) {
                    Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (batch.size() > 0) {
                SESSION.execute(batch);
                batch.clear();
            }
            long end = System.nanoTime();
            long seconds = TimeUnit.NANOSECONDS.toSeconds(end - start);
            System.out.println("All alignments in experiment " + experimentId + " on thread " + threadId + " are updated, it took " + seconds + " seconds");

            return threadId;
        }

        Integer computeMismatch1Coordinate(String s, String r, boolean rc) {
            Integer m1c = 0;
            Fuzzy fuzzy = new Fuzzy();
            double distance = fuzzy.containability(s, r) * r.length();
            int st = fuzzy.getResultStart() - 1;
            int end = fuzzy.getResultEnd();
            if (end <= s.length()) {
                DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                        = DamerauLevenshteinEditDistance.compute(s.substring(st, end), r);
                if (ledr.getDistance() == 1.0) {
                    String editLine = ledr.getEditSequence();
                    String eString = ledr.getTopAlignmentRow();
                    String rString = ledr.getBottomAlignmentRow();
                    String mt = "";
                    String v = "";
                    if (editLine.contains("S")) {
                        m1c = editLine.indexOf("S");
                        mt = "S";
                        v = r.substring(m1c, m1c + 1);
                    } else if (editLine.contains("TT")) {
                        m1c = editLine.indexOf("TT");
                        mt = "TT";
                        v = r.substring(m1c, m1c + 2);
                    } else if (editLine.contains("I")) {
                        m1c = eString.indexOf("-");
                        mt = "I";
                        v = r.substring(m1c, m1c + 1);
                    } else if (editLine.contains("D")) {
                        m1c = rString.indexOf("-");
                        mt = "D";
                        v = s.substring(m1c, m1c + 1);
                    }
                    if (!v.equals("") && rc) {
                        m1c = r.length() - m1c - 1;
                    }
                }
            }
            return m1c;
        }

        Triplet[] computeMismatch2Coordinate(String s, String r, boolean rc) {
            Integer mc1 = 0, mc2 = 0;
            String mt1 = "", mt2 = "";
            String v1 = "", v2 = "";
            Fuzzy fuzzy = new Fuzzy();
            double distance = fuzzy.containability(s, r) * r.length();
            if (Math.rint(distance) == 2.0) {
                int st = fuzzy.getResultStart() - 1;
                int end = fuzzy.getResultEnd();
                int length = end - st;
                if (length < r.length()) {
                    end += r.length() - length;
                }
                if (end <= s.length()) {
                    DamerauLevenshteinEditDistance.LevenshteinEditDistanceResult ledr
                            = DamerauLevenshteinEditDistance.compute(s.substring(st, end), r);
                    if (ledr.getDistance() == 2.0) {
                        String editLine = ledr.getEditSequence();
                        String eString = ledr.getTopAlignmentRow();
                        String rString = ledr.getBottomAlignmentRow();
                        if (editLine.endsWith("DNI")) {
                            editLine = StringUtils.replace(editLine, "DNI", "SS");
                            eString = StringUtils.replace(eString, "-", "");
                            rString = StringUtils.replace(rString, "-", "");
                        } else if (editLine.endsWith("IND")) {
                            editLine = StringUtils.replace(editLine, "IND", "SS");
                            eString = StringUtils.replace(eString, "-", "");
                            rString = StringUtils.replace(rString, "-", "");
                        }
                        if (StringUtils.countMatches(editLine, "S") == 2) {
                            mc1 = editLine.indexOf("S");
                            mt1 = "S";
                            v1 = r.substring(mc1, mc1 + 1);
                            editLine = StringUtils.replaceFirst(editLine, "S", StringUtils.repeat("X", 1));
                            mc2 = StringUtils.indexOf(editLine, "S");
                            mt2 = "S";
                            v2 = r.substring(mc2, mc2 + 1);
                        } else if (StringUtils.countMatches(editLine, "TT") == 2) {
                            mc1 = editLine.indexOf("TT");
                            mt1 = "TT";
                            v1 = r.substring(mc1, mc1 + 2);
                            editLine = StringUtils.replaceFirst(editLine, "TT", StringUtils.repeat("X", 2));
                            mc2 = StringUtils.indexOf(editLine, "TT");
                            mt2 = "TT";
                            v2 = r.substring(mc2, mc2 + 2);
                        } else if (StringUtils.countMatches(editLine, "I") == 2) {
                            int iIndex = eString.indexOf("-");
                            mc1 = iIndex;
                            v1 = rString.substring(iIndex, iIndex + 1);
                            mt1 = "I";
                            eString = StringUtils.replaceFirst(eString, "-", StringUtils.repeat("X", 1));
                            iIndex = eString.indexOf("-");
                            mc2 = iIndex;
                            v2 = rString.substring(iIndex, iIndex + 1);
                            mt2 = "I";
                        } else if (StringUtils.countMatches(editLine, "D") == 2) {
                            int dIndex = rString.indexOf("-");
                            mc1 = dIndex;
                            v1 = eString.substring(dIndex, dIndex + 1);
                            mt1 = "D";
                            rString = StringUtils.replaceFirst(rString, "-", StringUtils.repeat("X", 1));
                            dIndex = rString.indexOf("-");
                            mc2 = dIndex;
                            v2 = eString.substring(dIndex, dIndex + 1);
                            mt2 = "D";
                        } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "TT") == 1) {
                            mc1 = editLine.indexOf("S");
                            mt1 = "S";
                            v1 = r.substring(mc1, mc1 + 1);
                            mc2 = editLine.indexOf("TT");
                            mt2 = "TT";
                            v2 = r.substring(mc2, mc2 + 2);
                        } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "I") == 1) {
                            mc1 = eString.indexOf("-");
                            v1 = rString.substring(mc1, mc1 + 1);
                            mt1 = "I";
                            mc2 = editLine.indexOf("S");
                            v2 = rString.substring(mc2, mc2 + 1);
                            mt2 = "S";
                        } else if (StringUtils.countMatches(editLine, "S") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                            mc1 = rString.indexOf("-");
                            v1 = eString.substring(mc1, mc1 + 1);
                            mt1 = "D";
                            mc2 = editLine.indexOf("S");
                            v2 = rString.substring(mc2, mc2 + 1);
                            if (mc2 > mc1) {
                                mc2--;
                            }
                            mt2 = "S";
                        } else if (StringUtils.countMatches(editLine, "TT") == 1 && StringUtils.countMatches(editLine, "I") == 1) {
                            mc1 = eString.indexOf("-");
                            v1 = rString.substring(mc1, mc1 + 1);
                            mt1 = "I";
                            mc2 = editLine.indexOf("TT");
                            v2 = rString.substring(mc2, mc2 + 2);
                            mt2 = "TT";
                        } else if (StringUtils.countMatches(editLine, "TT") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                            mc1 = rString.indexOf("-");
                            v1 = eString.substring(mc1, mc1 + 1);
                            mt1 = "D";
                            mc2 = editLine.indexOf("TT");
                            v2 = rString.substring(mc2, mc2 + 2);
                            if (mc2 > mc1) {
                                mc2--;
                            }
                            mt2 = "TT";
                        } else if (StringUtils.countMatches(editLine, "I") == 1 && StringUtils.countMatches(editLine, "D") == 1) {
                            int iIndex = eString.indexOf("-");
                            v1 = rString.substring(iIndex, iIndex + 1);
                            mt1 = "I";
                            int dIndex = rString.indexOf("-");
                            v2 = eString.substring(dIndex, dIndex + 1);
                            mt2 = "D";
                            if (iIndex > dIndex) {
                                mc1 = iIndex - 1;
                                mc2 = dIndex;
                            } else {
                                mc1 = iIndex;
                                mc2 = dIndex;
                            }
                        }
                    }
                }
            }
            Triplet[] mm;
            if (mc1 <= mc2) {
                mm = new Triplet[]{new Triplet(mt1, v1, mc1), new Triplet(mt2, v2, mc2)};
            } else {
                mm = new Triplet[]{new Triplet(mt2, v2, mc2), new Triplet(mt1, v1, mc1)};
            }

            return mm;
        }

        class Triplet {

            Triplet(String _m, String _v, Integer _p) {
                m = _m;
                v = _v;
                p = _p;
            }
            String m;
            String v;
            Integer p;
        }
    }

    public static void main(String[] args) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(args[0])));
            Map<Long, Integer> indexMap = new HashMap<>();
            String line = "";
            System.out.println(args[0]);
            System.out.println(ElasticSearchAligner.class.getName());
            while ((line = br.readLine()) != null) {
                String[] tokens = line.split("\t");
                System.out.println("tokens: " + tokens[0] + ", " + tokens[1]);
                indexMap.put(Long.parseLong(tokens[0]), Integer.parseInt(tokens[1]));
            }
            br.close();

            if (TRANSCRIPTS.isEmpty()) {
                Statement stmt = QueryBuilder
                        .select()
                        .all()
                        .from("cache", "transcript")
                        .where(eq("key", 1));
                ResultSet rs1 = SESSION.execute(stmt);
                while (!rs1.isExhausted()) {
                    Row row = rs1.one();
                    Long tid = row.getLong("transcriptid");
                    String s = row.getString("sequence");
                    TRANSCRIPTS.put(tid, s);
                }
            }

            for (final Long key : indexMap.keySet()) {
                final String index;
                switch (indexMap.get(key)) {
                    case 98:
                        index = "101";
                        break;
                    case 50:
                        index = "50";
                        break;
                    case 75:
                        index = "75";
                        break;
                    case 65:
                        index = "65";
                        break;
                    case 20:
                        index = "20";
                        break;
                    case 97:
                        index = "97";
                        break;
                    default:
                        index = "101";
                        break;
                }

                Callable callable = (Callable) () -> {
                    UpdateTranscriptalignment sstp0 = new UpdateTranscriptalignment();
                    sstp0.runAlignment1Mismatch(key, Integer.parseInt(index));
                    return "All alignments in datasets " + key + " completed.";
                };
                CONSUMER_QUEUE.put(callable);
            }
        } catch (InterruptedException ex) {
            Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(UpdateTranscriptalignment.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
