package cache.dataimportes.util;

import cache.dataimportes.holders.TranscriptMappingResults;
import cache.dataimportes.model.Bartelpolyareads;
import cache.dataimportes.model.Polyareads;
import cache.dataimportes.model.Reads;
import cache.dataimportes.model.Transcript;
import cache.dataimportes.model.Transcriptalignment;
import cache.dataimportes.model.Transcriptalignment1mismatch;
import cache.dataimportes.model.Transcriptalignmentbartelpolya;
import cache.dataimportes.model.Transcriptalignmentpolya;
import cache.dataimportes.model.Transcriptalignmenttss;
import cache.dataimportes.model.Tssreads;
import static cache.dataimportes.util.DataAccess.BARTELPOLYAREADSMAPPER;
import static cache.dataimportes.util.DataAccess.POLYAREADSMAPPER;
import static cache.dataimportes.util.DataAccess.READSMAPPER;
import static cache.dataimportes.util.DataAccess.SESSION;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTPOLYATRANSPOSEQUERY;
import static cache.dataimportes.util.DataAccess.TRANSCRIPTALIGNMENTTSSTRANSPOSEQUERY;
import static cache.dataimportes.util.DataAccess.TSSREADSMAPPER;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.biojava.nbio.core.exceptions.CompoundNotFoundException;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.views.ComplementSequenceView;
import org.biojava.nbio.core.sequence.views.ReversedSequenceView;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;

/**
 * @author Manjunath Kustagi
 */
public class Stack1Mismatch {

    /**
     * Name width
     */
    private static final int NAME_WIDTH = 75;
    /**
     * Position width
     */
    private static final int POSITION_WIDTH_RIGHT = 8;
    /**
     * Position width
     */
    private static final int POSITION_WIDTH_LEFT = 4;
    /**
     * Space
     */
    private static final String DOT = ".";
    /**
     * Line Break
     */
    private static final String BREAK = "<br/>";
    /**
     * Width for read statistics
     */
    private static final int READ_STATISTICS_WIDTH = 22;
    /**
     * Maximum read width
     */
    private static final String STATISTICS_DELIMITER = "-";
    private static final String COLON = ":";

    static enum READ_TYPE {

        CODING, POLYA, BARTEL, TSS
    };

    /**
     * Constructor
     *
     */
    public Stack1Mismatch() {
        super();
    }

    /**
     * Formats an alignment object to the Pair_FORMAT format
     *
     * @param tmr
     * @param transcript
     * @param alignments
     * @param tssAlignments
     * @param polyaAlignments
     * @param bartelPolyaAlignments
     * @return
     */
    public TranscriptMappingResults formatWithCAGEPolyAAndBartelPolyA(TranscriptMappingResults tmr,
            Transcript transcript, List<Transcriptalignment1mismatch> alignments, List<Transcriptalignmentpolya> polyaAlignments,
            List<Transcriptalignmenttss> tssAlignments, List<Transcriptalignmentbartelpolya> bartelPolyaAlignments) {
        StringBuilder buffer = new StringBuilder((int) 5e8);
        try {
            String name = transcript.getName();
            String sequence = transcript.getSequence();
            name = name.replaceAll(" ", "_");
            name = name.replaceAll("-", "_");
            name = name.replaceAll("\\r\\n|\\r|\\n", "");
            name = name.replaceAll("\\(|\\)", "");
            name = adjustName(name);
            buffer.append("<html><font face=\"monospace\">");
            buffer.append(name);
            buffer.append(DOT);
            buffer.append(transcript.getSequence());
            buffer.append(BREAK);

            Long maxTssCount = 0l;
            for (Transcriptalignmenttss tatss : tssAlignments) {
                if (maxTssCount < tatss.getCount()) {
                    maxTssCount = tatss.getCount();
                }
            }

            Long maxPolyACount = 0l;
            for (Transcriptalignmentpolya tatss : polyaAlignments) {
                if (maxPolyACount < tatss.getCount()) {
                    maxPolyACount = tatss.getCount();
                }
            }

            Long maxBartelPolyACount = 0l;
            for (Transcriptalignmentbartelpolya tatss : bartelPolyaAlignments) {
                if (maxBartelPolyACount < tatss.getCount()) {
                    maxBartelPolyACount = tatss.getCount();
                }
            }

            Map<String, Long> counts = new HashMap<>();
            Map<String, List<Transcriptalignment1mismatch>> taMap = new HashMap<>();
            for (Transcriptalignment1mismatch ta : alignments) {
                if (!ta.getReversecomplement()) {
                    Reads read = READSMAPPER.get(ta.getExperimentid(), ta.getReadid());
                    String rs = read.getSequence();
                    if (!counts.containsKey(rs)) {
                        counts.put(rs, read.getCount());
                    }
                    if (!taMap.containsKey(rs)) {
                        List<Transcriptalignment1mismatch> taL = new ArrayList<>();
                        taL.add(ta);
                        taMap.put(rs, taL);
                    } else {
                        List<Transcriptalignment1mismatch> taL = taMap.get(rs);
                        taL.add(ta);
                        taMap.put(rs, taL);
                    }
                }
            }

            Map<String, Integer> countsTss = new HashMap<>();
            Map<String, Integer> removedATss = new HashMap<>();
            Map<String, Transcriptalignmenttss> taTssMap = new HashMap<>();
            for (Transcriptalignmenttss ta : tssAlignments) {
                if (!ta.getReversecomplement()) {
                    Tssreads read = TSSREADSMAPPER.get(1, ta.getReadid());
                    String rs = read.getSequence();
                    if (read.getCount() >= (maxTssCount / 100l)) {
                        if (!countsTss.containsKey(rs)) {
                            countsTss.put(rs, (int) read.getCount().intValue());
                            removedATss.put(rs, read.getRemoveda());
                            taTssMap.put(rs, ta);
                        }
                    }
                }
            }

            Map<String, Integer> countsPolya = new HashMap<>();
            Map<String, Integer> removedAPolya = new HashMap<>();
            Map<String, Transcriptalignmentpolya> taPolyaMap = new HashMap<>();
            for (Transcriptalignmentpolya ta : polyaAlignments) {
                if (!ta.getReversecomplement()) {
                    Polyareads read = POLYAREADSMAPPER.get(1, ta.getReadid());
                    String rs = read.getSequence();
                    if (read.getCount() >= (maxPolyACount / 100l)) {
                        if (!countsPolya.containsKey(rs)) {
                            countsPolya.put(rs, (int) read.getCount().intValue());
                            removedAPolya.put(rs, read.getRemoveda());
                            taPolyaMap.put(rs, ta);
                        }
                    }
                }
            }

            Map<String, Integer> countsBartelPolya = new HashMap<>();
            Map<String, Integer> removedABartelPolya = new HashMap<>();
            Map<String, Transcriptalignmentbartelpolya> taBartelPolyaMap = new HashMap<>();
            for (Transcriptalignmentbartelpolya ta : bartelPolyaAlignments) {
                if (!ta.getReversecomplement()) {
                    Bartelpolyareads read = BARTELPOLYAREADSMAPPER.get(1, ta.getReadid());
                    String rs = read.getSequence();
                    if (read.getCount() >= (maxBartelPolyACount / 100l)) {
                        if (!countsBartelPolya.containsKey(rs)) {
                            countsBartelPolya.put(rs, (int) read.getCount().intValue());
                            removedABartelPolya.put(rs, read.getRemoveda());
                            taBartelPolyaMap.put(rs, ta);
                        }
                    }
                }
            }

            List<Coordinates> coordinates = new ArrayList<>();

            for (String rs : countsTss.keySet()) {
                Transcriptalignmenttss ta = taTssMap.get(rs);
                if (!ta.getReversecomplement()) {
                    int readLength = rs.length();
                    int seqStart = (int) ta.getStartcoordinate();
                    int readStart = 0;
                    int seqStop = seqStart + readLength;
                    int readStop = readLength - readStart;
                    if (seqStop >= sequence.length()) {
                        readStop -= (seqStop - sequence.length());
                        seqStop = sequence.length() - 1;
                    }
                    Map<Integer, Markup> mismatches = new HashMap<>();
                    Object[] aa = new Object[]{mismatches, seqStart};
                    Coordinates coordinate = new Coordinates((Integer) aa[1] + (name.length() + 1), (name.length() + 1) + seqStop + READ_STATISTICS_WIDTH, readStart, ta.getReadid(), -1, false, (Map<Integer, Markup>) aa[0], rs, countsTss.get(rs), READ_TYPE.TSS);
                    coordinate.removedA = removedATss.get(rs);
                    coordinates.add(coordinate);
                }
            }

            for (String rs : countsPolya.keySet()) {
                Transcriptalignmentpolya ta = taPolyaMap.get(rs);
                if (!ta.getReversecomplement()) {
                    int readLength = rs.length();
                    int seqStart = (int) ta.getStartcoordinate();
                    int readStart = 0;
                    int seqStop = seqStart + readLength;
                    int readStop = readLength - readStart;
                    if (seqStop >= sequence.length()) {
                        readStop -= (seqStop - sequence.length());
                        seqStop = sequence.length() - 1;
                    }
                    Map<Integer, Markup> mismatches = new HashMap<>();
                    Object[] aa = new Object[]{mismatches, seqStart};
                    Coordinates coordinate = new Coordinates((Integer) aa[1] + (name.length() + 1), (name.length() + 1) + seqStop + READ_STATISTICS_WIDTH, readStart, ta.getReadid(), -1, false, (Map<Integer, Markup>) aa[0], rs, countsPolya.get(rs), READ_TYPE.POLYA);
                    coordinate.removedA = removedAPolya.get(rs);
                    coordinates.add(coordinate);
                }
            }

            for (String rs : countsBartelPolya.keySet()) {
                Transcriptalignmentbartelpolya ta = taBartelPolyaMap.get(rs);
                if (!ta.getReversecomplement()) {
                    int readLength = rs.length();
                    int seqStart = (int) ta.getStartcoordinate();
                    int readStart = 0;
                    int seqStop = seqStart + readLength;
                    int readStop = readLength - readStart;
                    if (seqStop >= sequence.length()) {
                        readStop -= (seqStop - sequence.length());
                        seqStop = sequence.length() - 1;
                    }
                    Map<Integer, Markup> mismatches = new HashMap<>();
                    Object[] aa = new Object[]{mismatches, seqStart};
                    Coordinates coordinate = new Coordinates((Integer) aa[1] + (name.length() + 1), (name.length() + 1) + seqStop + READ_STATISTICS_WIDTH, readStart, ta.getReadid(), -1, false, (Map<Integer, Markup>) aa[0], rs, countsBartelPolya.get(rs), READ_TYPE.BARTEL);
                    coordinate.removedA = removedABartelPolya.get(rs);
                    coordinates.add(coordinate);
                }
            }

            for (String rs : counts.keySet()) {
                List<Transcriptalignment1mismatch> taL = taMap.get(rs);
                for (Transcriptalignment1mismatch ta : taL) {
                    if (!ta.getReversecomplement()) {
                        int readLength = rs.length();
                        int seqStart = (int) ta.getStartcoordinate();
                        int readStart = 0;
                        int seqStop = seqStart + readLength;
                        int readStop = readLength - readStart;
                        if (seqStop >= sequence.length()) {
                            readStop -= (seqStop - sequence.length());
                            seqStop = sequence.length() - 1;
                        }
                        Map<Integer, Markup> mismatches = new HashMap<>();
                        Markup m = new Markup(ta.getMismatchcoordinate().longValue(), ta.getMismatchtype(), ta.getVariant());
                        mismatches.put(ta.getMismatchcoordinate(), m);
                        Object[] aa = new Object[]{mismatches, seqStart};
                        Coordinates coordinate = new Coordinates((Integer) aa[1] + (name.length() + 1), (name.length() + 1) + seqStop + READ_STATISTICS_WIDTH, readStart, ta.getReadid(), ta.getExperimentid(), false, (Map<Integer, Markup>) aa[0], rs, counts.get(rs).intValue(), READ_TYPE.CODING);
                        coordinates.add(coordinate);
                    }
                }
            }

            Collections.sort(coordinates);

            Map<Integer, Float> stackMaxY = new HashMap<>();
            final Map<Integer, List<Coordinates>> stacks = Collections.synchronizedMap(new HashMap<>());
            int __i = 0;
            int line = 0;
            int maxLines = 0;
            float previousX = Float.MAX_VALUE;
            float previousY = Float.MAX_VALUE;
            for (Coordinates coordinate : coordinates) {
                if (!stacks.containsKey(__i)) {
                    stacks.put(__i, new ArrayList<>());
                    stackMaxY.put(__i, Float.MIN_VALUE);
                }
                List<Coordinates> currentStack = stacks.get(__i);
                if (coordinate.x < stackMaxY.get(__i)) {
                    currentStack.add(coordinate);
                    previousX = coordinate.x;
                    previousY = coordinate.y;
                    if (previousY > stackMaxY.get(__i)) {
                        stackMaxY.put(__i, previousY);
                    }
                    line++;
                } else {
                    __i++;
                    line = 0;
                    if (!stacks.containsKey(__i)) {
                        stacks.put(__i, new ArrayList<>());
                        stackMaxY.put(__i, Float.MIN_VALUE);
                    }
                    stacks.get(__i).add(coordinate);
                    previousX = coordinate.x;
                    previousY = coordinate.y;
                    if (previousY > stackMaxY.get(__i)) {
                        stackMaxY.put(__i, previousY);
                    }
                    line++;
                }
                maxLines = (line > maxLines) ? line : maxLines;
            }

            ExecutorService ex = Executors.newFixedThreadPool(10, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });

            final int ml = maxLines;
            final int numStacks = stacks.keySet().size();
            List<Callable<Object>> callables = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                final int counter = i;

                Callable t = (Callable) new Callable() {
                    @Override
                    public Object call() throws Exception {
                        int batchSize = (ml) / 10;
                        int min = (counter * batchSize);
                        int max = (counter < (10 - 1)) ? min + batchSize : ml;
                        for (int i1 = min; i1 < max; i1++) {
                            for (int key = 0; key < numStacks; key++) {
                                List<Coordinates> stack = stacks.get(key);
                                if (stack.size() > i1) {
                                    Coordinates coordinate = stack.get(i1);
                                    long readMapCount = 0;
                                    if (null != coordinate.readType) {
                                        switch (coordinate.readType) {
                                            case POLYA:
                                                BoundStatement bs = TRANSCRIPTALIGNMENTPOLYATRANSPOSEQUERY.bind().setUUID("ri", coordinate.readId);
                                                ResultSet rcs = SESSION.execute(bs);
                                                readMapCount = 0;
                                                Map<Long, Integer> mm = new HashMap<>();
                                                while (!rcs.isExhausted()) {
                                                    Row row = rcs.one();
                                                    Long tid = row.getLong("transcriptid");
                                                    mm.put(tid, 1);
                                                    readMapCount++;
                                                }
                                                coordinate.mappedTranscripts = mm;
                                                break;
                                            case TSS:
                                                bs = TRANSCRIPTALIGNMENTTSSTRANSPOSEQUERY.bind().setUUID("ri", coordinate.readId);
                                                rcs = SESSION.execute(bs);
                                                readMapCount = 0;
                                                mm = new HashMap<>();
                                                while (!rcs.isExhausted()) {
                                                    Row row = rcs.one();
                                                    Long tid = row.getLong("transcriptid");
                                                    mm.put(tid, 1);
                                                    readMapCount++;
                                                }
                                                coordinate.mappedTranscripts = mm;
                                                break;
                                            case BARTEL:
                                                bs = TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEQUERY.bind().setUUID("ri", coordinate.readId);
                                                rcs = SESSION.execute(bs);
                                                readMapCount = 0;
                                                mm = new HashMap<>();
                                                while (!rcs.isExhausted()) {
                                                    Row row = rcs.one();
                                                    Long tid = row.getLong("transcriptid");
                                                    mm.put(tid, 1);
                                                    readMapCount++;
                                                }
                                                coordinate.mappedTranscripts = mm;
                                                break;
                                            default:
                                                bs = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEQUERY.bind().setLong("ei", coordinate.experimentId).setUUID("ri", coordinate.readId);
                                                rcs = SESSION.execute(bs);
                                                readMapCount = 0;
                                                mm = new HashMap<>();
                                                while (!rcs.isExhausted()) {
                                                    Row row = rcs.one();
                                                    Long tid = row.getLong("transcriptid");
                                                    Integer count = row.getInt("count");
                                                    mm.put(tid, count);
                                                    readMapCount++;
                                                }
                                                coordinate.mappedTranscripts = mm;
                                                break;
                                        }
                                    }
                                }
                            }

                        }
                        return stacks;
                    }
                };
                callables.add(t);
            }

            ex.invokeAll(callables);
            ex.shutdown();

            for (__i = 0; __i < maxLines; __i++) {
                int lineMarker = 0;
                buffer.append(StringUtils.repeat(DOT, NAME_WIDTH));
                lineMarker += NAME_WIDTH;
                buffer.append(DOT);
                lineMarker++;
                for (int key = 0; key < numStacks; key++) {
                    List<Coordinates> stack = stacks.get(key);
                    if (stack.size() > __i) {
                        Coordinates coordinate = stack.get(__i);
                        buffer.append(StringUtils.repeat(DOT, (int) (coordinate.x - lineMarker)));
                        lineMarker += (int) (coordinate.x - lineMarker);

                        if (null != coordinate.readType) {
                            switch (coordinate.readType) {
                                case POLYA: {
                                    String readSequence = coordinate.readSequence;
                                    char[] mappedRead = readSequence.substring(coordinate.readStart).toCharArray();
                                    buffer.append("<font color=\"Blue\">");
                                    buffer.append(mappedRead);
                                    lineMarker += mappedRead.length;
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(STATISTICS_DELIMITER);
                                    lineMarker += STATISTICS_DELIMITER.length();
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(adjustPositionRight(Integer.toString(coordinate.readCount)));
                                    lineMarker += POSITION_WIDTH_RIGHT;
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    String _rc = Integer.toString(sumInt(coordinate.mappedTranscripts.values()));
                                    buffer.append(_rc);
                                    lineMarker += _rc.length();
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    buffer.append(adjustPosition(Integer.toString(coordinate.removedA)));
                                    lineMarker += POSITION_WIDTH_LEFT;
                                    buffer.append(DOT);
                                    buffer.append("</font>");
                                    lineMarker++;
                                    break;
                                }
                                case TSS: {
                                    String readSequence = coordinate.readSequence;
                                    char[] mappedRead = readSequence.substring(coordinate.readStart).toCharArray();
                                    buffer.append("<font color=\"Magenta\">");
                                    buffer.append(mappedRead);
                                    lineMarker += mappedRead.length;
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(STATISTICS_DELIMITER);
                                    lineMarker += STATISTICS_DELIMITER.length();
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(adjustPositionRight(Integer.toString(coordinate.readCount)));
                                    lineMarker += POSITION_WIDTH_RIGHT;
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    String _rc = Integer.toString(sumInt(coordinate.mappedTranscripts.values()));
                                    buffer.append(_rc);
                                    lineMarker += _rc.length();
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    buffer.append(adjustPosition(Integer.toString(coordinate.removedA)));
                                    lineMarker += POSITION_WIDTH_LEFT;
                                    buffer.append(DOT);
                                    buffer.append("</font>");
                                    lineMarker++;
                                    break;
                                }
                                case BARTEL: {
                                    String readSequence = coordinate.readSequence;
                                    char[] mappedRead = readSequence.substring(coordinate.readStart).toCharArray();
                                    buffer.append("<font color=\"Green\">");
                                    buffer.append(mappedRead);
                                    lineMarker += mappedRead.length;
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(STATISTICS_DELIMITER);
                                    lineMarker += STATISTICS_DELIMITER.length();
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(adjustPositionRight(Integer.toString(coordinate.readCount)));
                                    lineMarker += POSITION_WIDTH_RIGHT;
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    String _rc = Integer.toString(sumInt(coordinate.mappedTranscripts.values()));
                                    buffer.append(_rc);
                                    lineMarker += _rc.length();
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    buffer.append(adjustPosition(Integer.toString(coordinate.removedA)));
                                    lineMarker += POSITION_WIDTH_LEFT;
                                    buffer.append(DOT);
                                    buffer.append("</font>");
                                    lineMarker++;
                                    break;
                                }
                                default: {
                                    String readSequence = coordinate.readSequence;
                                    char[] mappedRead = readSequence.substring(coordinate.readStart).toCharArray();
                                    Pair p = adjustMarkup(mappedRead, coordinate.mismatches);
                                    buffer.append(p.s);
                                    lineMarker += (mappedRead.length - p.o);
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(STATISTICS_DELIMITER);
                                    lineMarker += STATISTICS_DELIMITER.length();
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(adjustPositionRight(Integer.toString(coordinate.readCount)));
                                    lineMarker += POSITION_WIDTH_RIGHT;
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    buffer.append(adjustPosition(Integer.toString(sumInt(coordinate.mappedTranscripts.values()))));
                                    lineMarker += POSITION_WIDTH_LEFT;
                                    buffer.append(DOT);
                                    lineMarker++;
                                    break;
                                }
                            }
                        }
                    }
                }
                buffer.append(BREAK);
            }
            buffer.append(BREAK);

            Map<Integer, List<Long>> multiMappers = new HashMap<>();
            for (int len = 0; len < sequence.length(); len++) {
                multiMappers.put(len, new ArrayList<>());
            }
            for (int key = 0; key < stacks.keySet().size(); key++) {
                List<Coordinates> stack = stacks.get(key);
                for (Coordinates c : stack) {
                    if (c.readType.equals(READ_TYPE.CODING)) {
                        int x = (int) c.x - (name.length() + 1);
                        int y = (int) c.y - (name.length() + 1) - READ_STATISTICS_WIDTH;
                        for (int _c = x; _c < y; _c++) {
                            List<Long> _mc = multiMappers.get(_c);
                            c.mappedTranscripts.keySet().stream().forEach((tid) -> {
                                Integer count = c.mappedTranscripts.get(tid);
                                Integer _ce = Collections.frequency(_mc, tid);
                                if (count > _ce) {
                                    for (int _i = 0; _i < (count - _ce); _i++) {
                                        _mc.add(tid);
                                    }
                                    Collections.sort(_mc);
                                }
                            });
                            multiMappers.put(_c, _mc);
                        }
                    }
                }
            }
            tmr.multiMappers = multiMappers;

            // Start Reverse Complement
            buffer.append(name);
            buffer.append(DOT);
            buffer.append(sequence);
            buffer.append(BREAK);

            Map<String, Long> countsRC = new HashMap<>();
            taMap = new HashMap<>();
            for (Transcriptalignment1mismatch ta : alignments) {
                if (ta.getReversecomplement()) {
                    Reads read = READSMAPPER.get(ta.getExperimentid(), ta.getReadid());
                    String rs = read.getSequence();
                    if (!countsRC.containsKey(rs)) {
                        countsRC.put(rs, read.getCount());
                    }
                    if (!taMap.containsKey(rs)) {
                        List<Transcriptalignment1mismatch> taL = new ArrayList<>();
                        taL.add(ta);
                        taMap.put(rs, taL);
                    } else {
                        List<Transcriptalignment1mismatch> taL = taMap.get(rs);
                        taL.add(ta);
                        taMap.put(rs, taL);
                    }
                }
            }

            countsTss = new HashMap<>();
            removedATss = new HashMap<>();
            taTssMap = new HashMap<>();
            for (Transcriptalignmenttss ta : tssAlignments) {
                if (ta.getReversecomplement()) {
                    Tssreads read = TSSREADSMAPPER.get(1, ta.getReadid());
                    String rs = read.getSequence();
                    if (read.getCount() >= (maxTssCount / 100l)) {
                        if (!countsTss.containsKey(rs)) {
                            countsTss.put(rs, (int) read.getCount().intValue());
                            removedATss.put(rs, read.getRemoveda());
                            taTssMap.put(rs, ta);
                        }
                    }
                }
            }

            countsPolya = new HashMap<>();
            removedAPolya = new HashMap<>();
            taPolyaMap = new HashMap<>();
            for (Transcriptalignmentpolya ta : polyaAlignments) {
                if (ta.getReversecomplement()) {
                    Polyareads read = POLYAREADSMAPPER.get(1, ta.getReadid());
                    String rs = read.getSequence();
                    if (read.getCount() >= (maxPolyACount / 100l)) {
                        if (!countsPolya.containsKey(rs)) {
                            countsPolya.put(rs, (int) read.getCount().intValue());
                            removedAPolya.put(rs, read.getRemoveda());
                            taPolyaMap.put(rs, ta);
                        }
                    }
                }
            }

            countsBartelPolya = new HashMap<>();
            removedABartelPolya = new HashMap<>();
            taBartelPolyaMap = new HashMap<>();
            for (Transcriptalignmentbartelpolya ta : bartelPolyaAlignments) {
                if (ta.getReversecomplement()) {
                    Bartelpolyareads read = BARTELPOLYAREADSMAPPER.get(1, ta.getReadid());
                    String rs = read.getSequence();
                    if (read.getCount() >= (maxBartelPolyACount / 100l)) {
                        if (!countsBartelPolya.containsKey(rs)) {
                            countsBartelPolya.put(rs, (int) read.getCount().intValue());
                            removedABartelPolya.put(rs, read.getRemoveda());
                            taBartelPolyaMap.put(rs, ta);
                        }
                    }
                }
            }

            coordinates = new ArrayList<>();

            for (String rs : countsTss.keySet()) {
                Transcriptalignmenttss ta = taTssMap.get(rs);
                if (ta.getReversecomplement()) {
                    DNASequence _s = new DNASequence(rs);
                    org.biojava.nbio.core.sequence.template.Sequence rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(_s));
                    String revComp = rc.getSequenceAsString();

                    int readLength = revComp.length();
                    int seqStart = (int) ta.getStartcoordinate();
                    int readStart = 0;
                    int seqStop = seqStart + readLength;
                    int readStop = readLength - readStart;
                    if (seqStop >= sequence.length()) {
                        readStop -= (seqStop - sequence.length());
                        seqStop = sequence.length() - 1;
                    }
                    Map<Integer, Markup> mismatches = new HashMap<>();
                    Object[] aa = new Object[]{mismatches, seqStart};
                    int start = (Integer) aa[1] + (name.length() + 1);
                    int stop = (name.length() + 1) + seqStop + READ_STATISTICS_WIDTH;
                    Coordinates coordinate = new Coordinates(start, stop, readStart, ta.getReadid(), -1, false, (Map<Integer, Markup>) aa[0], revComp, countsTss.get(rs), READ_TYPE.TSS);
                    coordinate.removedA = removedATss.get(rs);
                    coordinates.add(coordinate);
                }
            }

            for (String rs : countsPolya.keySet()) {
                Transcriptalignmentpolya ta = taPolyaMap.get(rs);
                if (ta.getReversecomplement()) {
                    DNASequence _s = new DNASequence(rs);
                    org.biojava.nbio.core.sequence.template.Sequence rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(_s));
                    String revComp = rc.getSequenceAsString();

                    int readLength = revComp.length();
                    int seqStart = (int) ta.getStartcoordinate();
                    int readStart = 0;
                    int seqStop = seqStart + readLength;
                    int readStop = readLength - readStart;
                    if (seqStop >= sequence.length()) {
                        readStop -= (seqStop - sequence.length());
                        seqStop = sequence.length() - 1;
                    }
                    Map<Integer, Markup> mismatches = new HashMap<>();
                    Object[] aa = new Object[]{mismatches, seqStart};
                    int start = (Integer) aa[1] + (name.length() + 1);
                    int stop = (name.length() + 1) + seqStop + READ_STATISTICS_WIDTH;
                    Coordinates coordinate = new Coordinates(start, stop, readStart, ta.getReadid(), -1, false, (Map<Integer, Markup>) aa[0], revComp, countsPolya.get(rs), READ_TYPE.POLYA);
                    coordinate.removedA = removedAPolya.get(rs);
                    coordinates.add(coordinate);
                }
            }

            for (String rs : countsBartelPolya.keySet()) {
                Transcriptalignmentbartelpolya ta = taBartelPolyaMap.get(rs);
                if (ta.getReversecomplement()) {
                    DNASequence _s = new DNASequence(rs);
                    org.biojava.nbio.core.sequence.template.Sequence rc
                            = new ReversedSequenceView<>(
                                    new ComplementSequenceView<>(_s));
                    String revComp = rc.getSequenceAsString();

                    int readLength = revComp.length();
                    int seqStart = (int) ta.getStartcoordinate();
                    int readStart = 0;
                    int seqStop = seqStart + readLength;
                    int readStop = readLength - readStart;
                    if (seqStop >= sequence.length()) {
                        readStop -= (seqStop - sequence.length());
                        seqStop = sequence.length() - 1;
                    }
                    Map<Integer, Markup> mismatches = new HashMap<>();
                    Object[] aa = new Object[]{mismatches, seqStart};
                    int start = (Integer) aa[1] + (name.length() + 1);
                    int stop = (name.length() + 1) + seqStop + READ_STATISTICS_WIDTH;
                    Coordinates coordinate = new Coordinates(start, stop, readStart, ta.getReadid(), -1, false, (Map<Integer, Markup>) aa[0], revComp, countsBartelPolya.get(rs), READ_TYPE.BARTEL);
                    coordinate.removedA = removedABartelPolya.get(rs);
                    coordinates.add(coordinate);
                }
            }

            for (String rs : countsRC.keySet()) {
                List<Transcriptalignment1mismatch> taL = taMap.get(rs);
                for (Transcriptalignment1mismatch ta : taL) {
                    if (ta.getReversecomplement()) {
                        DNASequence _s = new DNASequence(rs);
                        org.biojava.nbio.core.sequence.template.Sequence rc
                                = new ReversedSequenceView<>(
                                        new ComplementSequenceView<>(_s));
                        String revComp = rc.getSequenceAsString();

                        int readLength = revComp.length();
                        int seqStart = (int) ta.getStartcoordinate();
                        int readStart = 0;
                        int seqStop = seqStart + readLength;
                        int readStop = readLength - readStart;
                        if (seqStop >= sequence.length()) {
                            readStop -= (seqStop - sequence.length());
                            seqStop = sequence.length() - 1;
                        }
                        Map<Integer, Markup> mismatches = new HashMap<>();
                        if (ta.getMismatchtype().equals("S")) {
                            Markup m = new Markup(readLength - ta.getMismatchcoordinate().longValue() - 1, ta.getMismatchtype(), ta.getVariant());
                            mismatches.put(readLength - ta.getMismatchcoordinate() - 1, m);
                        } else {
                            Markup m = new Markup(ta.getMismatchcoordinate().longValue(), ta.getMismatchtype(), ta.getVariant());
                            mismatches.put(ta.getMismatchcoordinate(), m);
                        }
                        Object[] aa = new Object[]{mismatches, seqStart};
                        int start = (Integer) aa[1] + (name.length() + 1);
                        int stop = (name.length() + 1) + seqStop + READ_STATISTICS_WIDTH;
                        Coordinates coordinate = new Coordinates(start, stop, readStart, ta.getReadid(), ta.getExperimentid(), false, (Map<Integer, Markup>) aa[0], revComp, countsRC.get(rs).intValue(), READ_TYPE.CODING);
                        coordinates.add(coordinate);
                    }
                }
            }

            Collections.sort(coordinates);

            stackMaxY = new HashMap<>();
            final Map<Integer, List<Coordinates>> stacksRC = Collections.synchronizedMap(new HashMap<>());
            __i = 0;
            line = 0;
            maxLines = 0;
            previousX = Float.MAX_VALUE;
            previousY = Float.MAX_VALUE;
            for (Coordinates coordinate : coordinates) {
                if (!stacksRC.containsKey(__i)) {
                    stacksRC.put(__i, new ArrayList<>());
                    stackMaxY.put(__i, Float.MIN_VALUE);
                }
                List<Coordinates> currentStack = stacksRC.get(__i);
                if (coordinate.x < stackMaxY.get(__i)) {
                    currentStack.add(coordinate);
                    previousX = coordinate.x;
                    previousY = coordinate.y;
                    if (previousY > stackMaxY.get(__i)) {
                        stackMaxY.put(__i, previousY);
                    }
                    line++;
                } else {
                    __i++;
                    line = 0;
                    if (!stacksRC.containsKey(__i)) {
                        stacksRC.put(__i, new ArrayList<>());
                        stackMaxY.put(__i, Float.MIN_VALUE);
                    }
                    stacksRC.get(__i).add(coordinate);
                    previousX = coordinate.x;
                    previousY = coordinate.y;
                    if (previousY > stackMaxY.get(__i)) {
                        stackMaxY.put(__i, previousY);
                    }
                    line++;
                }
                maxLines = (line > maxLines) ? line : maxLines;
            }

            ex = Executors.newFixedThreadPool(10, (Runnable r) -> {
                Thread t = new Thread(r);
                t.setPriority(Thread.MAX_PRIORITY);
                return t;
            });

            final int mlRC = maxLines;
            final int numStacksRC = stacksRC.keySet().size();
            callables = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                final int counter = i;

                Callable t = (Callable) new Callable() {
                    @Override
                    public Object call() throws Exception {
                        int batchSize = (mlRC) / 10;
                        int min = (counter * batchSize);
                        int max = (counter < (10 - 1)) ? min + batchSize : mlRC;
                        for (int i1 = min; i1 < max; i1++) {
                            for (int key = 0; key < numStacksRC; key++) {
                                List<Coordinates> stack = stacksRC.get(key);
                                if (stack.size() > i1) {
                                    Coordinates coordinate = stack.get(i1);
                                    if (null != coordinate.readType) {
                                        switch (coordinate.readType) {
                                            case POLYA:
                                                BoundStatement bs = TRANSCRIPTALIGNMENTPOLYATRANSPOSEQUERY.bind().setUUID("ri", coordinate.readId);
                                                ResultSet rcs = SESSION.execute(bs);
                                                Map<Long, Integer> mm = new HashMap<>();
                                                while (!rcs.isExhausted()) {
                                                    Row row = rcs.one();
                                                    Long tid = row.getLong("transcriptid");
                                                    mm.put(tid, 1);
                                                }
                                                coordinate.mappedTranscripts = mm;
                                                break;
                                            case TSS:
                                                bs = TRANSCRIPTALIGNMENTTSSTRANSPOSEQUERY.bind().setUUID("ri", coordinate.readId);
                                                rcs = SESSION.execute(bs);
                                                mm = new HashMap<>();
                                                while (!rcs.isExhausted()) {
                                                    Row row = rcs.one();
                                                    Long tid = row.getLong("transcriptid");
                                                    mm.put(tid, 1);
                                                }
                                                coordinate.mappedTranscripts = mm;
                                                break;
                                            case BARTEL:
                                                bs = TRANSCRIPTALIGNMENTBARTELPOLYATRANSPOSEQUERY.bind().setUUID("ri", coordinate.readId);
                                                rcs = SESSION.execute(bs);
                                                mm = new HashMap<>();
                                                while (!rcs.isExhausted()) {
                                                    Row row = rcs.one();
                                                    Long tid = row.getLong("transcriptid");
                                                    mm.put(tid, 1);
                                                }
                                                coordinate.mappedTranscripts = mm;
                                                break;
                                            default:
                                                bs = TRANSCRIPTALIGNMENT1MISMATCHTRANSPOSEQUERY.bind().setLong("ei", coordinate.experimentId).setUUID("ri", coordinate.readId);
                                                rcs = SESSION.execute(bs);
                                                mm = new HashMap<>();
                                                while (!rcs.isExhausted()) {
                                                    Row row = rcs.one();
                                                    Long tid = row.getLong("transcriptid");
                                                    Integer count = row.getInt("count");
                                                    mm.put(tid, count);
                                                }
                                                coordinate.mappedTranscripts = mm;
                                                break;
                                        }
                                    }
                                }
                            }

                        }
                        return stacks;
                    }
                };
                callables.add(t);
            }

            ex.invokeAll(callables);
            ex.shutdown();

            for (__i = 0; __i < maxLines; __i++) {
                int lineMarker = 0;
                buffer.append(StringUtils.repeat(DOT, NAME_WIDTH));
                lineMarker += NAME_WIDTH;
                buffer.append(DOT);
                lineMarker++;
                for (int key = 0; key < numStacksRC; key++) {
                    List<Coordinates> stack = stacksRC.get(key);
                    if (stack.size() > __i) {
                        Coordinates coordinate = stack.get(__i);
                        buffer.append(StringUtils.repeat(DOT, (int) (coordinate.x - lineMarker)));
                        lineMarker += (int) (coordinate.x - lineMarker);

                        if (null != coordinate.readType) {
                            switch (coordinate.readType) {
                                case POLYA: {
                                    String readSequence = coordinate.readSequence;
                                    char[] mappedRead = readSequence.substring(coordinate.readStart).toCharArray();
                                    buffer.append("<font color=\"Blue\">");
                                    buffer.append(mappedRead);
                                    lineMarker += mappedRead.length;
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(STATISTICS_DELIMITER);
                                    lineMarker += STATISTICS_DELIMITER.length();
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(adjustPositionRight(Integer.toString(coordinate.readCount)));
                                    lineMarker += POSITION_WIDTH_RIGHT;
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    String _rc = Integer.toString(sumInt(coordinate.mappedTranscripts.values()));
                                    buffer.append(_rc);
                                    lineMarker += _rc.length();
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    buffer.append(adjustPosition(Integer.toString(coordinate.removedA)));
                                    lineMarker += POSITION_WIDTH_LEFT;
                                    buffer.append(DOT);
                                    buffer.append("</font>");
                                    lineMarker++;
                                    break;
                                }
                                case TSS: {
                                    String readSequence = coordinate.readSequence;
                                    char[] mappedRead = readSequence.substring(coordinate.readStart).toCharArray();
                                    buffer.append("<font color=\"Magenta\">");
                                    buffer.append(mappedRead);
                                    lineMarker += mappedRead.length;
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(STATISTICS_DELIMITER);
                                    lineMarker += STATISTICS_DELIMITER.length();
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(adjustPositionRight(Integer.toString(coordinate.readCount)));
                                    lineMarker += POSITION_WIDTH_RIGHT;
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    String _rc = Integer.toString(sumInt(coordinate.mappedTranscripts.values()));
                                    buffer.append(_rc);
                                    lineMarker += _rc.length();
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    buffer.append(adjustPosition(Integer.toString(coordinate.removedA)));
                                    lineMarker += POSITION_WIDTH_LEFT;
                                    buffer.append(DOT);
                                    buffer.append("</font>");
                                    lineMarker++;
                                    break;
                                }
                                case BARTEL: {
                                    String readSequence = coordinate.readSequence;
                                    char[] mappedRead = readSequence.substring(coordinate.readStart).toCharArray();
                                    buffer.append("<font color=\"Green\">");
                                    buffer.append(mappedRead);
                                    lineMarker += mappedRead.length;
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(STATISTICS_DELIMITER);
                                    lineMarker += STATISTICS_DELIMITER.length();
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(adjustPositionRight(Integer.toString(coordinate.readCount)));
                                    lineMarker += POSITION_WIDTH_RIGHT;
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    String _rc = Integer.toString(sumInt(coordinate.mappedTranscripts.values()));
                                    buffer.append(_rc);
                                    lineMarker += _rc.length();
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    buffer.append(adjustPosition(Integer.toString(coordinate.removedA)));
                                    lineMarker += POSITION_WIDTH_LEFT;
                                    buffer.append(DOT);
                                    buffer.append("</font>");
                                    lineMarker++;
                                    break;
                                }
                                default: {
                                    String readSequence = coordinate.readSequence;
                                    char[] mappedRead = readSequence.substring(coordinate.readStart).toCharArray();
                                    Pair p = adjustMarkup(mappedRead, coordinate.mismatches);
                                    buffer.append(p.s);
                                    lineMarker += (mappedRead.length - p.o);
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(STATISTICS_DELIMITER);
                                    lineMarker += STATISTICS_DELIMITER.length();
                                    buffer.append(DOT);
                                    lineMarker++;
                                    buffer.append(adjustPositionRight(Integer.toString(coordinate.readCount)));
                                    lineMarker += POSITION_WIDTH_RIGHT;
                                    buffer.append(COLON);
                                    lineMarker += COLON.length();
                                    buffer.append(adjustPosition(Integer.toString(sumInt(coordinate.mappedTranscripts.values()))));
                                    lineMarker += POSITION_WIDTH_LEFT;
                                    buffer.append(DOT);
                                    lineMarker++;
                                    break;
                                }
                            }
                        }
                    }
                }
                buffer.append(BREAK);
            }
            buffer.append("</font></html>");

            Map<Integer, List<Long>> rcMultiMappers = new HashMap<>();
            for (int len = 0; len < sequence.length(); len++) {
                rcMultiMappers.put(len, new ArrayList<>());
            }
            for (int key = 0; key < stacksRC.keySet().size(); key++) {
                List<Coordinates> stack = stacksRC.get(key);
                for (Coordinates c : stack) {
                    if (c.readType.equals(READ_TYPE.CODING)) {
                        int x = (int) c.x - (name.length() + 1);
                        int y = (int) c.y - (name.length() + 1) - READ_STATISTICS_WIDTH;
                        for (int _c = x; _c < y; _c++) {
                            List<Long> _mc = rcMultiMappers.get(_c);
                            c.mappedTranscripts.keySet().stream().forEach((tid) -> {
                                Integer count = c.mappedTranscripts.get(tid);
                                Integer _ce = Collections.frequency(_mc, tid);
                                if (count > _ce) {
                                    for (int _i = 0; _i < (count - _ce); _i++) {
                                        _mc.add(tid);
                                    }
                                    Collections.sort(_mc);
                                }
                            });
                            rcMultiMappers.put(_c, _mc);
                        }
                    }
                }
            }
            tmr.rcMultiMappers = rcMultiMappers;

            tmr.name = name.split("::::")[0];

            counts.putAll(countsRC);
            tmr.mappedCount = (long) counts.size();
            tmr.totalMappedCount = counts.values().stream().mapToLong(i -> (long) (i)).sum();
        } catch (InterruptedException | CompoundNotFoundException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        tmr.mappedAlignments = buffer.toString().replaceAll("::::", "....");
        tmr.coverages = computeCoverages(buffer.toString());
        return tmr;
    }

    public Map<String, List<Integer>> computeCoverages(String html) {
        Document doc = Jsoup.parse(html);
        Element root = doc.body();
        Element font = root.child(0);
        List<TextNode> textNodes = font.textNodes();
        TextNode transcript = textNodes.get(0);
        String ttext = transcript.text();
        StringBuilder buffer = new StringBuilder(ttext);
        buffer.append(System.getProperty("line.separator"));
        int length = buffer.length() - NAME_WIDTH;
        List<Integer> coverageNonRedundant = new ArrayList<>(length);
        List<Integer> coverageTotal = new ArrayList<>(length);
        List<Integer> coverageMagentaTotal = new ArrayList<>(length);
        List<Integer> coverageGreenTotal = new ArrayList<>(length);
        List<Integer> coverageBlueTotal = new ArrayList<>(length);
        List<Integer> coveragePurpleTotal = new ArrayList<>(length);
        List<Integer> coverageNonRedundantRC = new ArrayList<>(length);
        List<Integer> coverageTotalRC = new ArrayList<>(length);
        List<Integer> coverageMagentaTotalRC = new ArrayList<>(length);
        List<Integer> coverageGreenTotalRC = new ArrayList<>(length);
        List<Integer> coverageBlueTotalRC = new ArrayList<>(length);
        List<Integer> coveragePurpleTotalRC = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            coverageNonRedundant.add(0);
            coverageTotal.add(0);
            coverageMagentaTotal.add(0);
            coverageGreenTotal.add(0);
            coverageBlueTotal.add(0);
            coveragePurpleTotal.add(0);
            coverageNonRedundantRC.add(0);
            coverageTotalRC.add(0);
            coverageMagentaTotalRC.add(0);
            coverageGreenTotalRC.add(0);
            coverageBlueTotalRC.add(0);
            coveragePurpleTotalRC.add(0);
        }
        int nodeSize = font.childNodeSize();
        List<org.jsoup.nodes.Node> children = font.childNodes();
        int count = 0;
        boolean RC = true;
        while (count < children.size()) {
            boolean readCompleted = false;
            String readText = "";
            String blueReadText = "";
            String greenReadText = "";
            String magentaReadText = "";
            String purpleReadText = "";
            String readTextRC = "";
            String blueReadTextRC = "";
            String greenReadTextRC = "";
            String magentaReadTextRC = "";
            String purpleReadTextRC = "";
            while (!readCompleted) {
                if (count < children.size()) {
                    org.jsoup.nodes.Node node = children.get(count++);
                    if (node instanceof Element) {
                        Element element = (Element) node;
                        if (element.tagName().equalsIgnoreCase("br")) {
                            readCompleted = true;
                        } else if (element.tagName().equalsIgnoreCase("font")) {
                            Attributes att = node.attributes();
                            String color = att.get("color");
                            color = (color == null) ? "" : color.trim();
                            if (color.equalsIgnoreCase("Blue")) {
                                if (!RC) {
                                    blueReadText += element.text();
                                    readText += StringUtils.leftPad("", element.text().length(), ".");
                                    greenReadText += StringUtils.leftPad("", element.text().length(), ".");
                                    magentaReadText += StringUtils.leftPad("", element.text().length(), ".");
                                    purpleReadText += StringUtils.leftPad("", element.text().length(), ".");
                                } else {
                                    blueReadTextRC += element.text();
                                    readTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                    greenReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                    magentaReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                    purpleReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                }
                            } else if (color.equalsIgnoreCase("Green")) {
                                if (!RC) {
                                    greenReadText += element.text();
                                    readText += StringUtils.leftPad("", element.text().length(), ".");
                                    blueReadText += StringUtils.leftPad("", element.text().length(), ".");
                                    magentaReadText += StringUtils.leftPad("", element.text().length(), ".");
                                    purpleReadText += StringUtils.leftPad("", element.text().length(), ".");
                                } else {
                                    greenReadTextRC += element.text();
                                    readTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                    blueReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                    magentaReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                    purpleReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                }
                            } else if (color.equalsIgnoreCase("Magenta")) {
                                if (!RC) {
                                    magentaReadText += element.text();
                                    readText += StringUtils.leftPad("", element.text().length(), ".");
                                    blueReadText += StringUtils.leftPad("", element.text().length(), ".");
                                    greenReadText += StringUtils.leftPad("", element.text().length(), ".");
                                    purpleReadText += StringUtils.leftPad("", element.text().length(), ".");
                                } else {
                                    magentaReadTextRC += element.text();
                                    readTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                    blueReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                    greenReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                    purpleReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                }
                            } else if (color.equalsIgnoreCase("Purple")) {
                                if (!RC) {
                                    purpleReadText += element.text();
                                    readText += StringUtils.leftPad("", element.text().length(), ".");
                                    blueReadText += StringUtils.leftPad("", element.text().length(), ".");
                                    greenReadText += StringUtils.leftPad("", element.text().length(), ".");
                                } else {
                                    purpleReadTextRC += element.text();
                                    readTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                    blueReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                    greenReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                }
                            } else if (!RC) {
                                readText += element.text();
                                blueReadText += StringUtils.leftPad("", element.text().length(), ".");
                                greenReadText += StringUtils.leftPad("", element.text().length(), ".");
                                magentaReadText += StringUtils.leftPad("", element.text().length(), ".");
                                purpleReadText += StringUtils.leftPad("", element.text().length(), ".");
                            } else {
                                readTextRC += element.text();
                                blueReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                greenReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                magentaReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                                purpleReadTextRC += StringUtils.leftPad("", element.text().length(), ".");
                            }
                        }
                    } else {
                        String text = ((TextNode) node).text();
                        if (text.contains("::::")) {
                            RC = !RC;
                        } else if (!RC) {
                            readText += text;
                            blueReadText += StringUtils.leftPad("", text.length(), ".");
                            greenReadText += StringUtils.leftPad("", text.length(), ".");
                            magentaReadText += StringUtils.leftPad("", text.length(), ".");
                            purpleReadText += StringUtils.leftPad("", text.length(), ".");
                        } else {
                            readTextRC += text;
                            blueReadTextRC += StringUtils.leftPad("", text.length(), ".");
                            greenReadTextRC += StringUtils.leftPad("", text.length(), ".");
                            magentaReadTextRC += StringUtils.leftPad("", text.length(), ".");
                            purpleReadTextRC += StringUtils.leftPad("", text.length(), ".");
                        }
                    }
                }
            }

            if (!readText.contains("::::") && readText.contains(":")) {
                int numReads = StringUtils.countMatches(readText, "-");
                String[] tokens = readText.split(":");
                List<Integer> counts = new ArrayList<>();
                for (int i = 0; i < numReads; i++) {
                    String[] tokens1 = tokens[i].split("\\.");
                    String readCount = tokens1[tokens1.length - 1];
                    int c = Integer.parseInt(readCount);
                    counts.add(c);
                }
                if (numReads >= 1) {
                    StringBuilder b = new StringBuilder(readText);
                    b.append(System.getProperty("line.separator"));
                    int readCounter = 0;
                    char previousChar = '.';
                    for (int j = NAME_WIDTH; j < buffer.length(); j++) {
                        if (j < b.length()) {
                            char c = b.charAt(j);
                            if (c == 'A' || c == 'C' || c == 'T' || c == 'G') {
                                int value = coverageNonRedundant.get(j - NAME_WIDTH);
                                coverageNonRedundant.set(j - NAME_WIDTH, ++value);
                                int tValue = coverageTotal.get(j - NAME_WIDTH);
                                coverageTotal.set(j - NAME_WIDTH, (tValue + counts.get(readCounter)));
                            } else if (c == '.' && (previousChar == 'A' || previousChar == 'C' || previousChar == 'T' || previousChar == 'G')) {
                                readCounter++;
                            }
                            previousChar = c;
                        }
                    }
                }
            }

            if (!readTextRC.contains("::::") && readTextRC.contains(":")) {
                int numReads = StringUtils.countMatches(readTextRC, "-");
                String[] tokens = readTextRC.split(":");
                List<Integer> counts = new ArrayList<>();
                for (int i = 0; i < numReads; i++) {
                    String[] tokens1 = tokens[i].split("\\.");
                    String readCount = tokens1[tokens1.length - 1];
                    int c = Integer.parseInt(readCount);
                    counts.add(c);
                }
                if (numReads >= 1) {
                    StringBuilder b = new StringBuilder(readTextRC);
                    b.append(System.getProperty("line.separator"));
                    int readCounter = 0;
                    char previousChar = '.';
                    for (int j = NAME_WIDTH; j < buffer.length(); j++) {
                        if (j < b.length()) {
                            char c = b.charAt(j);
                            if (c == 'A' || c == 'C' || c == 'T' || c == 'G') {
                                int value = coverageNonRedundantRC.get(j - NAME_WIDTH);
                                coverageNonRedundantRC.set(j - NAME_WIDTH, --value);
                                int tValue = coverageTotalRC.get(j - NAME_WIDTH);
                                coverageTotalRC.set(j - NAME_WIDTH, (tValue - counts.get(readCounter)));
                            } else if (c == '.' && (previousChar == 'A' || previousChar == 'C' || previousChar == 'T' || previousChar == 'G')) {
                                readCounter++;
                            }
                            previousChar = c;
                        }
                    }
                }
            }

            if (!blueReadText.contains("::::") && blueReadText.contains(":")) {
                int numReads = StringUtils.countMatches(blueReadText, "-");
                String[] tokens = blueReadText.split(":");
                List<Integer> counts = new ArrayList<>();
                for (int i = 0; i < numReads; i++) {
                    String[] tokens1 = tokens[i].split("\\.");
                    String readCount = tokens1[tokens1.length - 1];
                    int c = Integer.parseInt(readCount);
                    counts.add(c);
                }
                if (numReads >= 1) {
                    StringBuilder b = new StringBuilder(blueReadText);
                    b.append(System.getProperty("line.separator"));
                    int readCounter = 0;
                    char previousChar = '.';
                    for (int j = NAME_WIDTH; j < buffer.length(); j++) {
                        if (j < b.length()) {
                            char c = b.charAt(j);
                            if (c == 'A' || c == 'C' || c == 'T' || c == 'G') {
                                int tValue = coverageBlueTotal.get(j - NAME_WIDTH);
                                coverageBlueTotal.set(j - NAME_WIDTH, (tValue + counts.get(readCounter)));
                            } else if (c == '.' && (previousChar == 'A' || previousChar == 'C' || previousChar == 'T' || previousChar == 'G')) {
                                readCounter++;
                            }
                            previousChar = c;
                        }
                    }
                }
            }

            if (!blueReadTextRC.contains("::::") && blueReadTextRC.contains(":")) {
                int numReads = StringUtils.countMatches(blueReadTextRC, "-");
                String[] tokens = blueReadTextRC.split(":");
                List<Integer> counts = new ArrayList<>();
                for (int i = 0; i < numReads; i++) {
                    String[] tokens1 = tokens[i].split("\\.");
                    String readCount = tokens1[tokens1.length - 1];
                    int c = Integer.parseInt(readCount);
                    counts.add(c);
                }
                if (numReads >= 1) {
                    StringBuilder b = new StringBuilder(blueReadTextRC);
                    b.append(System.getProperty("line.separator"));
                    int readCounter = 0;
                    char previousChar = '.';
                    for (int j = NAME_WIDTH; j < buffer.length(); j++) {
                        if (j < b.length()) {
                            char c = b.charAt(j);
                            if (c == 'A' || c == 'C' || c == 'T' || c == 'G') {
                                int tValue = coverageBlueTotalRC.get(j - NAME_WIDTH);
                                coverageBlueTotalRC.set(j - NAME_WIDTH, (tValue - counts.get(readCounter)));
                            } else if (c == '.' && (previousChar == 'A' || previousChar == 'C' || previousChar == 'T' || previousChar == 'G')) {
                                readCounter++;
                            }
                            previousChar = c;
                        }
                    }
                }
            }

            if (!greenReadText.contains("::::") && greenReadText.contains(":")) {
                int numReads = StringUtils.countMatches(greenReadText, "-");
                String[] tokens = greenReadText.split(":");
                List<Integer> counts = new ArrayList<>();
                for (int i = 0; i < numReads; i++) {
                    String[] tokens1 = tokens[i].split("\\.");
                    String readCount = tokens1[tokens1.length - 1];
                    int c = Integer.parseInt(readCount);
                    counts.add(c);
                }
                if (numReads >= 1) {
                    StringBuilder b = new StringBuilder(greenReadText);
                    b.append(System.getProperty("line.separator"));
                    int readCounter = 0;
                    char previousChar = '.';
                    for (int j = NAME_WIDTH; j < buffer.length(); j++) {
                        if (j < b.length()) {
                            char c = b.charAt(j);
                            if (c == 'A' || c == 'C' || c == 'T' || c == 'G') {
                                int tValue = coverageGreenTotal.get(j - NAME_WIDTH);
                                coverageGreenTotal.set(j - NAME_WIDTH, (tValue + counts.get(readCounter)));
                            } else if (c == '.' && (previousChar == 'A' || previousChar == 'C' || previousChar == 'T' || previousChar == 'G')) {
                                readCounter++;
                            }
                            previousChar = c;
                        }
                    }
                }
            }

            if (!greenReadTextRC.contains("::::") && greenReadTextRC.contains(":")) {
                int numReads = StringUtils.countMatches(greenReadTextRC, "-");
                String[] tokens = greenReadTextRC.split(":");
                List<Integer> counts = new ArrayList<>();
                for (int i = 0; i < numReads; i++) {
                    String[] tokens1 = tokens[i].split("\\.");
                    String readCount = tokens1[tokens1.length - 1];
                    int c = Integer.parseInt(readCount);
                    counts.add(c);
                }
                if (numReads >= 1) {
                    StringBuilder b = new StringBuilder(greenReadTextRC);
                    b.append(System.getProperty("line.separator"));
                    int readCounter = 0;
                    char previousChar = '.';
                    for (int j = NAME_WIDTH; j < buffer.length(); j++) {
                        if (j < b.length()) {
                            char c = b.charAt(j);
                            if (c == 'A' || c == 'C' || c == 'T' || c == 'G') {
                                int tValue = coverageGreenTotalRC.get(j - NAME_WIDTH);
                                coverageGreenTotalRC.set(j - NAME_WIDTH, (tValue - counts.get(readCounter)));
                            } else if (c == '.' && (previousChar == 'A' || previousChar == 'C' || previousChar == 'T' || previousChar == 'G')) {
                                readCounter++;
                            }
                            previousChar = c;
                        }
                    }
                }
            }

            if (!magentaReadText.contains("::::") && magentaReadText.contains(":")) {
                int numReads = StringUtils.countMatches(magentaReadText, "-");
                String[] tokens = magentaReadText.split(":");
                List<Integer> counts = new ArrayList<>();
                for (int i = 0; i < numReads; i++) {
                    String[] tokens1 = tokens[i].split("\\.");
                    String readCount = tokens1[tokens1.length - 1];
                    int c = Integer.parseInt(readCount);
                    counts.add(c);
                }
                if (numReads >= 1) {
                    StringBuilder b = new StringBuilder(magentaReadText);
                    b.append(System.getProperty("line.separator"));
                    int readCounter = 0;
                    char previousChar = '.';
                    for (int j = NAME_WIDTH; j < buffer.length(); j++) {
                        if (j < b.length()) {
                            char c = b.charAt(j);
                            if (c == 'A' || c == 'C' || c == 'T' || c == 'G') {
                                int tValue = coverageMagentaTotal.get(j - NAME_WIDTH);
                                coverageMagentaTotal.set(j - NAME_WIDTH, (tValue + counts.get(readCounter)));
                            } else if (c == '.' && (previousChar == 'A' || previousChar == 'C' || previousChar == 'T' || previousChar == 'G')) {
                                readCounter++;
                            }
                            previousChar = c;
                        }
                    }
                }
            }

            if (!magentaReadTextRC.contains("::::") && magentaReadTextRC.contains(":")) {
                int numReads = StringUtils.countMatches(magentaReadTextRC, "-");
                String[] tokens = magentaReadTextRC.split(":");
                List<Integer> counts = new ArrayList<>();
                for (int i = 0; i < numReads; i++) {
                    String[] tokens1 = tokens[i].split("\\.");
                    String readCount = tokens1[tokens1.length - 1];
                    int c = Integer.parseInt(readCount);
                    counts.add(c);
                }
                if (numReads >= 1) {
                    StringBuilder b = new StringBuilder(magentaReadTextRC);
                    b.append(System.getProperty("line.separator"));
                    int readCounter = 0;
                    char previousChar = '.';
                    for (int j = NAME_WIDTH; j < buffer.length(); j++) {
                        if (j < b.length()) {
                            char c = b.charAt(j);
                            if (c == 'A' || c == 'C' || c == 'T' || c == 'G') {
                                int tValue = coverageMagentaTotalRC.get(j - NAME_WIDTH);
                                coverageMagentaTotalRC.set(j - NAME_WIDTH, (tValue - counts.get(readCounter)));
                            } else if (c == '.' && (previousChar == 'A' || previousChar == 'C' || previousChar == 'T' || previousChar == 'G')) {
                                readCounter++;
                            }
                            previousChar = c;
                        }
                    }
                }
            }

            if (!purpleReadText.contains("::::") && purpleReadText.contains(":")) {
                int numReads = StringUtils.countMatches(purpleReadText, "-");
                String[] tokens = purpleReadText.split(":");
                List<Integer> counts = new ArrayList<>();
                for (int i = 0; i < numReads; i++) {
                    String[] tokens1 = tokens[i].split("\\.");
                    String readCount = tokens1[tokens1.length - 1];
                    int c = Integer.parseInt(readCount);
                    counts.add(c);
                }
                if (numReads >= 1) {
                    StringBuilder b = new StringBuilder(purpleReadText);
                    b.append(System.getProperty("line.separator"));
                    int readCounter = 0;
                    char previousChar = '.';
                    for (int j = NAME_WIDTH; j < buffer.length(); j++) {
                        if (j < b.length()) {
                            char c = b.charAt(j);
                            if (c == 'A' || c == 'C' || c == 'T' || c == 'G') {
                                int tValue = coveragePurpleTotal.get(j - NAME_WIDTH);
                                coveragePurpleTotal.set(j - NAME_WIDTH, (tValue + counts.get(readCounter)));
                            } else if (c == '.' && (previousChar == 'A' || previousChar == 'C' || previousChar == 'T' || previousChar == 'G')) {
                                readCounter++;
                            }
                            previousChar = c;
                        }
                    }
                }
            }

            if (!purpleReadTextRC.contains("::::") && purpleReadTextRC.contains(":")) {
                int numReads = StringUtils.countMatches(purpleReadTextRC, "-");
                String[] tokens = purpleReadTextRC.split(":");
                List<Integer> counts = new ArrayList<>();
                for (int i = 0; i < numReads; i++) {
                    String[] tokens1 = tokens[i].split("\\.");
                    String readCount = tokens1[tokens1.length - 1];
                    int c = Integer.parseInt(readCount);
                    counts.add(c);
                }
                if (numReads >= 1) {
                    StringBuilder b = new StringBuilder(purpleReadTextRC);
                    b.append(System.getProperty("line.separator"));
                    int readCounter = 0;
                    char previousChar = '.';
                    for (int j = NAME_WIDTH; j < buffer.length(); j++) {
                        if (j < b.length()) {
                            char c = b.charAt(j);
                            if (c == 'A' || c == 'C' || c == 'T' || c == 'G') {
                                int tValue = coveragePurpleTotalRC.get(j - NAME_WIDTH);
                                coveragePurpleTotalRC.set(j - NAME_WIDTH, (tValue - counts.get(readCounter)));
                            } else if (c == '.' && (previousChar == 'A' || previousChar == 'C' || previousChar == 'T' || previousChar == 'G')) {
                                readCounter++;
                            }
                            previousChar = c;
                        }
                    }
                }
            }
        }

        Map<String, List<Integer>> coverages = new HashMap<>();
        coverages.put("coverageNonRedundant", coverageNonRedundant);
        coverages.put("coverageTotal", coverageTotal);
        coverages.put("coverageMagentaTotal", coverageMagentaTotal);
        coverages.put("coverageGreenTotal", coverageGreenTotal);
        coverages.put("coverageBlueTotal", coverageBlueTotal);
        coverages.put("coveragePurpleTotal", coveragePurpleTotal);
        coverages.put("coverageNonRedundantRC", coverageNonRedundantRC);
        coverages.put("coverageTotalRC", coverageTotalRC);
        coverages.put("coverageMagentaTotalRC", coverageMagentaTotalRC);
        coverages.put("coverageGreenTotalRC", coverageGreenTotalRC);
        coverages.put("coverageBlueTotalRC", coverageBlueTotalRC);
        coverages.put("coveragePurpleTotalRC", coveragePurpleTotalRC);

        return coverages;
    }

    /**
     * Formats an alignment object to the extract coverage distribution plot
     *
     * @param tmr
     * @param transcript
     * @param alignments
     * @return
     */
    public TranscriptMappingResults formatCoverage(TranscriptMappingResults tmr, Transcript transcript, List<Transcriptalignment> alignments) {
        Map<Integer, Integer> mappedAlignmentMap = new HashMap<>();
        List<Integer> mappedAlignmentCounts = new ArrayList<>();
        try {
            String sequence = transcript.getSequence();
            String name = transcript.getName();
            name = name.replaceAll("(\\r|\\n)", "");

            ArrayList<Coordinates> coordinates = new ArrayList<>();
            alignments.stream().map((ta) -> {
                Reads read = READSMAPPER.get(ta.getExperimentid(), ta.getReadid());
                String rs = read.getSequence();
                int readLength = rs.length();
                int seqStart = (int) ta.getStartcoordinate();
                int readStart = 0;
                int seqStop = seqStart + readLength;
                int readStop = readLength - readStart;
                if (seqStop >= sequence.length()) {
                    readStop -= (seqStop - sequence.length());
                    seqStop = sequence.length() - 1;
                }
                Coordinates coordinate = new Coordinates(seqStart, seqStop, 0, ta.getReadid(), ta.getExperimentid(), false, null, rs, read.getCount().intValue(), READ_TYPE.CODING);
                return coordinate;
            }).forEach((coordinate) -> {
                coordinates.add(coordinate);
            });

            Collections.sort(coordinates);
            for (int i = 0; i < sequence.length(); i++) {
                mappedAlignmentMap.put(i, 0);
            }
            coordinates.stream().forEach((coordinate) -> {
                float x = coordinate.x;
                float y = coordinate.y;
                int count = coordinate.readCount;
                for (int i = (int) x; i <= (int) y; i++) {
                    int c = mappedAlignmentMap.get(i);
                    c += count;
                    mappedAlignmentMap.put(i, c);
                }
            });

            for (int i = 0; i < sequence.length(); i++) {
                mappedAlignmentCounts.add(mappedAlignmentMap.get(i));
            }
        } catch (Exception ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
        tmr.mappedAlignmentCounts = mappedAlignmentCounts;

        return tmr;
    }

    /**
     *
     * @param name name to adjusted
     * @return adjusted name
     */
    private String adjustName(String name) {
        StringBuilder buffer = new StringBuilder();
        name += "::::";
        if (name.length() >= NAME_WIDTH) {
            name = name.substring(0, NAME_WIDTH - 5);
            name += "::::";
        }
        buffer.append(name);
        for (int j = buffer.length(); j < NAME_WIDTH; j++) {
            buffer.append(DOT);
        }

        return buffer.toString();
    }

    private Integer computeMismatch1Coordinate(String s, String r, boolean rc) {
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
            }
        }
        return m1c;
    }

    /**
     *
     * @param rs read sequence to be marked up
     * @param markups markup coordinates
     * @return adjusted name
     */
    private Pair adjustMarkup(char[] rs, Map<Integer, Markup> markups) {
        StringBuilder buffer = new StringBuilder();

        int i = 0, indelOffset = 0;
        while (i < rs.length) {
            if (markups.containsKey(i)) {
                Markup m = markups.get(i);
                switch (m.type) {
                    case "S":
                        buffer.append("<span style=\"background-color: #a3c2db\">");
                        buffer.append(rs[i]);
                        buffer.append("</span>");
                        break;
                    case "TT":
                        if ((i + 1) < rs.length) {
                            buffer.append("<span style=\"background-color: #ffcccc\">");
                            buffer.append(rs[i]);
                            i++;
                            buffer.append(rs[i]);
                            buffer.append("</span>");
                        }
                        break;
                    case "I":
                        if ((i + 1) < rs.length && buffer.length() > 0) {
                            char c = buffer.charAt(buffer.length() - 1);
                            buffer.deleteCharAt(buffer.length() - 1);
                            buffer.append("<span style=\"background-color: #ff7f00\" class=\"read\" title=\"Insertion: ");
                            buffer.append(rs[i]);
                            buffer.append("\">");
                            buffer.append(c);
                            i++;
                            buffer.append(rs[i]);
                            buffer.append("</span>");
//                            buffer.append("<span style=\"background-color: rgb(255,127,0)\">");
//                            buffer.append(c);
//                            i++;
//                            buffer.append(rs[i]);
//                            buffer.append("</span>");
                            indelOffset++;
                        }
                        break;
                    case "D":
                        buffer.append("<span style=\"background-color: #ff0000\">");
                        buffer.append(m.variant);
                        buffer.append("</span>");
                        buffer.append(rs[i]);
                        indelOffset--;
                        break;
                    default:
                        break;
                }
            } else {
                buffer.append(rs[i]);
            }
            i++;
        }

        return new Pair(buffer.toString(), indelOffset);
    }

    /**
     *
     * @param position
     * @return string
     */
    private String adjustPosition(String position) {
        StringBuilder buffer1 = new StringBuilder();
        StringBuilder buffer2 = new StringBuilder();

        if (position.length() > POSITION_WIDTH_LEFT) {
            buffer1.append(position.substring(position.length() - POSITION_WIDTH_LEFT, position.length()));
        } else {
            buffer1.append(position);
        }

        buffer2.append(buffer1.toString());
        for (int j = 0, n = POSITION_WIDTH_LEFT - buffer1.length(); j < n; j++) {
            buffer2.append(DOT);
        }

        return buffer2.toString();
    }

    /**
     *
     * @param position
     * @return string
     */
    private String adjustPositionRight(String position) {
        StringBuilder buffer1 = new StringBuilder();
        StringBuilder buffer2 = new StringBuilder();

        if (position.length() > POSITION_WIDTH_RIGHT) {
            buffer1.append(position.substring(position.length() - POSITION_WIDTH_RIGHT, position.length()));
        } else {
            buffer1.append(position);
            for (int j = 0, n = POSITION_WIDTH_RIGHT - buffer1.length(); j < n; j++) {
                buffer2.append(DOT);
            }
            buffer2.append(buffer1.toString());
        }

        return buffer2.toString();
    }

    private static Integer sumInt(final Collection<? extends Number> numbers) {
        Integer sum = 0;
        sum = numbers.stream().map((number) -> number.intValue()).reduce(sum, Integer::sum);
        return sum;
    }

    class Coordinates extends Point2D.Float implements Comparable {

        UUID readId;
        long experimentId;
        int readStart;
        boolean revComplement;
        Map<Integer, Markup> mismatches;
        Map<Long, Integer> mappedTranscripts;
        String readSequence;
        int readCount;
        READ_TYPE readType;
        int removedA = 0;

        public Coordinates(float x, float y, int rs, UUID rid, long eid, boolean rc, Map<Integer, Markup> mm, String rseq, int rCount, READ_TYPE rt) {
            super(x, y);
            readId = rid;
            experimentId = eid;
            readStart = rs;
            revComplement = rc;
            mismatches = mm;
            readSequence = rseq;
            readCount = rCount;
            readType = rt;
        }

        @Override
        public int compareTo(Object t) {
            if (t instanceof Coordinates) {
                Coordinates po = (Coordinates) t;
                if (!revComplement) {
                    return (new java.lang.Float(this.x)).compareTo(po.x);
                } else {
                    return (new java.lang.Float(this.y)).compareTo(po.y);
                }

            }
            return 0;
        }

        @Override
        public String toString() {
            String string = "";
            string += "[" + readId + ", " + this.x + ", " + this.y + "]";
            return string;
        }
    }

    class Markup implements Comparable {

        Long position;
        String type;
        String variant;

        public Markup(Long p, String t, String v) {
            this.position = p;
            this.type = t;
            this.variant = v;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Markup) {
                Markup po = (Markup) o;
                return position.equals(po.position);
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 71 * hash + Objects.hashCode(this.position);
            return hash;
        }

        @Override
        public int compareTo(Object o) {
            if (o instanceof Markup) {
                Markup po = (Markup) o;
                return position.compareTo(po.position);
            }
            return 0;
        }
    }

    class Pair {

        Pair(String _s, Integer _o) {
            s = _s;
            o = _o;
        }
        String s = "";
        Integer o = 0;
    }

}
