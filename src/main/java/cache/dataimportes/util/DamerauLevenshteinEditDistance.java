package cache.dataimportes.util;

import java.awt.Point;
import java.util.HashMap;
import java.util.Map;

/**
 * This class implements the Levenshtein edit distance algorthm.
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Apr 20, 2016)
 * 
 * Added DamerauLevenshstein implementation to handle transpositions.
 * @author Manjunath Kustagi
 * @version 2.0 (Feb 17, 2017)
 */
public class DamerauLevenshteinEditDistance {

    /**
     * Denotes the fact that one character in one input string was removed.
     */
    public static final String GAP = "-";

    public static final class LevenshteinEditDistanceResult {
        private final int distance;
        private final String editSequence;
        private final String topAlignmentRow;
        private final String bottomAlignmentRow;

        LevenshteinEditDistanceResult(final int distance,
                                      final String editSequence,
                                      final String topAlignmentRow,
                                      final String bottomAlignmentRow) {
            this.distance           = distance;
            this.editSequence       = editSequence;
            this.topAlignmentRow    = topAlignmentRow;
            this.bottomAlignmentRow = bottomAlignmentRow;
        }

        public int getDistance() {
            return distance;
        }

        public String getEditSequence() {
            return editSequence;
        }

        public String getTopAlignmentRow() {
            return topAlignmentRow;
        }

        public String getBottomAlignmentRow() {
            return bottomAlignmentRow;
        }
    }

    private static enum EditOperation {
        INSERT     ("I"),
        SUBSTITUTE ("S"),
        DELETE     ("D"),
        TRANSPOSE  ("T"),
        NONE       ("N");

        private final String s;

        private EditOperation(String s) {
            this.s = s;
        }

        @Override
        public String toString() {
            return s;
        }
    }

    public static LevenshteinEditDistanceResult compute(String s, String z) {
        // This is required to keep the parent map invariant. If we did not do
        // this, the very first edit operation would not end up in the output.
        // For more details, comment out the following two rows and see what
        // happens.
        s = "\u0000" + s;
        z = "\u0000" + z;

        Map<Character, Integer> da = new HashMap<>();
        da.put('A', 1);
        da.put('T', 1);
        da.put('C', 1);
        da.put('G', 1);

        final int n = s.length();
        final int m = z.length();
        final int[][] d = new int[m + 1][n + 1];
        final Map<Point, Point> parentMap = new HashMap<>();
        final Map<Point, Point> transposeMap = new HashMap<>();

        for (int i = 1; i <= m; ++i) {
            d[i][0] = i;
        }

        for (int j = 1; j <= n; ++j) {
            d[0][j] = j;
        }

        for (int j = 1; j <= n; ++j) {
            int db = 1;
            for (int i = 1; i <= m; ++i) {
                int k = 1;
                if (z.charAt(i - 1) != '\u0000') {
                   k = da.get(z.charAt(i - 1));
                }

                int l = db;
                final int delta = (s.charAt(j - 1) == z.charAt(i - 1)) ? 0 : 1;
                if (s.charAt(j - 1) == z.charAt(i - 1)) {
                    db = i;
                }

                int tentativeDistance = d[i - 1][j] + 1;
                EditOperation editOperation = EditOperation.INSERT;

                if (tentativeDistance > d[i][j - 1] + 1) {
                    tentativeDistance = d[i][j - 1] + 1;
                    editOperation = EditOperation.DELETE;
                }

                if (tentativeDistance > d[i - 1][j - 1] + delta) {
                    tentativeDistance = d[i - 1][j - 1] + delta;
                    editOperation = EditOperation.SUBSTITUTE;
                }

                if (tentativeDistance > (d[k - 1][l -1] + (j - k - 1) + 1 + (i - l - 1))) {
                    tentativeDistance = (d[k - 1][l -1] + (j - k - 1) + 1 + (i - l - 1));
                    editOperation = EditOperation.TRANSPOSE;
                }
                d[i][j] = tentativeDistance;

                switch (editOperation) {
                    case SUBSTITUTE:
                        parentMap.put(new Point(i, j), new Point(i - 1, j - 1));
                        break;

                    case INSERT:
                        parentMap.put(new Point(i, j), new Point(i - 1, j));
                        break;

                    case DELETE:
                        parentMap.put(new Point(i, j), new Point(i, j - 1));
                        break;

                    case TRANSPOSE:
                        parentMap.put(new Point(i, j), new Point(i - 1, j - 1));
                        transposeMap.put(new Point(i, j), new Point(i - 1, j - 1));
                        transposeMap.put(new Point(i - 1, j - 1), new Point(i - 1, j - 1));
                        break;
                }
            }
            da.put(s.charAt(j - 1), j);
        }

        final StringBuilder topLineBuilder      = new StringBuilder(n + m);
        final StringBuilder bottomLineBuilder   = new StringBuilder(n + m);
        final StringBuilder editSequenceBuilder = new StringBuilder(n + m);
        Point current = new Point(m, n);

        while (true) {
            Point predecessor = parentMap.get(current);
            Point tp = transposeMap.get(current);

            if (predecessor == null) {
                break;
            }
            if (tp == null) {
              if (current.x != predecessor.x && current.y != predecessor.y) {
                final char schar = s.charAt(predecessor.y);
                final char zchar = z.charAt(predecessor.x);

                topLineBuilder.append(schar);
                bottomLineBuilder.append(zchar);
                editSequenceBuilder.append(schar != zchar ?
                                           EditOperation.SUBSTITUTE :
                                           EditOperation.NONE);
              } else if (current.x != predecessor.x) {
                topLineBuilder.append(GAP);
                bottomLineBuilder.append(z.charAt(predecessor.x));
                editSequenceBuilder.append(EditOperation.INSERT);
              } else if (current.y != predecessor.y) {
                topLineBuilder.append(s.charAt(predecessor.y));
                bottomLineBuilder.append(GAP);
                editSequenceBuilder.append(EditOperation.DELETE);
              }
            } else {
                if (current.x != predecessor.x && current.y != predecessor.y) {
                    final char schar = s.charAt(predecessor.y);
                    final char zchar = z.charAt(predecessor.x);
                    topLineBuilder.append(schar);
                    bottomLineBuilder.append(zchar);
                    editSequenceBuilder.append(schar != zchar ?
                                      EditOperation.TRANSPOSE :
                                      EditOperation.NONE);
                }
            }

            current = predecessor;
        }

        // Remove the last characters that correspond to the very beginning
        // of the alignments and edit sequence (since the path reconstructoin
        // proceeds from the "end" to the "beginning" of the distance matrix.
        topLineBuilder     .deleteCharAt(topLineBuilder.length() - 1);
        bottomLineBuilder  .deleteCharAt(bottomLineBuilder.length() - 1);
        editSequenceBuilder.deleteCharAt(editSequenceBuilder.length() - 1);

        // Our result data is backwards, reverse them.
        topLineBuilder     .reverse();
        bottomLineBuilder  .reverse();
        editSequenceBuilder.reverse();

        return new LevenshteinEditDistanceResult(d[m][n]+1,
                                                 editSequenceBuilder.toString(),
                                                 topLineBuilder.toString(),
                                                 bottomLineBuilder.toString());
    }

    public static void main(String[] args) {
        LevenshteinEditDistanceResult result = compute(args[0], args[1]);
        System.out.println("Distance: " + result.getDistance());
        System.out.println("Edit sequence: " + result.getEditSequence());
        System.out.println("Alignment:");
        System.out.println(result.getTopAlignmentRow());
        System.out.println(result.getBottomAlignmentRow());
    }
}
