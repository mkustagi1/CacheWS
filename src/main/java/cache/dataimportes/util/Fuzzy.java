package cache.dataimportes.util;

import java.util.Collection;
import java.util.List;

public final class Fuzzy {

    public int resultStart;
    public int resultEnd;
    public int resultIndex;
    public String matchedPattern;
    public double result;
    public double threshold = 0.34;
    protected final int MAX_PATTERN = 64;
    protected final int MAX_SOURCE = 256;
    protected final int BIG_VALUE = 1000000;
    protected int[][] e;
    protected WayType[][] w;

    public int substrStart(CharSequence charSequence, CharSequence charSequence2) {
        if (this.containability(charSequence, charSequence2) < this.threshold) {
            return this.resultStart;
        }
        return -1;
    }

    public int substrEnd(CharSequence charSequence, CharSequence charSequence2) {
        if (this.containability(charSequence, charSequence2) < this.threshold) {
            return this.resultEnd;
        }
        return -1;
    }

    public boolean equals(CharSequence charSequence, CharSequence charSequence2) {
        return this.similarity(charSequence, charSequence2) < this.threshold;
    }

    public /* varargs */ boolean containsOneOf(CharSequence charSequence, CharSequence... arrcharSequence) {
        for (CharSequence charSequence2 : arrcharSequence) {
            if (this.containability(charSequence, charSequence2) >= this.threshold) {
                continue;
            }
            return true;
        }
        return false;
    }

    public /* varargs */ boolean containsOneOfExactMatch(String charSequence, List<String> arrcharSequence) {
        return arrcharSequence.stream().anyMatch((charSequence2) -> (charSequence.contains(charSequence2)));
    }    
    
    public double containability(CharSequence charSequence, CharSequence charSequence2) {
        int n;
        int n2 = charSequence2.length() + 1;
        int n3 = charSequence.length() + 1;
        e = new int[n2][n3];
        w = new WayType[n2][n3];
        for (n = 0; n < n3; ++n) {
            this.e[0][n] = 0;
        }
        char c = '\u0000';
        for (n = 1; n < n2; ++n) {
            this.e[n][0] = n;
            this.w[n][0] = WayType.DELETE;
            char c2 = c;
            c = Character.toUpperCase(charSequence2.charAt(n - 1));
            char c3 = '\u0000';
            for (int i = 1; i < n3; ++i) {
                char c4 = c3;
                c3 = Character.toUpperCase(charSequence.charAt(i - 1));
                int n4 = c == c3 ? 0 : 1;
                int n5 = this.e[n - 1][i - 1] + n4;
                this.w[n][i] = WayType.SUBST;
                int n6 = this.e[n - 1][i] + 1;
                if (n5 > n6) {
                    n5 = n6;
                    this.w[n][i] = WayType.DELETE;
                }
                if (n5 > (n6 = this.e[n][i - 1] + 1)) {
                    n5 = n6;
                    this.w[n][i] = WayType.INSERT;
                }
                if (c2 == c3 && c == c4 && n5 > (n6 = this.e[n - 2][i - 2] + n4)) {
                    n5 = n6;
                    this.w[n][i] = WayType.SWAP;
                }
                this.e[n][i] = n5;
            }
        }
        int n7 = n3 - 1;
        for (n = 0; n < n3; ++n) {
            if (this.e[n2 - 1][n] >= this.e[n2 - 1][n7]) {
                continue;
            }
            n7 = n;
        }
        int n8 = n7;
        n = n2 - 1;
        block9:
        while (n > 0) {
            switch (this.w[n][n8]) {
                case INSERT: {
                    --n8;
                    continue block9;
                }
                case DELETE: {
                    --n;
                    continue block9;
                }
                case SWAP: {
                    n -= 2;
                    n8 -= 2;
                    continue block9;
                }
            }
            --n8;
            --n;
        }
        this.resultStart = n8 + 1;
        this.resultEnd = n7;
        this.result = (double) this.e[n2 - 1][n7] / (double) charSequence2.length();
        return this.result;
    }

    public int getResultStart() {
        return resultStart;
    }
    
    public int getResultEnd() {
        return resultEnd;
    }

    public double bestEqual(String string, Object object, boolean bl) {
        String[] arrstring;
        double d = Double.POSITIVE_INFINITY;
        if (object instanceof String[]) {
            arrstring = (String[]) object;
        } else if (object instanceof Collection) {
            Collection<String> collection = (Collection<String>) object;
            arrstring = collection.toArray(new String[collection.size()]);
        } else {
            throw new IllegalArgumentException();
        }
        this.resultIndex = -1;
        for (int i = 0; i < arrstring.length; ++i) {
            double d2;
            double d3 = d2 = bl ? this.similarity(string, arrstring[i]) : this.containability(string, arrstring[i]);
            if (d2 >= d) {
                continue;
            }
            d = d2;
            this.resultIndex = i;
            this.matchedPattern = arrstring[i];
        }
        return d;
    }

    public double similarity(CharSequence charSequence, CharSequence charSequence2) {
        int n = charSequence2.length() + 1;
        int n2 = charSequence.length() + 1;
        int n3 = 0;
        e = new int[n][n2];
        w = new WayType[n][n2];
        while (n3 < n2) {
            this.e[0][n3] = n3++;
        }
        char c = '\u0000';
        for (n3 = 1; n3 < n; ++n3) {
            this.e[n3][0] = n3;
            char c2 = c;
            c = Character.toUpperCase(charSequence2.charAt(n3 - 1));
            char c3 = '\u0000';
            for (int i = 1; i < n2; ++i) {
                int n4 = 1000000;
                char c4 = c3;
                c3 = Character.toUpperCase(charSequence.charAt(i - 1));
                int n5 = c3 == c ? 0 : 1;
                n4 = this.e[n3 - 1][i - 1] + n5;
                n4 = Math.min(this.e[n3][i - 1] + 1, n4);
                n4 = Math.min(this.e[n3 - 1][i] + 1, n4);
                if (c3 == c2 && c == c4) {
                    n4 = Math.min(this.e[n3 - 2][i - 2] + n5, n4);
                }
                this.e[n3][i] = n4;
            }
        }
        this.result = (double) (2 * this.e[n - 1][n2 - 1]) / ((double) (n + n2) - 2.0);
        return this.result;
    }

    private static enum WayType {
        TRANSIT,
        INSERT,
        DELETE,
        SUBST,
        SWAP;

        private WayType() {
        }
    }

}
