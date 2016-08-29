package org.qcri.rheem.spark.operators.subOperators;

import org.qcri.rheem.basic.data.Data;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by khayyzy on 5/28/16.
 */
public class DataComparator<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>>
        implements Serializable, Comparator<Data<Type0, Type1>> {

    private static final long serialVersionUID = 1L;

    boolean asc1;
    boolean asc2;

    public DataComparator(boolean asc1, boolean asc2) {
        this.asc1 = asc1;
        this.asc2 = asc2;
    }

    public int compare(Data o1, Data o2) {
        // first level of sorting
        int dff = 0;
        if (asc1) {
            dff = o1.compareTo(o2);
        } else {
            dff = o2.compareTo(o1);
        }
        // second level of sorting
        if (dff == 0) {
            int dff2 = 0;
            if (asc2) {
                dff2 = o1.compareRank(o2);
            } else {
                dff2 = o2.compareRank(o1);
            }
            // third level of sorting
            if (dff2 == 0) {
                if ((o1.isPivot() && o2.isPivot())
                        || (!o1.isPivot() && !o2.isPivot())) {
                    return ((int) o1.getRowID() - (int) o2.getRowID());
                } else if (o1.isPivot()) {
                    if (asc1) {
                        return -1;
                    } else {
                        return 1;
                    }
                } else if (o2.isPivot()) {
                    if (!asc1) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            }
            return dff2;
        }
        return dff;
    }
}
