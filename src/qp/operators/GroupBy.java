package qp.operators;

import java.util.ArrayList;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;

/**
 * Defines a groupby operator which groups the table records by attribute(s).
 */
public class GroupBy extends Distinct {
    /**
     * Creates a new GROUP_BY operator.
     *
     * @param base is the base operator.
     */
    public GroupBy(Operator base, boolean isAsc, boolean isDesc, ArrayList<Attribute> sortOn, int type, int numBuff) {
        super(base, isAsc, isDesc,  sortOn, type,  numBuff);
    }
}