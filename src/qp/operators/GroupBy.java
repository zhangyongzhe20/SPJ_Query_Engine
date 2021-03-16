package qp.operators;

import qp.utils.Attribute;

import java.util.ArrayList;

public class GroupBy extends Distinct {
    public GroupBy(Operator base, boolean isAsc, boolean isDesc, ArrayList<Attribute> sortOn, int type, int numBuff) {
        super(base, isAsc, isDesc, sortOn, type, numBuff);
    }
}
