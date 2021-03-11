package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Distinct extends Sort{
    Batch inbatch;
    Batch outbatch;
    boolean eos;    // Indicate whether end of stream is reached or not
    Tuple lastTuple;
    int start;       // Cursor position in the input buffer
    ArrayList<Integer> sortOnIndexList;
    Schema schema;
    Operator base;
    ArrayList<Attribute> sortOn;

    public Distinct(Operator base, boolean isAsc, boolean isDesc, ArrayList<Attribute> sortOn, int type, int numBuff) {
        super(base, isAsc, isDesc, sortOn, type, numBuff);
        this.base = base;
        this.sortOn = sortOn;
    }

    @Override
    public boolean open() {
        System.out.println("DISTINCT CALLED OPEN");
        eos = false;
        start = 0;
        lastTuple = null;
        this.schema = base.getSchema();
        this.sortOnIndexList = new ArrayList<>();
        System.out.println("DISTINCT: Number of SORTON attributes:"+sortOn.size());
        for(Attribute a : sortOn) {
            if (schema == null) System.out.println("SCHEMA NULL");
            int index = schema.indexOf(a);
            System.out.println("DISTINCT: Adding attribute with index:"+index);
            sortOnIndexList.add(index);
        }
        return super.open();
    }
    @Override
    public Batch next() {
        System.out.println("DISTINCT CALLED NEXT");
        int i;
        if (eos) {
            System.out.println("DISTINCT: EOS REACHED....CLOSING");
            super.close();
            return null;
        }

        /** An output buffer is initiated**/
        outbatch = new Batch(batchSize);

        /** keep on checking the incoming pages until
         ** the output buffer is full
         **/
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch= super.next();
                /** There is no more incoming pages from base operator **/
                if (inbatch == null) {
                    System.out.println("DISTINCT: NO MORE INCOMING");
                    eos = true;
                    return outbatch;
                }
            }

            /** Continue this for loop until this page is fully observed
             ** or the output buffer is full
             **/
            for (i = start; i < inbatch.size() && (!outbatch.isFull()); i++) {
                System.out.println("DISTINCT: RETRIEVING NEXT TUPLE");
                Tuple present = inbatch.get(i);


                /** If the condition is satisfied then
                 ** this tuple is added to the output buffer
                 **/
                if (lastTuple == null || Tuple.compareTuples(lastTuple, present, sortOnIndexList, sortOnIndexList) != 0) {
                    System.out.println("DISTINCT: FOUND NEW DISTINCT ELEMENT...ADDING..");
                    outbatch.add(present);
                    lastTuple = present;
                }
            }

            /** Modify the cursor to the position requierd
             ** when the base operator is called next time;
             **/

            if (i == inbatch.size())
                start = 0;
            else
                start = i;
        }

        return outbatch;
    }
}
