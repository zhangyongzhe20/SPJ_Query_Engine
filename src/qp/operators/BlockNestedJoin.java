/**
 * Block Nested Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    ArrayList<Batch> blocks;        // Block containing buffer pages
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int blockcurs;                  // Cursor for blocks
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        blocks = new ArrayList<>(numBuff - 2);

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        blockcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

        /** Right hand side table is to be materialized
         ** for the Block Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        if (eosr && eosl) {
            return null;
        }
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {

            if (eosr && eosl) {
                return outbatch;
            }

            // tops up block to max or if EOF left reached
            if (!eosl && lcurs == 0 && blockcurs == 0 && rcurs == 0) {
                startScanRightTableAtTop(); // eosr set true here
                setBlock();
            }
            if (blocks.size() < numBuff - 2) {
                eosl = true;
            }

            while (!eosr) {
                try {
                    if (lcurs == 0 && blockcurs == 0 && rcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }

                    for (int i = rcurs; i < rightbatch.size(); ++i) {
                        Tuple righttuple = rightbatch.get(i);
                        for (int j = blockcurs; j < blocks.size(); j++) {
                            leftbatch = blocks.get(j);
                            for (int k = lcurs; k < leftbatch.size(); k++) {
                                Tuple lefttuple = leftbatch.get(k);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        storePointers(i, j, k);
                                        return outbatch;
                                    }
                                }
                            }
                            lcurs = 0;
                        }
                        lcurs = 0;
                        blockcurs = 0;
                    }
                    lcurs = 0;
                    blockcurs = 0;
                    rcurs = 0;
                } catch (EOFException e) {
                    // rightbatch will be size of 0 here
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }

    private void storePointers(int right, int block, int left) {

        if (right == rightbatch.size() - 1 && block == blocks.size() - 1 && left == leftbatch.size() - 1) {
            // end of right, end of left
            lcurs = 0;
            blockcurs = 0;
            rcurs = 0;
        } else if (right == rightbatch.size() - 1 && block == blocks.size() - 1 && left != leftbatch.size() - 1) {
            // end of right, middle of left
            lcurs = left + 1;
            blockcurs = block;
            rcurs = right;
        } else if (right == rightbatch.size() - 1 && block != blocks.size() - 1 && left == leftbatch.size() - 1) {
            // end of right, middle of left
            lcurs = 0;
            blockcurs = block + 1;
            rcurs = right;
        } else if (right == rightbatch.size() - 1 && block != blocks.size() - 1 && left != leftbatch.size() - 1) {
            // end of right, middle of left
            lcurs = left + 1;
            blockcurs = block;
            rcurs = right;
        } else if (right != rightbatch.size() - 1 && block == blocks.size() - 1 && left == leftbatch.size() - 1) {
            // middle of right, end of left
            lcurs = 0;
            blockcurs = 0;
            rcurs = right + 1;
        } else if (right != rightbatch.size() - 1 && block == blocks.size() - 1 && left != leftbatch.size() - 1) {
            // middle of right, middle of left
            lcurs = left + 1;
            blockcurs = block;
            rcurs = right;
        } else if (right != rightbatch.size() - 1 && block != blocks.size() - 1 && left == leftbatch.size() - 1) {
            // middle of right, middle of left
            lcurs = 0;
            blockcurs = block + 1;
            rcurs = right;
        } else if (right != rightbatch.size() - 1 && block != blocks.size() - 1 && left != leftbatch.size() - 1) {
            // middle of right, middle of left
            lcurs = left + 1;
            blockcurs = block;
            rcurs = right;
        } else {
            System.out.printf("Unknown combination right=%d, block=%d, left=%d\n", right, block, left);
        }
    }

    private void setBlock() {
        blocks.clear();
        do {
            Batch b = left.next();
            if (b == null) {
                break;
            }
            blocks.add(b);
        } while(blocks.size() < numBuff - 2);
    }

    private void startScanRightTableAtTop() {
        /** Whenever a new left page came, we have to start the
         ** scanning of right table
         **/
        try {
            in = new ObjectInputStream(new FileInputStream(rfname));
            eosr = false;
        } catch (IOException io) {
            System.err.println("BlockNestedJoin:error in reading the file");
            System.exit(1);
        }
    }

}
