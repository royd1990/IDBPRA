package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;

/**
 * Created by royd1990 on 11/3/16.
 */
public class TupleIteratorImpl implements TupleIterator {

    int position;
    int numCols;
    long colBmp;
    LowLevelPredicate[] preds;
    TablePageImpl page;

    public TupleIteratorImpl(TablePageImpl page, int numCols, long colBmp){
        this.page = page;
        this.numCols = numCols;
        this.colBmp = colBmp;
        position = -1;
    }

    public TupleIteratorImpl(TablePageImpl page, LowLevelPredicate[] preds, int numCols, long colBmp){
        this.page = page;
        this.numCols = numCols;
        this.colBmp = colBmp;
        this.preds = preds;
        position = -1;
    }
    @Override
    public boolean hasNext() throws PageTupleAccessException {
        int numRecords = page.getNumRecordsOnPage();



        for (int i = position +1; i < numRecords; i++){

            if ((page.getAliveBit(i) & 0x1) == 0) {

                if (preds == null)
                    return true;

                if (page.getDataTuple(preds, i, colBmp, numCols) != null)
                    return true;

            }
        }
        return false;
    }

    @Override
    public DataTuple next() throws PageTupleAccessException {
        int numRecords = page.getNumRecordsOnPage();

        for (int i = position +1; i < numRecords; i++){

            if ((page.getAliveBit(i) & 0x1) == 0) {
                if (preds == null) {
                    position = i;
                    return page.getDataTuple( i, colBmp, numCols);
                }


                DataTuple tuple = page.getDataTuple(preds, i, colBmp, numCols);

                if (tuple != null) {
                    position = i;
                    return tuple;
                }
            }
        }
        return null;
    }
}

