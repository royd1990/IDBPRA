package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.RID;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;
import de.tuberlin.dima.minidb.util.Pair;

/**
 * Created by royd1990 on 11/3/16.
 */
public class TupleRIDIteratorImpl implements TupleRIDIterator {

    int it;
    int cols;
    long columnBitmap;
    LowLevelPredicate[] preds;
    TablePageImpl page;
    public TupleRIDIteratorImpl(TablePageImpl page,int cols, long columnBitmap){
        this.page = page;
        this.cols = cols;
        this.columnBitmap = columnBitmap;
        it=-1;
    }
    @Override
    public boolean hasNext() throws PageTupleAccessException {
        int numRecords = page.getNumRecordsOnPage();
        for (int i = it +1; i < numRecords; i++){

            if ((page.getAliveBit(i) & 0x1) == 0) {

                if (preds == null)
                    return true;

                if (page.getDataTuple(preds, i, columnBitmap, cols) != null)
                    return true;

            }
        }
        return false;
    }
    @Override
    public Pair<DataTuple, RID> next() throws PageTupleAccessException {
        int numRecords = page.getNumRecordsOnPage();

        Pair<DataTuple, RID> result = new Pair<DataTuple, RID>();



        for (int i = it +1; i < numRecords; i++){

            if ((page.getAliveBit(i) & 0x1) == 0) {
                if (preds == null) {
                    it = i;
                    result.setFirst(page.getDataTuple( i, columnBitmap, cols));
                    result.setSecond(new RID(page.getPageNumber(), i));

                    return result;
                }


                DataTuple tuple = page.getDataTuple(preds, i, columnBitmap, cols);

                if (tuple != null) {
                    it = i;
                    result.setFirst(tuple);
                    result.setSecond(new RID(page.getPageNumber(), i));

                    return result;
                }
            }
        }
        return null;
    }
}

