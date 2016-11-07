package de.tuberlin.dima.minidb.io.tables;

import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.core.DataField;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.core.IntField;
import de.tuberlin.dima.minidb.io.cache.PageExpiredException;
import de.tuberlin.dima.minidb.io.cache.PageFormatException;
import de.tuberlin.dima.minidb.qexec.LowLevelPredicate;

/**
 * Created by royd1990 on 11/1/16.
 */
public class TablePageImpl implements TablePage {

    private byte[] buffer;
    private boolean isExpired;
    private boolean hasBeenModified;
    private int pageNumber;
    private int numOfRecords;
   // private int recordWidth;
    private TableSchema schema;


    public TablePageImpl(TableSchema schema,byte[] buffer) throws PageFormatException{
        this.schema = schema;
        this.buffer = buffer;
        isExpired = false;
        hasBeenModified = false;

        if (IntField.getIntFromBinary(buffer,0) != TablePage.TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER){
            throw new PageFormatException("Magic number doesn't match");
        }

    }

    public TablePageImpl(TableSchema schema,byte[] buffer,int pageNumber) throws PageFormatException{
        this.buffer = buffer;
        this.schema = schema;
        this.pageNumber = pageNumber;
        hasBeenModified = true;
        isExpired = false;
        int recordWidth = 4;
        for(int i=0;i< schema.getNumberOfColumns();i++){
            DataType dataType = schema.getColumn(i).getDataType();
            if(dataType.isFixLength()){
                recordWidth+=dataType.getNumberOfBytes();
            }
            else{
                recordWidth+=8;
            }
        }

        encodeIntAsBinary(TablePage.TABLE_DATA_PAGE_HEADER_MAGIC_NUMBER,buffer,0);
        encodeIntAsBinary(pageNumber,buffer,4);
        encodeIntAsBinary(0,buffer,8);
        encodeIntAsBinary(recordWidth,buffer,12);
        encodeIntAsBinary(buffer.length,buffer,16);
    }

    @Override
    public boolean hasBeenModified() throws PageExpiredException {
        if (isExpired) throw new PageExpiredException();
        return hasBeenModified;
    }

    @Override
    public void markExpired() {
        isExpired = true;
    }

    @Override
    public boolean isExpired() {
        return isExpired;
    }

    @Override
    public byte[] getBuffer() {
        return buffer;
    }

    @Override
    public int getPageNumber() throws PageExpiredException {
        if (isExpired) throw new PageExpiredException();
        pageNumber = IntField.getIntFromBinary(buffer,4);
        return pageNumber;
    }

    @Override
    public int getNumRecordsOnPage() throws PageExpiredException {
        if (isExpired) throw new PageExpiredException();
        numOfRecords = IntField.getIntFromBinary(buffer,8);
        return  numOfRecords;
    }

    private int getRecordWidth() throws PageExpiredException{
        if (isExpired) throw new PageExpiredException();
       // recordWidth = IntField.getIntFromBinary(buffer,12);
        return   IntField.getIntFromBinary(buffer,12);
    }

    private int getChunkOffset() throws PageExpiredException{
        if (isExpired) throw new PageExpiredException();
       // recordWidth = IntField.getIntFromBinary(buffer,16);
        return  IntField.getIntFromBinary(buffer,16);
    }

    public int getAliveBit(int position) throws PageExpiredException{
        if (isExpired) throw new PageExpiredException();
        int offset = 32 + position *IntField.getIntFromBinary(buffer,12);
        return IntField.getIntFromBinary(buffer,offset);
    }



    /**
     * Encodes a given int into the binary array using little endian encoding.
     *
     * @param value The value to be encoded.
     * @param buffer The buffer to encode the value into.
     * @param offset The offset into the buffer where to start the encoding.
     */
    private void encodeIntAsBinary(int value, byte[] encoded, int offset)
    {
        encoded[offset]     = (byte) value;
        encoded[offset + 1] = (byte) (value >>> 8);
        encoded[offset + 2] = (byte) (value >>> 16);
        encoded[offset + 3] = (byte) (value >>> 24);
    }
    @Override
    public boolean insertTuple(DataTuple tuple) throws PageFormatException, PageExpiredException {
        if(isExpired) throw new PageExpiredException();
        int varRecLength = 0;
        int newRecordOffset = getNumRecordsOnPage()*getRecordWidth() + 32;
        for (int i = 0; i < tuple.getNumberOfFields(); i++) {


            DataField dataField = tuple.getField(i);

            if (!dataField.getBasicType().equals(schema.getColumn(i).getDataType().getBasicType()))
                throw new PageFormatException("Error");
            if (!dataField.getBasicType().isFixLength()) {
                varRecLength += dataField.getNumberOfBytes();
            }
        }
        if (varRecLength + getRecordWidth()  > getChunkOffset() - newRecordOffset)
            return false;
        //Write to the bytes
        encodeIntAsBinary(0,buffer,newRecordOffset);
        int dataRecordOffset = newRecordOffset + 4;
        int newChunkOffset = getChunkOffset();
        for(int i = 0;i < tuple.getNumberOfFields();i++){
            DataField dataField = tuple.getField(i);
            if (!dataField.getBasicType().equals(schema.getColumn(i).getDataType().getBasicType()))
                throw new PageFormatException("Error");
            if(dataField.getBasicType().isFixLength()){                //In case of fixed length fields
                dataField.encodeBinary(buffer,dataRecordOffset);
                dataRecordOffset+=schema.getColumn(i).getDataType().getNumberOfBytes();
            }
            else{
                if(dataField.isNULL()){
                    encodeIntAsBinary(0,buffer,dataRecordOffset);
                    encodeIntAsBinary(0,buffer,dataRecordOffset+4);

                }
                else {
                    newChunkOffset -= dataField.getNumberOfBytes();         //In case of variable length fields
                    dataField.encodeBinary(buffer, newChunkOffset);
                    encodeIntAsBinary(newChunkOffset, buffer, dataRecordOffset);//write to byte array dataRecordOffset points to newChuckOffset = Represents Lower 4 bytes of variable records
                    encodeIntAsBinary(dataField.getNumberOfBytes(), buffer, dataRecordOffset + 4);//write to byte array dataRecordOffset points to newChuckOffset + 4 points to length = Represents upper 4 bytes of variable records
                }
                dataRecordOffset+=8; //To skip the smallInt and bigInt part when there is varialble length record
            }

        }
        encodeIntAsBinary(getNumRecordsOnPage()+1,buffer,8);//Update Number of Records in byte array
        encodeIntAsBinary(newChunkOffset,buffer,16);//Update Variable Chunk Offset array
        this.hasBeenModified = true;
        return true;   //Overflow logic yet to be written
    }

    @Override
    public void deleteTuple(int position) throws PageTupleAccessException, PageExpiredException {
        if (isExpired) throw new PageExpiredException();
        int newRecordOffset = IntField.getIntFromBinary(buffer,12)*position + 32;//Traverse to record position
        encodeIntAsBinary(1,buffer,newRecordOffset);//Make first bit 1
        this.hasBeenModified = true;
    }
    @Override
    public DataTuple getDataTuple(int position, long columnBitmap, int numCols) throws PageTupleAccessException, PageExpiredException {
        if (isExpired) throw new PageExpiredException();
        if(position > IntField.getIntFromBinary(buffer,8) || position < 0)
            throw new PageTupleAccessException(position,"Error");
        int newRecordOffset = IntField.getIntFromBinary(buffer,12)*position + 32;//Traverse to record position
        if (( IntField.getIntFromBinary(buffer, newRecordOffset) & 0x1) == 1)
            return null;
        newRecordOffset += 4;
        DataTuple tuple = new DataTuple(numCols);
        DataType type;
        DataField dataField;
        int currentCol = 0;
        for (int i =0; i < schema.getNumberOfColumns(); i++) {
            type = schema.getColumn(i).getDataType();

            if ((columnBitmap & 0x1) == 0) {

                if (type.isFixLength()) {
                    newRecordOffset += type.getNumberOfBytes();

                } else {
                    newRecordOffset += 8;
                }

            } else {

                if (type.isFixLength()) {

                    dataField = type.getFromBinary(buffer, newRecordOffset);

                    newRecordOffset += type.getNumberOfBytes();
                } else  {
                    int start = IntField.getIntFromBinary(buffer, newRecordOffset);
                    int length = IntField.getIntFromBinary(buffer, newRecordOffset + 4);
                    if(start == 0 && length ==0) {
                        dataField = type.getNullValue();

                    } else {
                        dataField = type.getFromBinary(buffer, start, length);

                    }
                    newRecordOffset += 8;
                }
                tuple.assignDataField(dataField, currentCol);
                currentCol++;
            }
            columnBitmap >>>=1;
        }
        return tuple;
    }

    @Override
    public DataTuple getDataTuple(LowLevelPredicate[] preds, int position, long columnBitmap, int numCols) throws PageTupleAccessException, PageExpiredException{
        if (isExpired) throw new PageExpiredException();
        if (position > IntField.getIntFromBinary(buffer, 8) || position < 0)
            throw new PageTupleAccessException(position, "index negative or larger than the number of tuple on the page");
        int recordOffset = IntField.getIntFromBinary(buffer, 12) * position + 32;
        if (( IntField.getIntFromBinary(buffer, recordOffset) & 0x1) == 1)
            return null;
        recordOffset += 4;
        DataTuple tuple = new DataTuple(numCols);
        DataType type;
        DataField field;
        int currentColumn = 0;
        for (int i =0; i < schema.getNumberOfColumns(); i++) {
            type = schema.getColumn(i).getDataType();
            if (type.isFixLength()) {
                field = type.getFromBinary(buffer, recordOffset);
                recordOffset += type.getNumberOfBytes();
            } else  {
                int start = IntField.getIntFromBinary(buffer, recordOffset);
                int length = IntField.getIntFromBinary(buffer, recordOffset+4);

                if(start == 0 && length ==0) {
                    field = type.getNullValue();

                } else {
                    field = type.getFromBinary(buffer, start, length);
                }
                recordOffset += 8;
            }
            for(int j = 0; j < preds.length; j++) {
                if (preds[j].getColumnIndex() == i) {
                    if (!preds[j].evaluateWithNull(field))
                        return null;
                }
            }
            if ((columnBitmap & 0x1) == 1) {
                tuple.assignDataField(field, currentColumn);

                currentColumn++;
            }
            columnBitmap >>>=1;
        }
        return tuple;
    }


    @Override
    public TupleIterator getIterator(int numCols, long columnBitmap) throws PageTupleAccessException, PageExpiredException {
        if (isExpired) throw new PageExpiredException();
        return new TupleIteratorImpl(this, numCols, columnBitmap);
    }

    @Override
    public TupleIterator getIterator(LowLevelPredicate[] preds, int numCols, long columnBitmap) throws PageTupleAccessException, PageExpiredException {
        if (isExpired) throw new PageExpiredException();
        return new TupleIteratorImpl(this,preds,numCols,columnBitmap);
    }

    @Override
    public TupleRIDIterator getIteratorWithRID() throws PageTupleAccessException, PageExpiredException {
        if (isExpired) throw new PageExpiredException();
        int cols = schema.getNumberOfColumns();
        return new TupleRIDIteratorImpl(this, cols, Long.MAX_VALUE);
    }
}


