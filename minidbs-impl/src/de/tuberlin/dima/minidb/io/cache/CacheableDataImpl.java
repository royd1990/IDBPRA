package de.tuberlin.dima.minidb.io.cache;

/**
 * Created by royd1990 on 11/8/16.
 */
public class CacheableDataImpl implements CacheableData {
    @Override
    public boolean hasBeenModified() throws PageExpiredException {
        return false;
    }

    @Override
    public int getPageNumber() throws PageExpiredException {
        return 0;
    }

    @Override
    public void markExpired() {

    }

    @Override
    public boolean isExpired() {
        return false;
    }

    @Override
    public byte[] getBuffer() {
        return new byte[0];
    }
}
