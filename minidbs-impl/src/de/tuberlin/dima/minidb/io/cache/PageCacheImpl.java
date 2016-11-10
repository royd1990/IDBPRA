    package de.tuberlin.dima.minidb.io.cache;

import de.tuberlin.dima.minidb.io.tables.TablePage;
import sun.misc.Cache;

import java.util.*;

    /**
 * Created by royd1990 on 11/8/16.
 */
public class PageCacheImpl implements PageCache {
    private PageSize pz;
    private int size;
    private int targetSize;
    private TablePage tablePage;
    private boolean expelled;
    HashMap<Identifier,CachePage> lru;
    HashMap<Identifier,CachePage> mfu;
    List<Identifier> lru_ghost;
    List<Identifier> mfu_ghost;

    public PageCacheImpl(PageSize pz, int size){
        this.pz = pz;
        this.size = size;
        expelled = false;
        targetSize=size/2;
        lru = new HashMap<Identifier,CachePage>();
        mfu = new HashMap<Identifier,CachePage>();

        lru_ghost = new LinkedList<Identifier>();
        mfu_ghost = new LinkedList<Identifier>();
    }

    @Override
    public CacheableData getPage(int resourceId, int pageNumber) {
        Identifier i = new Identifier(resourceId,pageNumber);
        CachePage page = lru.get(i);
        if(page!=null && !page.isExpelled() ){
            page.hit();
            if(page.getHits() > 1){
                mfu.put(i,page);
            }
            else{
                lru.put(i,page);
            }
            return page.getPage();
        }
        page = mfu.get(i);
        if(page!=null && !page.isExpelled()){
            page.hit();
            if(page.getHits() > 1){
                mfu.put(i,page);
                return page.getPage();
            }
        }
        return null;
    }

    @Override
    public CacheableData getPageAndPin(int resourceId, int pageNumber) {
        Identifier i = new Identifier(resourceId,pageNumber);
        CachePage page = lru.get(i);
        if(page!=null && !page.isExpelled()){
            page.hit();
            page.pin();
            if(page.getHits() > 1){
                mfu.put(i,page);
            }
            else{
                lru.put(i,page);
            }
            return page.getPage();
        }
        page = mfu.get(i);
        if(page!=null && !page.isExpelled()){
            page.hit();
            page.pin();
            if(page.getHits() > 1){
                mfu.put(i,page);
                return page.getPage();
            }
        }
        return null;
    }

    @Override
    public EvictedCacheEntry addPage(CacheableData newPage, int resourceId) throws CachePinnedException, DuplicateCacheEntryException {
        Identifier i = new Identifier(resourceId,newPage.getPageNumber());
        if(lru.containsKey(i) || mfu.containsKey(i)){
            throw new DuplicateCacheEntryException(resourceId,newPage.getPageNumber());
        }
        if(lru_ghost.remove(i)){
            int b1 = lru_ghost.size();
            int b2 = mfu_ghost.size();
            int delta = ( b1 >= b2 || b1 == 0 ? 1 : b2/b1);
            targetSize  = Math.min(targetSize + delta, size/2);
            EvictedCacheEntry evicted = evictPage();
            mfu.put(i,new CachePage(newPage,false));
            EvictedCacheEntry evictedCacheEntry = evictPage();
            return  evictedCacheEntry;
        }
        else if(mfu_ghost.remove(i)){
            int b1 = lru_ghost.size();
            int b2 = mfu_ghost.size();
            int delta = ( b2 >= b1 || b2 == 0 ? 1 : b1/b2);
            targetSize  = Math.min(targetSize - delta, 0);
            EvictedCacheEntry evictedCacheEntry = evictPage();
            mfu.put(i,new CachePage(newPage,false));
            return evictedCacheEntry;
        }
        else{
            EvictedCacheEntry evicted = evictPage();
            lru.put(i,new CachePage(newPage,false));
            return evicted;
        }
    }

    @Override
    public EvictedCacheEntry addPageAndPin(CacheableData newPage, int resourceId) throws CachePinnedException, DuplicateCacheEntryException {
        Identifier i = new Identifier(resourceId,newPage.getPageNumber());
        if(lru.containsKey(i) || mfu.containsKey(i)){
            throw new DuplicateCacheEntryException(resourceId,newPage.getPageNumber());
        }
        if(lru_ghost.remove(i)){
            int b1 = lru_ghost.size();
            int b2 = mfu_ghost.size();
            int delta = ( b1 >= b2 || b1 == 0 ? 1 : b2/b1);
            targetSize  = Math.min(targetSize + delta, size/2);
            EvictedCacheEntry evicted = evictPage();
            mfu.put(i,new CachePage(newPage,true));
            EvictedCacheEntry evictedCacheEntry = evictPage();
            return  evictedCacheEntry;
        }
        else if(mfu_ghost.remove(i)){
            int b1 = lru_ghost.size();
            int b2 = mfu_ghost.size();
            int delta = ( b2 >= b1 || b2 == 0 ? 1 : b1/b2);
            targetSize  = Math.min(targetSize - delta, 0);
            EvictedCacheEntry evictedCacheEntry = evictPage();
            mfu.put(i,new CachePage(newPage,true));
            return evictedCacheEntry;
        }
        else{
            EvictedCacheEntry evicted = evictPage();
            lru.put(i,new CachePage(newPage,true));
            return evicted;
        }
    }

    @Override
    public void unpinPage(int resourceId, int pageNumber) {
        Identifier i = new Identifier(resourceId,pageNumber);
        CachePage page = lru.get(i);
        if(page!=null){
            page.unpin();

        }
        else{
            if(mfu.get(i) !=null){
                mfu.get(i).unpin();
            }
        }

    }

    @Override
    public CacheableData[] getAllPagesForResource(int resourceId) {
        return new CacheableData[0];
    }

    @Override
    public void expellAllPagesForResource(int resourceId) {

    }

    @Override
    public int getCapacity() {
        return size;
    }

    @Override
    public void unpinAllPages() {
        Iterator<CachePage> it = lru.values().iterator();
        while (it.hasNext()){
            CachePage cp = it.next();
            cp.unpin();
        }
        it = mfu.values().iterator();
        while (it.hasNext()){
            CachePage cp = it.next();
            cp.unpin();
        }
    }

    private EvictedCacheEntry evictPage() throws CachePinnedException{
        Iterator<Map.Entry<Identifier,CachePage>> it;
        boolean fromFrequent;
        if ((mfu_ghost.size() + lru_ghost.size()) < size)
            return new EvictedCacheEntry(new byte[pz.getNumberOfBytes()]);
        if (expelled) {
            it = lru.entrySet().iterator();
            while (it.hasNext())
            {
                Map.Entry<Identifier,CachePage> entry = it.next();

                if (entry.getValue().isExpelled()) {
                    CacheableData page = entry.getValue().getPage();
                    it.remove();
                    return new EvictedCacheEntry(page.getBuffer(), page, entry.getKey().getResourceId());
                }
            }
            it = mfu.entrySet().iterator();
            while (it.hasNext())
            {
                Map.Entry<Identifier, CachePage> entry = it.next();

                if (entry.getValue().isExpelled()) {
                    CacheableData page = entry.getValue().getPage();
                    it.remove();
                    return new EvictedCacheEntry(page.getBuffer(), page, entry.getKey().getResourceId());
                }
            }
            expelled = false;
        }
        if (lru.size() >= targetSize) {
            it = lru.entrySet().iterator();
            fromFrequent = false;
        } else {
            it = mfu.entrySet().iterator();
            fromFrequent = true;
        }
        Map.Entry<Identifier,CachePage> entry;
        while (it.hasNext())
        {
            entry = it.next();

            if (!entry.getValue().isPinned()) {
                CacheableData page = entry.getValue().getPage();
                if (fromFrequent) {
                    if (mfu_ghost.size() >= targetSize)
                        mfu_ghost.remove(0);
                    mfu_ghost.add(entry.getKey());

                } else {
                    if (lru_ghost.size() >= size - targetSize)
                        lru_ghost.remove(0);
                    lru_ghost.add(entry.getKey());
                }
                it.remove();
                return new EvictedCacheEntry(page.getBuffer(), page, entry.getKey().getResourceId());
            }
        }
        // The frequent (resp. recent) part of the cache is entirely pinned, check the other part
        if (fromFrequent) {
            it = lru.entrySet().iterator();
            fromFrequent = false;
        } else {
            it = mfu.entrySet().iterator();
            fromFrequent = true;
        }
        while (it.hasNext())
        {
            entry = it.next();

            if (!entry.getValue().isPinned()) {
                CacheableData page = entry.getValue().getPage();

                if (fromFrequent) {
                    if (mfu_ghost.size() >= targetSize)
                        mfu_ghost.remove(0);
                    mfu_ghost.add(entry.getKey());
                } else {
                    if (lru_ghost.size() >= size - targetSize)
                        lru_ghost.remove(0);
                    lru_ghost.add(entry.getKey());
                }
                it.remove();
                return new EvictedCacheEntry(page.getBuffer(), page, entry.getKey().getResourceId());
            }
        }
        

        throw new CachePinnedException();
    }

    private class CachePage{
        private int hit;
        private CacheableData cacheableData;
        private boolean pinned;
        private boolean expelled;

        public CachePage(CacheableData cacheableData,boolean pinned){
            this.cacheableData = cacheableData;
            this.pinned = pinned;
            this.expelled = false;

        }
        public void pin(){
            this.pinned = true;
        }
        public void unpin(){
            this.pinned = false;
        }
        public CacheableData getPage(){
            return this.cacheableData;
        }
        public boolean isPinned(){
            return pinned; 
        }
        public void hit(){
            this.hit++;
        }
       public int getHits(){
            return hit;
        }
        public boolean isExpelled(){
            return expelled;
        }

    }

    private class Identifier{
        private int resourceID;
        private int pageNumber;

        public Identifier(int resourceId,int pageNumber){
            this.resourceID = resourceId;
            this.pageNumber = pageNumber;

        }

        public int getResourceId(){
            return this.resourceID;
        }
        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result += prime * result + this.resourceID;
            result += prime * result + this.pageNumber;
            return result;
        }
        @Override
        public boolean equals(Object obj){
            Identifier i = (Identifier) obj;
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Identifier other = (Identifier) obj;
            if (this.resourceID != other.resourceID)
                return false;
            if (this.pageNumber != other.pageNumber)
                return false;
            return true;
        }

    }
}
