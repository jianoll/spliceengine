package com.splicemachine.access.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.BaseHRegionUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * 
 * 
 */
public abstract class SkeletonClientSideRegionScanner implements RegionScanner{
    private boolean isClosed = false;
    private static final Logger LOG = Logger.getLogger(SkeletonClientSideRegionScanner.class);
    private static final Comparator<Cell> timeComparator=new Comparator<Cell>(){
        @Override
        public int compare(Cell o1,Cell o2){
            return Long.compare(o2.getTimestamp(),o1.getTimestamp());
        }
    };
	private HRegion region;
	private RegionScanner scanner;
	private Configuration conf;
	private FileSystem fs;
	private Path rootDir;
	private HTableDescriptor htd;
	private HRegionInfo hri;
	private Scan scan;
	private Cell topCell;
	private List<KeyValueScanner>	memScannerList = new ArrayList<>(1);
	private boolean flushed;
	private List<Cell> nextResults = new ArrayList<>();
	private boolean nextResponse;
	private long numberOfRows = 0;

	
	public SkeletonClientSideRegionScanner(Configuration conf,
                                           FileSystem fs,
                                           Path rootDir,
                                           HTableDescriptor htd,
                                           HRegionInfo hri,
                                           Scan scan) throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "init for regionInfo=%s, scan=%s", hri,scan);
		scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
		this.conf = conf;
		this.fs = fs;
		this.rootDir = rootDir;
		this.htd = htd;
		this.hri = new SpliceHRegionInfo(hri);
		this.scan = scan;
	}

    @Override
	public void close() throws IOException {
        if (isClosed)
            return;
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "close");
		if (scanner != null)
			scanner.close();
		memScannerList.get(0).close();
		region.close();
        isClosed = true;
    }

    @Override
	public HRegionInfo getRegionInfo() {
		return scanner.getRegionInfo();
	}

    @Override
	public boolean reseek(byte[] row) throws IOException {
		return scanner.reseek(row);
	}

    @Override
	public long getMvccReadPoint() {
		return scanner.getMvccReadPoint();
	}

    @Override
    public boolean next(List<Cell> result,int limit) throws IOException{
        return nextRaw(result,limit);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException{
        return nextRaw(results);
    }

    @Override
    public boolean nextRaw(List<Cell> result,int limit) throws IOException{
        return nextRaw(result);
    }

    @Override
    public long getMaxResultSize(){
        return scanner.getMaxResultSize();
    }

    @Override
    public boolean isFilterDone() throws IOException{
        return scanner.isFilterDone();
    }

    @Override
	public boolean nextRaw(List<Cell> result) throws IOException {
		boolean res = nextMerged(result, true);
        Collections.sort(result,timeComparator);
        boolean returnValue = updateTopCell(res,result);
        if (returnValue)
            numberOfRows++;
		return returnValue;
	}


	/**
	 * refresh underlying RegionScanner we call this when new store file gets
	 * created by MemStore flushes or current scanner fails due to compaction
	 */
	public void updateScanner() throws IOException {
		if (LOG.isDebugEnabled()){
            SpliceLogUtils.debug(LOG,
                    "updateScanner with hregionInfo=%s, tableName=%s, rootDir=%s, scan=%s",
                    hri,htd.getNameAsString(),rootDir,scan);
        }
		if (flushed) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG, "Flush occurred");
            if (this.topCell != null) {
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG, "setting start row to %s", topCell);
                //noinspection deprecation
                scan.setStartRow(Bytes.add(topCell.getRow(), new byte[] {0}));
            }
        }
	    memScannerList.add(getMemStoreScanner());
		this.region = openHRegion();		
		RegionScanner regionScanner = BaseHRegionUtil.getScanner(region, scan,memScannerList);
        if (flushed) {
            if (scanner != null)
                scanner.close();
        }
        scanner = regionScanner;
	}

    public HRegion getRegion(){
        return region;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private boolean updateTopCell(boolean response, List<Cell> results) throws IOException {
        if (!results.isEmpty() &&
                CellUtil.matchingFamily(results.get(0),ClientRegionConstants.FLUSH)){
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"received flush message " + results.get(0));
            flushed = true;
            updateScanner();
            nextResults.clear();
            results.clear();
            return nextRaw(results);
        } else
        if (response)
            topCell = results.get(results.size() - 1);
        return response;
    }

    private boolean matchingFamily(List<Cell> result, byte[] family) {
        if (result.isEmpty())
            return false;
        int size = result.size();
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i<size; i++) {
            if(CellUtil.matchingFamily(result.get(i),family))
                return true;
        }
        return false;
    }

    private boolean nextMerged(List<Cell> result, boolean recurse) throws IOException {
        boolean res;
        if (!nextResults.isEmpty()) {
            // Removed AddAll in an attempt to not perform an array copy
            // result.addAll(nextResults);
            for (int i = 0; i<nextResults.size();i++) {
                result.add(nextResults.get(i));
            }
            nextResults.clear();
            res = nextResponse;
        } else {
            res = scanner.nextRaw(result);
        }
        if (matchingFamily(result,ClientRegionConstants.HOLD)) {
            if (result.get(0).getTimestamp()== HConstants.LATEST_TIMESTAMP) {
                result.clear();
                return false;
            }
            else {
                result.clear();
                return nextMerged(result, recurse);
            }
        }
        if (res && recurse) {
            nextResponse = nextMerged(nextResults, false);
            if (!nextResults.isEmpty() && CellUtil.matchingRow(nextResults.get(0),result.get(0))){
                result.addAll(nextResults);
                nextResults.clear();
            }
            return true;
        }
        return res;
    }

    private HRegion openHRegion() throws IOException {
        return HRegion.openHRegion(conf, fs, rootDir, hri, new ReadOnlyTableDescriptor(htd), null,null, null);
    }

    private KeyValueScanner getMemStoreScanner() throws IOException {
        Scan memScan = new Scan(scan);
        memScan.setAttribute(ClientRegionConstants.SPLICE_SCAN_MEMSTORE_ONLY,SIConstants.TRUE_BYTES);
        ResultScanner scanner=newScanner(memScan);
        return new MemstoreKeyValueScanner(scanner);
    }

    protected abstract ResultScanner newScanner(Scan memScan) throws IOException;

    private static class ReadOnlyTableDescriptor extends HTableDescriptor {
        ReadOnlyTableDescriptor(HTableDescriptor desc) {
            super(desc.getTableName(), desc.getColumnFamilies(), desc.getValues());
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }
    }

    @Override
    public String toString() {
        return String.format("SkeletonClienSideregionScanner[scan=%s,region=%s,numberOfRows=%d",scan,region.getRegionInfo(),numberOfRows);
    }
}