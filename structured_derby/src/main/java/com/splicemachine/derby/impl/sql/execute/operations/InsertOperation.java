package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.execute.FKInfo;
import org.apache.derby.impl.sql.execute.InsertConstantAction;
import org.apache.derby.impl.sql.execute.RISetChecker;
import org.apache.derby.impl.sql.execute.TriggerEventActivator;
import org.apache.derby.impl.sql.execute.TriggerInfo;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.ZookeeperTransaction;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * 
 * @author Scott Fines
 *
 * TODO:
 * 	1. Basic Inserts (insert 1 row, insert multiple small rows) - Done SF
 *  2. Insert with subselect (e.g. insert into t (name) select name from a) - Done SF
 *  3. Triggers (do with Coprocessors)
 *  4. Primary Keys (do with Coprocessors)
 *  5. Secondary Indices (do with Coprocessors)
 */
public class InsertOperation extends SpliceBaseOperation {
	private static final Logger LOG = Logger.getLogger(InsertOperation.class);
	private NoPutResultSet source;
	private NoPutResultSet savedSource;
	InsertConstantAction constants;
	private GeneratedMethod generationClauses;
	private GeneratedMethod checkGM;
	private long heapConglom;
	
	private ResultSet autoGeneratedKeysResultSet;
//	private TemporaryRowHolderImpl autoGeneratedKeysRowHolder;
	
	private ResultDescription resultDescription;
	private RowChanger rowChanger;
	
	private ConglomerateController heapCC;
	
	private TransactionController tc;
	private ExecRow row;
	
	boolean userSpecifiedBulkInsert;
	boolean bulkInsertPerformed;
	
	//bulk insert
	protected boolean bulkInsert;
	private boolean bulkInsertReplace;
	private boolean firstRow = true;
	private boolean[] needToDropSort;
	
	private Hashtable indecConversionTable;
	
	private FormatableBitSet indexedCols;
	private ConglomerateController bulkHeapCC;
	
	protected DataDictionary dd;
	protected TableDescriptor td;
	
	private ExecIndexRow[] indexRows;
	private ExecRow fullTemplate;
	private long[] sortIds;
	private RowLocationRetRowSource[] rowSources;
	private ScanController bulkHeapSC;
	private ColumnOrdering[][] ordering;
	private int[][] collation;
	private SortController[] sorters;
//	private TemporaryRowHolderImpl rowHolder;
	private RowLocation rl;
	
	private SpliceOperation tableScan;
	
	
	private int numOpens;
	private boolean firstExecute;
	
	private FKInfo[] fkInfoArray;
	private TriggerInfo triggerInfo;
	private RISetChecker fkChecker;
	private TriggerEventActivator triggerActivator;
	
	private NumberDataValue aiCache[];
	
	protected boolean autoIncrementGenerated;
	private long identityVal;
	private boolean setIdentity;
	
	protected static List<NodeType> parallelNodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SCAN);
	protected static List<NodeType> sequentialNodeTypes = Arrays.asList(NodeType.SCAN);
	
	private boolean isScan = true;
	
	public InsertOperation(){
		super();
	}
	
	public InsertOperation(NoPutResultSet source,
							GeneratedMethod generationClauses, 
							GeneratedMethod checkGM) throws StandardException{
		super(source.getActivation(),-1,0d,0d);
		this.source = source;
		Activation a = source.getActivation();

        init(SpliceOperationContext.newContext(a));
	}
	
	@Override
	public List<NodeType> getNodeTypes() {
		return isScan ? parallelNodeTypes : sequentialNodeTypes;
	}

	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		super.readExternal(in);
		source = (NoPutResultSet)in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeObject(source);
	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"init with regionScanner %s",regionScanner);
		super.init(context);
		((SpliceOperation)source).init(context);
		
		constants = (InsertConstantAction)activation.getConstantAction();
		heapConglom = constants.getConglomerateId();
		//Use HTable to do inserts instead of HeapConglomerateController - see Bug 188

		List<SpliceOperation> opStack = getOperationStack();
		boolean hasScan = false;
		for(SpliceOperation op:opStack){
			if(this!=op&&op.getNodeTypes().contains(NodeType.REDUCE)||op instanceof ScanOperation){
				hasScan =true;
				break;
			}
		}
		isScan = hasScan;
	}

	@Override
	public SpliceOperation getLeftOperation() {
		return (SpliceOperation)source;
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		return Collections.singletonList((SpliceOperation)source);
	}

	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceLogUtils.trace(LOG,"executeScan");
		/*
		 * Write the data from the source sequentially. 
		 * We make a distinction here, because Inserts either happen in
		 * parallel or sequentially, but never both; Thus, if we have a Reduce
		 * nodetype, this should be executed in parallel, so *don't* attempt to
		 * insert here.
		 */
		return new SpliceNoPutResultSet(activation,this,insertProvider,false);
	}
	
	
	@Override
	public void open() throws StandardException {
		SpliceLogUtils.trace(LOG,"Open");
		super.open();
	}

	@Override
	public ExecRow getExecRowDefinition() {
		ExecRow row = ((SpliceOperation)source).getExecRowDefinition();
		SpliceLogUtils.trace(LOG,"execRowDefinition=%s",row);
		return row;
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		return null;
	}

	@Override
	public long sink() {
		SpliceLogUtils.trace(LOG,"sink on transactinID="+transactionID);
		/*
		 * write out the data to the correct location.
		 * 
		 * If you compare this implementation to that of InsertResultSet, you'll notice
		 * that there is a whole lot less going on. That's because Triggers, Primary Keys, Check
		 * Constraints, and Secondary Indices are all handled through Coprocessors, and are thus transparent
		 * to the writer. This dramatically simplifies this code, at the cost of adding conceptual complexity
		 * in coprocessor logic
		 */
		long numSunk=0l;
		ExecRow nextRow=null;
		HTableInterface htable = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(""+heapConglom));
		try {
			while((nextRow = source.getNextRowCore())!=null){
				SpliceLogUtils.trace(LOG,"InsertOperation sink, nextRow="+nextRow);
				htable.put(SpliceUtils.insert(nextRow.getRowArray(), this.transactionID.getBytes())); // Buffered
				numSunk++;
			}
			htable.flushCommits();
			htable.close();
		} catch (Exception e) {
			//TODO -sf- abort transaction
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
		return numSunk;
	}

	
	@Override
	public String toString() {
		return "Insert{destTable="+heapConglom+",source=" + source + "}";
	}

	private final RowProvider insertProvider = new RowProvider(){
		private long rowsInserted=0;
		@Override public boolean hasNext() { return false; }

		@Override public ExecRow next() { return null; }

		@Override public void remove() { throw new UnsupportedOperationException(); }

		@Override
		public void open() {
			SpliceLogUtils.trace(LOG, "open");
			try {
				source.openCore();
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG, e);
			}
			if(!getNodeTypes().contains(NodeType.REDUCE)){
				//For PreparedStatement inserts, on autocommit on, every execute/executeUpdate commits the changes. After 
				//commit, transaction state will be set as IDLE. However since we do not recreate the operations for next 
				//set of values, we stuck with the same idled  transaction. I simply made the transaction active so that 
				//next executeUpdate can be committed. Note setting transaction active is called on caller/client's derby, 
				//not on hbase derby. Thus executeUpdate from caller can trigger another commit. So on and so forth. See
				//Bug 185 - jz
				try {
					if (((ZookeeperTransaction)trans).isIdle()) {
						((ZookeeperTransaction)trans).setActiveState();
						transactionID = SpliceUtils.getTransIDString(trans);
					}
				} catch (Exception e) {
					SpliceLogUtils.logAndThrowRuntime(LOG, e);
				}
				rowsInserted = (int)sink();
			}
		}

		@Override
		public void close() {
			try {
				source.close();
			} catch (StandardException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG, e);
			}
		}

		@Override public RowLocation getCurrentRowLocation() { return null; }
        @Override public Scan toScan() { return null; }
        @Override public byte[] getTableName() { return null; }

		@Override
		public int getModifiedRowCount() {
			return (int)(rowsInserted+rowsSunk);
		}

		@Override
		public String toString(){
			return "InsertRowProvider";
		}
	};
	
}
