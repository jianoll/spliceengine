package com.splicemachine.derby.impl.ast;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.splicemachine.hbase.HBaseRegionLoads;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.*;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.impl.sql.compile.*;
import org.apache.derby.impl.sql.compile.Predicate;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author P Trolard
 *         Date: 09/09/2013
 */
public class JoinSelector extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(JoinSelector.class);

    // JoinStrategies are stateless so can be reused
    public final static BroadcastJoinStrategy BCAST  = new BroadcastJoinStrategy();
    public final static MergeSortJoinStrategy MSJ    = new MergeSortJoinStrategy();
    public final static NestedLoopJoinStrategy NLJ   = new NestedLoopJoinStrategy();

    public final static long BROADCAST_REGION_MB_THRESHOLD =
        Runtime.getRuntime().maxMemory() / (1024l * 1024) / 100l;
    /*
    static {
        LOG.error(String.format("maxMem at static init %s",
                                            Runtime.getRuntime().maxMemory()));
        long size = Runtime.getRuntime().maxMemory() / (1024l * 1024) / 100l;
        LOG.error(String.format("Broadcast threshold %sMB", size));
        BROADCAST_REGION_MB_THRESHOLD = (int)size;
    }
    */

    public QueryTreeNode visit(JoinNode j) throws StandardException {
        try {
            JoinInfo info = joinInfo(j);
            JoinStrategy chosen = chooseStrategy(info);
            if (!info.strategy.getClass().equals(chosen.getClass())) {
                if (LOG.isInfoEnabled()){
                    LOG.info(String.format("Strategy changed from %s to %s for join %s",
                                info.strategy, chosen, info));
                }
                return withStrategy(j, chosen);
            } else {
                return j;
            }

        } catch (RuntimeException re) {
            LOG.info(String.format("Exception choosing join strategy for %s, bailing", query), re);
            return j;
        }
    }

    @Override
    public QueryTreeNode visit(HalfOuterJoinNode j) throws StandardException {
        return visit((JoinNode) j);
    }

    public static JoinStrategy chooseStrategy(JoinInfo info) throws StandardException {
        // If reasons to bail present, return Derby's strategy
        if (info.userSuppliedStrategy ||
                info.isSystemTable ||
                info.hasRightIndex){
            LOG.debug("--> BAILING");
            return info.strategy;
        }
        // If cross-join or non-equijoin, use NLJ
        if (info.joinPredicates.size() == 0 ||
                !info.isEquiJoin){
            return NLJ;
        }
        // If right leaves are in-memory, or right table is small enough to fit in memory,
        // use Broadcast
        if (Iterables.all(info.rightLeaves, Predicates.instanceOf(RowResultSetNode.class))
                || (info.rightLeaves.size() == 1
                        && info.rightSingleRegionSize > -1
                        && info.rightSingleRegionSize < BROADCAST_REGION_MB_THRESHOLD))
            return BCAST;
        // If right join column is PK, use NLJ
        if (info.rightEquiJoinColIsPK){
            return NLJ;
        }

        return MSJ;
    }

    public static JoinNode withStrategy(JoinNode j, JoinStrategy s) throws StandardException {
        LOG.debug(String.format("--> SETTING STRATEGY %s", s));
        RSUtils.ap(j).setJoinStrategy(s);
        // With new strategy set, regenerate access path
        j.getRightResultSet().changeAccessPath();
        return j;
    }

    public static JoinInfo joinInfo(JoinNode j) throws StandardException {
        List<ResultSetNode> rightNodes = RSUtils.getSelfAndDescendants(j.getRightResultSet());
        List<ResultSetNode> rightLeaves = RSUtils.getLeafNodes(j.getRightResultSet());

        // Predicates
        List<Predicate> preds = Lists.newLinkedList(getRightPreds(j));
        List<Predicate> joinPreds = new ArrayList<Predicate>(preds.size());
        List<Predicate> otherPreds = new ArrayList<Predicate>(preds.size());

        for (Predicate p: preds){
            if (p.isJoinPredicate()){
                joinPreds.add(p);
            } else {
                otherPreds.add(p);
            }
        }

        // Index?
        boolean hasRightIndex = containsClass(rightNodes, IndexToBaseRowNode.class);

        boolean userSupplied = joinContainsStrategyHint(j);

        ConglomerateDescriptor cd = RSUtils.ap(j).getConglomerateDescriptor();
        boolean isSystemTable = cd != null &&
                                    cd.getSchemaID().toString()
                                        .equals(SchemaDescriptor.SYSTEM_SCHEMA_UUID);

        // Region size
        int singleRegionSize = -1;
        if (rightLeaves.size() == 1
                && rightLeaves.get(0) instanceof FromBaseTable) {
            FromBaseTable fbt = (FromBaseTable) rightLeaves.get(0);
            Collection<HServerLoad.RegionLoad> regionLoads =
                HBaseRegionLoads
                    .getCachedRegionLoadsForTable(Long.toString(fbt.getTableDescriptor()
                                                                    .getHeapConglomerateId()));
            if (regionLoads != null
                    && regionLoads.size() == 1) {
                singleRegionSize = HBaseRegionLoads
                                       .memstoreAndStorefileSize(regionLoads.iterator().next());
            }


        }

        return new JoinInfo(strategy(j),
                            userSupplied,
                            isSystemTable,
                            isEquijoin(joinPreds),
                            false,
                            hasRightIndex,
                            joinPreds,
                            otherPreds,
                            rightNodes,
                            rightLeaves,
                            singleRegionSize);
    }

    public static boolean joinContainsStrategyHint(JoinNode j) throws StandardException {
        Iterable<ResultSetNode> rsns = Iterables.concat(nodesUntilJoin(j.getRightResultSet()),
                                                        nodesUntilJoin(j.getLeftResultSet()));
        for (ResultSetNode rsn: rsns){
            Properties props = ((Optimizable)rsn).getProperties();
            if (props != null &&
                    props.getProperty("joinStrategy") != null &&
                    (j == rsn ||
                            (j.getReferencedTableMap() != null &&
                                    rsn.getReferencedTableMap() != null &&
                                    j.getReferencedTableMap().contains(rsn.getReferencedTableMap())))) {
                return true;
            }
        }
        return false;
    }

    public static List<ResultSetNode> nodesUntilJoin(ResultSetNode n) throws StandardException {
        //return RSUtils.collectNodesUntil(n, ResultSetNode.class, Predicates.instanceOf(JoinNode.class));
        return CollectNodes.collector(ResultSetNode.class)
                   .onAxis(RSUtils.isRSN)
                   .until(Predicates.instanceOf(JoinNode.class))
                   .collect(n);
    }

    public static Iterable<Predicate> getRightPreds(JoinNode j) throws StandardException {
        Iterable<ResultSetNode> rightsUntilBinary = Iterables.filter(
                RSUtils.collectNodesUntil(j.getRightResultSet(), ResultSetNode.class,
                        RSUtils.isBinaryRSN),
                RSUtils.rsnHasPreds);
        return Iterables.concat(
                Iterables.transform(rightsUntilBinary,
                        new Function<ResultSetNode, List<Predicate>>() {
                            @Override
                            public List<Predicate> apply(ResultSetNode rsn) {
                                try {
                                    return preds(rsn);
                                } catch (StandardException se){
                                    throw new RuntimeException(se);
                                }
                            }
                        }));
    }

    public static List<Predicate> preds(ResultSetNode t) throws StandardException {
        PredicateList pl = t instanceof FromBaseTable ?
                                RSUtils.getPreds((FromBaseTable) t) : RSUtils.getPreds((ProjectRestrictNode) t);
        return PredicateUtils.PLtoList(pl);
    }

    public static boolean isEquijoin(List<Predicate> preds) throws StandardException {
        for (Predicate p: preds){
            if (PredicateUtils.isEquiJoinPred.apply(p)){
                return true;
            }
        }
        return false;
    }

    public static JoinStrategy strategy(JoinNode j){
        return RSUtils.ap(j).getJoinStrategy();
    }

    public static boolean containsClass(List<?> list, Class clazz){
        for (Object o: list){
            if (clazz.isInstance(o)){
                return true;
            }
        }
        return false;
    }

    public static boolean rightColIsPK(JoinNode j, FromBaseTable fbt, List<Predicate> equiPreds){
        return false;
    }

}
