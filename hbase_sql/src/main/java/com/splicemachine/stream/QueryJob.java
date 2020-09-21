/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.stream;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.function.CloneFunction;
import com.splicemachine.derby.stream.function.IdentityFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import com.splicemachine.derby.stream.spark.SparkDataSetProcessor;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.status.api.v1.StageData;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class QueryJob implements Callable<Void>{

    private static final Logger LOG = Logger.getLogger(QueryJob.class);

    private final OlapStatus status;
    private final RemoteQueryJob queryRequest;

    public QueryJob(RemoteQueryJob queryRequest,
                    OlapStatus jobStatus) {
        this.status = jobStatus;
        this.queryRequest = queryRequest;
    }

    // see https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/status/AppStatusListener.scala
    // https://wanghao989711.gitbooks.io/spark-2-translation/content/spark-SparkListener.html
    // https://books.japila.pl/apache-spark-internals/apache-spark-internals/2.4.4/webui/spark-webui-JobProgressListener.html

    // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/ui/ConsoleProgressBar.scala
    // https://queirozf.com/entries/apache-spark-architecture-overview-clusters-jobs-stages-tasks
    class ListenerCounting extends SparkListener implements AutoCloseable
    {
        OperationContext context;
        SparkContext sc;
        String uuid;
        String sql;

        public ListenerCounting(OperationContext context, String uuid, String sql) {
//            this.sc = SparkContext.getOrCreate( SpliceSpark.getSession().sparkContext().getConf() );
            //this.sc = SparkContext.getOrCreate( SpliceSpark.getContext().getConf() );
            this.sql = sql;
            this.sc = SpliceSpark.getSession().sparkContext();
            this.uuid = uuid;
            this.sc.getLocalProperties().setProperty("operation-uuid", uuid);

            sc.addSparkListener(this);
            log("build listenercounting " + this + "\n");
            this.context = context;
        }
        @Override
        public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
            numStages++;

            log("Stage completed " + numStages + " / " + totalNumStages + "\n");
            super.onStageCompleted(stageCompleted);
        }

        @Override
        public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
            log("Stage submitted " + numStages + " / " + totalNumStages + "\n");
            super.onStageSubmitted(stageSubmitted);
        }

        @Override
        public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
            super.onTaskGettingResult(taskGettingResult);
        }

        int numStages = 0;
        int totalNumStages = 0;
        List<StageInfo> toWatch;
        @Override
        public void onJobStart(SparkListenerJobStart jobStart) {
            if (jobStart.properties().getProperty("operation-uuid", "").equals(uuid)) {
                toWatch = JavaConverters.seqAsJavaListConverter(jobStart.stageInfos()).asJava();
            }
            else
            {
//                log("JOB NOT INTERESTING FOR US!\n");
                return;
            }

            totalNumStages = 0;
            numStages = 0;
            for(StageInfo info : toWatch)
            {
                totalNumStages++;
            }
            super.onJobStart(jobStart);
            log(" job with " + totalNumStages + " stages\n" );
        }

        @Override
        public void onJobEnd(SparkListenerJobEnd jobEnd) {
            log(" job ended\n" );
            super.onJobEnd(jobEnd);
        }

        @Override
        public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
            super.onEnvironmentUpdate(environmentUpdate);
        }

        @Override
        public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
            super.onBlockManagerAdded(blockManagerAdded);
        }

        @Override
        public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
            super.onBlockManagerRemoved(blockManagerRemoved);
        }

        @Override
        public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
            super.onUnpersistRDD(unpersistRDD);
        }

        @Override
        public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
            log(" onApplicationStart\n" );
            super.onApplicationStart(applicationStart);
        }

        @Override
        public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
            log(" onApplicationEnd\n" );
            super.onApplicationEnd(applicationEnd);
        }

        @Override
        public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
            super.onExecutorMetricsUpdate(executorMetricsUpdate);
        }

        @Override
        public void onStageExecutorMetrics(SparkListenerStageExecutorMetrics executorMetrics) {
            super.onStageExecutorMetrics(executorMetrics);
        }

        @Override
        public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
            super.onExecutorAdded(executorAdded);
        }

        @Override
        public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
            super.onExecutorRemoved(executorRemoved);
        }

        @Override
        public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
            super.onExecutorBlacklisted(executorBlacklisted);
        }

        @Override
        public void onExecutorBlacklistedForStage(SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage) {
            super.onExecutorBlacklistedForStage(executorBlacklistedForStage);
        }

        @Override
        public void onNodeBlacklistedForStage(SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage) {
            super.onNodeBlacklistedForStage(nodeBlacklistedForStage);
        }

        @Override
        public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted executorUnblacklisted) {
            super.onExecutorUnblacklisted(executorUnblacklisted);
        }

        @Override
        public void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted) {
            super.onNodeBlacklisted(nodeBlacklisted);
        }

        @Override
        public void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted) {
            super.onNodeUnblacklisted(nodeUnblacklisted);
        }

        @Override
        public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
            super.onBlockUpdated(blockUpdated);
        }

        @Override
        public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted speculativeTask) {
            super.onSpeculativeTaskSubmitted(speculativeTask);
        }

        @Override
        public void onOtherEvent(SparkListenerEvent event) {
            super.onOtherEvent(event);
        }

        @Override
        public void onTaskStart(SparkListenerTaskStart taskStart) {
            //log( this + " Starting: " + numStages + " / " + totalNumStages + "\n");

        }

        void log(String s)
        {
            try { String name = ""; java.io.FileOutputStream fos = new java.io.FileOutputStream("/tmp/log.txt", true);
                fos.write((sql + ": " + s).getBytes()); fos.close(); } catch( Exception e) {}
        }
        long lastUpdate = 0;
        long updateDelay = 500;


        @Override
        public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
//            if( toWatch.stream().map( stageInfo -> stageInfo.stageId() ).anyMatch( id -> id == taskEnd.stageId() ) == false )
//            {
//                return;
//            }
            long now = System.currentTimeMillis();
            if( now < lastUpdate + updateDelay)
               return;
            lastUpdate = now;

            context.recordPipelineWrites( taskEnd.taskMetrics().outputMetrics().recordsWritten() );
            context.recordRead( taskEnd.taskMetrics().inputMetrics().recordsRead());
//            log(taskEnd.taskMetrics().inputMetrics().toString() + "\n");

            StringBuilder sb = new StringBuilder();
            for(StageData sd : JavaConverters.seqAsJavaListConverter(sc.statusStore().activeStages()).asJava())
            {
                int total = sd.numTasks();
                if( toWatch.stream().map( stageInfo -> stageInfo.stageId() ).anyMatch( id -> id == sd.stageId() ) == false ) {
//                    log("DOES NOT CONTAIN " + taskEnd.stageId() + "\n");
                    continue;
                }
                String header = "[ Stage " + sd.stageId() + " ";
                String tailer = " (" + sd.numCompleteTasks() + " + " + sd.numActiveTasks() + ") / " + sd.numTasks() + " ]";
                sb.append(header);
                int width = 70;
                int w = width - header.length() - tailer.length();
                if( w > 0 )
                {
                    int percent = w * sd.numCompleteTasks() / total;
                    for( int i = 0; i < w ; i++)
                    {
                        if( i < percent )
                            sb.append('=');
                        else if (i == percent)
                            sb.append('>');
                        else
                            sb.append('-');
                    }
                }
                sb.append(tailer);
            }
            sb.append("\n");
            log(sb.toString());

//            log( " records Written: " + taskEnd.taskMetrics().outputMetrics().recordsWritten() + "\n");
//            log( " bytes Written: " + taskEnd.taskMetrics().outputMetrics().bytesWritten() + "\n");
//            log( " bytes Read: " + taskEnd.taskMetrics().inputMetrics().bytesRead() + "\n");
//            log( " records Read: " + taskEnd.taskMetrics().inputMetrics().recordsRead() + "\n");
        }

        @Override
        public void close() {
            sc.removeSparkListener(this);
        }
    }

    @Override
    public Void call() throws Exception {

        if(!status.markRunning()){
            //the client has already cancelled us or has died before we could get started, so stop now
            return null;
        }

        ActivationHolder ah = queryRequest.ah;
        SpliceOperation root = ah.getOperationsMap().get(queryRequest.rootResultSetNumber);
        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        DataSet<ExecRow> dataset;
        OperationContext<SpliceOperation> context;
        String jobName = null;
        boolean resetSession = false;
        try {
            if (queryRequest.shufflePartitions != null) {
                SpliceSpark.getSession().conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), queryRequest.shufflePartitions);
                resetSession = true;
            }
            ah.reinitialize(null);
            Activation activation = ah.getActivation();
            root.setActivation(activation);
            if (!(activation.isMaterialized()))
                activation.materialize();
            TxnView parent = root.getCurrentTransaction();
            long txnId = parent.getTxnId();
            String sql = queryRequest.sql;
            String session = queryRequest.session;
            String userId = queryRequest.userId;
            jobName = userId + " <" + session + "," + txnId + ">";

            LOG.info("Running query for user/session: " + userId + "," + session);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Query: " + sql);
            }

            dsp.setJobGroup(jobName, sql);
            context = dsp.createOperationContext(root);
            try(ListenerCounting counter = new ListenerCounting(context, queryRequest.uuid.toString(), queryRequest.sql) ) {
                dataset = root.getDataSet(dsp);
                SparkDataSet<ExecRow> sparkDataSet = (SparkDataSet<ExecRow>) dataset
                        .map(new CloneFunction<>(context))
                        .map(new IdentityFunction<>(context)); // force materialization into Derby's format
                String clientHost = queryRequest.host;
                int clientPort = queryRequest.port;
                UUID uuid = queryRequest.uuid;
                int numPartitions = sparkDataSet.rdd.rdd().getNumPartitions();

                JavaRDD rdd = sparkDataSet.rdd;
                StreamableRDD streamableRDD = new StreamableRDD<>(rdd, context, uuid, clientHost, clientPort,
                        queryRequest.streamingBatches, queryRequest.streamingBatchSize,
                        queryRequest.parallelPartitions);
                streamableRDD.setJobStatus(status);
                streamableRDD.submit();

                status.markCompleted(new QueryResult(numPartitions));

                LOG.info("Completed query for session: " + session);
            }
        } catch (CancellationException e) {
            if (jobName != null)
                SpliceSpark.getContext().sc().cancelJobGroup(jobName);
            throw e;
        } finally {
            if(resetSession)
                SpliceSpark.resetSession();
            ah.close();
        }

        return null;
    }
}
