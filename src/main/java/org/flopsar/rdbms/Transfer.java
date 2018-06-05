package org.flopsar.rdbms;

import com.flopsar.api.AgentId;
import com.flopsar.fdbc.api.*;
import org.flopsar.rdbms.jdbc.DML;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class Transfer {

    private final DML dialect;
    private final java.sql.Connection jdbc;
    private final Connection fdbc;


    public Transfer(DML databaseDialect,Connection fdbc,java.sql.Connection jdbc){
        this.dialect = databaseDialect;
        this.fdbc = fdbc;
        this.jdbc = jdbc;
    }


    public void transferAll(long tfrom, long tto) throws SQLException {
        transferKV(tfrom,tto);
        transferInvocationTrees(tfrom,tto);
    }




    public void transferInvocationTrees(long tfrom, long tto) {
        List<AgentId> agents = fdbc.getAllAgents();
        for (final AgentId a: agents){
            transferInvocationTrees(a,tfrom,tto);
        }
    }




    public void transferInvocationTrees(final AgentId agent, long tfrom, long tto) {

        QueryOption queryOption = new QueryOption().setResultWithException(true).setResultWithParameters(true);
        fdbc.findMethodInvocationTrees(agent, null, null, queryOption, tfrom, tto,
                new FindEvent<MethodInvocationTree>() {
                    @Override
                    public boolean onFound(MethodInvocationTree methodInvocationTree) {
                        try {
                            dialect.insertInvocationTree(agent,methodInvocationTree,jdbc);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        return false;
                    }
        });
    }




    public long transferKV(final AgentId agent, final Symbol symbol, final long tfrom, final long tto) {

        final int batchMax = 1000;
        final AtomicInteger batchSize = new AtomicInteger(0);
        final AtomicInteger transferSize = new AtomicInteger(0);
        final List<MetricValue> metricValues = new ArrayList<MetricValue>();
        fdbc.findKeyValues(agent, symbol, tfrom, tto, new FindEvent<MetricValue>() {
            @Override
            public boolean onFound(final MetricValue metricValue) {
                metricValues.add(new MetricValue() {
                    @Override
                    public long getTimeStamp() { return metricValue.getTimeStamp(); }
                    @Override
                    public long getValue() { return metricValue.getValue(); }
                });
                transferSize.incrementAndGet();
                if (batchMax == batchSize.incrementAndGet()){
                    batchSize.set(0);
                    try {
                        dialect.insertKV(agent,symbol,metricValues,jdbc);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    metricValues.clear();
                }
                return false;
            }
        });
        if (!metricValues.isEmpty()){
            try {
                dialect.insertKV(agent,symbol,metricValues,jdbc);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return transferSize.get();
    }



    public long transferKV(AgentId agent,final long tfrom, final long tto) {
            final List<Symbol> keys = new ArrayList<Symbol>();
            fdbc.findKeysWithData(agent, new FindEvent<Symbol>() {
                @Override
                public boolean onFound(Symbol symbol) {
                    keys.add(symbol);
                    return false;
                }
            });
            long transferSize = 0;
            for (final Symbol s : keys){
                transferSize += transferKV(agent,s,tfrom,tto);
            }
            return transferSize;
    }






    public long transferKV(final long tfrom, final long tto) {
        List<AgentId> agents = fdbc.getAllAgents();
        long transferSize = 0;
        for (final AgentId a: agents){
            transferSize += transferKV(a,tfrom,tto);
        }
        return transferSize;
    }




}

