package com.yourname.minisql.region.ha;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class MasterElection implements LeaderSelectorListener {
    private static final Logger log = LoggerFactory.getLogger(MasterElection.class);
    
    private final String masterId;
    private final LeaderSelector leaderSelector;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private MasterStateListener stateListener;
    private volatile boolean stopped = false;
    
    public interface MasterStateListener {
        void onBecomeLeader();
        void onBecomeFollower();
        void onMasterFailed();
    }
    
    public MasterElection(CuratorFramework client, String masterPath, String masterId) {
        this.masterId = masterId;
        this.leaderSelector = new LeaderSelector(client, masterPath, this);
        this.leaderSelector.autoRequeue();
    }
    
    public void start() {
        if (stopped) {
            log.warn("MasterElection already stopped, cannot start");
            return;
        }
        leaderSelector.start();
        log.info("Master election started, id: {}", masterId);
    }
    
    public void stop() {
        if (stopped) {
            return;
        }
        stopped = true;
        try {
            leaderSelector.close();
            log.info("Master election stopped, id: {}", masterId);
        } catch (IllegalStateException e) {
            log.debug("LeaderSelector already closed: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Error stopping MasterElection", e);
        }
    }
    
    public boolean isLeader() {
        return isLeader.get();
    }
    
    public String getMasterId() {
        return masterId;
    }
    
    public void setStateListener(MasterStateListener listener) {
        this.stateListener = listener;
    }
    
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        isLeader.set(true);
        log.info("=== This master ({}) became LEADER ===", masterId);
        
        if (stateListener != null) {
            stateListener.onBecomeLeader();
        }
        
        // 等待直到失去领导权
        while (isLeader.get() && !stopped) {
            Thread.sleep(1000);
        }
        
        log.info("=== This master ({}) lost LEADERSHIP ===", masterId);
        isLeader.set(false);
        
        if (stateListener != null && !stopped) {
            stateListener.onBecomeFollower();
        }
    }
    
    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
            if (isLeader.get()) {
                log.warn("Connection lost while being leader, may lose leadership");
                isLeader.set(false);
                if (stateListener != null) {
                    stateListener.onMasterFailed();
                }
            }
        } else if (newState == ConnectionState.RECONNECTED) {
            log.info("Connection reconnected");
        }
    }
}