package com.zkexample.election;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.zkexample.ZooKeeperBaseTest;

public class LeaderElector1Test extends ZooKeeperBaseTest {

    @Test
    public void testNominateLeader() throws IOException, InterruptedException {
        // client1
        LeaderElector1 le1 = createLeaderElector("client1");
        // client1 becomes leader!
        assertTrue(le1.nominateSelfForLeader());

        // client2
        LeaderElector1 le2 = createLeaderElector("client2");
        // sorry, client1 still the leader
        assertFalse(le2.nominateSelfForLeader());

        // client3
        LeaderElector1 le3 = createLeaderElector("client3");
        // sorry, client1 still the leader
        assertFalse(le3.nominateSelfForLeader());

        // client1 disconnect, client2 & client3 raise for leader now!
        le1.getZooKeeper().close();
        Thread.sleep(500);
    }

    private LeaderElector1 createLeaderElector(String clientName) throws IOException {
        LeaderElector1 le = new LeaderElector1(clientName);
        le.setZooKeeper(createClient());

        return le;
    }

}
