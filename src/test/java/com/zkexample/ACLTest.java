package com.zkexample;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.Test;

public class ACLTest extends ZooKeeperBaseTest {

    private static final String DIGEST = "digest";

    @Test
    public void testACL() throws KeeperException, InterruptedException, IOException {
        // create a znode /temp1
        ZooKeeper zk = createClient();
        zk.addAuthInfo(DIGEST, "jeff:123456".getBytes());

        zk.create("/temp1", "abc".getBytes(), Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        zk.close();

        // another client to getData with incorrect digest
        zk = createClient();
        zk.addAuthInfo(DIGEST, "jeff:xxxxxx".getBytes());
        // exist() will not be check
        zk.exists("/temp1", false);
        try {
            zk.getData("/temp1", null, null);
            fail("should fail as no permission");
        } catch (KeeperException e) {
            assertEquals(Code.NOAUTH, e.code());
        }
        zk.close();
    }

    @Test
    public void testACL2() throws KeeperException, InterruptedException, IOException {
        // create a znode /temp1
        ZooKeeper zk = createClient();
        zk.addAuthInfo(DIGEST, "jeff:123456".getBytes());

        Id id = new Id(DIGEST, "jeff:123456");
        ACL acl = new ACL(ZooDefs.Perms.READ, id);
        List<ACL> aclList = new ArrayList<ACL>();
        aclList.add(acl);

        zk.create("/temp2", "abc".getBytes(), aclList, CreateMode.PERSISTENT);
        zk.close();

        // another client to getData with incorrect digest
        zk = createClient();
        zk.addAuthInfo(DIGEST, "jeff:123456".getBytes());
        // exist() will not be check
        zk.exists("/temp2", false);
        //
        zk.getChildren("/temp2", false);
        try {
            zk.setData("/temp2", "dc".getBytes(), -1);
            fail("should fail as no permission");
        } catch (KeeperException e) {
            assertEquals(Code.NOAUTH, e.code());
        }
        zk.close();
    }

}
