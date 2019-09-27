/**
 * Copyright 2014-2015 BloomReach, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bloomreach.zk.replicate;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Read and Write utilities for interacting with {@link org.apache.zookeeper} data.
 * It deals with Path construction and existence checks.
 *
 * @author nitin
 * @since 6/21/15.
 */
public class ZKAccessUtils {
  private static final Logger logger = Logger.getLogger(ZKAccessUtils.class);

  /**
   * Check and create a path if does not exist.
   *
   * @return boolean if zk Path Exists
   */
  public static boolean zkPathExists(ZooKeeper zkHandle, String path) throws KeeperException, InterruptedException {
    return !(zkHandle.exists(path, false) == null);
  }

  public static boolean sourceNewer(ZooKeeper zkHandle, String path, long mtimeSource) throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    zkHandle.getData(path, false, stat);
    return mtimeSource > stat.getMtime();
  }

  /**
   * Check and create a path if does not exist.
   *
   * @return if path exists or is created
   */
  public static boolean validateAndCreateZkPath(ZooKeeper zkHandle, String path, byte[] nodeData) throws KeeperException, InterruptedException {
    if (!zkPathExists(zkHandle, path)) {
      zkHandle.create(path, nodeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      logger.info("Created ZK Path " + path);
    }
    return true;
  }

  /**
   * Check and create a path if does not exist.
   *
   * @return {@link org.apache.zookeeper.data.Stat} stats on the current zk node.
   */
  public static Stat setDataOnZkNode(ZooKeeper zkHandle, String path, byte[] nodeData) throws KeeperException, InterruptedException {
    return zkHandle.setData(path, nodeData, -1);
  }
}
