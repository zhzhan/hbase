package org.apache.hadoop.hbase.group;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.LoadBalancer;

import java.io.IOException;

@InterfaceAudience.Private
public interface GroupableBalancer extends LoadBalancer {

  void setGroupInfoManager(GroupInfoManager groupInfoManager) throws IOException;
}
