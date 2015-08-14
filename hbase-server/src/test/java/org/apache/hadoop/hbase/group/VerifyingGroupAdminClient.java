/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.group;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HostPort;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class VerifyingGroupAdminClient implements GroupAdmin {
  private HTableInterface table;
  private ZooKeeperWatcher zkw;
  private GroupSerDe serDe;
  private GroupAdmin wrapped;

  public VerifyingGroupAdminClient(GroupAdmin groupAdmin, Configuration conf)
      throws IOException {
    wrapped = groupAdmin;
    table = HConnectionManager.createConnection(conf).getTable(GroupInfoManager.GROUP_TABLE_NAME);
    zkw = new ZooKeeperWatcher(conf, this.getClass().getSimpleName(), null);
    serDe = new GroupSerDe();
  }

  @Override
  public void addGroup(String groupName) throws IOException {
    wrapped.addGroup(groupName);
    verify();
  }

  @Override
  public GroupInfo getGroupInfo(String groupName) throws IOException {
    return wrapped.getGroupInfo(groupName);
  }

  @Override
  public GroupInfo getGroupInfoOfTable(TableName tableName) throws IOException {
    return wrapped.getGroupInfoOfTable(tableName);
  }

  @Override
  public void moveServers(Set<HostPort> servers, String targetGroup) throws IOException {
    wrapped.moveServers(servers, targetGroup);
    verify();
  }

  @Override
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    wrapped.moveTables(tables, targetGroup);
    verify();
  }

  @Override
  public void removeGroup(String name) throws IOException {
    wrapped.removeGroup(name);
    verify();
  }

  @Override
  public boolean balanceGroup(String name) throws IOException {
    return wrapped.balanceGroup(name);
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    return wrapped.listGroups();
  }

  @Override
  public GroupInfo getGroupOfServer(HostPort hostPort) throws IOException {
    return wrapped.getGroupOfServer(hostPort);
  }

  public void verify() throws IOException {
    Get get = new Get(GroupInfoManager.ROW_KEY);
    get.addFamily(GroupInfoManager.META_FAMILY_BYTES);
    Map<String, GroupInfo> groupMap = Maps.newHashMap();
    Set<GroupInfo> zList = Sets.newHashSet();

    Result result = table.get(get);
    if(!result.isEmpty()) {
      NavigableMap<byte[],NavigableMap<byte[],byte[]>> dataMap =
          result.getNoVersionMap();
      for(byte[] groupNameBytes:
          dataMap.get(GroupInfoManager.META_FAMILY_BYTES).keySet()) {
        RSGroupProtos.GroupInfo proto =
            RSGroupProtos.GroupInfo.parseFrom(
                dataMap.get(GroupInfoManager.META_FAMILY_BYTES).get(groupNameBytes));
        GroupInfo groupInfo = ProtobufUtil.toGroupInfo(proto);
        groupMap.put(groupInfo.getName(), groupInfo);
      }
    }
    Assert.assertEquals(Sets.newHashSet(groupMap.values()),
        Sets.newHashSet(wrapped.listGroups()));
    try {
      String groupBasePath = ZKUtil.joinZNode(zkw.baseZNode, "groupInfo");
      for(String znode: ZKUtil.listChildrenNoWatch(zkw, groupBasePath)) {
        byte[] data = ZKUtil.getData(zkw, ZKUtil.joinZNode(groupBasePath, znode));
        if(data.length > 0) {
          ProtobufUtil.expectPBMagicPrefix(data);
          ByteArrayInputStream bis = new ByteArrayInputStream(
              data, ProtobufUtil.lengthOfPBMagic(), data.length);
          zList.add(ProtobufUtil.toGroupInfo(RSGroupProtos.GroupInfo.parseFrom(bis)));
        }
      }
      Assert.assertEquals(zList.size(), groupMap.size());
      for(GroupInfo groupInfo: zList) {
        Assert.assertTrue(groupMap.get(groupInfo.getName()).equals(groupInfo));
      }
    } catch (KeeperException e) {
      throw new IOException("ZK verification failed", e);
    } catch (DeserializationException e) {
      throw new IOException("ZK verification failed", e);
    }
  }

  @Override
  public void close() throws IOException {
  }
}
