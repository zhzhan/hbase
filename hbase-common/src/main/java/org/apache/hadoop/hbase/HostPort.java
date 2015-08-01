/**
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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Addressing;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HostPort implements Comparable<HostPort> {
  private final String hostnameOnly;
  private final int port;

  public HostPort(final String hostname, final int port) {
    this.hostnameOnly = hostname;
    this.port = port;
  }

  public String getHostname() {
    return hostnameOnly;
  }

  public int getPort() {
    return port;
  }

  public static HostPort valueOf(final String hostport) {
    String splits[] = hostport.split(":",2);
    if(splits.length < 2)
      throw new IllegalArgumentException("Server list contains not a valid <HOST>:<PORT> entry");
    return new HostPort(splits[0], Integer.parseInt(splits[1]));
  }

  @Override
  public String toString() {
    return Addressing.createHostAndPortStr(this.hostnameOnly, this.port);
  }

  @Override
  public int compareTo(HostPort other) {
    int compare = this.getHostname().compareToIgnoreCase(other.getHostname());
    if (compare != 0) return compare;
    compare = this.getPort() - other.getPort();
    return compare;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null) return false;
    if (!(o instanceof HostPort)) return false;
    return this.compareTo((HostPort)o) == 0;
  }
}
