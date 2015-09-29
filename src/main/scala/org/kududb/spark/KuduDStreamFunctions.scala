/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kududb.spark

import org.apache.spark.streaming.dstream.DStream
import org.kududb.client._

import scala.reflect.ClassTag

/**
 * HBaseDStreamFunctions contains a set of implicit functions that can be
 * applied to a Spark DStream so that we can easily interact with HBase
 */
object KuduDStreamFunctions {

  /**
   * These are implicit methods for a DStream that contains any type of
   * data.
   *
   * @param dStream  This is for dStreams of any type
   * @tparam T       Type T
   */
  implicit class GenericKuduDStreamFunctions[T](val dStream: DStream[T]) {


    /**
     * Implicit method that gives easy access to HBaseContext's
     * foreachPartition method.  This will ack very much like a normal DStream
     * foreach method but for the fact that you will now have a HBase connection
     * while iterating through the values.
     *
     * @param kc  The kuduContext object to identify which HBase
     *            cluster connection to use
     * @param f   This function will get an iterator for a Partition of an
     *            DStream along with a connection object to HBase
     */
    def kuduForeachPartition(kc: KuduContext,
                              f: (Iterator[T], KuduClient, AsyncKuduClient) => Unit): Unit = {
      kc.streamForeachPartition(dStream, f)
    }

    /**
     * Implicit method that gives easy access to HBaseContext's
     * mapPartitions method.  This will ask very much like a normal DStream
     * map partitions method but for the fact that you will now have a
     * HBase connection while iterating through the values
     *
     * @param kc  The kuduContext object to identify which HBase
     *            cluster connection to use
     * @param f   This function will get an iterator for a Partition of an
     *            DStream along with a connection object to HBase
     * @tparam R  This is the type of objects that will go into the resulting
     *            DStream
     * @return    A resulting DStream of type R
     */
    def kuduMapPartitions[R: ClassTag](kc: KuduContext,
                                        f: (Iterator[T], KuduClient, AsyncKuduClient) => Iterator[R]):
    DStream[R] = {
      kc.streamMapPartitions(dStream, f)
    }
  }
}
