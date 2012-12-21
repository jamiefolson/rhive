/**
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

package com.sonamine.hive.udf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.TaskExecutionException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * GenericUDTFExplode.
 *
 */
@Description(name = "multisplode",
    value = "_FUNC_(a,...) - separates the elements of array a into multiple rows.  " +
      "Multiple arguments are expanded together, but must have the same number of elements")
public class GenericUDTFMultisplode extends GenericUDTF {

  private ObjectInspector[] inputOI = null;
  private Object[] forwardObj = null;

  @Override
  public void close() throws HiveException {
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length < 1) {
      throw new UDFArgumentException("multisplode() requires at least one argument");
    }

    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    forwardObj = new Object[args.length];

    for (int i = 0; i < args.length; i++) {
    switch (args[i].getCategory()) {
    case LIST:
      inputOI[i] = args[i];
      fieldNames.add("col"+i);
      fieldOIs.add(((ListObjectInspector)inputOI[i]).getListElementObjectInspector());
      break;
   /* case MAP:
      inputOI[i] = args[i];
      fieldNames.add("key"+i);
      fieldNames.add("value"+i);
      fieldOIs.add(((MapObjectInspector)inputOI[i]).getMapKeyObjectInspector());
      fieldOIs.add(((MapObjectInspector)inputOI[i]).getMapValueObjectInspector());
      break;*/
    default:
      throw new UDFArgumentException("multisplode() takes one or more arrays as a parameter");
    }
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
        fieldOIs);
  }

  private final Object[] forwardListObj = new Object[1];
  private final Object[] forwardMapObj = new Object[2];

  @Override
  public void process(Object[] args) throws HiveException {
	List[] lists = new List[args.length];
	lists[0] =((ListObjectInspector)inputOI[0]).getList(args[0]); 
	int n = lists[0].size();
	
	for (int i=1;i<args.length;i++){
		lists[i] = ((ListObjectInspector)inputOI[i]).getList(args[i]); 
		int ni = lists[i].size(); 
		if (ni!=n){
			throw new TaskExecutionException("multisplode() array arguments " +
					"must have same length, but argument: 0 has length: "+n+
					" but argument: "+i+" has length: "+ni);
		}
	}
	for (int j=0;j<n;j++){
		for (int i=0;i<args.length;i++){
			forwardObj[i] = lists[i].get(j);
		}
		forward(forwardObj);
	}
	/*switch (inputOI[0].getCategory()) {
    case LIST:
      ListObjectInspector listOI = (ListObjectInspector)inputOI;
      List<?> list = listOI.getList(o[0]);
      if (list == null) {
        return;
      }
      
      for (Object r : list) {
        forwardListObj[0] = r;
        forward(forwardListObj);
      }
      break;
    case MAP:
      MapObjectInspector mapOI = (MapObjectInspector)inputOI;
      Map<?,?> map = mapOI.getMap(o[0]);
      if (map == null) {
        return;
      }
      for (Entry<?,?> r : map.entrySet()) {
        forwardMapObj[0] = r.getKey();
        forwardMapObj[1] = r.getValue();
        forward(forwardMapObj);
      }
      break;
    default:
      throw new TaskExecutionException("explode() can only operate on an array or a map");
    }*/
  }

  @Override
  public String toString() {
    return "multisplode";
  }
}
