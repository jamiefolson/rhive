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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 * Melts a table into key value pairs
 */
@Description(
    name = "melt",
    value = "_FUNC_(cols...) - turns k columns into k rows with one value from a valuecol"
)
public class GenericUDTFMelt extends GenericUDTF {
  @Override
  public void close() throws HiveException {
  }

  ArrayList<ObjectInspector> argOIs = new ArrayList<ObjectInspector>();
  Object[] forwardObj = null;
  ArrayList<ReturnObjectInspectorResolver> returnOIResolvers =
      new ArrayList<ReturnObjectInspectorResolver>();
  ObjectInspector keyOI = null;
  Integer numVars = null;
  Integer numKeys = null;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args)
      throws UDFArgumentException {
    if (args.length < 1)  {
      throw new UDFArgumentException("MELT() expects at least one arguments.");
    }
    
    // Divide and round up.
    numVars = (args.length);
    returnOIResolvers.add(new ReturnObjectInspectorResolver());
    returnOIResolvers.get(0).update(ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
    returnOIResolvers.add(new ReturnObjectInspectorResolver());
    //returnOIResolvers.get(0).update(ObjectInspectorFactory.getReflectionObjectInspector(IntWritable.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
    //returnOIResolvers.get(0).update(ObjectInspectorFactory.getReflectionObjectInspector(Int.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA));
    for (int jj = 0; jj < numVars; ++jj) {
      if (!returnOIResolvers.get(1).update(args[jj])) {
          throw new UDFArgumentException(
              "Argument " + (jj + 1) + "'s type (" +
              args[jj].getTypeName() + ") should be convertible to other arguments' type (" + returnOIResolvers.get(1).get().getTypeName() + ")");
        }
      
    }

    forwardObj = new Object[2];
    for (ObjectInspector arg : args){
    	argOIs.add(arg);
    }
    
    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    fieldNames.add("Variable");
    fieldOIs.add(returnOIResolvers.get(0).get());
    fieldNames.add("Value");
    fieldOIs.add(returnOIResolvers.get(1).get());
    
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] args)
      throws HiveException, UDFArgumentException {
      for (int jj = 0; jj < numVars; ++jj) {
    	  /*try{*/
    	  forwardObj[0] = returnOIResolvers.get(0).convertIfNecessary(new IntWritable(jj+1),
    			  returnOIResolvers.get(0).get() );
    	  
    	  forwardObj[1] = returnOIResolvers.get(1).convertIfNecessary(args[jj], 
    			  argOIs.get(jj));
    	  forward(forwardObj);
    	  /*} catch (ClassCastException e){
    		  throw new UDFArgumentException("Error forwarding column "+(jj+1)+" with value "+args[jj].toString() + " of type "+args[jj].getClass().getCanonicalName()+"\n\t"+e.getStackTrace());
    	  }*/
    }
  }

  @Override
  public String toString() {
    return "stack";
  }
}
