package com.sonamine.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class GenericUDFArrayMax extends GenericUDFArrayReduce {


     
    protected Object evaluateElement(Object current, Object next)
			throws UDFArgumentException {
		boolean lessThan = false;
		if (next==null)
    		return current;
    	if (current == null)
    		return next;
        switch (elementOI.getPrimitiveCategory()) {
          case INT: lessThan = (((IntObjectInspector)elementOI).get(next) <
                                         ((IntObjectInspector)elementOI).get(current));
                                     break;
          case FLOAT: lessThan = (((FloatObjectInspector)elementOI).get(next) <
                                         ((FloatObjectInspector)elementOI).get(current));
                                     break;
          case DOUBLE: lessThan = (((DoubleObjectInspector)elementOI).get(next) <
                                         ((DoubleObjectInspector)elementOI).get(current));
                                    break;
          case LONG: lessThan = (((LongObjectInspector)elementOI).get(next) <
                                         ((LongObjectInspector)elementOI).get(current));
                                      break;
          case SHORT: lessThan = (((ShortObjectInspector)elementOI).get(next) <
                                         ((ShortObjectInspector)elementOI).get(current));
                                       break;
          default: throw new UDFArgumentException("Currently only int, float, double, long and short types are supported for this operation");
        }
        if (!lessThan){
        	return next;
        }
        return current;
	}

    @Override
	public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("array_max(");
    if (children.length>0) {
      sb.append(children[0]);
      for (int i = 1; i < children.length; i++) {
        sb.append(",");
        sb.append(children[i]);
      }
    }
    sb.append(")");
    return sb.toString();
	}

}
