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

public class GenericUDFCut extends GenericUDF {
	private static Log LOG = LogFactory
		      .getLog(GenericUDFCut.class.getName());
	  ObjectInspector[] argumentOI;
	  ObjectInspector[] elementOI = new ObjectInspector[2];
    boolean[] isPrimitive = new boolean[2];

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
	if (arguments[0].get()==null || arguments[1].get()==null)return null;
    List<IntWritable> results = new ArrayList<IntWritable>();
    List<Object> valueList = new ArrayList<Object>();
    List<Object> compareList = new ArrayList<Object>();
    PrimitiveCategory compareCategory = 
      PrimitiveCategory.INT;
    if (isPrimitive[0]) {
      valueList.add(arguments[0].get());
    }else {
      valueList.addAll(((ListObjectInspector)argumentOI[0]).getList(arguments[0].get()));
    }
    if (isPrimitive[1]) {
      compareCategory = ((PrimitiveObjectInspector)argumentOI[1]).getPrimitiveCategory();
      compareList.add(arguments[1].get());
    }else {
      compareCategory = ((PrimitiveObjectInspector)
          ((ListObjectInspector)argumentOI[1]).getListElementObjectInspector()
          ).getPrimitiveCategory();
      compareList.addAll(((ListObjectInspector)argumentOI[1]).getList(arguments[1].get()));
    }
    for (Object value : valueList) {
      IntWritable resultElement = new IntWritable(-1);
      for (int idx = 0; idx < compareList.size(); idx++) {
        Object compare = compareList.get(idx);
        boolean lessThan = false;
        switch (compareCategory) {
          case INT: lessThan = (((IntObjectInspector)elementOI[0]).get(value) <
                                         ((IntObjectInspector)elementOI[1]).get((compare)));
                                     break;
          case FLOAT: lessThan = (((FloatObjectInspector)elementOI[0]).get(value) <
                                         ((FloatObjectInspector)elementOI[1]).get(compare));
                                     break;
          case DOUBLE: lessThan = (((DoubleObjectInspector)elementOI[0]).get(value) <
                                         ((DoubleObjectInspector)elementOI[1]).get(compare));
                                    break;
          case LONG: lessThan = (((LongObjectInspector)elementOI[0]).get(value) <
                                         ((LongObjectInspector)elementOI[1]).get(compare));
                                      break;
          case SHORT: lessThan = (((ShortObjectInspector)elementOI[0]).get(value) <
                                         ((ShortObjectInspector)elementOI[1]).get(compare));
                                       break;
          default: throw new UDFArgumentException("Currently only int, float, double, long and short types are supported for this operation");
        }
        if (lessThan) {
          resultElement.set(idx);
          break;
        }
      }
      results.add(resultElement);
    }
    if (isPrimitive[0])
      return results.get(0);

		return results;
	}

	@Override
	public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("cut(");
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

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments)
			throws UDFArgumentException {
        argumentOI = arguments;
		if (arguments.length != 2) {
	        throw new UDFArgumentTypeException(arguments.length-1,
	            "Two arguments are required");
	      }
		
		
		if (arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			 if (arguments[1].getCategory() != ObjectInspector.Category.LIST) {
				 throw new UDFArgumentTypeException(1,
				            "The argument of function should be primative" + ", but \""
				            + arguments[1].getTypeName() + "\" is found");
      }else {
        ListObjectInspector loi = (ListObjectInspector)arguments[1];
        if (loi.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
          throw new UDFArgumentTypeException(1,
                    "The argument of function should be primative" + ", but \""
                    + arguments[0].getTypeName() + "\" is found");
        }else {
          elementOI[1] = loi.getListElementObjectInspector();
          isPrimitive[1] = false;
        }

      }
    }else {
      elementOI[1] = arguments[1];
      isPrimitive[1] = true;
    }
		
		ObjectInspector returnObjectInspector = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
		if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			 if (arguments[0].getCategory() != ObjectInspector.Category.LIST) {
				 throw new UDFArgumentTypeException(0,
				            "The argument of function should be primitive" + ", but \""
				            + arguments[0].getTypeName() + "\" is found");
				}else {
					ListObjectInspector loi = (ListObjectInspector)arguments[0];
					if (loi.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
						throw new UDFArgumentTypeException(0,
					            "The argument of function should be primitive" + ", but \""
					            + arguments[0].getTypeName() + "\" is found");
					}else{
            elementOI[0] = loi.getListElementObjectInspector();
            isPrimitive[0] = false;
						returnObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(
								PrimitiveObjectInspectorFactory.writableIntObjectInspector);
					}

				}

	        
	      }else {
          elementOI[0] = arguments[0];
          isPrimitive[0] = true;
        }
		
	    return returnObjectInspector;
	}

}
