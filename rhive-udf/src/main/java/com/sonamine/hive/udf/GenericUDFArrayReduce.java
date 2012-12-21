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

public abstract class GenericUDFArrayReduce extends GenericUDF {
	private static Log LOG = LogFactory
		      .getLog(GenericUDFArrayReduce.class.getName());
	  protected ListObjectInspector listOI;
	  protected PrimitiveObjectInspector elementOI;

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
	if (arguments[0].get()==null)return null;
    List<Object> valueList = new ArrayList<Object>();
    valueList.addAll((listOI).getList(arguments[0].get()));
    Object result = valueList.get(0);
     
    for (Object value : valueList) {
         result = evaluateElement(result, value);
      }
    return elementOI.getPrimitiveWritableObject(result);
    }

	

	protected abstract Object evaluateElement(Object result, Object value) throws UDFArgumentException ;



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
       if (arguments.length != 1) {
	        throw new UDFArgumentTypeException(arguments.length-1,
	            "One arguments are required");
	      }
		
		
		
		
		ObjectInspector returnObjectInspector = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
		if (arguments[0].getCategory() != ObjectInspector.Category.LIST) {
				 throw new UDFArgumentTypeException(0,
				            "The argument of function should be a list of primatives, but \""
				            + arguments[0].getTypeName() + "\" is found");
				}else {
					ListObjectInspector loi = (ListObjectInspector)arguments[0];
					if (loi.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
						throw new UDFArgumentTypeException(0,
					            "The argument of function should be a list of primatives, but \""
					            + arguments[0].getTypeName() + "\" is found");
					}else{
						
					listOI = loi;
					elementOI = (PrimitiveObjectInspector) loi.getListElementObjectInspector();
			        switch (elementOI.getPrimitiveCategory()) {
			          case INT: returnObjectInspector = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
			                                     break;
			          case FLOAT: returnObjectInspector = PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
			                                     break;
			          case DOUBLE: returnObjectInspector = PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
			                                    break;
			          case LONG: returnObjectInspector = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
			                                      break;
			          case SHORT: returnObjectInspector = PrimitiveObjectInspectorFactory.writableShortObjectInspector;
			                                       break;
			          default: throw new UDFArgumentException("Currently only int, float, double, long and short types are supported for this operation");
			        }
						returnObjectInspector = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
								((PrimitiveObjectInspector)loi.getListElementObjectInspector()).getPrimitiveCategory());
					}

				}

	        
	    
	    return returnObjectInspector;
	}

}
