package com.sonamine.hive.udf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Convert two aggregated columns into a key-value map.
 * 
 * The value may be a primitive or a complex type (structs, maps, arrays).
 * 
 * @see https://cwiki.apache.org/Hive/genericudafcasestudy.html
 */
@Description(name = "to_array", value = "_FUNC_(value) - Convert one aggregated columns into a value array")
public class UDAFToArray extends AbstractGenericUDAFResolver {

	static final Log LOG = LogFactory.getLog(UDAFToArray.class.getName());

	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) {

		/*if (parameters.length != 1) {
			throw new UDFArgumentTypeException(parameters.length - 1,
					"Exactly one argument is expected.");
		}*/

		/*if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(0,
					"Only primitive type arguments are accepted for the key but "
							+ parameters[0].getTypeName()
							+ " was passed as parameter 1.");
		}*/

		return new UDAFToListEvaluator();
	}

	public static class UDAFToListEvaluator extends GenericUDAFEvaluator {

		protected ObjectInspector inputValueOI;
		protected StandardListObjectInspector loi;

		protected StandardListObjectInspector internalMergeOI;

		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1) {
                inputValueOI = (ObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardListObjectInspector(
                		(ObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputValueOI));
            } else {
                if (!(parameters[0] instanceof StandardListObjectInspector)) {
                    inputValueOI = (ObjectInspector) parameters[0];
                    return ObjectInspectorFactory.getStandardListObjectInspector(
                    		(ObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputValueOI));
                } else {
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputValueOI = (ObjectInspector) internalMergeOI.getListElementObjectInspector();
                    loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);                 
                    return loi;
                }
            }
        }

		static class MkListAggregationBuffer implements AggregationBuffer {
			List<Object> container;
		}

		public void reset(AggregationBuffer agg) throws HiveException {
			((MkListAggregationBuffer) agg).container = new ArrayList<Object>(144);
		}

		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			MkListAggregationBuffer ret = new MkListAggregationBuffer();
			reset(ret);
			return ret;
		}

		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			assert (parameters.length == 1);
			Object value = parameters[0];

			MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
			putIntoList(value, myagg);
		}

		public Object terminatePartial(AggregationBuffer agg)
				throws HiveException {
			MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
			return myagg.container;
		}

		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
			List<Object> partialResult = (List<Object>) internalMergeOI.getList(partial);
			for (Object value: partialResult) {
				putIntoList(value, myagg);
			}
		}

		public Object terminate(AggregationBuffer agg) throws HiveException {
			MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
			return myagg.container;
		}

		protected void putIntoList(Object value, MkListAggregationBuffer myagg) {
			Object pValueCopy = ObjectInspectorUtils.copyToStandardObject(value, this.inputValueOI);
			myagg.container.add(pValueCopy);
		}
	}

}
