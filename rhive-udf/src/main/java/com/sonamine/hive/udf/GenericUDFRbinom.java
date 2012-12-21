package com.sonamine.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math.random.RandomGenerator;
import org.apache.commons.math.random.MersenneTwister;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.BinomialDistribution;
import org.apache.commons.math.distribution.BinomialDistributionImpl;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
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
import org.apache.hadoop.hive.ql.udf.UDFType;

@UDFType(deterministic=false)
public class GenericUDFRbinom extends GenericUDF {
	private static Log LOG = LogFactory
			.getLog(GenericUDFRbinom.class.getName());
	ObjectInspector[] argumentOI;
	boolean providedSamplesArgument = false;
	RandomGenerator random;

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if (random == null){
			random = new MersenneTwister();
		}
		for (int i=0;i<arguments.length;i++){
			if (arguments[i].get()==null)
				return null;
		}
		List<IntWritable> results = new ArrayList<IntWritable>();
		PrimitiveCategory compareCategory = 
				PrimitiveCategory.INT;
		int N = ((IntObjectInspector)argumentOI[0]).get(arguments[0].get());
		double p = ((DoubleObjectInspector)argumentOI[1]).get(arguments[1].get());
		
		int samples = 1;
		if (providedSamplesArgument){
			samples = ((IntObjectInspector)argumentOI[2]).get(arguments[2].get());
		}
		BinomialDistribution dist = new BinomialDistributionImpl(N,p);

		
		for (int i = 0; i < samples; i++){
			IntWritable value = new IntWritable();
			try {
				// Must add one for some goofy reason
				// either their rounding down even though documentation suggests otherwise
				// or their encoding outcomes weirdly
				int val = dist.inverseCumulativeProbability(random.nextDouble())+1;
				if (val<0)val = 0;
				value.set(val);
			} catch (MathException e) {
				value.set(-1);
				e.printStackTrace();
			}
			results.add(value);
		}
		if (providedSamplesArgument)
			return results;

		return results.get(0);
	}

	@Override
	public String getDisplayString(String[] children) {
		StringBuilder sb = new StringBuilder();
		sb.append("rbinom(");
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
		if (arguments.length < 2) {
			throw new UDFArgumentTypeException(arguments.length-1,
					"Two arguments are required");
		}


		if (arguments[0].getCategory() == ObjectInspector.Category.PRIMITIVE) {
			if (((PrimitiveObjectInspector)arguments[0]).getPrimitiveCategory() != PrimitiveCategory.INT) {
				throw new UDFArgumentTypeException(0,
						"The first argument of function should be an int" + ", but \""
								+ arguments[0].getTypeName() + "\" is found");
			}
		}else {
			throw new UDFArgumentTypeException(1,
					"The first argument of function should be an int" + ", but \""
							+ arguments[0].getTypeName() + "\" is found");
		}
		if (arguments[1].getCategory() == ObjectInspector.Category.PRIMITIVE) {
			if (((PrimitiveObjectInspector)arguments[1]).getPrimitiveCategory() != PrimitiveCategory.DOUBLE) {
				throw new UDFArgumentTypeException(1,
						"The second argument of function should be a double" + ", but \""
								+ arguments[1].getTypeName() + "\" is found");
			}
		}else {
			throw new UDFArgumentTypeException(1,
					"The second argument of function should be a double" + ", but \""
							+ arguments[1].getTypeName() + "\" is found");
		}
		
		if (arguments.length>2){
			providedSamplesArgument = true;
			if (arguments[2].getCategory() == ObjectInspector.Category.PRIMITIVE) {
				if (((PrimitiveObjectInspector)arguments[2]).getPrimitiveCategory() != PrimitiveCategory.INT) {
					throw new UDFArgumentTypeException(2,
							"The third argument of function should be an int" + ", but \""
									+ arguments[2].getTypeName() + "\" is found");
				}
			}else {
				throw new UDFArgumentTypeException(2,
						"The third argument of function should be an int" + ", but \""
								+ arguments[2].getTypeName() + "\" is found");
			}
		}
		
		if (arguments.length>3){
			throw new UDFArgumentTypeException(arguments.length-1,
					"More than three arguments is not permitted");
		}


			if (providedSamplesArgument) return ObjectInspectorFactory.getStandardListObjectInspector(
								PrimitiveObjectInspectorFactory.writableIntObjectInspector);
			
			return PrimitiveObjectInspectorFactory.writableIntObjectInspector;		}

	}
