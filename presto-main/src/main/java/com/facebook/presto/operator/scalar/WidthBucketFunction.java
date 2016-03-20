package com.facebook.presto.operator.scalar;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.String.format;

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

@Description("Get the bin number of a value given a bin specfication")
@ScalarFunction("width_bucket")
public final class WidthBucketFunction
{
	@SqlType(StandardTypes.BIGINT)
	public static long widthBucketScalar(@SqlType(StandardTypes.DOUBLE) double operand, @SqlType(StandardTypes.DOUBLE) double bound1, @SqlType(StandardTypes.DOUBLE) double bound2, @SqlType(StandardTypes.BIGINT) long bucketCount)
	{
        checkCondition(bucketCount > 0, INVALID_FUNCTION_ARGUMENT, "Number of buckets must be greater than 0");
        checkCondition(!Double.isNaN(operand), INVALID_FUNCTION_ARGUMENT, "Operand must not be NaN");
        checkCondition(Double.isFinite(bound1), INVALID_FUNCTION_ARGUMENT, "First bound must be finite");
        checkCondition(Double.isFinite(bound2), INVALID_FUNCTION_ARGUMENT, "Second bound must be finite");
        checkCondition(bound1 != bound2, INVALID_FUNCTION_ARGUMENT, "Bounds cannot equal each other");

        long result = 0;

        double lower = Math.min(bound1, bound2);
        double upper = Math.max(bound1, bound2);

        if (operand < lower) {
            result = 0;
        }
        else if (operand >= upper) {
            try {
                result = Math.addExact(bucketCount, 1);
            }
            catch (ArithmeticException e) {
                throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("Bucket for value %s is out of range", operand));
            }
        }
        else {
            result = (long) ((double) bucketCount * (operand - lower) / (upper - lower) + 1);
        }

        if (bound1 > bound2) {
            result = (bucketCount - result) + 1;
        }

        return result;
	}
	
	@SqlType(StandardTypes.BIGINT)
	public static long widthBucketArray(@SqlType(StandardTypes.DOUBLE) double operand, @SqlType("array(double)") Block bins)
	{
		checkCondition(!Double.isNaN(operand), INVALID_FUNCTION_ARGUMENT, "Operand cannot be NaN");

		int nbins = bins.getPositionCount();

		double first = bins.getDouble(0, 0);
		if (operand < first) {
			return 0;
		}

		double last = bins.getDouble(nbins - 1, 0);
		if (operand >= last) {
			return nbins + 1;
		}

		int lower = 0;
		int upper = nbins;
		
		int index;
		double bin;

		while (lower < upper) {
			index = (lower + upper) / 2;
			bin = bins.getDouble(index, 0);
			
			checkCondition(Double.isFinite(bin), INVALID_FUNCTION_ARGUMENT, format("Bin %.3g is not finite", bin));

			if (operand < bin) {
				upper = index;
			}
			else {
				lower = index + 1;
			}
		}
		return lower;
	}
}
