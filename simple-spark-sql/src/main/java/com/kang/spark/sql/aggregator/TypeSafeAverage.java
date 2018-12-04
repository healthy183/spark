package com.kang.spark.sql.aggregator;

import com.kang.spark.sql.dto.Average;
import com.kang.spark.sql.dto.Employee;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TypedColumn;
import org.apache.spark.sql.expressions.Aggregator;

/**
 * User:
 * Description:
 * Date: 2018-09-23
 * Time: 18:22
 */
public class TypeSafeAverage extends Aggregator<Employee, Average, Double> {

    @Override
    public Average zero() {
        return new Average(0L, 0L);
    }

    @Override
    public Average reduce(Average buffer, Employee employee) {
        long newSum = buffer.getSum() + employee.getSalary();
        long newCount = buffer.getCount() + 1;
        buffer.setSum(newSum);
        buffer.setCount(newCount);
        return buffer;
    }

    @Override
    public Average merge(Average b1, Average b2) {
        long mergedSum = b1.getSum() + b2.getSum();
        long mergedCount = b1.getCount() + b2.getCount();
        b1.setSum(mergedSum);
        b1.setCount(mergedCount);
        return b1;
    }

    @Override
    public Double finish(Average reduction) {
        return ((double) reduction.getSum()) / reduction.getCount();
    }

    // Specifies the Encoder for the intermediate value type
    @Override
    public Encoder<Average> bufferEncoder() {
        return Encoders.bean(Average.class);
    }
    // Specifies the Encoder for the final output value type
    @Override
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("jsonDemo")
                .config("spark.some.config.option", "some-value")
                .master("spark://192.168.59.130:7077")
                .config("spark.driver.host","192.168.59.3")
                .getOrCreate();

        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = "hdfs://192.168.59.130:9000/jsonDemoDir/employees.json";
        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
        ds.show();


        /*
        ClassCastException: cannot assign instance of scala.collection.immutable.List$SerializationProxy to
        field org.apache.spark.sql.execution.aggregate.ComplexTypedAggregateExpression.bufferSerializer of
        type scala.collection.Seq in instance of org.apache.spark.sql.execution.aggregate.ComplexTypedAggregateExpression
	    at java.io.ObjectStreamClass$FieldReflector.setObjFieldValues
        * */
        TypeSafeAverage myAverage = new TypeSafeAverage();
        // Convert the function to a `TypedColumn` and give it a name
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        result.show();


    }
}
