package ir.son.ix.dataIngestion;

import ir.huri.jcal.JalaliCalendar;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.udf;

public class ageUDF {
    public UserDefinedFunction calculateAge  = udf(
            (UDF2<Object, Object, Long>) (cicdat, ciborn) -> {

                if (cicdat == null || ciborn == null) {
                    return null;
                }

                String cicdatString = cicdat.toString();
                String cibornString = ciborn.toString();

                if (cicdatString.matches("\\d{8}") && cibornString.matches("\\d{8}")) {

                    JalaliCalendar cicdatJalali = new JalaliCalendar(Integer.valueOf(cicdatString.substring(0, 4)), Integer.valueOf(cicdatString.substring(4, 6)), Integer.valueOf(cicdatString.substring(6, 8)));
                    JalaliCalendar cibornJalali = new JalaliCalendar(Integer.valueOf(cibornString.substring(0, 4)), Integer.valueOf(cibornString.substring(4, 6)), Integer.valueOf(cibornString.substring(6, 8)));

                    return (cicdatJalali.toGregorian().getTimeInMillis() - cibornJalali.toGregorian().getTimeInMillis()) / (1000 * 60 * 60 * 24);
                } else {
                    return null;
                }
            },
            DataTypes.LongType
    );
}



