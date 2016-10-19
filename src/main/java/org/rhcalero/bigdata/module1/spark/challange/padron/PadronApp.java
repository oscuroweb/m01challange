package org.rhcalero.bigdata.module1.spark.challange.padron;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rhcalero.bigdata.module1.spark.challange.padron.model.Padron;

import scala.Tuple2;

/**
 * 
 * PadronApp:
 * <p>
 * Spark program to obtain data from Malaga's 2014 padron
 * </p>
 * 
 * @author Hidalgo Calero, R.
 * @since Oct 19, 2016
 */
public class PadronApp {

    /** Log instance. */
    private static Logger log = Logger.getLogger(PadronApp.class.getName());

    private static SparkConf conf = null;

    private static JavaSparkContext context = null;

    /**
     * 
     * Method main.
     * <p>
     * Execute PadronApp program.
     * </p>
     * 
     * @param args Input arguments
     */
    public static void main(String[] args) {

        // Validate arguments list. File name or directory is required
        if (args.length < 1) {
            log.fatal("[ERROR] RuntimeException: There must be at least one argument (a file name or directory)");
            throw new RuntimeException();
        }

        // Create a SparkConf object
        conf = new SparkConf();

        // Create a Java Spark Context
        context = new JavaSparkContext(conf);

        // Get Padron RDD
        JavaRDD<Padron> padronRDD = getPadronMap(args[0]).cache();

        // Obtain the total population for each district.
        getPopulationByDistrict(padronRDD);

        // STEP 6: Stop the spark context
        context.stop();
        context.close();
    }

    /**
     * 
     * Method getPopulationByDistrict.
     * <p>
     * Obtain the total population for each district.
     * </p>
     * 
     * @param padronRDD Padron info.
     */
    private static void getPopulationByDistrict(JavaRDD<Padron> padronRDD) {

        // Create map where key is the district and value is 1
        JavaPairRDD<Integer, Integer> districtPair = padronRDD.mapToPair(padron -> new Tuple2<Integer, Integer>(padron
                .getDistrictCod(), 1));

        // Reduce padron by population by district
        JavaPairRDD<Integer, Integer> populationByDistrict = districtPair.reduceByKey((value1, value2) -> value1
                + value2);

        // Order by population
        JavaPairRDD<Integer, Integer> orderByPopulation = populationByDistrict.mapToPair(
                districtPopulationTuple -> new Tuple2<Integer, Integer>(districtPopulationTuple._2(),
                        districtPopulationTuple._1())).sortByKey(false);

        // Get the list of result
        List<Tuple2<Integer, Integer>> listOfResult = orderByPopulation.collect();

        // Print the result
        for (Tuple2<Integer, Integer> result : listOfResult) {
            StringBuffer resultStr = new StringBuffer();
            resultStr.append("District: ").append(result._2()).append(" | Population: ").append(result._1());
            System.out.println(resultStr.toString());
        }
        System.out.println("Number of district: " + listOfResult.size());
    }

    /**
     * 
     * Method getPadronMap.
     * <p>
     * Get a Map of padron info from a file
     * </p>
     * 
     * @param filePath Path of file
     * @return PAaron RDD
     */
    private static JavaRDD<Padron> getPadronMap(String filePath) {
        // Get lines from file
        JavaRDD<String> lines = context.textFile(filePath);

        // Remove empty lines
        JavaRDD<String> filteredLines = lines.filter(line -> !line.isEmpty());

        // Remove quote character
        JavaRDD<String> noQuoteLines = filteredLines.map(line -> line.replace("\"", StringUtils.EMPTY));

        // Get Padron map
        JavaRDD<Padron> padronRDD = noQuoteLines.map(line -> new Padron(line));

        // Remove empty records
        JavaRDD<Padron> filteredPadron = padronRDD.filter(padron -> padron.getNhop() != null).cache();

        return filteredPadron;

    }
}
