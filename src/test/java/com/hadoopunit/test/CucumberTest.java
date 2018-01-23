package com.hadoopunit.test;

import com.hadoopunit.test.spark.TestSparkSession;
import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.AfterClass;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features={
        "src/test/resources/features/dimension.feature",
        "src/test/resources/features/fact.feature"})
public class CucumberTest {

    @AfterClass
    public static void stop() {
        TestSparkSession.getInstance().getSparkSession().close();
    }


}
