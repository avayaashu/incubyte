package org.incubyte;

import org.apache.beam.sdk.options.PipelineOptions;

public interface HospitalETLConfig extends PipelineOptions {
    String getInputFile();
    void setInputFile(String inputFile);

    String getStagingDbUrl();
    void setStagingDbUrl(String stagingDbUrl);

    String getTargetDbUrl();
    void setTargetDbUrl(String targetDbUrl);

    String getDbUsername();
    void setDbUsername(String dbUsername);

    String getDbPassword();
    void setDbPassword(String dbPassword);

}
