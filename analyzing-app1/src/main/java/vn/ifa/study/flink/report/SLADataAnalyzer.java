/*
 * Class: ReportSLA
 *
 * Created on May 24, 2022
 *
 * (c) Copyright Swiss Post Solutions Ltd, unpublished work
 * All use, disclosure, and/or reproduction of this material is prohibited
 * unless authorized in writing.  All Rights Reserved.
 * Rights in this program belong to:
 * Swiss Post Solution.
 * Floor 4-5-8, ICT Tower, Quang Trung Software City
 */
package vn.ifa.study.flink.report;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.sps.cdipp.AbstractTableAnalyzer;
import vn.sps.cdipp.factory.SourceFactory;

public class SLADataAnalyzer extends AbstractTableAnalyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SLADataAnalyzer.class);

    private static final long serialVersionUID = 4919141782930956120L;

    public SLADataAnalyzer(final String[] args) throws IOException {
        super(args);
    }

    @Override
    protected String sourceTableStatement() {
        return "CREATE TABLE kafkaTable (`case` ROW<caseId STRING>, " 
                + "managementData ROW< status String, stepsMetadata ARRAY< ROW<operator STRING, endTime STRING> > >, "
                + "processingData ARRAY< ROW< captureQualityControlData ARRAY< ROW<fieldName STRING, typistData STRING, qcData STRING, errorType STRING> > > >, "
                + "ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp', " 
                + "WATERMARK FOR ts AS ts)";
    }

    @Override
    protected RowTypeInfo rowTypeInfo() {

        final TypeInformation<?>[] typeInformations = new TypeInformation[3];
        typeInformations[0] = BasicTypeInfo.STRING_TYPE_INFO;
        typeInformations[1] = BasicTypeInfo.STRING_TYPE_INFO;
        typeInformations[2] = BasicTypeInfo.STRING_TYPE_INFO;


        final String[] fieldNames = new String[3];
        fieldNames[0] = "caseId";
        fieldNames[1] = "fieldName";
        fieldNames[2] = "typistData";
        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations, fieldNames);

        return rowTypeInfo;
    }

    @Override
    protected Table analyzeData() {

        final Properties properties = this.getConfiguration("userInfo");

        final Schema userInfoSchema = Schema.newBuilder()
                                            .column("f0", "STRING NOT NULL")
                                            .column("f1", "STRING NOT NULL")
                                            .column("f2", "STRING NOT NULL")
                                            .primaryKey("f0")
                                            .build();
        SourceFactory.createTableFromS3DataStream("userInfoTable", userInfoSchema, properties,
                getStreamExecutionEnvironment(), getStreamTableEnvironment());
        
        Table flattenQCTable = getStreamTableEnvironment().sqlQuery(
                "SELECT `case`.caseId, fieldName, typistData, qcData, errorType FROM kafkaTable CROSS JOIN UNNEST(processingData[1].captureQualityControlData) AS captureQualityControlData (fieldName, typistData, qcData, errorType) WHERE managementData.status = 'QUALITY_CONTROL_DONE'");
        getStreamTableEnvironment().createTemporaryView("flattenQCTable", flattenQCTable);
        
        Table flattenCapturingTable = getStreamTableEnvironment().sqlQuery(
                "SELECT `case`.caseId, managementData.stepsMetadata[2].operator as operator FROM kafkaTable WHERE managementData.status = 'CAPTURE_DONE'");
        getStreamTableEnvironment().createTemporaryView("flattenCapturingTable", flattenCapturingTable);

        Table table = getStreamTableEnvironment().sqlQuery(
                "SELECT cap.caseId, qc.fieldName, qc.typistData, qc.qcData, qc.errorType, operator as username FROM flattenQCTable qc, flattenCapturingTable cap");

        final Table userInfoTable = getStreamTableEnvironment().from("userInfoTable");

        final Table resultTable = table.join(userInfoTable)
                                      .where(Expressions.$("username")
                                                        .isEqual(Expressions.$("f1")))
                                      .select(Expressions.$("caseId"), Expressions.$("fieldName"),
                                              Expressions.$("username"));
        return resultTable;
    }
}