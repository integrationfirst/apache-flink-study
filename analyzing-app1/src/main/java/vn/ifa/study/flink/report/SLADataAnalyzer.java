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
        return "CREATE TABLE kafkaTable (eventId STRING, "
                + "username STRING, "
                + "managementData ROW< stepsMetadata ARRAY< ROW<startTime STRING, endTime STRING> > >, "
                + "ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp', " + "WATERMARK FOR ts AS ts)";
    }

    @Override
    protected RowTypeInfo rowTypeInfo() {

        final TypeInformation<?>[] typeInformations = new TypeInformation[3];
        typeInformations[0] = BasicTypeInfo.STRING_TYPE_INFO;
        typeInformations[1] = BasicTypeInfo.STRING_TYPE_INFO;
        typeInformations[2] = BasicTypeInfo.STRING_TYPE_INFO;

        final String[] fieldNames = new String[3];
        fieldNames[0] = "startTime";
        fieldNames[1] = "endTime";
        fieldNames[2] = "name";

        RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations, fieldNames);

        return rowTypeInfo;
    }

    @Override
    protected Table analyzeData() {
        Table sourceTable = getStreamTableEnvironment().from("kafkaTable");

        final Properties properties = this.getConfiguration("userInfo");

        final Schema userInfoSchema = Schema.newBuilder()
                                            .column("f0", "STRING NOT NULL")
                                            .column("f1", "STRING NOT NULL")
                                            .column("f2", "STRING NOT NULL")
                                            .primaryKey("f0")
                                            .build();
        SourceFactory.createTableFromS3DataStream("userInfoTable", userInfoSchema, properties,
                getStreamExecutionEnvironment(), getStreamTableEnvironment());

        Table userInfo = getStreamTableEnvironment().from("userInfoTable");

        Table result = sourceTable.join(userInfo)
                                  .where(Expressions.$("username")
                                                    .isEqual(Expressions.$("f1")))
                                  .select(Expressions.$("managementData")
                                                     .get("stepsMetadata")
                                                     .at(1)
                                                     .get("startTime"),
                                          Expressions.$("managementData")
                                                     .get("stepsMetadata")
                                                     .at(1)
                                                     .get("endTime"),
                                          Expressions.$("f2"));
        return result;
    }
}