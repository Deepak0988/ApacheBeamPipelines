package com.learning.pipeline;

import avro.shaded.com.google.common.collect.Lists;
import com.learning.utils.CustomRowMapper;
import com.learning.utils.TableData;
import com.learning.utils.TableMetaData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MainPipeline {
    private static final Logger logger = LoggerFactory.getLogger(MainPipeline.class.getName());
    public static void main (String[] args){

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        List<String> filePattern = new ArrayList<>();
        List<String> tableName = new ArrayList<>();
        filePattern.add("D:\\accounts.csv");
        tableName.add("accounts");

        TupleTag<KV<String, TableData>> insertRecords = new TupleTag<>(){};
        TupleTag<KV<String, TableData>> updateRecords = new TupleTag<>(){};

        JdbcIO.DataSourceConfiguration dsc = JdbcIO.DataSourceConfiguration
                .create("com.mysql.cj.jdbc.Driver", "jdbc:mysql://localhost:3306/beam")
                .withUsername("root")
                .withPassword("root");

        PCollection<KV<String, Iterable<String>>> tableData = p.apply("Create Table Transform", Create.of(tableName))
                .apply("Tablename Data Mappping", JdbcIO.<String, KV<String, String>>readAll().withDataSourceConfiguration(dsc)
                                .withQuery("SELECT * from accounts ")
                                .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                                .withParameterSetter(new JdbcIO.PreparedStatementSetter<String>() {
                                    private static final long serialVersionUID = 1L;
                                    @Override
                                    public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {

                                    }
                                }).withRowMapper(new CustomRowMapper.readTable()))
                .apply("Grouping By Tablename", GroupByKey.create());
        PCollectionView<Map<String, Iterable<String>>> tableDataView = tableData.apply("Create View asMap ", View.<String, Iterable<String>>asMap());

        PCollectionTuple insertUpdateTuple = p.apply("Create File Transform", Create.of(filePattern))
                .apply("File Match", FileIO.matchAll())
                .apply("File Read", FileIO.readMatches())
                .apply("Filename Data Mapping",ParDo.of(new DoFn<FileIO.ReadableFile, KV<String, TableData>>() {
                            private static final long serialVersionUID = 1L;
                            @ProcessElement
                            public void processElement(ProcessContext context) throws IOException {
                                String fileName = context.element().getMetadata().resourceId().getFilename();
                                logger.info("filename is " +fileName);
                                TableData tableData = new TableData();
                                List<Object> fileData = Lists.newArrayList(
                                        Splitter.on("\r\n").split(context.element().readFullyAsUTF8String()));
                                logger.info("Size of records - " + fileData.size());
                                tableData.setData(fileData);
                                context.output(KV.of(fileName, tableData));
                            }
                        }))
                .apply("Multiple Outputs for Insert And Update", ParDo.of(new DoFn<KV<String, TableData>, KV<String, TableData>>() {
                    private static final long serialVersionUID = 1L;
                    @ProcessElement
                    public void processElement(ProcessContext context) throws IOException {
                        TableData updateTd = new TableData();
                        TableData insertTd = new TableData();
                        List<Object> updateTableData = new ArrayList<>();
                        List<Object> insertTableData = new ArrayList<>();
                        Map<String,Iterable<String>> map = context.sideInput(tableDataView);
                        List<String> tableRecords = Lists.newArrayList(map.get("accounts"));
                        List<String> fileRecords = new ArrayList<>();
                        List<String> updateList = new ArrayList<>();
                        List<String> insertList = new ArrayList<>();
                        context.element().getValue().getData().forEach(d -> {
                            fileRecords.add(d.toString());
                        });
                        fileRecords.forEach(f -> {
                            String primaryKey = f.split(",")[0];
                            if(tableRecords.stream().anyMatch(t -> t.contains(primaryKey)))
                                updateList.add(f);
                            else
                                insertList.add(f);
                        });
                        updateList.removeAll(tableRecords);
                        updateList.forEach(u -> {
                            updateTableData.add(u);
                        });
                        updateTd.setData(updateTableData);
                        insertList.forEach(in -> {
                            insertTableData.add(in);
                        });
                        insertTd.setData(insertTableData);
                        context.output(insertRecords,KV.of(context.element().getKey(),insertTd));
                        context.output(updateRecords,KV.of(context.element().getKey(),updateTd));
                    }
                }).withSideInputs(tableDataView).withOutputTags(insertRecords,TupleTagList.of(updateRecords)));
        insertUpdateTuple.get(insertRecords).apply(ParDo.of(new DoFn<KV<String, TableData>, KV<String, TableData>>() {
            private static final long serialVersionUID = 1L;
            @ProcessElement
            public void processElement(ProcessContext context) throws IOException {
                logger.info("Insert Record:"+context.element().getValue().getData().toString());
                context.output(context.element());
            }
        }));
        insertUpdateTuple.get(updateRecords).apply(ParDo.of(new DoFn<KV<String, TableData>, KV<String, TableData>>() {
            private static final long serialVersionUID = 1L;
            @ProcessElement
            public void processElement(ProcessContext context) throws IOException {
                logger.info("Update Record:"+context.element().getValue().getData().toString());
                context.output(context.element());
            }
        }));

        p.run();

    }
}
