package com.shf.flink.sample.stream.sink.extend.parquet;

import org.apache.avro.Schema;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Description:
 *
 * @author: songhaifeng
 * @date: 2019/8/10 23:22
 */
public class SinkParquetWriter<T> implements Writer<T> {

    private transient ParquetWriter writer = null;
    private String schemaLocation;
    private transient Schema schemaInstance;

    public SinkParquetWriter(String schemaLocation) {
        this.schemaLocation = schemaLocation;
        try {
            this.schemaInstance = new Schema.Parser().parse(new FileInputStream(new File(schemaLocation)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void open(FileSystem fileSystem, Path path) throws IOException {
        writer = AvroParquetWriter.builder(path)
                .withSchema(this.schemaInstance)
                .withConf(new Configuration())
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
    }


    @Override
    public long flush() throws IOException {
        return writer.getDataSize();
    }

    @Override
    public long getPos() throws IOException {
        return writer.getDataSize();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(T t) throws IOException {
        writer.write(t);
    }

    @Override
    public Writer<T> duplicate() {
        return new SinkParquetWriter<>(schemaLocation);
    }
}
