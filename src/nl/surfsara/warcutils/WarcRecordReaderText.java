/**
 * Copyright 2014 SURFsara
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.surfsara.warcutils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.jwat.common.Payload;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * Hadoop RecordReader for regular (or compressed) warc wat and wet files. Used
 * by the WarcInputFormat. Values are provides as WarcRecords from the Java Web
 * Archive Toolkit.
 *
 * @author mathijs.kattenberg@surfsara.nl
 */
public class WarcRecordReaderText extends RecordReader<LongWritable, Text> {
    private DataInputStream in;
    private long start;
    private long pos;
    private long end;
    private Seekable filePosition;

    private CompressionCodecFactory compressionCodecs = null;
    private CompressionCodec codec;
    private Decompressor decompressor;

    private LongWritable key = null;
    private Text value = null;
    private WarcReader warcReader;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = context.getConfiguration();
        final Path file = split.getPath();

        start = split.getStart();
        end = start + split.getLength();
        compressionCodecs = new CompressionCodecFactory(conf);
        codec = compressionCodecs.getCodec(file);

        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(split.getPath());

        if (isCompressedInput()) {
            in = new DataInputStream(codec.createInputStream(fileIn, decompressor));
            filePosition = fileIn;
        } else {
            fileIn.seek(start);
            in = fileIn;
            filePosition = fileIn;
        }

        warcReader = WarcReaderFactory.getReaderUncompressed(in);

        warcReader.setWarcTargetUriProfile(WarcIOConstants.URIPROFILE);
        warcReader.setBlockDigestEnabled(WarcIOConstants.BLOCKDIGESTENABLED);
        warcReader.setPayloadDigestEnabled(WarcIOConstants.PAYLOADDIGESTENABLED);
        warcReader.setRecordHeaderMaxSize(WarcIOConstants.HEADERMAXSIZE);
        warcReader.setPayloadHeaderMaxSize(WarcIOConstants.PAYLOADHEADERMAXSIZE);

        this.pos = start;
    }

    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = new LongWritable();
        }
        pos = filePosition.getPos();
        key.set(pos);

//		value = warcReader.getNextRecord();
        WarcRecord wr = warcReader.getNextRecord();

        value = getTextFromWarcRecord(wr);

        if (value == null) {
            return false;
        }
        return true;
    }

    private Text getTextFromWarcRecord(WarcRecord wr) throws IOException {
        String result = "";
        if ("text/plain".equals(wr.header.contentTypeStr)) {
            // Get the text payload
            Payload payload = wr.getPayload();
            if (payload == null) {
                // NOP
            } else {
                String warcContent = IOUtils.toString(payload.getInputStreamComplete());

                if (warcContent == null) {
                    // NOP
                } else {
                    result = warcContent;
                    // Classify text
                }
            }
        }
        return new Text(result);
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
        }
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            if (in != null) {
                in.close();
            }
        } finally {
            if (decompressor != null) {
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }

    private boolean isCompressedInput() {
        return (codec != null);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (isCompressedInput() && null != filePosition) {
            retVal = filePosition.getPos();
        } else {
            retVal = pos;
        }
        return retVal;
    }
}
