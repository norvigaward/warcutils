/**
 * Copyright 2014 SURFsara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.surfsara.warcutils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcReaderUncompressed;
import org.jwat.warc.WarcRecord;

/**
 * Hadoop RecordReader for warc wat and wet files which have been converted to
 * sequencefiles. The sequencefiles are expected to be in the format
 * <LongWritable, Text>. Where each text value parses as one WarcRecord from the
 * Java Web Archive Toolkit. Used by the WarcSequenceFileInputFormat.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class WarcSequenceFileRecordReader extends RecordReader<LongWritable, WarcRecord> {
	private SequenceFile.Reader in;
	private long start;
	private long end;
	private boolean done = false;

	private LongWritable key = null;
	private Text value = null;
	private WarcRecord derivedValue = null;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = context.getConfiguration();
		final Path path = split.getPath();

		Option optPath = SequenceFile.Reader.file(path);
		in = new SequenceFile.Reader(conf, optPath);

		this.end = split.getStart() + inputSplit.getLength();
		if (split.getStart() > in.getPosition()) {
			in.sync(split.getStart());
		}
		start = in.getPosition();
		done = start >= end;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public WarcRecord getCurrentValue() throws IOException, InterruptedException {
		return derivedValue;
	}

	@Override
	public synchronized boolean nextKeyValue() throws IOException, InterruptedException {
		if (done) {
			return false;
		}
		key = new LongWritable();
		value = new Text();
		boolean next = in.next(key, value);
		if (next) {
			InputStream in = new ByteArrayInputStream(value.getBytes());
			WarcReaderUncompressed readerUncompressed = WarcReaderFactory.getReaderUncompressed(in);
			readerUncompressed.setUriProfile(WarcIOConstants.URIPROFILE);
			readerUncompressed.setBlockDigestEnabled(WarcIOConstants.BLOCKDIGESTENABLED);
			readerUncompressed.setPayloadDigestEnabled(WarcIOConstants.PAYLOADDIGESTENABLED);
			readerUncompressed.setRecordHeaderMaxSize(WarcIOConstants.HEADERMAXSIZE);
			readerUncompressed.setPayloadHeaderMaxSize(WarcIOConstants.PAYLOADHEADERMAXSIZE);
			derivedValue = readerUncompressed.getNextRecord();
		}
		return next;
	}

	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (end == start) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (float) ((in.getPosition() - start) / (double) (end - start)));
		}
	}
}
