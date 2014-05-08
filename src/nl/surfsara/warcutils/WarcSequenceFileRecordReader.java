// Copyright (c) 2014, Norvig Web Data Science Award
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
		in.close();
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
