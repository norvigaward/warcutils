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
package nl.surfsara.warcutils.pig;

import java.io.IOException;

import nl.surfsara.warcutils.WarcSequenceFileInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.jwat.warc.WarcRecord;

/**
 * Pig load function for warc, wat and wet files that have been converted to
 * sequencefiles. Values from the reader are returned as WarcRecords from the
 * Java Web Archive Toolkit.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class WarcSequenceFileLoader extends LoadFunc {

	private RecordReader<LongWritable, WarcRecord> in;
	private TupleFactory mTupleFactory = TupleFactory.getInstance();

	@Override
	public InputFormat<LongWritable, WarcRecord> getInputFormat() throws IOException {
		return new WarcSequenceFileInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		WarcRecord warcRecord = null;
		try {
			if (in.nextKeyValue()) {
				warcRecord = in.getCurrentValue();

				/*
				 * You can expand this loader by returning values from
				 * the warcRecord as needed.  
				 */
				Tuple t = mTupleFactory.newTuple(1);
				t.set(0, warcRecord.header.contentTypeStr);

				return t;
			} else {
				return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void prepareToRead(RecordReader reader, PigSplit arg1) throws IOException {
		in = reader;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}
}
