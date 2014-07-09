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
package nl.surfsara.warcutils.pig;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcRecord;

/**
 * Pig load function for regular (or compressed) warc, wat and wet files. Values
 * from the reader are returned as WarcRecords from the Java Web Archive
 * Toolkit.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 * @author Jeroen Schot <jeroen.schot@surfsara.nl>
 */
public abstract class GenericWarcFileLoader extends LoadFunc {

	private RecordReader<LongWritable, WarcRecord> in;
	private TupleFactory mTupleFactory = TupleFactory.getInstance();

	@Override
	public Tuple getNext() throws IOException {
		WarcRecord warcRecord = null;
		try {
			if (in.nextKeyValue()) {
				warcRecord = in.getCurrentValue();
				String url = warcRecord.header.warcTargetUriStr;
				String length = null;
				String type = null;

				HttpHeader httpheader = warcRecord.getHttpHeader();
				if (httpheader != null) {
					length = new Long(httpheader.payloadLength).toString();
					/*
					 * The Content-Type field is often of the form 'text/html; encoding="utf-8"; filename="...";'.
					 * We are only interested in the first part, so we strip and normalize the type field.
					 */
					if (httpheader.contentType != null) {
						Scanner splitter = new Scanner(httpheader.contentType);
						splitter.useDelimiter(";");
						if(splitter.hasNext()) {
							type = splitter.next().toLowerCase();
						}
						splitter.close();
					}
				}

				/*
				 * You can expand this loader by returning values from
				 * the warcRecord as needed.  
				 */
				Tuple t = mTupleFactory.newTuple(3);
				t.set(0, url);
				t.set(1, length);
				t.set(2, type);

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
