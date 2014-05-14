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

import nl.surfsara.warcutils.WarcInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.jwat.warc.WarcRecord;

/**
 * Pig load function for regular (or compressed) warc, wat and wet files. Values
 * from the reader are returned as WarcRecords from the Java Web Archive
 * Toolkit.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 * @author Jeroen Schot <jeroen.schot@surfsara.nl>
 */
public class WarcFileLoader extends GenericWarcFileLoader {

	@Override
	public InputFormat<LongWritable, WarcRecord> getInputFormat() throws IOException {
		return new WarcInputFormat();
	}

}
