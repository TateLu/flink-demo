/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package demo.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	//public static void main(String[] args) throws Exception {
	//	// Sets up the execution environment, which is the main entry point
	//	// to building Flink applications.
	//	final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	//
	//	/*
	//	 * Here, you can start creating your execution plan for Flink.
	//	 *
	//	 * Start with getting some data from the environment, like
	//	 * 	env.fromSequence(1, 10);
	//	 *
	//	 * then, transform the resulting DataStream<Long> using operations
	//	 * like
	//	 * 	.filter()
	//	 * 	.flatMap()
	//	 * 	.window()
	//	 * 	.process()
	//	 *
	//	 * and many more.
	//	 * Have a look at the programming guide:
	//	 *
	//	 * https://nightlies.apache.org/flink/flink-docs-stable/
	//	 *
	//	 */
	//
	//	// Execute program, beginning computation.
	//	env.execute("Flink Java API Skeleton");
	//}

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		//通过字符串构建数据集
		DataSet<String> text = env.fromElements(
				"风急天高猿啸哀，渚清沙白鸟飞回。" +
						"无边落木萧萧下，不尽长江滚滚来。" +
						"万里悲秋常作客，百年多病独登台。" +
						"艰难苦恨繁霜鬓，潦倒新停浊酒杯。");
		// 分割字符串、按照key进行分组、统计相同的key个数
		DataSet<Tuple2<String, Integer>> wordCounts = text
				.flatMap(new LineSplitter())
				.groupBy(0)
				.sum(1);
		// 打印
		wordCounts.print();
	}

	// 分割字符串的方法
	public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		@Override
		public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
			for (String word : line.split("")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}

}
