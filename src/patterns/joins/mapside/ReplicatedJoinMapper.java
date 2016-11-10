package patterns.joins.mapside;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import patterns.util.MRDPUtils;

public class ReplicatedJoinMapper extends Mapper<Object, Text, Text, Text> {
	
	private static final Text EMPTY_TEXT = new Text("");
	private HashMap<String, String> userIdToInfo = new HashMap<String, String>();
	private Text outvalue = new Text();
	private String joinType = null;

	public void setup(Context context) throws IOException, InterruptedException {
		Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		// Read all files in the DistributedCache
		for (Path p : files) {
			BufferedReader rdr = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(p.toString())))));
			String line = null;
			// For each record in the user file
			while ((line = rdr.readLine()) != null) {
				// Get the user ID for this record
				Map<String, String> parsed = MRDPUtils.transformXmlToMap(line);
				String userId = parsed.get("Id");
				// Map the user ID to the record
				userIdToInfo.put(userId, line);
			}
			rdr.close();
		}
		// Get the join type from the configuration
		joinType = context.getConfiguration().get("join.type");
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
		String userId = parsed.get("UserId");
		String userInformation = userIdToInfo.get(userId);
		// If the user information is not null, then output
		if (userInformation != null) {
			outvalue.set(userInformation);
			context.write(value, outvalue);
		} else if (joinType.equalsIgnoreCase("leftouter")) {
			// If we are doing a left outer join,
			// output the record with an empty value
			context.write(value, EMPTY_TEXT);
		}
	}
}
