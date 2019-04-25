package pl.malyszko.jerzy;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.json.JSONObject;

public class ImageLocationAndSizeCrawler {

	private static final String API_QUERY_COMMAND_PART_ONE = "https://en.wikipedia.org/w/api.php?action=query&titles=";
	private static final String API_QUERY_COMMAND_PART_TWO = "&prop=imageinfo&iiprop=url&format=json";

	private static BigInteger filesInKiloBytes = BigInteger.ZERO;

	public static void main(String... args) throws IOException {
		File source = new File(args[0]);
		int linesToSkip = 0;
		if(args.length > 1) {
			linesToSkip = Integer.valueOf(args[1]);
		}
		HttpsPreparator.prepare();
		try (BufferedReader reader = Files.newBufferedReader(source.toPath(), Charset.forName("UTF-8"));
				BufferedWriter writer = Files.newBufferedWriter(new File("output"+System.currentTimeMillis()+".file").toPath(),
						Charset.forName("UTF-8"))) {
			
			String line = null;
			Set<String> paintings = new HashSet<>();
			while ((line = reader.readLine()) != null) {
				if(linesToSkip > 0) {
					linesToSkip--;
					continue;
				}
				if (paintings.size() < 20) {
					paintings.add(line.split("@@")[0]);
					continue;
				}
				Map<String, Object[]> batchResult = dealWithSingleBatch(paintings.toArray(new String[0]));
				long reductionResult = batchResult.values().stream().filter(obj -> obj != null).mapToLong(ind -> (long) ind[1]).reduce(0L,
						(l1, l2) -> l1 + l2);
				filesInKiloBytes = filesInKiloBytes.add(BigInteger.valueOf(reductionResult / 1024L));
				batchResult.entrySet().forEach(val -> {
					try {
						if(val.getValue() == null) {
							writer.write(val.getKey()+ "!!! Z O N K !!!");
							return;
						} 
						writer.write(val.getValue()[0] + "@" + val.getValue()[1] + "\n");
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				});
		
				writer.flush();
				paintings.clear();
			}

		}

		System.out.println(filesInKiloBytes);

		// dealWithSingleBatch("File:Cole Thomas Sunny Morning on the Hudson River
		// 1827.jpg");
	}

	private static Map<String, Object[]> dealWithSingleBatch(String... paintingNames) throws IOException {
		Map<String, Object[]> retVal = new HashMap<>();
		String address = prepareAddress(paintingNames);
		URL url = new URL(address);
		HttpURLConnection con = (HttpURLConnection) url.openConnection();
		con.setRequestMethod("GET");
		String jestemKontent = writeToString(con);
		JSONObject json = new JSONObject(jestemKontent);
		JSONObject pagesObject = json.getJSONObject("query").getJSONObject("pages");
		Iterator<String> keysIterator = pagesObject.keys();
		while (keysIterator.hasNext()) {
			JSONObject keyObject = pagesObject.getJSONObject(keysIterator.next());
			String tit = keyObject.getString("title");
			try {
				String paintingUrl = keyObject.getJSONArray("imageinfo").getJSONObject(0).getString("url");
				URL fileUrl = new URL(paintingUrl);
				Long fileSize = fileUrl.openConnection().getContentLengthLong();
				retVal.put(tit, new Object[] { paintingUrl, fileSize });
			} catch (Exception e) {
				System.err.println("Wyjebka na " + tit);
				retVal.put(tit, null);
			}

		}
		return retVal;
	}

	private static String prepareAddress(String... paintingNames) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder(API_QUERY_COMMAND_PART_ONE);
		for (String name : paintingNames) {
			sb.append(URLEncoder.encode(name, StandardCharsets.UTF_8.toString())).append('|');
		}
		sb.delete(sb.length() - 1, sb.length());
		sb.append(API_QUERY_COMMAND_PART_TWO);
		return sb.toString();
	}

	private static String writeToString(HttpURLConnection con) throws IOException {
		StringWriter sw = new StringWriter();
		try (InputStream is = new BufferedInputStream((InputStream) con.getContent())) {
			int read;
			while ((read = is.read()) != -1) {
				sw.append((char) read);
			}
		}
		return sw.toString();
	}

}
