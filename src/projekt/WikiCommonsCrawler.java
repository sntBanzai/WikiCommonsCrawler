package projekt;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class WikiCommonsCrawler {

	private static final String $_CM_TYPE = "${cmType}";
	private static final String $_PAGE_ID = "${pageID}";
	private static final String $_CATEGORY_ID = "${categoryID}";
	private static final String CATEGORYMEMBERS = "categorymembers";
	private static final String BASE_ADDRESS = "https://commons.wikimedia.org/w/api.php?action=query&list=categorymembers&cmtype="
			+ $_CM_TYPE + "&cmtitle=" + $_CATEGORY_ID + "&format=json&cmlimit=500";
	private static final String LOCATION_COMMAND_LINK = "https://en.wikipedia.org/w/api.php?action=query&prop=info&pageids="
			+ $_PAGE_ID + "&inprop=url&format=json";
	private static final String CMCONTINUE = "cmcontinue";

	static Map<String, Set<String>> outcome = Collections.synchronizedMap(new HashMap<>());
	static FileWriter logWriter;
	//static ForkJoinPool fjp = ForkJoinPool.commonPool();

	static void log(String content) throws IOException {
		synchronized (WikiCommonsCrawler.class) {
			logWriter.write(content);
		}
	}

	public static void main(String... args) throws  ExecutionException, IOException, KeyManagementException, NoSuchAlgorithmException, InterruptedException {

		try (FileWriter lw = new FileWriter(new File("log.output"))) {
			logWriter = lw;

			httpsPreparation();

			ForkJoinTask<Void> forkJoinTask = new CategoryRecursiveAction("Category:Paintings").fork();

			forkJoinTask.get();

			ForkJoinPool.commonPool().awaitTermination(24, TimeUnit.HOURS);
			
		} finally {
			logWriter.flush();
			File output = new File("file.output");
			try (FileWriter fw = new FileWriter(output)) {
				outcome.forEach((key, val) -> {
					try {
						log((key + " " + Arrays.deepToString(val.toArray())));
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				});
				fw.flush();
			}
		}

	}

	static class CategoryRecursiveAction extends RecursiveAction {

		static Set<String> categoryCache = Collections.synchronizedSet(new HashSet<>());

		private final String category;

		public CategoryRecursiveAction(String category) {
			super();
			this.category = category;
		}

		@Override
		protected void compute() {
			if (categoryCache.contains(category))
				return;
			categoryCache.add(category);
			String cmContinueToken = null;
			try {
				do {
					HttpURLConnection con = fireRequest(category, cmContinueToken, "file");

					Thread.sleep(100);
					String contentAsString = writeToString(con);
					JSONObject json = new JSONObject(contentAsString);
					JSONArray jsonArray = json.getJSONObject("query").getJSONArray(CATEGORYMEMBERS);
					Iterator<Object> iterator = jsonArray.iterator();
					while (iterator.hasNext()) {
						JSONObject arr = (JSONObject) iterator.next();
						Set<String> newOne = new HashSet<>();
						Set<String> cats = outcome.putIfAbsent(arr.getString("title"), newOne);
						if (cats == null)
							cats = newOne;
						cats.add(category.replace("Category:", ""));
					}

					try {
						JSONObject cont = json.getJSONObject("continue");
						cmContinueToken = cont.getString(CMCONTINUE);
					} catch (JSONException jsone) {
						cmContinueToken = null;
					}
				} while (cmContinueToken != null);

				cmContinueToken = null;
				do {
					String replacement = "subcat";
					HttpURLConnection con = fireRequest(category, cmContinueToken, replacement);

					Thread.sleep(100);
					String contentAsString = writeToString(con);
					JSONObject json = new JSONObject(contentAsString);
					JSONArray jsonArray = json.getJSONObject("query").getJSONArray(CATEGORYMEMBERS);
					Set<String> subcats = new HashSet<>();
					jsonArray.forEach(arr -> subcats.add(((JSONObject) arr).getString("title")));
					for (String subc : subcats) {
						new CategoryRecursiveAction(subc).fork();
					}

					try {
						JSONObject cont = json.getJSONObject("continue");
						cmContinueToken = cont.getString(CMCONTINUE);
					} catch (JSONException jsone) {
						cmContinueToken = null;
					}
				} while (cmContinueToken != null);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

		}

		private HttpURLConnection fireRequest(String category, String cmContinueToken, String type)
				throws MalformedURLException, IOException, ProtocolException {
			category = category.replace(" ", "_");
			String address = BASE_ADDRESS;
			address = address.replace($_CATEGORY_ID, category);
			address = address.replace($_CM_TYPE, type);
			if (cmContinueToken != null) {
				address = address + "&cmcontinue=" + cmContinueToken;
			}

			URL url = new URL(address);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			logWriter.write(address + "\n");
			return con;
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

	private static void httpsPreparation() throws NoSuchAlgorithmException, KeyManagementException {
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
			public java.security.cert.X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			@Override
			public void checkClientTrusted(java.security.cert.X509Certificate[] arg0, String arg1)
					throws CertificateException {
				// TODO Auto-generated method stub

			}

			@Override
			public void checkServerTrusted(java.security.cert.X509Certificate[] arg0, String arg1)
					throws CertificateException {
				// TODO Auto-generated method stub

			}

		} };

		SSLContext sc = SSLContext.getInstance("SSL");
		sc.init(null, trustAllCerts, new java.security.SecureRandom());
		HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

		// Create all-trusting host name verifier
		HostnameVerifier allHostsValid = new HostnameVerifier() {
			public boolean verify(String hostname, SSLSession session) {
				return true;
			}
		};
		// Install the all-trusting host verifier
		HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
	}

}
