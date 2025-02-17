package pl.malyszko.jerzy;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
	private static final String BASE_ADDRESS = "https://commons.wikimedia.org/w/api.php?action=query&list=categorymembers&format=json&cmlimit=500&cmtype="
			+ $_CM_TYPE + "&cmtitle=" + $_CATEGORY_ID;
	private static final String CMCONTINUE = "cmcontinue";

	static ForkJoinPool fjp;
	static ReadWriteLock logLock = new ReentrantReadWriteLock();
	static ReadWriteLock contentLock = new ReentrantReadWriteLock();
	static List<String> logBuffer = Collections.synchronizedList(new LinkedList<>());
	static List<String> contentBuffer = Collections.synchronizedList(new LinkedList<>());

	static String categoryReceived;

	static void log(String content) throws IOException {
		logLock.readLock().lock();
		logBuffer.add(content);
		logLock.readLock().unlock();
	}

	static void write(String content) throws IOException {
		contentLock.readLock().lock();
		contentBuffer.add(content);
		contentLock.readLock().unlock();
	}

	public static void main(String... args) throws ExecutionException, IOException, KeyManagementException,
			NoSuchAlgorithmException, InterruptedException {
		String rootCategory = "Paintings";
		int parallelism = Runtime.getRuntime().availableProcessors();
		int timeout = 30;
		TimeUnit tu = TimeUnit.HOURS;
		if (args != null && args.length > 0) {
			rootCategory = args[0];
			categoryReceived = rootCategory.replace(" ", "_");
			if (args.length > 1) {
				parallelism = Integer.parseInt(args[1]);
				if (args.length > 2) {
					timeout = Integer.parseInt(args[2]);
					if (args.length > 3) {
						tu = TimeUnit.valueOf(args[3]);
					}
				}
			}

		}

		fjp = new ForkJoinPool(parallelism);
		HttpsPreparator.prepare();

		ForkJoinTask<Void> forkJoinTask = fjp.submit(new CategoryRecursiveAction(rootCategory, new LinkedList<>()))
				.fork();
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new WriterFlushRecursiveAction(), 60, 60,
				TimeUnit.SECONDS);

		forkJoinTask.get();

		fjp.awaitTermination(timeout, tu);

	}

	static class WriterFlushRecursiveAction implements Runnable {

		static File logFile = new File(categoryReceived + ".log");
		static File contentFile = new File(categoryReceived + ".output");

		@Override
		public void run() {
			try {
				boolean empty = true;
				System.out.println("flushing...");
				try {
					contentLock.writeLock().lock();
					try (OutputStreamWriter lw = new OutputStreamWriter(new FileOutputStream(contentFile, true),
							Charset.forName("UTF-8"))) {
						for (String line : contentBuffer) {
							empty = false;
							lw.write(line);
						}
						contentBuffer.clear();
					}

				} finally {
					contentLock.writeLock().unlock();
				}
				try {
					logLock.writeLock().lock();
					try (OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(logFile, true),
							Charset.forName("UTF-8"))) {
						for (String line : logBuffer) {
							empty = false;
							fw.write(line);
						}
						logBuffer.clear();
					}

				} finally {
					logLock.writeLock().unlock();
				}
				if(empty) {
					System.out.println("Empty!!!");
					throw new InterruptedException("Empty!!!");
				}
				System.out.println("flushed!");
			} catch (InterruptedException | IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	static class CategoryRecursiveAction extends RecursiveAction {

		static Set<String> categoryCache = Collections.synchronizedSet(new HashSet<>());

		private final String category;

		private Deque<String> categoryTree;

		public CategoryRecursiveAction(String category, Deque<String> categoryTree) {
			super();
			this.category = category;
			Deque<String> cats = new LinkedList<>(categoryTree);
			cats.addFirst(category.replace("Category:", ""));
			this.categoryTree = cats;
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
						String content = arr.getString("title")
								+ this.categoryTree.stream().reduce("@", (s1, s2) -> s1 + '@' + s2) + "\n";
						write(content);
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
						ForkJoinTask<Void> submit = fjp.submit(new CategoryRecursiveAction(subc, categoryTree));
						submit.fork();
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
			if (!category.startsWith("Category:")) {
				category = "Category:" + category;
			}
			String address = BASE_ADDRESS;
			address = address.replace($_CATEGORY_ID, category);
			address = address.replace($_CM_TYPE, type);
			if (cmContinueToken != null) {
				address = address + "&cmcontinue=" + cmContinueToken;
			}

			URL url = new URL(address);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			log(address.substring(address.lastIndexOf("&cmtype=")) + "\n");
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


}
