package projekt;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
	private static final String BASE_ADDRESS = "https://commons.wikimedia.org/w/api.php?action=query&list=categorymembers&cmtype=" + $_CM_TYPE
			+ "&cmtitle=Category:"
			+ $_CATEGORY_ID + "&format=json&cmlimit=500";
	private static final String LOCATION_COMMAND_LINK = "https://en.wikipedia.org/w/api.php?action=query&prop=info&pageids=" + $_PAGE_ID + "&inprop=url&format=json";
	private static final String CMCONTINUE="cmcontinue";
	
	public static void main(String... args) throws MalformedURLException, IOException, NoSuchAlgorithmException,
			KeyManagementException, InterruptedException {

		httpsPreparation();

		Set<String> pageIds = new HashSet<>();	
		Map<Integer,String> pageUrls = new HashMap<>();
		
		String cmContinueToken = null;
		do {
			String address = BASE_ADDRESS;
			if(cmContinueToken != null ) {
				address = BASE_ADDRESS + "&cmcontinue=" + cmContinueToken;
			}
			URL url = new URL(address);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");

			Thread.sleep(1000);
			String contentAsString = writeToString(con);
			JSONObject json = new JSONObject(contentAsString);
			JSONArray jsonArray = json.getJSONObject("query").getJSONArray(CATEGORYMEMBERS);
			jsonArray.forEach(arr -> pageIds.add(((JSONObject) arr).getString("title")));
			try {
				JSONObject cont = json.getJSONObject("continue");
				cmContinueToken = cont.getString(CMCONTINUE);
			} catch(JSONException jsone) {
				cmContinueToken = null;
			}
		} while (cmContinueToken != null);
		
		for(String s :pageIds) {
			System.out.println(s);
		}
		
//		for(Integer pid : pageIds) {
//			String address = LOCATION_COMMAND_LINK.replace("${pageID}", String.valueOf(pid));
//			URL url = new URL(address);
//			HttpURLConnection con = (HttpURLConnection) url.openConnection();
//			con.setRequestMethod("GET");
//			String writeToString = writeToString(con);
//			System.out.println(pid+" $$$ "+ writeToString);
//			
//			
//		}
		
		
		System.out.println(pageIds.size());


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
