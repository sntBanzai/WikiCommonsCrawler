package pl.malyszko.jerzy;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

public class ImgDownloader {

	public static void main(String[] args) throws IOException {
		String sourceFile = args[0];
		String outputFolder = args[1];
		long limit = Long.MAX_VALUE;
		if (args.length > 2) {
			limit = Long.valueOf(args[2]);
		}
		HttpsPreparator.prepare();
		Consumer<String> imgLocationConsumer = new Consumer<String>() {

			@Override
			public void accept(String arg) {
				try {
					URL url = new URL(arg);
					String file = URLDecoder.decode(url.getFile(), "UTF-8");
					file = file.contains("?") ? url.getFile() : file; 
					File neu = new File(outputFolder + "\\" + file.substring(file.lastIndexOf("/") + 1));
					neu.createNewFile();
					try (ReadableByteChannel rbc = Channels.newChannel(url.openStream());
							FileChannel fc = FileChannel.open(neu.toPath(), StandardOpenOption.WRITE)) {
						ByteBuffer byteBuffer = ByteBuffer.allocate(1024 ^ 2);
						int stat = 0;
						while ((stat = rbc.read(byteBuffer)) != -1) {
							byteBuffer.flip();
							fc.write(byteBuffer);
							byteBuffer.compact();
						}
					}
				} catch (Exception e) {
					try {
						System.err.println("Fejl " + URLDecoder.decode(arg, "UTF-8"));
					} catch (UnsupportedEncodingException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

			}
		};
		Files.lines(Paths.get(sourceFile), Charset.forName("UTF-8")).filter(str -> !str.endsWith("@-1")).limit(limit).map(line -> line.split("@")[0]).forEach(imgLocationConsumer);
	}

}
