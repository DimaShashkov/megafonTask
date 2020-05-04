import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.jcraft.jsch.*;
import org.json.simple.JSONObject;

import java.io.*;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

/*
* Метод downloadFiles() создает подключение к sftp серверу и скачивает файлы,
*
* метод convert() конвертирует полученыые файлы из формата .cvs в .json (и немного меняет знаки,
* для возможности конвертирования с помощью библиотеки Jackson),
*
* метод addFileInfo() добавляет необходимую информацию о скаченном файле в .json файл.
*
*/

public class Main {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws IOException {
        downloadFiles();
    }

    public static void downloadFiles(){
        Session session = null;
        Channel channel = null;
        ChannelSftp channelSftp = null;


        try {
            File workDir = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath());
            FileInputStream propsFile = new FileInputStream(workDir.getParent() + "\\config.properties");

            Properties properties = new Properties();

            properties.load(propsFile);

            String login = properties.getProperty("login").trim();
            String password = properties.getProperty("password").trim();
            String host = properties.getProperty("host").trim();
            String port = properties.getProperty("port").trim();

            File downloadDir = new File(workDir.getParent() + "\\download");

            downloadDir.mkdir();

            JSch jsch = new JSch();

            session = jsch.getSession(login, host, Integer.parseInt(port));
            session.setPassword(password);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            System.out.println("Connecting to the sftp...");
            session.connect();
            System.out.println("Connected to the sftp.");


            channel = session.openChannel("sftp");
            channel.connect();

            channelSftp = (ChannelSftp) channel;

            Vector<ChannelSftp.LsEntry> list = channelSftp.ls("*.csv");

            String src = channelSftp.getHome();

            if (list.isEmpty()) {
                System.out.println("No file exist in the specified sftp folder location.");
            } else {
                for (ChannelSftp.LsEntry entry : list) {
                    try {
                        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                        String downloadedTime = sdf.format(timestamp);
                        String srcName = entry.getFilename();

                        channelSftp.get(srcName, new StringBuilder(downloadDir.getAbsolutePath())
                                .append(File.separator).append(srcName).toString());

                        System.out.println("File " + srcName + " was downloaded");

                        addFileInfo(convert(srcName), downloadedTime, src, srcName);

                    } catch (SftpException sftpException) {
                        System.out.println("Failed to download the file the sftp folder location.");
                    }
                }
            }
        } catch (Exception exception) {
            System.out.println("Failed to download the file(s) from SFTP.");
        } finally {
            if (channelSftp != null) {
                channelSftp.exit();
                System.out.println("Disconnected to sftp");
            }
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }

    }
    public static List<Object> convert(String srcName) throws IOException, URISyntaxException {

        File workDir = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        File input = new File(workDir.getParent() + "\\download\\" + srcName);

        try {
            BufferedReader file = new BufferedReader(new FileReader(input));
            StringBuffer inputBuffer = new StringBuffer();
            String line;

            while ((line = file.readLine()) != null) {
                inputBuffer.append(line.replaceAll("\"", "").replaceAll(";", ","));
                inputBuffer.append("\r\n");
            }
            file.close();
            String inputStr = inputBuffer.toString();

            FileOutputStream fileOut = new FileOutputStream(input);
            fileOut.write(inputStr.getBytes());
            fileOut.close();
        } catch (Exception e) {
            System.out.println("Problem reading file.");
        }

        CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build();
        CsvMapper csvMapper = new CsvMapper();

        csvMapper.enable(CsvParser.Feature.IGNORE_TRAILING_UNMAPPABLE);

        List<Object> readAll = csvMapper.readerFor(Map.class).with(csvSchema).readValues(input).readAll();

        System.out.println("File " + srcName + " was converted.");

        return readAll;
    }

    public static void addFileInfo(List<Object> list, String downloadedTime, String src, String srcName) throws IOException, URISyntaxException {
        File workDir = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        File result = new File(workDir.getParent() + "\\result");

        result.mkdir();

        Timestamp parsedTime = new Timestamp(System.currentTimeMillis());

        JSONObject resultobject = new JSONObject();
        JSONObject fileobject = new JSONObject();

        fileobject.put("downloadedTime", downloadedTime);
        fileobject.put("src", src);
        fileobject.put("srcName", srcName);
        fileobject.put("parsedTime",sdf.format(parsedTime));

        resultobject.put("file", fileobject);
        resultobject.put("csvData", list);

        try{
            File f = new File(result.getAbsolutePath() + "\\" + srcName.substring(0, srcName.lastIndexOf(".")) + ".json");
            FileWriter file = new FileWriter(f);
            file.write(resultobject.toJSONString());
            file.flush();
            file.close();
            System.out.println("File " + f.getName() + " was written");
        } catch (IOException ex){
            System.out.println("File is not written");
        }
    }

}
