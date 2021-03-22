package com.d2l;

import static java.io.File.createTempFile;
import static java.lang.String.format;
import static java.lang.System.exit;
import static java.lang.System.getProperty;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.client.fluent.Request.Get;
import static org.apache.http.client.fluent.Request.Post;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.fluent.Form;

public class JMB_Test {

    private static final DateTimeFormatter dtFormatter = DateTimeFormatter.ISO_INSTANT;
    private static final Calendar now = Calendar.getInstance();

    private static final JsonParser jsonParser = new JsonParser();

    // Represents the Enrollments and Withdrawals data set
    private static final String dataSetId = "c1bf7603-669f-4bef-8cf4-651b914c4678";

    public static void main(String[] args)
            throws URISyntaxException, IOException, InterruptedException {

        /* Data Hub related properties */
        String hostUrl = getProperty("hostUrl");
        String outputFolder = getProperty("outputFolder");

        /* OAuth 2.0 related properties */
        // Development purposes only; the default value should always suffice
        String tokenEndpoint = getProperty(
                "tokenEndpoint",
                "https://auth.brightspace.com/core/connect/token"
        );

        String clientId = getProperty("clientId");
        String clientSecret = getProperty("clientSecret");

        String refreshTokenFile = getProperty("refreshTokenFile");

        /* Pre-condition checks */
        assertAllArgumentsSpecified(hostUrl, clientId,
                clientSecret, outputFolder, refreshTokenFile);

        /* Retrieve a valid refresh token and use it to obtain a new access token */
        Path refreshTokenPath = Paths.get(refreshTokenFile);
        String oldRefreshToken = Files.readAllLines(refreshTokenPath).get(0);

        String authHeaderValue = getClientAuthHeaderValue(clientId, clientSecret);
    }
}
