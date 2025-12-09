/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */


package org.noise_planet.covadis.webserver.secure;

import com.atlassian.onetime.core.TOTPGenerator;
import com.atlassian.onetime.model.EmailAddress;
import com.atlassian.onetime.model.Issuer;
import com.atlassian.onetime.model.TOTPSecret;
import com.atlassian.onetime.service.*;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.zxing.BarcodeFormat;
import com.google.zxing.WriterException;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;
import io.javalin.http.Context;
import io.javalin.http.InternalServerErrorResponse;
import io.javalin.http.util.NaiveRateLimit;
import org.noise_planet.covadis.webserver.NoiseModellingServer;
import org.noise_planet.covadis.webserver.database.DatabaseManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.sql.DataSource;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Handle users management
 * Adapted from tutorial material from
 * <a href="https://github.com/javalin/javalin-samples/tree/main/javalin6/javalin-auth-example">javalin-auth-example</a>
 * Do not add SQL queries in this class
 */
public class UserController {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final DataSource serverDataSource;
    private final JWTProvider<User> provider;
    private final TOTPService totpService;
    private final TOTPGenerator totpGenerator;
    private final TOTPConfiguration totpConfiguration;


    public UserController(DataSource serverDataSource, JWTProvider<User> provider) {
        this.serverDataSource = serverDataSource;
        this.provider = provider;
        totpGenerator = new TOTPGenerator();
        totpConfiguration = new TOTPConfiguration();
        totpService = new DefaultTOTPService(
                totpGenerator,
                totpConfiguration
        );
    }

    User getUser(int userIdentifier) throws SQLException {
        try(Connection connection = serverDataSource.getConnection()) {
            return DatabaseManagement.getUser(connection, userIdentifier);
        }
    }

    /**
     * Retrieve a connected user using web context
     * @param context Javalin web context
     * @return User or not if not connected
     */
    User getUser(Context context) {
        int userIdentifier = JavalinJWT.getUserIdentifierFromContext(context, provider);
        if(userIdentifier >= 0) {
            try {
                return getUser(userIdentifier);
            } catch (SQLException e) {
                logger.error(e.getLocalizedMessage(), e);
                throw new InternalServerErrorResponse();
            }
        } else {
            return null;
        }
    }

    public void login(Context ctx ) {
        ctx.render("login.html", Map.of("messages", ctx.queryParams("messages")));
    }

    public void doLogin(Context ctx ) {
        // brute force protection
        NaiveRateLimit.requestPerTimeUnit(ctx, 5, TimeUnit.MINUTES);
    }


    /**
     * Post method of register page
     * @param ctx Context
     */
    public void doRegister(Context ctx ) {

    }


    private static String createQRCodeImage(URI totpUri) throws IOException {
        try {
            BitMatrix bitMatrix = new QRCodeWriter().encode(totpUri.toString(), BarcodeFormat.QR_CODE, 200, 200);
            BufferedImage image = MatrixToImageWriter.toBufferedImage(bitMatrix);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(image, "png", baos);
            byte[] bytes = baos.toByteArray();
            return Base64.getEncoder().encodeToString(bytes);
        } catch (WriterException e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Format totp secret using block 4 of chars to be more readable for the user
     * @param totpSecret totpSecret string (multiple of 4
     * @return formated totpSecret
     */
    private static String formatTotpSecret(String totpSecret) {
        StringBuilder formatted = new StringBuilder();
        for (int i = 0; i < totpSecret.length(); i += 4) {
            formatted.append(totpSecret, i, Math.min(i + 4, totpSecret.length()));
            if (i + 4 < totpSecret.length()) { // Don't add space at the end of string
                formatted.append(' ');
            }
        }
        return formatted.toString();
    }

    /**
     * Display register page
     * @param ctx Context
     */
    public void register(Context ctx ) {
        // brute force protection
        NaiveRateLimit.requestPerTimeUnit(ctx, 5, TimeUnit.MINUTES);
        String token = ctx.pathParam("token");
        TOTPSecret totpSecret = RandomSecretProvider.Companion.generateSecret();
        try(Connection connection = serverDataSource.getConnection()) {
            int userIdentifier = DatabaseManagement.getUserByRegisterToken(connection, token);
            if(userIdentifier >= 0) {
                User user = DatabaseManagement.getUser(connection, userIdentifier);
                URI totpUri = totpService.generateTOTPUrl(
                        totpSecret,
                        new EmailAddress(user.email),
                        new Issuer("NoiseModelling"));
                String qrCodeBytes = createQRCodeImage(totpUri);
                ctx.render("register.html", Map.of(
                        "token", token,
                        "totpUri", totpUri,
                        "totpSecret", formatTotpSecret(totpSecret.getBase32Encoded()),
                        "qrCodeBytes", qrCodeBytes));
            }  else {
                ctx.render("login.html", Map.of(
                        "messages", List.of("Register/Reset token is no longer valid")));
            }
        } catch (SQLException | IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalServerErrorResponse();
        }
    }

}