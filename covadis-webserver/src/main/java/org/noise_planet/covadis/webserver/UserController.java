/**
 * NoiseModelling is a library capable of producing noise maps. It can be freely used either for research and education, as well as by experts in a professional use.
 * <p>
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 * <p>
 * Official webpage : http://noise-planet.org/noisemodelling.html
 * Contact: contact@noise-planet.org
 */


package org.noise_planet.covadis.webserver;

import com.atlassian.onetime.core.TOTP;
import com.atlassian.onetime.core.TOTPGenerator;
import com.atlassian.onetime.model.EmailAddress;
import com.atlassian.onetime.model.Issuer;
import com.atlassian.onetime.model.TOTPSecret;
import com.atlassian.onetime.service.*;
import com.google.zxing.BarcodeFormat;
import com.google.zxing.WriterException;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;
import io.javalin.http.Context;
import io.javalin.http.InternalServerErrorResponse;
import io.javalin.http.util.NaiveRateLimit;
import org.apache.commons.io.FileUtils;
import org.noise_planet.covadis.webserver.database.DatabaseManagement;
import org.noise_planet.covadis.webserver.secure.JWTProvider;
import org.noise_planet.covadis.webserver.secure.JavalinJWT;
import org.noise_planet.covadis.webserver.secure.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.sql.DataSource;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    private final Configuration configuration;

    public UserController(DataSource serverDataSource, JWTProvider<User> provider, Configuration configuration) {
        this.serverDataSource = serverDataSource;
        this.provider = provider;
        this.configuration = configuration;
        TOTPGenerator totpGenerator = new TOTPGenerator();
        TOTPConfiguration totpConfiguration = new TOTPConfiguration();
        totpService = new DefaultTOTPService(totpGenerator, totpConfiguration
        );
    }

    public void login(Context ctx ) {
        List<String> messages = readMessagesArg(ctx);
        ctx.render("login.html", Map.of("messages", messages));
    }

    private static String getBaseURL(Context context) {
        return Optional.ofNullable((String) context.attribute("baseUrl")).orElse("");
    }

    public void doLogin(Context ctx ) {
        // brute force protection
        NaiveRateLimit.requestPerTimeUnit(ctx, 5, TimeUnit.MINUTES);
        String totpCode = Optional.ofNullable(ctx.formParam("TOTP_CODE")).orElse("");
        String email = Optional.ofNullable(ctx.formParam("EMAIL")).orElse("");

        try(Connection connection = serverDataSource.getConnection()) {
            String totpSecret = DatabaseManagement.getTotpSecretByUserEmail(connection, email);
            if(totpSecret.isEmpty()) {
                ctx.attribute("messages", "Invalid email or TOTP code");
                login(ctx);
            } else {
                TOTPVerificationResult result = totpService.verify(new TOTP(totpCode),
                        TOTPSecret.Companion.fromBase32EncodedString(totpSecret));
                if (result.isSuccess()) {
                    // Fetch user
                    int userId = DatabaseManagement.getUserIdByUserEmail(connection, email);
                    User user = DatabaseManagement.getUser(connection, userId);
                    String token = provider.generateToken(user);
                    // Register the JWT token in cookie
                    JavalinJWT.addTokenToCookie(ctx, token);
                    logger.info("User {} has logged into the system", email);
                    // redirect the user to the page
                    ctx.render("blank", Map.of(
                            "redirectUrl", getBaseURL(ctx),
                            "message", "Login successful, you will be redirected to the application"));
                } else {
                    ctx.attribute("messages", "Invalid email or TOTP code");
                    logger.info("Visitor with email {} failed to login {}", email, ctx.attribute("messages"));
                    login(ctx);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalServerErrorResponse();
        }
    }


    /**
     * Post method of register page
     * @param ctx Context
     */
    public void doRegister(Context ctx ) {
        String totpCode = ctx.formParam("TOTP_CODE");
        String totpSecret = ctx.formParam("TOTP_SECRET");
        String userToken = ctx.formParam("TOKEN");

        try(Connection connection = serverDataSource.getConnection()) {
            int userIdentifier = DatabaseManagement.getUserByRegisterToken(connection, userToken);
            if (totpCode != null && totpSecret != null && userToken != null && userIdentifier >= 0) {
                User user = DatabaseManagement.getUser(connection, userIdentifier);
                TOTPVerificationResult result = totpService.verify(new TOTP(totpCode),
                        TOTPSecret.Companion.fromBase32EncodedString(totpSecret));
                if(result.isSuccess()) {
                    DatabaseManagement.updateUserTotpToken(connection, user.getIdentifier(), totpSecret);
                    String message = "Account successfully created ! You will be directed to the login page to enter your credentials";
                    // redirect the user to the page
                    ctx.render("blank", Map.of(
                            "redirectUrl", getBaseURL(ctx) + "/login",
                            "message", message));
                } else {
                    ctx.attribute("messages", "Invalid TOTP code");
                    logger.info("User {} failed to register {}", user.getEmail(), ctx.attribute("messages"));
                    register(ctx);
                }
            } else {
                ctx.attribute("messages", "Invalid register page url, ask your administrator for a new link.");
                logger.info("User with token {} failed to register {}", userToken, ctx.attribute("messages"));
                register(ctx);
            }
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalServerErrorResponse();
        }
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

    private List<String> readMessagesArg(Context ctx) {
        String message = ctx.attributeMap().getOrDefault("messages", "").toString();
        if(message.isEmpty()) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(message);
        }
    }

    /**
     * Display register page
     * @param ctx Context
     */
    public void register(Context ctx) {
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
                        new EmailAddress(user.getEmail()),
                        new Issuer("NoiseModelling"));
                String qrCodeBytes = createQRCodeImage(totpUri);
                ctx.render("register.html", Map.of(
                        "messages", readMessagesArg(ctx),
                        "token", token,
                        "totpUri", totpUri,
                        "totpSecret", formatTotpSecret(totpSecret.getBase32Encoded()),
                        "qrCodeBytes", qrCodeBytes));
            }  else {
                ctx.render("login.html", Map.of(
                        "messages", List.of("Register/Reset token is no longer valid, ask your administrator for a new link.")));
            }
        } catch (SQLException | IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalServerErrorResponse();
        }
    }

    /**
     * Render user list HTML page
     * @param ctx web context
     */
    public void users(Context ctx) {
        try(Connection connection = serverDataSource.getConnection()) {
            List<Map<String, Object>> table = new ArrayList<>();
            List<User> users = DatabaseManagement.getUsers(connection);
            for(User user : users) {
                Map<String, Object> row = new HashMap<>();
                row.put("id", user.getIdentifier());
                row.put("email", user.getEmail());
                long size = 0;
                File databaseFile = new File(configuration.workingDirectory,
                        OwsController.getDatabaseName(user.getIdentifier()) + ".mv.db");
                if(databaseFile.exists()) {
                    size = databaseFile.length();
                }
                row.put("dbSize", FileUtils.byteCountToDisplaySize(size));
                row.put("registerUrl", user.getRegisterUrl("http", "localhost", configuration.getPort(),
                        configuration.getApplicationRootUrl()));
                row.put("groups", user.getRoles().stream().map(Enum::name).collect(Collectors.joining(", ")));
                table.add(row);
            }
            ctx.render("users", Map.of("users", table));
        } catch (SQLException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalServerErrorResponse();
        }
    }
}