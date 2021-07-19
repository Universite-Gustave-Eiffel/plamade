/**
 * NoiseModelling is an open-source tool designed to produce environmental noise maps on very large urban areas. It can be used as a Java library or be controlled through a user friendly web interface.
 *
 * This version is developed by the DECIDE team from the Lab-STICC (CNRS) and by the Mixt Research Unit in Environmental Acoustics (Université Gustave Eiffel).
 * <http://noise-planet.org/noisemodelling.html>
 *
 * NoiseModelling is distributed under GPL 3 license. You can read a copy of this License in the file LICENCE provided with this software.
 *
 * Contact: contact@noise-planet.org
 *
 */

/**
 * @Author Nicolas Fortin, Université Gustave Eiffel
 */
package org.noise_planet.plamade.api.secure;

import com.google.common.collect.Maps;
import groovy.util.Eval;
import org.h2gis.utilities.JDBCUtilities;
import org.pac4j.core.profile.CommonProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.pac4j.RatpackPac4j;
import ratpack.thymeleaf.Template;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.util.*;

import static ratpack.jackson.Jackson.json;

public class JobListPage implements Handler {
    private static final Logger LOG = LoggerFactory.getLogger(JobListPage.class);

    @Override
    public void handle(Context ctx) throws Exception {
        RatpackPac4j.userProfile(ctx).then(commonProfile -> {
            if (commonProfile.isPresent()) {
                CommonProfile profile = commonProfile.get();
                SecureEndpoint.getUserPk(ctx, profile).then(pkUser -> {
                    if (pkUser == -1) {
                        ctx.render(json(Eval.me("[errorCode : 403, error : 'Forbidden', redirect: '/manage#subscribe']")));
                    } else {
                        Blocking.get(() -> {
                            List<Map<String, Object>> table = new ArrayList<>();
                            try (Connection connection = ctx.get(DataSource.class).getConnection()) {
                                PreparedStatement statement = connection.prepareStatement("SELECT * FROM JOBS" + " " +
                                        "WHERE PK_USER = ? ORDER BY BEGIN_DATE DESC");
                                statement.setInt(1, pkUser);
                                try (ResultSet rs = statement.executeQuery()) {
                                    List<String> fields = JDBCUtilities.getFieldNames(rs.getMetaData());
                                    while (rs.next()) {
                                        Map<String, Object> row = new HashMap<>();
                                        for (int idField = 1; idField <= fields.size(); idField += 1) {
                                            DateFormat mediumDateFormatEN = DateFormat.getDateTimeInstance(
                                                    DateFormat.MEDIUM,
                                                    DateFormat.MEDIUM);
                                            row.put("pk_job", rs.getInt("pk_job"));
                                            Date bDate = rs.getDate("BEGIN_DATE");
                                            row.put("startDate", !rs.wasNull() ? mediumDateFormatEN.format(bDate) : "-");
                                            Date eDate = rs.getDate("END_DATE");
                                            row.put("endDate", !rs.wasNull() ? mediumDateFormatEN.format(eDate) : "-");
                                            row.put("status", rs.getInt("PROGRESSION") + " %");
                                            row.put("inseeDepartment", rs.getString("INSEE_DEPARTMENT"));
                                            row.put("conf_id", rs.getInt("CONF_ID"));
                                        }
                                        table.add(row);
                                    }
                                }
                            }
                            return table;
                        }).then(jobList -> {
                            final Map<String, Object> model = Maps.newHashMap();
                            model.put("jobs", jobList);
                            model.put("profile", profile);
                            ctx.render(Template.thymeleafTemplate(model, "joblist"));
                        });
                    }
                });
            } else {
                ctx.render(json(Collections.singletonMap("Error", "Not authenticated")));
            }
        });
    }
}
