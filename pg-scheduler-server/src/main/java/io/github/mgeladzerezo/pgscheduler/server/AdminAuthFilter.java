package io.github.mgeladzerezo.pgscheduler.server;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * HTTP Basic authentication for the dashboard, its API and the metrics endpoint; only the health probe is
 * open. A single admin account is enough for the demo and keeps the dependency list free of Spring Security.
 * The browser caches the credentials per origin, so the static pages, {@code fetch} and the
 * {@code EventSource} stream all authenticate without any script handling a token.
 */
@Component
public class AdminAuthFilter extends OncePerRequestFilter {

    /**
     * @param username account name
     * @param password account password; the demo default is deliberately obvious and logged as a warning
     * @param enabled  set to false only behind something else that authenticates
     */
    @ConfigurationProperties("dashboard.auth")
    public record Settings(@DefaultValue("admin") String username, @DefaultValue("admin") String password,
                           @DefaultValue("true") boolean enabled) {
    }

    private final byte[] expected;
    private final boolean enabled;

    public AdminAuthFilter(Settings settings) {
        this.enabled = settings.enabled();
        this.expected = Base64.getEncoder()
                .encode((settings.username() + ":" + settings.password()).getBytes(StandardCharsets.UTF_8));
        if (enabled && "admin".equals(settings.password())) {
            logger.warn("The dashboard uses the default password; set DASHBOARD_PASSWORD");
        }
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !enabled || request.getRequestURI().startsWith("/actuator/health");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String header = request.getHeader("Authorization");
        if (header != null && header.regionMatches(true, 0, "Basic ", 0, 6)) {
            byte[] presented = header.substring(6).trim().getBytes(StandardCharsets.UTF_8);
            if (MessageDigest.isEqual(presented, expected)) {
                chain.doFilter(request, response);
                return;
            }
        }
        response.setHeader("WWW-Authenticate", "Basic realm=\"pg-scheduler\", charset=\"UTF-8\"");
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
    }
}
