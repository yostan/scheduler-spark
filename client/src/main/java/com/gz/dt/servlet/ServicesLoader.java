package com.gz.dt.servlet;

import com.gz.dt.service.Services;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Created by naonao on 2015/10/27.
 */
public class ServicesLoader implements ServletContextListener {

    private static Services services;
    private static boolean sslEnabled = false;

    public void contextInitialized(ServletContextEvent event) {
        try {
            String ssl = event.getServletContext().getInitParameter("ssl.enabled");
            if (ssl != null) {
                sslEnabled = true;
            }

            services = new Services();
            services.init();
        }
        catch (Throwable ex) {
            System.out.println();
            System.out.println("ERROR: ServicesLoader could not be started");
            System.out.println();
            System.out.println("REASON: " + ex.toString());
            System.out.println();
            System.out.println("Stacktrace:");
            System.out.println("-----------------------------------------------------------------");
            ex.printStackTrace(System.out);
            System.out.println("-----------------------------------------------------------------");
            System.out.println();
            System.exit(1);
        }
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {

        services.destroy();

    }

    public static boolean isSSLEnabled() {
        return sslEnabled;
    }
}
