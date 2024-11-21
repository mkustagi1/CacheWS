package cache.dataimportes.util;

import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 *
 * @author Manjunath Kustagi
 */

public class Email {

    public static void sendMessage(String to, String subject, String messageText) {
        String host = "localhost";
        String from = "mkustagi@aws.com";
        String _to = "mkustagi@gmail.com";
        boolean sessionDebug = false;
        Properties props = System.getProperties();
        props.put("mail.host", host);
        props.put("mail.transport.protocol", "smtp");
        Session session = Session.getDefaultInstance(props, null);
        session.setDebug(sessionDebug);

        try {
            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(from));
            if (to == null || to.equals("")) {
                to = _to;
            }
            InternetAddress[] address = {new InternetAddress(to)};
            msg.setRecipients(Message.RecipientType.TO, address);
            msg.setSubject(subject);
            msg.setSentDate(new Date());
            msg.setText(messageText);
            Transport.send(msg);
        } catch (MessagingException ex) {
            Logger.getLogger(Email.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void main(String[] args) {
        String to = "mkustagi@gmail.com";
        String subject = "testing email service";
        String message = "looks like it works great";
        Email.sendMessage(to, subject, message);
    }
}
