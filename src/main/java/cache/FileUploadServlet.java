package cache;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

/**
 *
 * @author Manjunath Kustagi
 */
public class FileUploadServlet extends HttpServlet {

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        Logger.getLogger(FileUploadServlet.class.getName()).log(Level.INFO, "Uploading data..");
        doPost(request, response);
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        try {
            Logger.getLogger(FileUploadServlet.class.getName()).log(Level.INFO, "Uploading data..");
            Logger.getLogger(FileUploadServlet.class.getName()).log(Level.INFO, "Saving file to tmpdir: {0}", System.getProperty("temporary.files.directory"));

            // Check that we have a file upload request
            boolean isMultipart = ServletFileUpload.isMultipartContent(request);

            if (isMultipart) {
                // Create a new file upload handler
                ServletFileUpload upload = new ServletFileUpload();

                // Parse the request
                FileItemIterator iter = upload.getItemIterator(request);
                while (iter.hasNext()) {
                    FileItemStream item = iter.next();
                    String name = item.getFieldName();
                    InputStream stream = item.openStream();
                    if (item.isFormField()) {
                    } else {
                        File f = new File(System.getProperty("temporary.files.directory") + File.separator + item.getName());
                        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
                        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
                        String line;
                        while ((line = br.readLine()) != null) {
                            bw.write(line);
                            bw.newLine();
                        }
                        bw.flush();
                        bw.close();
                        br.close();
                    }
                }
            }
            Logger.getLogger(FileUploadServlet.class.getName()).log(Level.INFO, "Saved file to tmpdir");
        } catch (FileUploadException ex) {
            Logger.getLogger(FileUploadServlet.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
