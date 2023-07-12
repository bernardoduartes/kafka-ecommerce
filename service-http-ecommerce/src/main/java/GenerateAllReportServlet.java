import br.com.kafka.KafkaProducer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.ExecutionException;


public class GenerateAllReportServlet extends HttpServlet {

    private final KafkaProducer<String> bachDispatcher = new KafkaProducer<>();

    @Override
    public void destroy() {
        super.destroy();
        bachDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {

            bachDispatcher.send("SEND_MASSAGE_TO_ALL_USERS", "USER_GENERATE_READING_REPORT" ,"USER_GENERATE_READING_REPORT");

            System.out.println("Sent generate report to all users.");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Report request generated");

        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
