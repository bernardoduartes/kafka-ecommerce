import br.com.kafka.CurrelationId;
import br.com.kafka.LocalDatabase;
import br.com.kafka.producer.KafkaProducer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


public class NewOrderServlet extends HttpServlet {

    private final KafkaProducer<Order> orderProducer = new KafkaProducer<>();

    private final LocalDatabase database;

    NewOrderServlet() throws SQLException {
        this.database = new LocalDatabase("orders_database");
        this.database.createIfNotExists("CREATE TABLE IF NOT EXISTS Orders (" +
                "uuid varchar(200) primary key)");
    }


    @Override
    public void destroy() {
        super.destroy();
        orderProducer.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {

            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));
            var orderId = req.getParameter("uuid"); // UUID.randomUUID().toString();
            var order = new Order(orderId, amount, email);
            var database = new OrderDatabase();

            if(database.saveNew(order)){
                orderProducer.send(
                        "ECOMMERCE_NEW_ORDER",
                        email,
                        new CurrelationId(NewOrderServlet.class.getSimpleName()),
                        order
                );
               System.out.println("New order sent successfully.");
               resp.setStatus(HttpServletResponse.SC_OK);
               resp.getWriter().println("New order sent");
            } else {
                System.out.println("Old order received.");
                resp.setStatus(HttpServletResponse.SC_OK);
                resp.getWriter().println("Old order received.");
            }
        } catch (ExecutionException | InterruptedException | SQLException  e) {
            throw new ServletException(e);
        }
    }
}
