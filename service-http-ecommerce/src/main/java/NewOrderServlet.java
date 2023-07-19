import br.com.kafka.CurrelationId;
import br.com.kafka.producer.KafkaProducer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


public class NewOrderServlet extends HttpServlet {

    private final KafkaProducer<Order> orderProducer = new KafkaProducer<>();

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
            var orderId = UUID.randomUUID().toString();
            var order = new Order(orderId, amount, email);

            orderProducer.send(
                    "ECOMMERCE_NEW_ORDER",
                    email,
                    new CurrelationId(NewOrderServlet.class.getSimpleName()),
                    order
            );

           System.out.println("New order sent successfully.");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New order sent");

        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}
