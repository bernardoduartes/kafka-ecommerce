import br.com.kafka.LocalDatabase;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class OrderDatabase implements Closeable {

    private final LocalDatabase database;

    OrderDatabase() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        this.database.createIfNotExists("CREATE TABLE IF NOT EXISTS Orders (" +
                "uuid varchar(200) primary key)");
    }

    public boolean saveNew(Order order) throws SQLException {
        if (wasProcessed(order.getOrderId())){
            return false;
        }
        database.update("insert into Orders (uuid) values (?)", order.getOrderId());
        return true;
    }

    private boolean wasProcessed(final String orderId) throws SQLException {
        var results = database.query("select uuid from Orders where uuid = ? limit 1", orderId);
        return results.next();
    }

    @Override
    public void close() throws IOException {
        this.database.close();
    }
}
