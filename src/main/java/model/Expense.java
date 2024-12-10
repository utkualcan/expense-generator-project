package model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class Expense {
    private int userId;
    private String dateTime;
    private double count;
    private String description;
    private String expenseType;
    private double payment;

    private static final String[] EXPENSE_TYPES = {
            "Macaroni, food", "Jacket, clothe", "Car, vehicle","Tea, drink",
            "Restaurant, food", "Shoes, clothe", "Fuel, vehicle","Coffee, drink"

    };

    private static final Random random = new Random();

    public Expense(int userId) {
        this.userId = userId;
        this.dateTime = LocalDateTime.now()
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        this.count = random.nextInt(1, 5);

        String selectedType = EXPENSE_TYPES[random.nextInt(EXPENSE_TYPES.length)];
        this.description = selectedType.split(",")[0];
        this.expenseType = selectedType.split(",")[1].trim();

        this.payment = Math.round(
                (10 + random.nextDouble() * 490) * 100.0
        ) / 100.0;
    }

    public int getUserId() { return userId; }
    public String getDateTime() { return dateTime; }
    public double getCount() { return count; }
    public String getDescription() { return description; }
    public String getExpenseType() { return expenseType; }
    public double getPayment() { return payment; }
}
