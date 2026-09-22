def load_users():
    return [1, 2, 3, 4]

def load_orders():
    return [10, 20, 30]

def clean_users(users):
    return [x * 2 for x in users]

def clean_orders(orders):
    return [x + 1 for x in orders]

def calculate_statistics():
    return 42

def combine(users, orders):
    return sum(users) + sum(orders)

def create_report(combined, statistics):
    return combined + statistics


users = load_users()
orders = load_orders()
statistics = calculate_statistics()

cleaned_users = clean_users(users)
cleaned_orders = clean_orders(orders)

combined = combine(cleaned_users, cleaned_orders)
report = create_report(combined, statistics)

print(report)
