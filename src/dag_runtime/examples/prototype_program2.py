def load_users():
    return [10, 20, 30]

def load_orders():
    return [100, 200, 300]

def load_products():
    return [5, 10, 15]

def load_exchange_rate():
    return 1.2


def clean_users(users):
    return [x + 1 for x in users]

def clean_orders(orders):
    return [x * 2 for x in orders]

def index_products(products):
    return sum(products)


def enrich_orders(orders, product_index):
    return [x + product_index for x in orders]


def calculate_user_score(users):
    return sum(users)

def calculate_sales(orders, exchange_rate):
    return sum(orders) * exchange_rate

def calculate_risk(users, orders):
    return sum(users) + len(orders)


def combine_metrics(user_score, sales, risk):
    return user_score + sales + risk

def create_report(metrics):
    return f"Final report value: {metrics}"


users = load_users()
orders = load_orders()
products = load_products()
exchange_rate = load_exchange_rate()

cleaned_users = clean_users(users)
cleaned_orders = clean_orders(orders)
product_index = index_products(products)

enriched_orders = enrich_orders(cleaned_orders, product_index)

user_score = calculate_user_score(cleaned_users)
sales = calculate_sales(enriched_orders, exchange_rate)
risk = calculate_risk(cleaned_users, enriched_orders)

metrics = combine_metrics(user_score, sales, risk)
report = create_report(metrics)

print(report)
