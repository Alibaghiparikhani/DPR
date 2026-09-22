"""Two proved computational branches with a mutation confined to one list."""


def clean(values):
    return [x * 2 for x in values]


def adjust(values):
    return [x + 1 for x in values if x > 0]


users = [1, 2, 3]
orders = [10, 20, 30]
users_alias = users
cleaned_users = clean(users)
cleaned_orders = adjust(orders)
users.append(4)
latest_user = users_alias[-1]
result = sum(cleaned_users) + sum(cleaned_orders) + latest_user
