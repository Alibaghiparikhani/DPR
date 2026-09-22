# Small automatic proof example: no decorators or supplied purity hints.

def load():
    return 4

def clean(x):
    return x * 2

def score(x):
    return x + 10

def combine(x, y):
    return x + y

data = load()
left = clean(data)
right = score(data)
result = combine(left, right)
print(result)
