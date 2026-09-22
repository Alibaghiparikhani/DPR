def work(n):
    values = []
    for i in range(n):
        values.append(i * i)
    total = 0
    for x in values:
        total += x
    return total


a = work(100_000)
b = work(100_000)
c = work(100_000)
total = a + b + c
