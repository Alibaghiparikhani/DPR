def integrate(start, end, steps):
    h = (end - start) / steps
    total = 0.0
    for i in range(steps):
        x = start + (i + 0.5) * h
        total += 4.0 / (1.0 + x * x)
    return total * h


a = integrate(0.0, 0.33, 100_000)
b = integrate(0.33, 0.66, 100_000)
c = integrate(0.66, 1.0, 100_000)
total = a + b + c
