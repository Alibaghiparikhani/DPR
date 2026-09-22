from dag_runtime import task


@task
def count_primes(start, end):
    count = 0

    for n in range(max(2, start), end + 1):
        is_prime = True
        d = 2

        while d * d <= n:
            if n % d == 0:
                is_prime = False
                break
            d += 1

        if is_prime:
            count += 1

    return count


a = count_primes(1, 5_000_000)
b = count_primes(5_000_001, 10_000_000)
c = count_primes(10_000_001, 15_000_000)

total = a + b + c
