def collatz_score(start, end):
    checksum = 0
    longest = 0

    for n in range(start, end + 1):
        x = n
        steps = 0

        while x != 1:
            if x % 2 == 0:
                x = x // 2
            else:
                x = 3 * x + 1

            steps += 1

        checksum += steps

        if steps > longest:
            longest = steps

    return checksum + longest


a = collatz_score(1, 300_000)
b = collatz_score(300_001, 600_000)
c = collatz_score(600_001, 900_000)

total = a + b + c
