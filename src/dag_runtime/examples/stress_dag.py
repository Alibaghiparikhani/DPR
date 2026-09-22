def make_left():
    return [1, 2, 3, 4]


def make_right():
    return [5, 6, 7, 8]


def double(values):
    return [x * 2 for x in values]


def shift(values):
    return [x + 10 for x in values]


def combine(a, b):
    return sum(a) + sum(b)


# ---------------------------------------------------------
# 1. TWO INDEPENDENT BRANCHES
# ---------------------------------------------------------

left = make_left()
right = make_right()

left_clean = double(left)
right_clean = shift(right)

result = combine(left_clean, right_clean)


# ---------------------------------------------------------
# 2. EXACT SUBSCRIPTIONS
# ---------------------------------------------------------

first = left_clean[0]
last = right_clean[-1]

left_score = first + 100
right_score = last + 200

joined_score = left_score + right_score


# ---------------------------------------------------------
# 3. ALIAS + LOCAL OBJECT MUTATION
# ---------------------------------------------------------

alias = left_clean

before_append = sum(alias)

left_clean.append(99)

after_append = sum(alias)


# ---------------------------------------------------------
# 4. MAY-RAISE BUT CALLBACK-FREE OPERATION
#
# This should act as a completion fence.
# It should NOT destroy all namespace/type knowledge.
# ---------------------------------------------------------

divisor = 2
quotient = 100 // divisor

branch_a = quotient + 1
branch_b = quotient + 2

after_division = branch_a + branch_b


# ---------------------------------------------------------
# 5. GENERIC REFLECTION
#
# In the new revision this should NOT swallow the entire
# rest of the module.
#
# It should become a namespace barrier and analysis should
# continue afterwards.
# ---------------------------------------------------------

getattr(left_clean, "append")

1 + 2
3 * 4


# ---------------------------------------------------------
# 6. TRUE NAMESPACE CAPABILITY ESCAPE
#
# THIS SHOULD remain highly conservative.
#
# globals() exposes the live module namespace.
# The remaining suffix should be retained as one native
# tail rather than pretending these assignments are safely
# independent distributed work.
# ---------------------------------------------------------

namespace = globals()

tail_x = 10
tail_y = 20
tail_z = tail_x + tail_y
