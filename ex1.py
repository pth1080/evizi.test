import timeit
from datetime import datetime


def run(tickets: list, p: int):
    n = tickets[p]
    rs = 0
    print(f"position{p}; n: {n}")
    for i in range(0, len(tickets)):
        if i <= p:
            if tickets[i] >= n:
                rs += n
            else:
                rs += tickets[i]
        else:
            if tickets[i] >= n:
                rs += (n - 1)
            else:
                rs += tickets[i]
        print(i, tickets[i], rs)
    print(rs)


def restock(itemCount, target):
    product_in = 0
    i = 0
    s = timeit.timeit()
    while product_in < target and i < len(itemCount):
        product_in += itemCount[i]
        i += 1

    # for i in range(0, len(itemCount)):
    #     product_in += itemCount[i]
    #     if product_in > target:
    #         break
    e = timeit.timeit()
    print("rs:  ", (e - s))
    return abs(product_in - target)


def runa(arr: list):
    t = arr[0]
    x = max(arr)

    for i in range(1, len(arr)):
        print(arr[i])
        if arr[i] < t and t > arr[i]:
            return i


if __name__ == '__main__':
    runa([2, 6, 3, 4, ])
    print(restock([1, 2, 3, 2, 1], 4))
