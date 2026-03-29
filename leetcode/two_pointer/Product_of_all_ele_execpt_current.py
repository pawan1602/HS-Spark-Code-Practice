# class Solution:


def productExceptSelf( nums) -> list[int]:
    n = len(nums)
    res = []
    prefix = [1] * (n+1)
    suffix = [1] * (n+1)
    print("prefix:", prefix)
    print("suffix:", suffix)

    print("first for loop starts")
    for i in range(n):
        prefix[i+1] = prefix[i] * nums[i]
        print("prefix:", prefix)

    print("second for loop starts")
    for i in range(n-2, -1, -1):
        suffix[i] = suffix[i+1] * nums[i+1]
        print("suffix:", suffix)

    for i in range(n):
        res.append(prefix[i] * suffix[i])
    return res

nums = [1,2,3,4]

print(productExceptSelf(nums))