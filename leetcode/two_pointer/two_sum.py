

# Method 1 broot force
nums = [3,2,4]
target = 6
list_len = len(nums)


for i in range(list_len - 1):
    j = i + 1
    while j <= (list_len - 1):

        if nums[i] + nums[j] == target:
            print( [i, j])
            break
        else:
            j += 1


# Method 2 more optimized complexity o(n)
def twoSum( nums, target) :
    num_map = {}  # Hash table to store number and its index
    print(enumerate(nums))
    for i, num in enumerate(nums):
        # print("i ==", i , "num ==", num)
        complement = target - num  # Find the complement
        if complement in num_map:
            return [num_map[complement], i]  # Return indices of complement and current number
        num_map[num] = i  # Store the number with its index

print(twoSum(nums, target))


## enumerate is simply doing below code the main logic here is to use dict and usig target to find the difference
# for i in range(len(nums)):
#     print(i, nums[i])