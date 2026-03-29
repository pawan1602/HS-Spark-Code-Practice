def maxProfit( prices) -> int:
    buy = prices[0]
    diff = 0

    for i in range(1, len(prices)):

        if prices[i] < buy:
            buy = prices[i]
        #  pass

        if (prices[i] - buy) > diff:
            diff = prices[i] - buy
            # sell = prices[i]

    return diff


prices = [7,1,5,3,6,4]
# [7,6,4,3,1]

print(maxProfit(prices))