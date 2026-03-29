def merge(self, intervals) :
    intervals.sort()  # Sort by start time
    merged = []
    prev = intervals[0]

    for i in range(1, len(intervals)):
        if intervals[i][0] <= prev[1]:  # Overlap
            prev[1] = max(prev[1], intervals[i][1])  # Merge
        else:
            merged.append(prev)
            prev = intervals[i]

    merged.append(prev)
    return merged

intervals = [[4,7],[1,4]]
intervals.sort()
for i in range(len(intervals)):
    print(intervals[i])
# print(intervals.sort())
# print(merge(intervals))