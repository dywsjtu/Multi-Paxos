import os, sys

marks = []
latencies = []

with open("./out.txt", "r") as f:
    t = []
    for l in f.readlines():
        l = l.strip()
        # print(l)
        if l.split()[-1][0] == "+":
            marks.append(l)
            latencies.append(t)
            # print(t)
            # print(latencies)
            t = []
        elif l.split()[2][0] == "D":
            t.append(int(l.split()[-1]))

for i in range(1, len(marks)):
    print(marks[i])
    print(len(latencies[i])/10.0, (sum(latencies[i])+0.0)/len(latencies[i])/10.0)