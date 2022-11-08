import scipy
import numpy as np
import matplotlib.pyplot as plt
import csv


workloads = [100,200,500]

xs = []
ys = []
for work in workloads:
    with open(f'data/20221108T000243/measurements/data-{work}MB.csv') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            xs.extend(np.tile((work,float(row[0])),(len(row)-1,1)))
            ys.extend(list(map(float,row[1:])))

def time(workload, executors, c, k, parallelisable):
    """Calculate the time taken for a given workload

    Args:
        workload: file size
        executors: no. of cores
        constant: just a fixed constant
        parallelisable: [0,1] proportion of code that is paralisable
    """
    # uncomment this to use a log workload function instead
    #workload = workload * np.log(workload)
    return k + c * (workload * (1-parallelisable) + workload * parallelisable / executors)

xs = np.array(xs)
ys = np.array(ys)
popt, pcov = scipy.optimize.curve_fit(lambda xdata,c,k,parallelisable:time(xdata[:,0],xdata[:,1],c,k, parallelisable), xs, ys)

print(popt)
perr = np.sqrt(np.diag(pcov))
print('errors')
print(perr)

[c, k, parallelisable] = popt
y_pred = []
for i in range(0, len(xs)):
    y_pred.append(time(xs[i][0], xs[i][1], c, k, parallelisable))

y_pred = np.array(y_pred)

mse = (np.square(ys - y_pred)).mean(axis=0)
print("mean square error: " + str(mse))

work = np.linspace(1, 600, 100)
cores = np.linspace(1,8,50)

X, Y = np.meshgrid(work, cores)

timeVals = time(X,Y,*popt)

ax = plt.axes(projection='3d')
ax.plot_wireframe(X,Y,timeVals,rstride=10,cstride=10,color='lightblue')
ax.view_init(30,150)

ax.scatter3D(xs[:,0],xs[:,1],ys,c=ys,cmap='gist_rainbow')
ax.set_xlabel('file size (MB)')
ax.set_ylabel('executor count')
ax.set_zlabel('time (s)')

plt.savefig('3Dplt.png')