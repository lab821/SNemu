#!/usr/bin/env python3

class coflow:
    def __init__(self):
        self.rtime = 0
        self.dst = 0
        self.fdict = {}

if __name__ == "__main__":
    i = float(input('app Input size:'))
    if i:
        inp = int(i)
    else:
        inp = 247
    p = float(input('app parallelism:'))
    if p:
        pi = int(p)
    else:
        pi = 12
    x = i / p
    x2 = x * x

    time0_fb = 2148.84 + 450.36*x + 1.37*x2
    time1_fb = 58.93 + 276.65*x + 2.52*x2
    time2_fb = 51.21 + 265.85*x + 3.42*x2
    time3_fb = 17.44 + 190.46*x + 2.06*x2
    time4_fb = 13.90 + 183.40*x + 1.94*x2
    time5_fb = 64.09 + 41.92*x + 0.044*x2
    fbt = [time0_fb,time1_fb,time2_fb,time3_fb,time4_fb,time5_fb]

    time0_lb = 10.1 + 415.30 * x + 1.72 * x2
    time1_lb = 7.31 + 203.11 * x + 2.55 * x2
    time2_lb = 6.28 + 280.46 * x + 4.88 * x2
    time3_lb = 5.67 + 262.06 * x + 2.58 * x2
    time4_lb = 7.07 + 275.02 * x + 2.30 * x2
    time5_lb = 7.12 + 31.17 * x + 0.04 * x2
    lbt = [time0_lb,time1_lb,time2_lb,time3_lb,time4_lb,time5_lb]

    shu0_n = 0.0
    shu1_n = 0.2998 * x
    shu2_n = 0.2845 * x
    shu3_n = 0.1207 * x
    shu4_n = 0.1776 * x
    shu5_n = 0.1792 * x
    sn = [shu0_n,shu1_n,shu2_n,shu3_n,shu4_n,shu5_n]

    shu2_a = 0.5695 * x
    shu3_a = 0.3998 * x
    shu4_a = 0.4495 * x
    sa=[0,0,shu2_a,shu3_a,shu4_a,0]

    p2_a = 0.0001518 * i - 0.1157
    if 0.0 > p2_a:
        p2_a = 0.0
    n2_n = int(p * (1-p2_a))
    p3_a = 0.0005578 * i - 0.3586
    if 0.0 > p3_a:
        p3_a = 0.0
    n3_n = int(p * (1-p3_a))
    p4_a = 0.0004593 * i - 0.2240
    if 0.0 > p4_a:
        p4_a = 0.0
    n4_n = int(p * (1-p4_a))
    nn=[pi, pi, n2_n, n3_n, n4_n, pi]


    nh = 3
    hl = [i for i in range(1,nh+1)]
    cph = 4
    nc = cph*nh
    ctl = [0] * nc

    cfs = []
    for stage in range(6):
        for task in range(pi):
            cf = coflow()
            if task < nc:
                tasktime = fbt[stage]
            else:
                tasktime = lbt[stage]
            if task < nn[stage]:
                flow = sn[stage]
            else:
                flow = sa[stage]
            redu_c = ctl.index(min(ctl))
            redu_h = (redu_c // cph) + 1
            if flow != 0:
                cf.rtime = ctl[redu_c]
                map_h = hl.copy()
                map_h.remove(redu_h)
                cf.dst = redu_h
                for h in map_h:
                    cf.fdict[h] = flow
                cfs.append(cf)
            ctl[redu_c] += int(tasktime)
        sb = max(ctl)
        for i in range(len(ctl)):
            ctl[i] = sb

    with open('tasktrace-pr-gen-'+str(inp)+'-'+str(pi)+'.txt', 'w') as f:
        f.write(str(nh) + ' ' + str(len(cfs)) + '\n')
        index = 1
        for cf in cfs:
            line = ' '.join([str(index), str(cf.rtime), str(2)])
            for src in cf.fdict.keys():
                line = line + ' ' + str(src)
            line = ' '.join([line, str(1), str(cf.dst)])
            for src in cf.fdict.keys():
                line = line + ' ' + str(round((cf.fdict[src]), 2))
            f.write(line + '\n')
            index += 1


