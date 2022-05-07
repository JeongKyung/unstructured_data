import json
import csv
import os
import urllib.request
import random
from time import sleep
import argparse
import pandas as pd
import ast
from threading import Thread
from multiprocessing import Process
import sys


def check_state(zyx, odir, dat):
    f = open(zyx, 'r')
    dict_string = json.load(f)
    id_img = dict()
    for k, v in dict_string.items():
        print(k, len(v))
        id_img[k] = len(v)
    del dict_string

    data = pd.read_csv(dat)
    dir_count = []

    for i in range(len(data)):
        dir = data.loc[i]['Directory']
        id = data.loc[i]['areaID']
        imgs = data.loc[i]['count']
        dir_count.append((str(dir), id, imgs)) #220101 jkwon
    dir_count.sort(key=lambda x: x[1])
    f = open('./' + odir + '/' + 'dir_imgcount.csv', 'w', newline='')
    dd = csv.writer(f)
    dd.writerow(['Directory', 'AreaID', '# Images', 'MATCH'])

    n = 0
    tot = 0
    cur = 0
    finished_area = []
    for k, i, v in dir_count:
        true_c = v
        tot += int(true_c)
        try:
            actual_c = len(os.listdir(odir + '/' + k))
            if true_c != actual_c:
                dd.writerow([k, i, v, 'MISSMATCH'])
            else:
                n += 1
                finished_area.append(i)
                dd.writerow([k, i, v, 'MATCH'])
            cur += actual_c
        except:
            dd.writerow([k, i, v, 'NOT YET STARTED'])
            pass
    print("%d/%d Finished" % (n, len(dir_count)))
    return finished_area


def work2(id, curr_zyxs, base_url, token, odir, row):
    for curr_zyx in curr_zyxs:
        url = base_url + str(curr_zyx[0]) + '/' + str(curr_zyx[1]) + '/' + str(curr_zyx[2])
        filename = './' + odir + '/' + str(row['Directory']) + '/' + str(curr_zyx[1]) + '_' + str(
            curr_zyx[2]) + '.png'
        try:
            if os.path.isfile(filename):
                continue
            # print('tid: %2d' % id, 'try', filename)
            urllib.request.urlretrieve(url, filename)
        except:
            # sleep(0.5)
            # print('tid: %2d' % id, 'sleep and try', filename)
            urllib.request.urlretrieve(url, filename)
            pass


def work(id, data_zyx, dfx, zfill_val, odir, base_url, token, th):
    dirs = []
    n = th
    for index, row in dfx.iterrows():
        areaid = str(row['areaID'])
        if not areaid[0].isalpha():
            areaid = areaid.zfill(zfill_val)

        curr_zyxs = data_zyx[areaid]
        dirs.append(row['Directory'])
        print("areaID:", areaid, "Directory:", row['Directory'], "# img:", len(curr_zyxs))
        if not os.path.isdir('./' + odir + '/' + str(row['Directory'])):
            os.mkdir('./' + odir + '/' + str(row['Directory']))

        len_part = len(curr_zyxs) // n
        loads = []
        for i in range(n-1):
            loads.append(curr_zyxs[i*len_part:(i+1)*len_part])
        loads.append(curr_zyxs[n*len_part-len_part:])
        threads = [Thread(target=work2,
                          args=(i, loads[i], base_url, token, odir, row))
                   for i in range(n)]
        for i in range(n):
            threads[i].start()
        for i in range(n):
            threads[i].join()

        print('DIR: %d finished!' % row['Directory'])
    # end process
    print('PID: %d finished!' % id, str(dirs))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--token', help='input temporal token')
    parser.add_argument('--ozyx', help='input json zyx location file path')
    parser.add_argument('--shufdat', help='input shuffled csv data file path')
    parser.add_argument('--odir', help='output directory name')
    parser.add_argument('--process', help='number of processes')
    parser.add_argument('--thread', help='number of threads')
    parser.add_argument('--wayback', help='wayback timestamp')
    args = parser.parse_args()

    if args.wayback == None or args.ozyx == None or args.shufdat == None or args.token == None or args.odir is None or args.process is None or args.thread is None:
        print("Error! please fill all zyx json, shufdat csv, output directory, token, skip information")
        exit(0)

    token = args.token
    zyx = args.ozyx
    dat = args.shufdat
    odir = args.odir
    n = int(args.process)
    th = int(args.thread)
    way = args.wayback
    base_url = "https://wayback.maptiles.arcgis.com/arcgis/rest/services/World_Imagery/WMTS/1.0.0/default028mm/MapServer/tile/"+way+"/"
    if not os.path.isdir('./' + odir):
        os.mkdir('./' + odir)

    json_file_zyx = open(zyx, 'r')
    zfill_val = 5

    data_zyx = json.load(json_file_zyx)
    data_zyx_keylist = list(data_zyx.keys())
    zfill_len = list(map(lambda x: len(x), data_zyx_keylist))
    if min(zfill_len) == max(zfill_len):
        zfill_val = min(zfill_len)

    dfx = pd.read_csv(dat)
    dfx = dfx.sort_values(by=['areaID'])
    finished_area = check_state(zyx, odir, dat)
    area = dfx.areaID
    area = [i not in finished_area for i in area]
    dfx = dfx[area]

    print(dfx.head())

    len_part = len(dfx) // n

    loads = []
    for i in range(n - 1):
        loads.append(dfx[i * len_part:(i + 1) * len_part])
    loads.append(dfx[n * len_part - len_part:])

    procs = [Process(target=work,
                     args=(i, data_zyx, loads[i], zfill_val, odir, base_url, token, th))
             for i in range(n)]
    for i in range(n):
        procs[i].start()
    for i in range(n):
        procs[i].join()


if __name__ == '__main__':
    main()
