#!/usr/bin/python3
# 用于自动运行chaosblade

import os
import argparse
import logging

def create_cpu():
    res = os.popen('/home/hgl/chaosblade-1.7.0/blade create cpu load').readlines()
    logging.warning(res)

def create_mem():
    res = os.popen('/home/hgl/chaosblade-1.7.0/blade create mem load percent=90').readlines()
    logging.warning(res)

def get_blade_uid():
    res = [
        _.strip().split('=')[-1] for _ in
        os.popen('ps -ef | grep "/home/hgl/chaosblade-1.7.0/bin/chaos_os"').readlines()
        if 'grep' not in _
    ]
    return res

def destroy_all():
    uids = get_blade_uid()
    for uid in uids:
        res = os.popen(f'blade destroy {uid}').readlines()
        logging.warning(res)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', type=str, choices=['cpu', 'mem', 'all', 'clear'], default='clear')
    args = parser.parse_args()

    mode = args.m
    if mode == 'cpu':
        create_cpu()
    elif mode == 'mem':
        create_mem()
    elif mode == 'all':
        create_mem()
        create_cpu()
    elif mode == 'clear':
        destroy_all()
