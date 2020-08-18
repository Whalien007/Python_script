#!/usr/bin/env python3.7
# -*- coding:utf-8 -*-

import os
import re
import sys
import time
import glob
import copy
import parsel
import requests
import argparse
import traceback
import subprocess

from requests.api import get


# 创建SRA输出文件夹
def mkdir_or_die(dir_to_make):
    target_dir = os.path.abspath(dir_to_make)
    if not os.path.isdir(target_dir):
        try:
            os.makedirs(target_dir)
        except FileExistsError:  # in case of Race Condition
            print('Check your directory!!!')


# subprocess run cmd命令
def run_or_die(cmd):  # python 3.6+
    time_start = time.time()
    run_cmd = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    return_code = run_cmd.returncode
    if return_code != 0:
        errors = run_cmd.stderr.decode('utf-8')
        raise RuntimeError(f'Running command failed !\n'
                           f'Failed command: {cmd}\n'
                           f'Error description:\n'
                           f'{errors}')
    time_end = time.time()
    print('Running command : {} finished: {}s elapsed.'.format(cmd, round((time_end - time_start), 4)), flush=True)


def clean_and_mkdir(dir_to_make):
    target_dir = os.path.abspath(dir_to_make)
    if len(target_dir) < 15:
        raise RuntimeError(f'Target dir too short(at least longer than /share/nas2/xxx = {len("/share/nas2/xxx")}),'
                           f'stop incase unExcepted error')
    if os.path.isdir(target_dir):
        run_or_die('rm -r {}'.format(target_dir))
    try:
        os.makedirs(target_dir)
    except FileExistsError:  # in case of Race Condition
        pass


def timer(defined_function):  # time decorator
    def wrapper(*args, **kwargs):
        time_start = time.time()
        wrapper_out = defined_function(*args, **kwargs)
        time_end = time.time()
        print('Done !\nRuning script {} finished: {}s elapsed.'.format(sys.argv[0], round((time_end - time_start), 4)))
        return wrapper_out

    return wrapper


def capture_or_die(cmd):
    """
    capture result form shell command
    Raise exception and return failed command if any error occurred
    Note: return raw results, '\n' not striped or processed
    """
    run_cmd = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    return_result = run_cmd.stdout
    return_code = run_cmd.returncode
    if return_code == 0:
        return return_result.decode('utf-8')
    if return_code != 0:
        errors = run_cmd.stderr.decode('utf-8')
        raise RuntimeError(f'Running command failed! \n'
                           f'Failed command: {cmd}!\n'
                           f'Error description: \n'
                           f'{errors}\n')


def download_or_die(cmd):  # python 3.6+
    time_start = time.time()
    run_cmd = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    return_code = run_cmd.returncode
    if return_code != 0:
        errors = run_cmd.stderr.decode('utf-8')
        raise DownLoadingSraFileError(f'Running command failed ! \n'
                                      f'Failed command: {cmd}\n'
                                      f'Error description:\n'
                                      f'{errors}')
    time_end = time.time()
    print('Running command : {} finished: {}s elapsed.'.format(cmd, round((time_end - time_start), 4)), flush=True)


# 自定义下载异常类
class DownLoadingSraFileError(Exception):

    def __init__(self, e='Error while Downloading SRA file!'):
        self.e = e
        super().__init__()

    def __str__(self):
        return self.e

# 定义下载sra接收类
class SraReceiver:

    def __init__(self, sra_name, tmp_dir):
        self.tmp_dir = os.path.abspath(tmp_dir)
        # self.argument_l = argument_l
        self.sra_name = sra_name
        # self.ascp = '/opt/aspera/connect/bin/ascp'
        # self.ascp_key = '/opt/aspera/connect/etc/asperaweb_id_dsa.openssh'
        # self.target_ftp = (f'anonftp@ftp-trace.ncbi.nlm.nih.gov:/sra/sra-instant/reads/ByRun/'
        #                    f'sra/{sra_name[0:3]}/{sra_name[0:6]}/{sra_name}/{sra_name}.sra')

    # 由于NCBI ftp服务器地址变更，因此该方法暂时失效
    # def fasp_method_receiver(self):
    #     clean_and_mkdir(self.tmp_dir)
    #     ascp_cmd = (f'{self.ascp} '
    #                 f'-T '
    #                 f'-k 1 '
    #                 f'-i {self.ascp_key} '
    #                 f'-l {self.argument_l} {self.target_ftp} {self.tmp_dir} ')
    #     print(f'try to download SRA file (ascp) for {self.sra_name}')
    #     for try_time in [x for x in range(1, 11)]:
    #         try:
    #             # print(f'Try to download SRA file {self.sra_name} via ascp in round{try_time}')
    #             download_or_die(ascp_cmd)
    #             return 'Succeed'
    #         except DownLoadingSraFileError:
    #             pass
    #     return 'Failed'

    # 定义静态方法抓取SRA下载链接
    @staticmethod
    def target_sra_https_finder(sra_name):  # get SRA download link from ncbi
        target_url = f'https://trace.ncbi.nlm.nih.gov/Traces/sra/?run={sra_name}'
        header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36 Edg/84.0.522.48'}
        context = requests.get(target_url, headers=header)
        context = parsel.Selector(context.text)
        download_url = context.xpath('//table[@class="geo_zebra run-viewer-download"]/tbody/tr[@class="first"]/td/a/@href').extract()
        sra_links = []
        sra_links.append(download_url[0])
        if len(sra_links) < 1:
            raise Exception(f'Can not find SRA file download link for: {sra_name} ')
        return sra_links

    # 定义静态方法抓取SRA文件大小
    @staticmethod
    def target_sra_size_finder(sra_name):
        target_url = f'https://trace.ncbi.nlm.nih.gov/Traces/sra/?run={sra_name}'
        header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36 Edg/84.0.522.48'}
        context = requests.get(target_url, headers=header)
        context = parsel.Selector(context.text)
        download_size = context.xpath('//div[@class="ph run"]//table[@class="zebra run-metatable"]//td[@align="right"]/text()').extract()[2]
        sra_sizes = []
        sra_sizes.append(float(download_size))
        if len(sra_sizes) < 1:
            raise Exception(f'Can not find SRA file size for: {sra_name} ')
        return sra_sizes

    def wget_method_receiver(self):  # try wget way
        clean_and_mkdir(self.tmp_dir)
        sra_link = SraReceiver.target_sra_https_finder(self.sra_name)[0]
        print(f'try to download SRA file (wget) for {self.sra_name} from {sra_link}')
        round_start = f'cd {self.tmp_dir} && wget --tries=200 {sra_link}'
        try:
            download_or_die(round_start)
            return 'Succeed'
        except DownLoadingSraFileError:
            pass
        for try_time in [x for x in range(1, 6)]:
            # print(f'Try to download SRA file {self.sra_name} via wget in round{try_time}')
            round_end = f'cd {self.tmp_dir} && wget --continue --tries=200 {sra_link}'
            try:
                download_or_die(round_end)
                return 'Succeed'
            except DownLoadingSraFileError:
                pass
        return 'Failed'
    
    def axel_method_receiver(self):# try axel way
        clean_and_mkdir(self.tmp_dir)
        sra_link = SraReceiver.target_sra_https_finder(self.sra_name)[0]
        print(f'try to download SRA file (axel) for {self.sra_name} from {sra_link}')
        round_start = f'cd {self.tmp_dir} && axel -n 20 -a -o {self.sra_name}.sra {sra_link}'
        try:
            download_or_die(round_start)
            return 'Succeed'
        except DownLoadingSraFileError:
            pass
        for try_time in [x for x in range(1, 6)]:
            round_end = f'cd {self.tmp_dir} && axel -n 20 -a -o {self.sra_name}.sra {sra_link}'
            try:
                download_or_die(round_end)
                return 'Succeed'
            except DownLoadingSraFileError:
                pass
        return 'Failed'

    @timer
    def sra_receiver(self):
        try_ascp = SraReceiver(self.sra_name, self.tmp_dir).axel_method_receiver()
        if try_ascp == 'Succeed':
            return 'Succeed'
        elif try_ascp == 'Failed':
            try_wget = SraReceiver(self.sra_name, self.tmp_dir).wget_method_receiver()
            if try_wget == 'Succeed':
                return 'Succeed'
            if try_wget == 'Failed':
                raise DownLoadingSraFileError

def get_filesize(filePath):
    fsize = os.path.getsize(filePath)
    fsize = fsize/float(1024*1024*1024)
    return round(fsize,1)

def receiver(sra_name, tmp_dir):
    tmp_dir = os.path.abspath(tmp_dir)
    SraReceiver(sra_name, tmp_dir).sra_receiver()
    download_file = glob.glob(f'{tmp_dir}/*{sra_name}*')[0]
    file_size = get_filesize(f"{tmp_dir} + '/' + {download_file}")
    sra_size = SraReceiver.target_sra_size_finder(sra_name)
    if file_size < sra_size[0]:
        SraReceiver(sra_name, tmp_dir).sra_receiver()
    elif not download_file.endswith('.sra'):
        run_or_die(f'mv {download_file} {tmp_dir}/{sra_name}.sra')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Spider download_url from NCBI and download sra file")
    parser.add_argument('-sra_name',
                        type=str,
                        dest='sra_name',
                        nargs='?',
                        help='input sra_name; SRR1649426 or ERR2984736')
    parser.add_argument('-tmp_dir',
                        type=str,
                        dest='tmp_dir',
                        nargs='?',
                        help='tmp_dir for download SRA file ')
    args = parser.parse_args()
    if not all([args.sra_name, args.tmp_dir]):
        parser.print_help()
        sys.exit(1)
    receiver(args.sra_name, args.tmp_dir)
