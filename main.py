import os
import warnings

warnings.filterwarnings(action="ignore")

from data import loading

from argparse import ArgumentParser

parser = ArgumentParser()

parser.add_argument("-a", "--atype", type=str)

args = parser.parse_args()

if __name__ == "__main__":
    FLAGS = args

    atype = FLAGS.atype if FLAGS.atype else "mysql"

    # loading data

    loading(atype=atype)

    # atype 별로 loading 방법
    ## 1. csv 파일 load 방법
    loading(atype="csv", file_path=None)

    ## 2. on-premise db
    loading(atype="mysql")

    ## 3. AWS S3
    loading(atype="athena")
