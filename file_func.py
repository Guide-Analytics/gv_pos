##

# Graph 2.0
# 'file_func'
# Gide Inc. 2019

##


import os

path_output = 'graph_output/output.txt'
path_keywords = 'keywords/keywords.txt'


def check_file():

    if os.path.exists(path=path_output):
        return True


def read_file(path=path_keywords):

    with open(path, 'r') as f:
        file = f.read()
        keywords = file.split(', ')

    return keywords
