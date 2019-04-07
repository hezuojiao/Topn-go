"""
Script for data generate.
"""

import os
import random
import argparse
import string
from collections import Counter


def data_generator(path, size, n):
    """
    Generate sample data and corresponding answers.
    :param path:
    :param size:
    :param n:
    :return:
    """
    filename = os.path.join(path, 'data_' + str(size) + 'g.txt')
    file_ans = os.path.join(path, 'ans_' + str(size) + 'g.txt')

    ans = Counter()

    with open(filename, 'w') as f:
        while get_file_size(filename) < size:
            strings = list()

            # generate data randomly.
            while len(strings) < 10000000:
                s = "https://google.com/" + "".join(random.choice(string.ascii_letters + string.digits)
                            for _ in range(random.randint(2, 100)))
                strings += [s] * (int(random.uniform(0, 1) * 10000) + random.randint(0, 1000) + random.randint(0, 100))

            # update answer.
            ans.update(strings)

            # shuffle strings
            random.shuffle(strings)

            # write data.
            for line in strings:
                f.write(line + '\n')

    # convert Counter to List.
    ans = ans.most_common(n)
    with open(file_ans, 'w') as f:
        for url, freq in ans:
            f.write(url + ': ' + str(freq) + '\n')


def get_file_size(filename: str):
    """
    Get file size. [GB]
    :param filename:
    :return: Size of file.[GB]
    """
    fsize = os.path.getsize(filename)
    fsize /= float(1024 * 1024 * 1024)
    return round(fsize, 2)


# ==============================================
# main
# ==============================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data generator')
    parser.add_argument('--output_path', type=str, default='./data/', help='Output data path.')
    parser.add_argument('--size', type=float, default=1, help='Size of data [GB].')
    parser.add_argument('--n', type=int, default=100, help='Top n answers will be store.')
    args = vars(parser.parse_args())
    print(args)
    data_generator(args['output_path'], args['size'], args['n'])
    print("Data generator done.")

