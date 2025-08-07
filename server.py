import sys
import getopt

from consumer_worker import ConsumerWorker

if __name__ == "__main__":
    ts = '20'
    f1 = 'Please try with below cmd \n python consumer_worker.py -t int value or --timeout int value'

    try:
        opts, [] = getopt.getopt(sys.argv[1:], 't:', ['timeout='])
    except getopt.error as err:
        print(err)
        sys.exit(2)

    for k, v in opts:
        if k in ('-t', '--timeout'):
            ts = v

    try:
        ts = eval(ts)
        ConsumerWorker().run(ts)
    except Exception as exp_err:
        print(exp_err)
        sys.exit(2)
