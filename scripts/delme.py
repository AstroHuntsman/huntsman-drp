import multiprocessing


def func(i):
    #if i != 0:
    #    raise RuntimeError("asfasfa")
    return i


def callback(i):
    print(i)


class Test():

    def __init__(self):
        pass

    def run(self):
        with multiprocessing.Pool(1) as pool:
            for i in range(10):
                pool.apply_async(func, (i, ), callback=callback)


if __name__ == "__main__":

    Test().run()
