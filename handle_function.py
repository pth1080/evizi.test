# This is a sample Python script.
import importlib

import numpy as np
import pandas as pd


def handle_function(path, class_name, func):
    try:
        module = importlib.import_module(path)
    except ImportError as e:
        raise Exception("Error importing %s: '%s'" % (path, e))
    try:
        my_class = getattr(module, class_name)
    except AttributeError:
        raise Exception("Module '%s' does not define a '%s'" % (path, class_name))
    my_instance = my_class()
    try:
        attr = getattr(my_instance, func)
    except AttributeError:
        raise Exception("Module '%s' does not define a '%s' with function %s" % (path, class_name, func))
    return attr


class PrintData(object):
    def print_hi(self):
        arr = {
            "name": ["Nguyen Van An", "Huynh Thi Be", "Cao Thi Hai"],
            "age": [18, 20, 18],
        }
        df = pd.DataFrame(arr)
        df["check"] = np.where(df["age"] == 18, "girl", "boy")
        print(df)

    def print_abc(self, abc):
        print(abc)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fn = handle_function('handle_function', 'PrintData', 'print_abc')
    fn('abc')
