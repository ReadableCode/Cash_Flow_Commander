def multiply_with_errors(x, y):
    if x == 0 or y == 0:
        return 0
    if x == "ERROR" or y == "ERROR":
        return "ERROR"
    else:
        return x * y


def add_with_errors(x, y):
    if x == "ERROR" or y == "ERROR":
        return "ERROR"
    else:
        return x + y


def subtract_with_errors(x, y):
    if x == "ERROR" or y == "ERROR":
        return "ERROR"
    else:
        return x - y


def divide_with_errors(x, y, decimal_places=0):
    if y == 0 and x == 0:
        return 0
    if x == "ERROR" or y == "ERROR":
        return "ERROR"
    elif y == 0:
        return "ERROR"
    elif round != 0:
        return round(x / y, decimal_places)
    else:
        return x / y


def abs_with_errors(x):
    if x == "ERROR":
        return "ERROR"
    else:
        return abs(x)


def is_diff_with_errors(delta_percent, threshold):
    if delta_percent == "ERROR":
        return "ERROR"
    else:
        if abs(delta_percent) > threshold:
            return "Diff"
        else:
            return ""
