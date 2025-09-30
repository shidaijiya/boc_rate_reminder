from lib.log_helper import log_print



def str_to_bool(value):
    try:
        if value.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif value.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
    except AttributeError:
        log_print.error(f"[WARN] Value '{value}' is not a string, defaulting to False")
        return False


def conv_to_float(list):
    conved = []
    try:
        for item in list:
            if not item:
                return list
            normalize = float(item.normalize())
            conved.append(normalize)
        return conved
    except (ValueError, AttributeError, TypeError):
        log_print.error("Invalid type")
        return list
