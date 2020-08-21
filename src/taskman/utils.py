
def fmt_time(seconds):
    if seconds >= 3600:
        return str(round(seconds / 3600)) + 'h'
    elif seconds >= 60:
        return str(round(seconds / 60)) + 'm'
    else:
        return str(round(seconds)) + 's'
