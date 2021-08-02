from src.main.python.dimensions import create_dimensions, update_values


def start(create_or_update):
    print("------------------------ Init ------------------------")
    import time
    start_time = time.time()
    if create_or_update:
        create_dimensions()
    else:
        update_values()
    print("---  Save duration: %s seconds ---" % (time.time() - start_time))
    print("------------------------ End ------------------------")

if __name__ == '__main__':
    start(True)
