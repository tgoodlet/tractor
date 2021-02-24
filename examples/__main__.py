"""
Needed on Windows.

This module is needed as the program entry point for invocation
with ``python -m <modulename>``. See the solution from @chrizzFTD
here:

    https://github.com/goodboy/tractor/pull/61#issuecomment-470053512

"""
if __name__ == '__main__':
    import multiprocessing
    multiprocessing.freeze_support()
    # ``tests/test_docs_examples.py::test_example`` will copy each
    # script from this examples directory into a module in a new
    # temporary dir and name it test_example.py. We import that script
    # module here and invoke it's ``main()``.
    from . import test_example
    test_example.trio.run(test_example.main)
