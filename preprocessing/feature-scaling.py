import pandas as pd


def normalise(col):

    # Apply normalisation to LoanAmount & TotalIncome column
    col = col.apply(lambda x: (x + col.min())/(col.max() - col.min()))

    return col


