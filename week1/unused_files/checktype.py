import pandas as pd

def main():
    csv_name = 'output.csv'
    deef_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    deef = next(deef_iter)
    print(deef.dtypes.to_dict())

main()