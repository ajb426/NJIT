import pandas as pd
import os


chunk_size = 4000000
directory = 'C:/Users/ajb426/Dropbox/NJIT/Spring 24/Intro to Big Data/Final/Data/dataverse_files'
csv_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]

# List to hold individual DataFrames
dfs = []


def process_chunks(files):
    # List to hold the concatenated chunks
    concatenated_df = pd.DataFrame()

    for file in files:
        # Read each file in chunks
        chunk_container = pd.read_csv(file, chunksize=chunk_size, encoding='iso-8859-1')

        for chunk in chunk_container:
            # Process each chunk (you can apply any filtering or transformations here)
            concatenated_df = pd.concat([concatenated_df, chunk], ignore_index=True)

    return concatenated_df


# Call the function to process the CSV files
df_merged = process_chunks(csv_files)

# Concatenate all the DataFrames in the list into one
#df_merged = pd.concat(dfs, ignore_index=True)


# Select only the relevant columns
df = df_merged[['Origin', 'Dest', 'TaxiIn', 'TaxiOut']]

# Drop rows where any of the required columns are missing
df.dropna(subset=['Origin', 'Dest', 'TaxiIn', 'TaxiOut'], inplace=True)

# Calculate average taxi in and taxi out times per origin airport
avg_taxi_out_origin = df.groupby('Origin')['TaxiOut'].mean().reset_index()
avg_taxi_out_origin.rename(columns={'TaxiOut': 'AvgTaxiOut', 'Origin': 'Airport'}, inplace=True)
# Calculate average taxi in and taxi out times per destination airport
avg_taxi_in_dest = df.groupby('Dest')['TaxiIn'].mean().reset_index()
avg_taxi_in_dest.rename(columns={'TaxiIn': 'AvgTaxiIn', 'Dest': 'Airport'}, inplace=True)
# Combine the two dataframes
combined = pd.merge(avg_taxi_out_origin, avg_taxi_in_dest, how='outer', on='Airport')
combined['OverallAvgTaxiTime'] = combined[['AvgTaxiOut', 'AvgTaxiIn']].mean(axis=1)
#onTime
df_onTime = df_merged.copy()
print(df_onTime)
df_onTime.dropna(subset=['UniqueCarrier', 'ArrDelay'], inplace=True)
# Define a flight as on-time if the delay is 10 minutes or less
df_onTime['OnTime'] = df_onTime['ArrDelay'].apply(lambda x: 1 if x <= 10 else 0)
# Group by 'Airline' and calculate on-time flights and total flights
grouped = df_onTime.groupby('UniqueCarrier')['OnTime'].agg(['sum', 'count'])
# Calculate on-time probability for each airline
grouped['OnTimeProbability'] = (grouped['sum'] / grouped['count']) * 100
# Reset index to turn grouped indices into columns
grouped.reset_index(inplace=True)
# Rename columns for clarity
grouped.rename(columns={'sum': 'OnTimeFlights', 'count': 'TotalFlights'}, inplace=True)
# Select only the columns we want to show
final_result = grouped[['UniqueCarrier', 'OnTimeFlights', 'TotalFlights', 'OnTimeProbability']]
# Print the results
print(final_result)


cancellation_counts = df_merged['CancellationCode'].value_counts().reset_index()
cancellation_counts.columns = ['CancellationCode', 'Count']
#onTimeProbability
# Output the results
print("Overall Average Taxi In Times per Destination Airport:\n", combined)
print(cancellation_counts)
