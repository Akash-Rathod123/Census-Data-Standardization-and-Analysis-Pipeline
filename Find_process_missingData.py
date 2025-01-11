import pandas as pd

# Load the dataset
file_path = r"C:\Users\DELL\Downloads\New_stateUT_dataset.xlsx"
data = pd.read_excel(file_path)

# Step 1: Calculate the percentage of missing data for each column
missing_data_before = data.isnull().mean() * 100
print("Percentage of missing data in each column before filling in:\n", missing_data_before)

# Step 2: Fill in missing data based on relationships between columns
# Hint: Population = Male + Female
data["Population"] = data["Population"].fillna(data["Male"] + data["Female"])

# Hint: Literate = Literate_Male + Literate_Female
data["Literate"] = data["Literate"].fillna(data["Literate_Male"] + data["Literate_Female"])

# Hint: Population = Young_and_Adult + Middle_Aged + Senior_Citizen + Age_Not_Stated
data["Population"] = data["Population"].fillna(data["Young_and_Adult"] + data["Middle_Aged"] + data["Senior_Citizen"] + data["Age_Not_Stated"])

# Hint: Households = Households_Rural + Households_Urban
data["Households"] = data["Households"].fillna(data["Households_Rural"] + data["Households_Urban"])

# Step 3: Calculate the percentage of missing data again after filling in values
missing_data_after = data.isnull().mean() * 100
print("\nPercentage of missing data in each column after filling in:\n", missing_data_after)

# Step 4: Save the updated dataset
output_file_path = r"C:\Users\DELL\Downloads\updated_filled_stateUT_dataset.xlsx"
data.to_excel(output_file_path, index=False)
print(f"\nUpdated dataset saved to {output_file_path}")
