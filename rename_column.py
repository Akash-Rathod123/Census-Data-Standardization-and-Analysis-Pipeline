import pandas as pd

# Load the Excel file
file_path = r"C:\Users\DELL\Downloads\census_2011.xlsx"  # Replace with the actual file path
data = pd.read_excel(file_path)

# Display the original column names
print("Original column names:", list(data.columns))

# Rename specific columns
# Rename specific columns
column_mapping = {
    "State name": "State_UT",
    "District name": "District",
    "Male_Literate": "Literate_Male",
    "Female_Literate": "Literate_Female",
    "Rural_Households": "Households_Rural",
    "Urban_Households": "Households_Urban",
    "Age_Group_0_29": "Young_and_Adult",
    "Age_Group_30_49": "Middle_Aged",
    "Age_Group_50": "Senior_Citizen",
    "Age not stated": "Age_Not_Stated"
}

data.rename(columns=column_mapping, inplace=True)

# Display updated column names
print("Updated column names:", list(data.columns))

# Truncate column names to 60 characters
data.columns = [col[:60] for col in data.columns]

# Display truncated column names
print("Truncated column names:", list(data.columns))

# Save the updated Excel file
output_file_path = r"C:\Users\DELL\Downloads\Rename_StateUT_dataset.xlsx"  # Specify the output file path
data.to_excel(output_file_path, index=False)

print(f"Updated dataset saved to {output_file_path}")