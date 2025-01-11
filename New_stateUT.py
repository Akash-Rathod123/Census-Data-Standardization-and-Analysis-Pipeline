import pandas as pd

# Step 1: Load the dataset
file_path = r"C:\Users\DELL\Downloads\Rename_StateUT_names_dataset.xlsx"  # Replace with your actual file path
data = pd.read_excel(file_path)

# Step 2: Display the original column values
print("Original State_UT column values:\n", data["State_UT"].unique())

# Step 3: Telangana formation
# Read the Telangana districts from a text file
telangana_districts_path = r"C:\Users\DELL\Downloads\Telangana.txt"  # Replace with the actual path
with open(telangana_districts_path, "r", encoding="utf-8-sig") as file:
    telangana_districts = file.read().splitlines()

# Convert districts to title case for consistency
telangana_districts = [district.title() for district in telangana_districts]

# Update the State_UT for districts in Telangana
data.loc[data["District"].isin(telangana_districts) & (data["State_UT"] == "Andhra_Pradesh"), "State_UT"] = "Telangana"

# Step 4: Ladakh formation
# Rename "Jammu and Kashmir" to "Ladakh" for the districts "Leh" and "Kargil"
ladakh_districts = ["Leh(Ladakh)", "Kargil"]
data.loc[data["District"].isin(ladakh_districts) & (data["State_UT"] == "Jammu_Kashmir"), "State_UT"] = "Ladakh"

# Step 5: Display the updated column values
print("Updated State_UT column values:\n", data["State_UT"].unique())

# Step 6: Save the updated dataset
output_file_path = r"C:\Users\DELL\Downloads\New_stateUT_dataset.xlsx"
data.to_excel(output_file_path, index=False)

print(f"Updated dataset saved to {output_file_path}")
