import pandas as pd

# Step 1: Load the dataset
file_path = r"C:\Users\DELL\Downloads\Rename_StateUT_dataset.xlsx"  # Replace with your actual file path
data = pd.read_excel(file_path)

# Step 2: Define the mapping of corrected state names
state_name_correction = {
    "JAMMU AND KASHMIR": "Jammu_Kashmir",
    "ANDHRA PRADESH": "Andhra_Pradesh",
    "ORISSA": "Odisha",
    "PONDICHERRY": "Pondicherry",
    "TAMIL NADU": "Tamil_Nadu",
    "ANDAMAN AND NICOBAR ISLANDS": "Andaman_Nicobar_Islands",
    "HIMACHAL PRADESH": "Himachal_Pradesh",
    "UTTAR PRADESH": "Uttar_Pradesh",
    "NCT OF DELHI": "NCT_of_Delhi",
    "MAHARASHTRA": "Maharashtra",
    "RAJASTHAN": "Rajasthan",
    "UTTARAKHAND": "Uttarakhand",
    "SIKKIM": "Sikkim",
    "TAMIL NADU": "Tamil_Nadu",
    "WEST BENGAL": "West_Bengal",
    # Add any other state corrections you need here
}

# Step 3: Apply the correction to the State_UT column
data["State_UT"] = data["State_UT"].replace(state_name_correction)

# Step 4: Verify the correction
print("Corrected State_UT column values:\n", data["State_UT"].unique())

# Step 5: Save the updated dataset
output_file_path = r"C:\Users\DELL\Downloads\Rename_StateUT_names_dataset.xlsx"
data.to_excel(output_file_path, index=False)

print(f"Updated dataset saved to {output_file_path}")
