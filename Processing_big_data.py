#!/usr/bin/env python
# coding: utf-8

# In[29]:


import pandas as pd


# In[30]:


# Local file directories
source_path = "C:/Users/Bethuel/Documents/MOVING BIG DATA/Data/Stocks/"
output_path = "C:/Users/Bethuel/Documents/MOVING BIG DATA/Data/Output/"
index_file_path = 'C:/Users/Bethuel/Documents/MOVING BIG DATA/Data/Top_companies/top_companies.txt'


# In[31]:


def extract_companies_from_index(index_file_path):
    """Generate a list of company files that need to be processed. 

    Args:
        index_file_path (str): path to index file

    Returns:
        list: Names of company names. 
    """
    company_file = open(index_file_path, "r")
    contents = company_file.read()
    contents = contents.replace("'","")
    contents_list = contents.split(",")
    cleaned_contents_list = [item.strip() for item in contents_list]
    company_file.close()
    return cleaned_contents_list


# In[32]:


list_of_company_names=extract_companies_from_index(index_file_path)


# In[33]:


#Create a function that attaches the source directory to each company csv file selected for processing
def get_path_to_company_data(list_of_company_names, source_data_path):
    """Creates a list of the paths to the company data
       that will be processed

    Args:
        list_of_companies (list): Extracted `.csv` file names of companies whose data needs to be processed.
        source_data_path (str): Path to where the company `.csv` files are stored. 

    Returns:
        [type]: [description]
    """
    path_to_company_data = []
    for file_name in list_of_company_names:
        path_to_company_data.append(source_path + file_name)
    return path_to_company_data


# In[34]:


file_paths = get_path_to_company_data(list_of_company_names, source_path)


# In[35]:


# Save pandas dataframe as csv file
def save_table(dataframe, output_path, file_name, header):
    print(f"Path = {output_path}, file = {file_name}")
    dataframe.to_csv(output_path + file_name + ".csv", index=False, header=header)


# In[36]:


# Data Processing Function
def data_processing(file_paths, output_path):
    combined_data = pd.DataFrame()  # Initialize an empty dataframe to store the combined data

    for file_path in file_paths:
        try:
            data = pd.read_csv(file_path)  # Read the csv file
            data["daily_percent_change"] = ((data["Close"] - data["Open"]) / data["Open"]) * 100
            data["value_change"] = data["Close"] - data["Open"]
            data["company_name"] = file_path.split("/")[-1].split(".")[0]  # Extract the company name from the file path

            combined_data = combined_data.append(data)  # Append the data to the combined dataframe
        except Exception as e:
            print(f"Error processing file: {file_path}")
            print(f"Error message: {str(e)}")
    combined_data = combined_data.drop('OpenInt', axis=1)
    # Save the combined data to the output path without column headers
    save_table(combined_data, output_path, "historical_stock_data", header=False)

    return combined_data
    

